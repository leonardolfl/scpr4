import fs from "fs";
import path from "path";
import os from "os";
import { chromium, devices } from "playwright";
import { supabase } from "./supabase.js";

/**
 * scraper.mjs (improved: selector-first + extension-method fallback, attempts handling)
 *
 * Instrução SQL recomendada (execute no Supabase SQL editor para adicionar coluna attempts):
 *   ALTER TABLE swipe_file_offers ADD COLUMN IF NOT EXISTS attempts integer DEFAULT 0;
 *
 * Recursos principais:
 * - Mantém chunking por WORKER_INDEX e atualizações status_updated
 * - Device pool + UA rotation
 * - Abort imagens, stylesheets, fonts e media para reduzir HTML carregado
 * - Usa selector/text regex como tentativa rápida, e em fallback injeta função idêntica
 *   à da extensão (document.querySelector('div[role="heading"][aria-level="3"]'))
 * - Normaliza número, parseInt e atualização segura no DB com attempts
 *
 * ENV:
 * - WORKER_INDEX (injetado pelo workflow)
 * - TOTAL_WORKERS (default 4)
 * - PROCESS_LIMIT (opcional)
 * - PARALLEL (default 3)
 * - WAIT_TIME (ms) default 7000
 * - NAV_TIMEOUT (ms) default 60000
 * - SELECTOR_TIMEOUT (ms) default 15000
 * - RETRY_ATTEMPTS (number of extra retries when selector not found) default 2
 * - DEBUG_DIR default ./debug
 * - WORKER_ID optional
 */

const TOTAL_WORKERS = parseInt(process.env.TOTAL_WORKERS || "4", 10);
const WORKER_INDEX = parseInt(process.env.WORKER_INDEX ?? "0", 10);
const PROCESS_LIMIT = process.env.PROCESS_LIMIT ? parseInt(process.env.PROCESS_LIMIT, 10) : null;
let PARALLEL = Math.max(1, parseInt(process.env.PARALLEL || "3", 10));
const WAIT_TIME = parseInt(process.env.WAIT_TIME || "7000", 10);
const NAV_TIMEOUT = parseInt(process.env.NAV_TIMEOUT || "60000", 10);
const SELECTOR_TIMEOUT = parseInt(process.env.SELECTOR_TIMEOUT || "15000", 10);
// Atualizado para 2 por padrão (configurável via env)
const RETRY_ATTEMPTS = Math.max(0, parseInt(process.env.RETRY_ATTEMPTS || "2", 10)); // number of retries when selector not found
const DEBUG_DIR = process.env.DEBUG_DIR || "./debug";
const WORKER_ID = process.env.WORKER_ID || `${os.hostname()}-${process.pid}-${Date.now()}`;

// Quantas falhas consecutivas antes de marcar activeAds = null permanentemente
const MAX_FAILS = parseInt(process.env.MAX_FAILS || "3", 10);

const DEVICE_NAMES = [
  "iPhone 13 Pro Max",
  "iPhone 12",
  "iPhone 11 Pro",
  "Pixel 7",
  "Pixel 5",
  "Galaxy S21 Ultra",
  "iPad Mini",
  "Desktop Chrome",
];
const DEVICE_POOL = DEVICE_NAMES.map((n) => devices[n]).filter(Boolean);

const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; rv:122.0) Gecko/20100101 Firefox/122.0",
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
];

function ensureDebugDir() {
  if (!fs.existsSync(DEBUG_DIR)) fs.mkdirSync(DEBUG_DIR, { recursive: true });
}
function nowIso() {
  return new Date().toISOString();
}
function nowTs() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}
function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}
function jitter(ms) {
  return Math.floor(Math.random() * ms);
}
function shuffle(array) {
  const a = array.slice();
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}
function pickDeviceAndUA(index) {
  const device = DEVICE_POOL.length > 0 ? DEVICE_POOL[index % DEVICE_POOL.length] : {};
  const ua = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
  return { device, ua };
}

async function saveDebug(pageOrHtml, offerId, note = "") {
  try {
    ensureDebugDir();
    const ts = nowTs();
    const safe = `offer-${offerId}-${ts}`;
    const htmlPath = path.join(DEBUG_DIR, `${safe}.html`);
    const pngPath = path.join(DEBUG_DIR, `${safe}.png`);
    if (typeof pageOrHtml === "string") {
      await fs.promises.writeFile(htmlPath, `<!-- ${note} -->\n` + pageOrHtml, "utf8");
      return { htmlPath, pngPath: null };
    } else {
      const content = await pageOrHtml.content();
      await fs.promises.writeFile(htmlPath, `<!-- ${note} -->\n` + content, "utf8");
      try {
        await pageOrHtml.screenshot({ path: pngPath, fullPage: true });
      } catch (e) {
        // ignore screenshot failures
      }
      return { htmlPath, pngPath };
    }
  } catch (err) {
    console.warn("❌ Falha ao salvar debug:", err?.message || err);
    return { htmlPath: null, pngPath: null };
  }
}

function parseCountFromText(text) {
  if (!text || typeof text !== "string") return null;
  const re = /(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d+)?)\s*(?:resultados|results)/i;
  const m = text.match(re);
  if (m && m[1]) {
    const digits = m[1].replace(/[^\d]/g, "");
    if (!digits) return null;
    return parseInt(digits, 10);
  }
  return null;
}

/**
 * Função in-page a ser injetada como fallback.
 * Copiada/Adaptada do método que funciona na extensão (background.js -> getPageDetailsAndAdCountInjectedForBackground)
 * Mantém foco em: document.querySelector('div[role="heading"][aria-level="3"]')
 */
function injectedGetPageDetailsAndAdCountForEvaluate(offerNameToUse) {
  // esta função roda no contexto da página; quando serializada via page.evaluate, tudo aqui é convertido para texto
  const nameElement = document.querySelector('div[role="heading"][aria-level="1"]');
  const advertiserName = nameElement ? nameElement.innerText.trim() : 'Unknown';
  let adCount = null;
  let extractedSuccessfully = false;
  let rawText = null;

  const adCountElement = document.querySelector('div[role="heading"][aria-level="3"]');
  if (adCountElement && adCountElement.innerText) {
    const textContent = adCountElement.innerText;
    rawText = textContent;
    // Primeiro tenta "~1,2k" ou "~123,4"
    let match = textContent.match(/~([0-9.,]+)/);
    if (!match) {
      // Fallback para "123 resultados" ou "123 results"
      match = textContent.match(/([\d,.]+)\s*(resultados|results)/i);
    }
    if (match && match[1]) {
      // Normaliza removendo separadores (ponto/virgula)
      const numberString = match[1].replace(/[.,]/g, '');
      const parsedCount = parseInt(numberString, 10);
      if (!isNaN(parsedCount)) {
        adCount = parsedCount;
        extractedSuccessfully = true;
      }
    }
  }

  return {
    name: advertiserName,
    results: adCount,
    offer: offerNameToUse,
    extractedSuccessfully,
    rawText
  };
}

// Main processing
async function processOffers(offersSlice) {
  const browser = await chromium.launch({ headless: true });
  let consecutiveFails = 0;
  let consecutiveSuccess = 0;

  for (let i = 0; i < offersSlice.length; i += PARALLEL) {
    const batch = offersSlice.slice(i, i + PARALLEL);
    console.log(`📦 Processando bloco: ${Math.floor(i / PARALLEL) + 1} (${batch.length} ofertas)`);

    await Promise.all(batch.map(async (offer, idxInBatch) => {
      if (!offer || !offer.adLibraryUrl) return;
      const globalIndex = i + idxInBatch;
      const { device, ua } = pickDeviceAndUA(globalIndex);
      const contextOptions = { ...(device || {}) };
      if (ua) contextOptions.userAgent = ua;

      const context = await browser.newContext(contextOptions);

      // Abort heavy resources to reduce HTML/RAM load
      await context.route("**/*", (route) => {
        const type = route.request().resourceType();
        if (["image", "stylesheet", "font", "media"].includes(type)) return route.abort();
        return route.continue();
      });

      const page = await context.newPage();

      try {
        console.log(`⌛ [${offer.id}] Acessando (${device?.name || "custom"} / UA=${(ua || "").slice(0,60)}): ${offer.adLibraryUrl}`);
        await page.goto(offer.adLibraryUrl, { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT });

        // small wait to let on-page JS run if necessary; still small to keep speed
        await page.waitForTimeout(WAIT_TIME + jitter(800));

        // Try selector-first extraction, with up to RETRY_ATTEMPTS retries
        let foundCount = null;
        let foundRawText = null;
        let attempt = 0;
        const selectorString = 'text=/' + '(\\d{1,3}(?:[.,]\\d{3})*(?:[.,]\\d+)?)\\s*(?:resultados|results)' + '/i';
        for (; attempt <= RETRY_ATTEMPTS && foundCount === null; attempt++) {
          try {
            // wait for the text matching the regex
            const locator = page.locator(selectorString).first();
            await locator.waitFor({ timeout: SELECTOR_TIMEOUT }).catch(() => { /* will handle below */ });
            const text = await locator.textContent().catch(() => null);
            if (text) {
              const parsed = parseCountFromText(text);
              if (parsed != null) {
                foundCount = parsed;
                foundRawText = text;
                console.log(`🔎 [${offer.id}] selector found on attempt ${attempt + 1}`);
                break;
              }
            }
          } catch (e) {
            // ignore, we'll retry if allowed
          }
          if (foundCount === null && attempt < RETRY_ATTEMPTS) {
            console.log(`· [${offer.id}] selector not found, retry ${attempt + 1}/${RETRY_ATTEMPTS}`);
            await page.waitForTimeout(1000 + Math.floor(Math.random() * 800));
          }
        }

        // If selector-first failed, use the exact method from the extension (in-page evaluate)
        if (foundCount === null) {
          try {
            const evalRes = await page.evaluate(injectedGetPageDetailsAndAdCountForEvaluate, offer.offerName || "");
            if (evalRes && evalRes.extractedSuccessfully && typeof evalRes.results === "number") {
              foundCount = evalRes.results;
              foundRawText = evalRes.rawText || null;
              console.log(`🔁 [${offer.id}] fallback evaluate succeeded: ${foundCount}`);
            } else {
              console.log(`🔁 [${offer.id}] fallback evaluate did not find counter`);
            }
          } catch (e) {
            console.warn(`⚠️ [${offer.id}] evaluate fallback failed: ${String(e?.message || e)}`);
          }
        }

        const updated_at = nowIso();

        if (foundCount != null) {
          const activeAds = foundCount;
          console.log(`✅ [${offer.id}] ${offer.offerName || "offer"}: ${activeAds} anúncios (raw: ${String(foundRawText).slice(0,120)})`);

          // Update in DB: success -> activeAds + attempts reset
          const { error } = await supabase
            .from("swipe_file_offers")
            .update({ activeAds, updated_at, status_updated: "success", attempts: 0 })
            .eq("id", offer.id);
          if (error) console.warn(`[${offer.id}] Erro ao atualizar DB:`, error.message || error);
          consecutiveSuccess++;
          consecutiveFails = 0;
        } else {
          console.warn(`❌ [${offer.id}] contador não encontrado após selector+fallback — salvando debug`);

          // Save debug snapshot for inspection
          try {
            const dbg = await saveDebug(page, offer.id, "no-counter-selector-and-fallback");
            console.log(`[${offer.id}] debug salvo: ${dbg.htmlPath || "no-html"} ${dbg.pngPath || ""}`);
          } catch (e) {
            console.warn(`[${offer.id}] falha ao salvar debug:`, e?.message || e);
          }

          // Determine current attempts (prefer using value from the fetched offer)
          const currentAttempts = Number(offer.attempts ?? 0);
          const newAttempts = currentAttempts + 1;

          // If attempts reached MAX_FAILS, mark activeAds = null final; otherwise only increment attempts and mark attempt status
          if (newAttempts >= MAX_FAILS) {
            try {
              const { error } = await supabase
                .from("swipe_file_offers")
                .update({ activeAds: null, updated_at, status_updated: "no_counter", attempts: newAttempts })
                .eq("id", offer.id);
              if (error) console.warn(`[${offer.id}] DB update (no_counter) error:`, error.message || error);
              console.log(`[${offer.id}] marcação final no_counter após ${newAttempts} tentativas`);
            } catch (e) {
              console.warn(`[${offer.id}] Erro ao gravar no_counter:`, e?.message || e);
            }
          } else {
            try {
              const { error } = await supabase
                .from("swipe_file_offers")
                .update({ updated_at, status_updated: `no_counter_attempt_${newAttempts}`, attempts: newAttempts })
                .eq("id", offer.id);
              if (error) console.warn(`[${offer.id}] DB update (attempt increment) error:`, error.message || error);
              console.log(`[${offer.id}] incrementado attempts -> ${newAttempts} (não sobrescrevendo activeAds)`);
            } catch (e) {
              console.warn(`[${offer.id}] Erro ao incrementar attempts:`, e?.message || e);
            }
          }

          consecutiveFails++;
        }
      } catch (err) {
        console.error(`🚫 [${offer.id}] Erro ao processar:`, err?.message || err);
        // Save debug page content if possible
        try {
          const html = await page.content().catch(() => null);
          if (html) {
            const dbg = await saveDebug(html, offer.id, `exception-${String(err?.message || err)}`);
            console.log(`[${offer.id}] debug salvo após exception: ${dbg.htmlPath || "no-html"}`);
          }
        } catch (e) {
          console.warn(`[${offer.id}] falha ao salvar debug na exception:`, e?.message || e);
        }
        try {
          // On exception we increment attempts similar to no-counter flow (avoid immediate null)
          const currentAttempts = Number(offer.attempts ?? 0);
          const newAttempts = currentAttempts + 1;
          if (newAttempts >= MAX_FAILS) {
            await supabase
              .from("swipe_file_offers")
              .update({ activeAds: null, updated_at: nowIso(), status_updated: "error", attempts: newAttempts })
              .eq("id", offer.id);
          } else {
            await supabase
              .from("swipe_file_offers")
              .update({ updated_at: nowIso(), status_updated: `error_attempt_${newAttempts}`, attempts: newAttempts })
              .eq("id", offer.id);
          }
        } catch (e) {
          console.warn(`[${offer.id}] DB update (error) failed:`, e?.message || e);
        }
        consecutiveFails++;
      } finally {
        try { await page.close(); } catch {}
        try { await context.close(); } catch {}
        await sleep(400 + jitter(900));
      }
    }));

    // adaptive throttling
    if (consecutiveFails >= 3 && PARALLEL > 3) {
      PARALLEL = 3;
      console.log("⚠️ Reduzindo paralelismo para 3.");
    } else if (consecutiveSuccess >= 20 && PARALLEL < 5) {
      PARALLEL = 5;
      console.log("✅ Restaurando paralelismo para 5.");
      consecutiveSuccess = 0;
    }

    await sleep(500 + jitter(1000));
  }

  await browser.close();
}

(async () => {
  console.log("🚀 Scraper iniciado (WORKER_INDEX:", WORKER_INDEX, "TOTAL_WORKERS:", TOTAL_WORKERS, "WORKER_ID:", WORKER_ID, ")");
  ensureDebugDir();

  const { data: offers, error } = await supabase.from("swipe_file_offers").select("*");
  if (error) {
    console.error("❌ Erro ao buscar ofertas:", error);
    process.exit(1);
  }
  if (!Array.isArray(offers) || offers.length === 0) {
    console.log("ℹ️ Nenhuma oferta encontrada. Encerrando.");
    process.exit(0);
  }

  const shuffledOffers = shuffle(offers);
  console.log(`🚀 Total de ofertas: ${shuffledOffers.length}`);

  const total = shuffledOffers.length;
  const chunkSize = Math.ceil(total / TOTAL_WORKERS);
  let myOffers = shuffledOffers.slice(WORKER_INDEX * chunkSize, Math.min((WORKER_INDEX + 1) * chunkSize, total));
  if (PROCESS_LIMIT) myOffers = myOffers.slice(0, PROCESS_LIMIT);

  console.log(`📂 Worker ${WORKER_INDEX} processando ${myOffers.length} ofertas (chunk size ${chunkSize})`);
  await processOffers(myOffers);

  console.log("✅ Worker finalizado");
  process.exit(0);
})();
