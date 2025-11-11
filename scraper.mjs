import fs from "fs";
import path from "path";
import os from "os";
import { chromium, devices } from "playwright";
import { supabase } from "./supabase.js";

/**
 * scraper.mjs - optimized
 *
 * SQL recomendado (execute no Supabase SQL editor se ainda não executou):
 *   ALTER TABLE swipe_file_offers ADD COLUMN IF NOT EXISTS attempts integer DEFAULT 0;
 *
 * Principais otimizações:
 * - Pool de contexts (CONTEXT_POOL_SIZE, default igual a PARALLEL)
 * - Abordagem selector-first rápida usando heading (igual à extensão)
 * - Fallback evaluate apenas quando necessário (lightweight)
 * - Redução de waits/timeouts e RETRY_ATTEMPTS=1 por padrão
 * - DEBUG flag para salvar HTML-only debug; sem screenshots
 * - LOG_LEVEL para controlar verbosidade
 *
 * ENV usados:
 * WORKER_INDEX, TOTAL_WORKERS, PROCESS_LIMIT
 * PARALLEL (default 4)
 * CONTEXT_POOL_SIZE (default = PARALLEL)
 * WAIT_TIME (ms) default 2500
 * NAV_TIMEOUT (ms) default 45000
 * SELECTOR_TIMEOUT (ms) default 8000
 * RETRY_ATTEMPTS default 1
 * DEBUG default "false" (quando true salva HTML debug)
 * LOG_LEVEL default "info" (info|warn|error|silent)
 * MAX_FAILS default 3
 * DEBUG_DIR default ./debug
 */

const TOTAL_WORKERS = parseInt(process.env.TOTAL_WORKERS || "4", 10);
const WORKER_INDEX = parseInt(process.env.WORKER_INDEX ?? "0", 10);
const PROCESS_LIMIT = process.env.PROCESS_LIMIT ? parseInt(process.env.PROCESS_LIMIT, 10) : null;

const PARALLEL = Math.max(1, parseInt(process.env.PARALLEL || "4", 10));
const CONTEXT_POOL_SIZE = Math.max(1, parseInt(process.env.CONTEXT_POOL_SIZE || String(PARALLEL), 10));

const WAIT_TIME = parseInt(process.env.WAIT_TIME || "2500", 10);
const NAV_TIMEOUT = parseInt(process.env.NAV_TIMEOUT || "45000", 10);
const SELECTOR_TIMEOUT = parseInt(process.env.SELECTOR_TIMEOUT || "8000", 10);
const RETRY_ATTEMPTS = Math.max(0, parseInt(process.env.RETRY_ATTEMPTS || "1", 10)); // requested default =1

const DEBUG = String(process.env.DEBUG || "false").toLowerCase() === "true";
const LOG_LEVEL = (process.env.LOG_LEVEL || "info").toLowerCase(); // info|warn|error|silent

const MAX_FAILS = parseInt(process.env.MAX_FAILS || "3", 10);

const DEBUG_DIR = process.env.DEBUG_DIR || "./debug";
const WORKER_ID = process.env.WORKER_ID || `${os.hostname()}-${process.pid}-${Date.now()}`;

const DEVICE_NAMES = [
  "Desktop Chrome",
  "iPhone 13 Pro Max",
  "Pixel 7",
  "iPad Mini",
];
const DEVICE_POOL = DEVICE_NAMES.map((n) => devices[n]).filter(Boolean);

const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
];

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

function logInfo(...args) {
  if (["info"].includes(LOG_LEVEL)) console.log(...args);
}
function logWarn(...args) {
  if (["info", "warn"].includes(LOG_LEVEL)) console.warn(...args);
}
function logError(...args) {
  if (["info", "warn", "error"].includes(LOG_LEVEL)) console.error(...args);
}

function ensureDebugDir() {
  if (!DEBUG) return;
  if (!fs.existsSync(DEBUG_DIR)) fs.mkdirSync(DEBUG_DIR, { recursive: true });
}
async function saveDebugHtml(htmlContent, offerId, note = "") {
  if (!DEBUG) return null;
  try {
    ensureDebugDir();
    const ts = nowTs();
    const safe = `offer-${offerId}-${ts}`;
    const htmlPath = path.join(DEBUG_DIR, `${safe}.html`);
    await fs.promises.writeFile(htmlPath, `<!-- ${note} -->\n` + htmlContent, "utf8");
    return { htmlPath };
  } catch (err) {
    logWarn("❌ Falha ao salvar debug HTML:", err?.message || err);
    return null;
  }
}

/**
 * Parser seguindo lógica da extensão (foco em heading)
 * - Recebe um texto (heading) e tenta dois padrões:
 *   1) "~123,4" estilo aproximado -> captura número
 *   2) "123 resultados" ou "123 results"
 * - Normaliza removendo '.' e ',' antes do parseInt.
 */
function parseCountFromHeadingText(text) {
  if (!text || typeof text !== "string") return null;
  // Primeiro tenta "~123,4" (aprox.)
  let m = text.match(/~\s*([0-9,.]+)/);
  if (!m) {
    m = text.match(/([\d\.,\s\u00A0\u202F]+)\s*(?:resultados|results)/i);
  }
  if (m && m[1]) {
    // remove dots/commas/spaces
    const cleaned = m[1].replace(/[.,\s\u00A0\u202F]/g, "");
    const n = parseInt(cleaned, 10);
    if (!isNaN(n)) return n;
  }
  return null;
}

/**
 * Função in-page para fallback (serializável)
 * Copiada/adaptada da extensão: busca heading role=3 e aplica regexs.
 * Retorna { name, results, extractedSuccessfully, rawText }
 */
function injectedGetPageDetailsAndAdCountForEvaluate(offerNameToUse) {
  const nameElement = document.querySelector('div[role="heading"][aria-level="1"]');
  const advertiserName = nameElement ? (nameElement.innerText || "").trim() : 'Unknown';
  let adCount = null;
  let extractedSuccessfully = false;
  let rawText = null;

  const adCountElement = document.querySelector('div[role="heading"][aria-level="3"]');
  if (adCountElement && adCountElement.innerText) {
    const textContent = adCountElement.innerText;
    rawText = textContent;
    let match = textContent.match(/~\s*([0-9.,]+)/);
    if (!match) {
      match = textContent.match(/([\d\.,\s\u00A0\u202F]+)\s*(resultados|results)/i);
    }
    if (match && match[1]) {
      const numberString = match[1].replace(/[.,\s\u00A0\u202F]/g, '');
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

// Create and configure contexts pool
async function createContexts(browser) {
  const contexts = [];
  for (let i = 0; i < CONTEXT_POOL_SIZE; i++) {
    const device = DEVICE_POOL[i % DEVICE_POOL.length] || {};
    const ua = USER_AGENTS[i % USER_AGENTS.length];
    const context = await browser.newContext({ ...(device || {}), userAgent: ua });
    // Abort heavy resources
    await context.route("**/*", (route) => {
      const type = route.request().resourceType();
      if (["image", "stylesheet", "font", "media"].includes(type)) return route.abort();
      return route.continue();
    });
    contexts.push(context);
  }
  return contexts;
}

async function closeContexts(contexts) {
  for (const ctx of contexts) {
    try { await ctx.close(); } catch (e) { /* ignore */ }
  }
}

// Main worker processing
async function processOffers(offersSlice) {
  // Launch browser with safer args for GH Actions
  const browser = await chromium.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  });

  const contexts = await createContexts(browser);
  let consecutiveFails = 0;
  let consecutiveSuccess = 0;

  try {
    for (let i = 0; i < offersSlice.length; i += PARALLEL) {
      const batch = offersSlice.slice(i, i + PARALLEL);
      logInfo(`📦 Processando bloco ${Math.floor(i / PARALLEL) + 1} (${batch.length} ofertas) — Worker ${WORKER_INDEX}`);

      await Promise.all(batch.map(async (offer, idxInBatch) => {
        if (!offer || !offer.adLibraryUrl) return;
        const globalIndex = i + idxInBatch;
        const ctxIndex = globalIndex % CONTEXT_POOL_SIZE;
        const context = contexts[ctxIndex];

        // Create page per offer (cheap)
        const page = await context.newPage();
        try {
          logInfo(`⌛ [${offer.id}] Acessando: ${offer.adLibraryUrl}`);
          await page.goto(offer.adLibraryUrl, { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT }).catch(e => {
            // fallback to continue — we'll handle error below
            logWarn(`⚠️ [${offer.id}] goto erro: ${String(e?.message || e)}`);
          });

          // Prefer networkidle but with a short timeout
          try {
            await page.waitForLoadState('networkidle', { timeout: Math.min(10000, NAV_TIMEOUT) });
          } catch (e) {
            // ignore, some pages don't reach networkidle quickly
          }

          // small wait to allow inline JS
          await page.waitForTimeout(WAIT_TIME + jitter(500));

          let foundCount = null;
          let foundRawText = null;

          // Attempt quick selector-first: heading role=3 (fast)
          try {
            const locator = page.locator('div[role="heading"][aria-level="3"]').first();
            // we try to fetch textContent directly with a small timeout (fast)
            const text = await locator.textContent({ timeout: SELECTOR_TIMEOUT }).catch(() => null);
            if (text) {
              const parsed = parseCountFromHeadingText(text);
              if (parsed != null) {
                foundCount = parsed;
                foundRawText = text;
                logInfo(`🔎 [${offer.id}] heading matched selector-first: ${foundCount}`);
              }
            }
          } catch (e) {
            // ignore selector-first errors
          }

          // If not found, try frames (some pages render in iframes)
          if (foundCount === null) {
            try {
              for (const frame of page.frames()) {
                try {
                  const fText = await frame.locator('div[role="heading"][aria-level="3"]').first().textContent().catch(() => null);
                  if (fText) {
                    const parsed = parseCountFromHeadingText(fText);
                    if (parsed != null) {
                      foundCount = parsed;
                      foundRawText = fText;
                      logInfo(`🔎 [${offer.id}] frame heading matched: ${foundCount}`);
                      break;
                    }
                  }
                } catch (e) {
                  // ignore frame errors and continue
                }
              }
            } catch (e) {
              // no-op
            }
          }

          // If still not found, fallback to injected function (replicates extension)
          if (foundCount === null) {
            try {
              const evalRes = await page.evaluate(injectedGetPageDetailsAndAdCountForEvaluate, offer.offerName || "");
              if (evalRes && evalRes.extractedSuccessfully && typeof evalRes.results === "number") {
                foundCount = evalRes.results;
                foundRawText = evalRes.rawText || null;
                logInfo(`🔁 [${offer.id}] fallback evaluate succeeded: ${foundCount}`);
              } else {
                logInfo(`🔁 [${offer.id}] fallback evaluate did not find counter`);
              }
            } catch (e) {
              logWarn(`⚠️ [${offer.id}] evaluate fallback failed: ${String(e?.message || e)}`);
            }
          }

          const updated_at = nowIso();

          if (foundCount != null) {
            const activeAds = foundCount;
            // Successful extraction: update DB and reset attempts to 0
            try {
              const { error } = await supabase
                .from("swipe_file_offers")
                .update({ activeAds, updated_at, status_updated: "success", attempts: 0 })
                .eq("id", offer.id);
              if (error) {
                logWarn(`[${offer.id}] Erro ao atualizar DB: ${error.message || error}`);
              } else {
                logInfo(`✅ [${offer.id}] atualizado activeAds=${activeAds}`);
              }
            } catch (e) {
              logWarn(`[${offer.id}] Supabase update error: ${String(e?.message || e)}`);
            }
            consecutiveSuccess++;
            consecutiveFails = 0;
          } else {
            // Not found: don't write null on first attempts. Increment attempts and only null after MAX_FAILS.
            logWarn(`❌ [${offer.id}] contador não encontrado — incrementando attempts`);
            // Save debug HTML only if DEBUG=true
            try {
              const html = DEBUG ? await page.content().catch(() => null) : null;
              if (html) {
                const dbg = await saveDebugHtml(html, offer.id, "no-counter");
                if (dbg && dbg.htmlPath) logInfo(`[${offer.id}] debug salvo: ${dbg.htmlPath}`);
              }
            } catch (e) {
              logWarn(`[${offer.id}] falha ao salvar debug: ${String(e?.message || e)}`);
            }

            const currentAttempts = Number(offer.attempts ?? 0);
            const newAttempts = currentAttempts + 1;

            try {
              if (newAttempts >= MAX_FAILS) {
                const { error } = await supabase
                  .from("swipe_file_offers")
                  .update({ activeAds: null, updated_at, status_updated: "no_counter", attempts: newAttempts })
                  .eq("id", offer.id);
                if (error) logWarn(`[${offer.id}] DB update (no_counter) error: ${error.message || error}`);
                else logInfo(`[${offer.id}] marcado no_counter após ${newAttempts} tentativas`);
              } else {
                const { error } = await supabase
                  .from("swipe_file_offers")
                  .update({ updated_at, status_updated: `no_counter_attempt_${newAttempts}`, attempts: newAttempts })
                  .eq("id", offer.id);
                if (error) logWarn(`[${offer.id}] DB update (attempt increment) error: ${error.message || error}`);
                else logInfo(`[${offer.id}] incrementado attempts -> ${newAttempts}`);
              }
            } catch (e) {
              logWarn(`[${offer.id}] Erro ao atualizar attempts no DB: ${String(e?.message || e)}`);
            }

            consecutiveFails++;
          }
        } catch (err) {
          logError(`🚫 [${offer.id}] Erro ao processar:`, err?.message || err);
          // On exception increment attempts similarly to not-found
          try {
            const currentAttempts = Number(offer.attempts ?? 0);
            const newAttempts = currentAttempts + 1;
            if (newAttempts >= MAX_FAILS) {
              await supabase
                .from("swipe_file_offers")
                .update({ activeAds: null, updated_at: nowIso(), status_updated: "error", attempts: newAttempts })
                .eq("id", offer.id);
              logWarn(`[${offer.id}] marcado error/no_counter após exception -> attempts ${newAttempts}`);
            } else {
              await supabase
                .from("swipe_file_offers")
                .update({ updated_at: nowIso(), status_updated: `error_attempt_${newAttempts}`, attempts: newAttempts })
                .eq("id", offer.id);
              logWarn(`[${offer.id}] incrementado attempts (exception) -> ${newAttempts}`);
            }
          } catch (e) {
            logWarn(`[${offer.id}] DB update (exception) failed: ${String(e?.message || e)}`);
          }
          consecutiveFails++;
        } finally {
          try { await page.close(); } catch (e) { /* ignore */ }
          // small delay between pages to avoid burst
          await sleep(200 + jitter(300));
        }
      }));

      // adaptive throttling (keep, but conservative)
      if (consecutiveFails >= 3 && CONTEXT_POOL_SIZE > 1) {
        // nothing heavy here; we could reduce concurrency externally if needed
        logWarn("⚠️ Consecutive fails >=3");
      } else if (consecutiveSuccess >= 20) {
        consecutiveSuccess = 0;
      }

      // small pause between batches
      await sleep(300 + jitter(600));
    }
  } finally {
    // close contexts and browser
    await closeContexts(contexts);
    try { await browser.close(); } catch (e) { /* ignore */ }
  }
}

(async () => {
  logInfo("🚀 Scraper otimizado iniciado", { WORKER_INDEX, TOTAL_WORKERS, WORKER_ID, PARALLEL, CONTEXT_POOL_SIZE });
  if (DEBUG) ensureDebugDir();

  const { data: offers, error } = await supabase.from("swipe_file_offers").select("*");
  if (error) {
    logError("❌ Erro ao buscar ofertas:", error);
    process.exit(1);
  }
  if (!Array.isArray(offers) || offers.length === 0) {
    logInfo("ℹ️ Nenhuma oferta encontrada. Encerrando.");
    process.exit(0);
  }

  const shuffledOffers = shuffle(offers);
  logInfo(`🚀 Total de ofertas (shuffled): ${shuffledOffers.length}`);

  const total = shuffledOffers.length;
  const chunkSize = Math.ceil(total / TOTAL_WORKERS);
  let myOffers = shuffledOffers.slice(WORKER_INDEX * chunkSize, Math.min((WORKER_INDEX + 1) * chunkSize, total));
  if (PROCESS_LIMIT) myOffers = myOffers.slice(0, PROCESS_LIMIT);

  logInfo(`📂 Worker ${WORKER_INDEX} processando ${myOffers.length} ofertas (chunk size ${chunkSize})`);
  await processOffers(myOffers);

  logInfo("✅ Worker finalizado");
  process.exit(0);
})();
