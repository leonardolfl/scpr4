#!/usr/bin/env node

/**
 * scraper.mjs - optimized + safer block detection + 1 retry + longer waits
 *
 * SQL recomendado (execute no Supabase SQL editor se ainda não executou):
 *   ALTER TABLE swipe_file_offers ADD COLUMN IF NOT EXISTS attempts integer DEFAULT 0;
 *
 * Principais características:
 * - selector-first (heading role=3) + fallback evaluate (cópia da extensão)
 * - Pool de contexts para performance (CONTEXT_POOL_SIZE)
 * - Randomização de contexto por página + criação temporária ocasional
 * - Reciclagem de contexts após PROCESS_PER_CONTEXT páginas processadas
 * - Detecção de bloqueios SÓ se ocorrerem em ambas tentativas (reduz falsos-positivos)
 * - 1 retry de extração (configurável via RETRY_ATTEMPTS; default 1)
 * - Mais tempo para carregamento: WAIT_TIME default 4000ms, SELECTOR_TIMEOUT 10000ms, NAV_TIMEOUT 60000ms
 * - Se extração falhar após retry: marca status_updated = "erro" (X vermelho na UI) e incrementa attempts
 * - DEBUG mode salva HTML apenas quando DEBUG=true (sem screenshots)
 *
 * ENV:
 * WORKER_INDEX, TOTAL_WORKERS, PROCESS_LIMIT
 * PARALLEL default 5
 * CONTEXT_POOL_SIZE default PARALLEL
 * PROCESS_PER_CONTEXT default 20
 * NEW_CONTEXT_PROB default 0.05
 * WAIT_TIME default 4000 (ms)
 * NAV_TIMEOUT default 60000 (ms)
 * SELECTOR_TIMEOUT default 10000 (ms)
 * RETRY_ATTEMPTS default 1
 * MAX_FAILS default 3
 * DEBUG default false
 * LOG_LEVEL default "info" (info|warn|error|silent)
 * DEBUG_DIR default ./debug
 */

import fs from "fs";
import path from "path";
import os from "os";
import { chromium, devices } from "playwright";
import { supabase } from "./supabase.js";

const TOTAL_WORKERS = parseInt(process.env.TOTAL_WORKERS || "4", 10);
const WORKER_INDEX = parseInt(process.env.WORKER_INDEX ?? "0", 10);
const PROCESS_LIMIT = process.env.PROCESS_LIMIT ? parseInt(process.env.PROCESS_LIMIT, 10) : null;

const PARALLEL = Math.max(1, parseInt(process.env.PARALLEL || "5", 10));
const CONTEXT_POOL_SIZE = Math.max(1, parseInt(process.env.CONTEXT_POOL_SIZE || String(PARALLEL), 10));
const PROCESS_PER_CONTEXT = Math.max(5, parseInt(process.env.PROCESS_PER_CONTEXT || "20", 10)); // recycle after this many pages
const NEW_CONTEXT_PROB = parseFloat(process.env.NEW_CONTEXT_PROB || "0.05"); // 5% chance to force new context

const WAIT_TIME = parseInt(process.env.WAIT_TIME || "4000", 10); // increased per request
const NAV_TIMEOUT = parseInt(process.env.NAV_TIMEOUT || "60000", 10);
const SELECTOR_TIMEOUT = parseInt(process.env.SELECTOR_TIMEOUT || "10000", 10);
const RETRY_ATTEMPTS = Math.max(0, parseInt(process.env.RETRY_ATTEMPTS || "1", 10)); // requested =1

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
  "Pixel 5",
  "Galaxy S21 Ultra",
  "iPhone 12",
];
const DEVICE_POOL = DEVICE_NAMES.map((n) => devices[n]).filter(Boolean);

const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; rv:122.0) Gecko/20100101 Firefox/122.0",
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
];

// Helpers
function nowIso() { return new Date().toISOString(); }
function nowTs() { return new Date().toISOString().replace(/[:.]/g, "-"); }
function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }
function jitter(ms) { return Math.floor(Math.random() * ms); }
function shuffle(array) {
  // Fisher-Yates
  const a = array.slice();
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    const tmp = a[i];
    a[i] = a[j];
    a[j] = tmp;
  }
  return a;
}
function randProb(p) { return Math.random() < p; }

function logInfo(...args) { if (["info"].includes(LOG_LEVEL)) console.log(...args); }
function logWarn(...args) { if (["info","warn"].includes(LOG_LEVEL)) console.warn(...args); }
function logError(...args) { if (["info","warn","error"].includes(LOG_LEVEL)) console.error(...args); }

function ensureDebugDir() { if (!DEBUG) return; if (!fs.existsSync(DEBUG_DIR)) fs.mkdirSync(DEBUG_DIR, { recursive: true }); }
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
 */
function parseCountFromHeadingText(text) {
  if (!text || typeof text !== "string") return null;
  // Try "~1,2k" or similar
  let m = text.match(/~\s*([0-9.,]+)/);
  if (!m) {
    m = text.match(/([\d\.,\s\u00A0\u202F]+)\s*(?:resultados|results)/i);
  }
  if (m && m[1]) {
    const cleaned = m[1].replace(/[.,\s\u00A0\u202F]/g, "");
    const n = parseInt(cleaned, 10);
    if (!isNaN(n)) return n;
  }
  return null;
}

/**
 * In-page fallback copied from extension (serializable)
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
    if (!match) match = textContent.match(/([\d\.,\s\u00A0\u202F]+)\s*(resultados|results)/i);
    if (match && match[1]) {
      const numberString = match[1].replace(/[.,\s\u00A0\u202F]/g, '');
      const parsedCount = parseInt(numberString, 10);
      if (!isNaN(parsedCount)) { adCount = parsedCount; extractedSuccessfully = true; }
    }
  }
  return { name: advertiserName, results: adCount, offer: offerNameToUse, extractedSuccessfully, rawText };
}

/**
 * Heurística para detectar bloqueios (palavras-chave no HTML)
 * NOTE: conservative -> used only to *suggest* blocked; marking blocked requires repeat confirmation.
 */
function contentLooksBlocked(html) {
  if (!html || typeof html !== "string") return false;
  const lowered = html.toLowerCase();
  const blockers = [
    "captcha",
    "verify",
    "access to this page has been restricted",
    "temporarily blocked",
    "unusual activity",
    "confirm you're human",
    "log in to facebook",
    "sign in to continue",
    "please enable javascript",
    "blocked",
  ];
  return blockers.some(k => lowered.includes(k));
}

/** Create a randomized context (device+UA) */
async function createContext(browser) {
  const device = DEVICE_POOL[Math.floor(Math.random() * DEVICE_POOL.length)] || {};
  const ua = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
  const contextOptions = { ...(device || {}), userAgent: ua };
  const context = await browser.newContext(contextOptions);
  await context.route("**/*", (route) => {
    const type = route.request().resourceType();
    if (["image", "stylesheet", "font", "media"].includes(type)) return route.abort();
    return route.continue();
  });
  context.__pagesProcessed = 0;
  return context;
}

/** Recreate context safely */
async function recreateContext(browser, contexts, idx) {
  try { await contexts[idx].close(); } catch (e) { /* ignore */ }
  contexts[idx] = await createContext(browser);
  logInfo(`🔄 Recreated context idx=${idx}`);
}

/** Create initial contexts pool */
async function createContexts(browser) {
  const contexts = [];
  for (let i = 0; i < CONTEXT_POOL_SIZE; i++) {
    contexts.push(await createContext(browser));
  }
  return contexts;
}

async function closeContexts(contexts) {
  for (const c of contexts) {
    try { await c.close(); } catch (e) { /* ignore */ }
  }
}

/** Extraction attempt function (single attempt) */
async function attemptExtractFromPage(page, offer) {
  // Try heading selector (fast)
  try {
    const text = await page.locator('div[role="heading"][aria-level="3"]').first().textContent({ timeout: SELECTOR_TIMEOUT }).catch(() => null);
    if (text) {
      const parsed = parseCountFromHeadingText(text);
      if (parsed != null) return { found: true, count: parsed, raw: text };
    }
  } catch (e) {
    // ignore
  }

  // Try frames
  try {
    for (const frame of page.frames()) {
      const ft = await frame.locator('div[role="heading"][aria-level="3"]').first().textContent().catch(() => null);
      if (ft) {
        const parsed = parseCountFromHeadingText(ft);
        if (parsed != null) return { found: true, count: parsed, raw: ft };
      }
    }
  } catch (e) {
    // ignore
  }

  // Fallback evaluate (extension method)
  try {
    const evalRes = await page.evaluate(injectedGetPageDetailsAndAdCountForEvaluate, offer.offerName || "");
    if (evalRes && evalRes.extractedSuccessfully && typeof evalRes.results === "number") {
      return { found: true, count: evalRes.results, raw: evalRes.rawText || null };
    }
  } catch (e) {
    // ignore
  }

  return { found: false };
}

/** Main processing */
async function processOffers(offersSlice) {
  const browser = await chromium.launch({ headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox'] });
  const contexts = await createContexts(browser);
  let consecutiveFails = 0;
  let consecutiveSuccess = 0;
  let blockedCount = 0;
  const workerStart = Date.now();

  try {
    for (let i = 0; i < offersSlice.length; i += PARALLEL) {
      const batch = offersSlice.slice(i, i + PARALLEL);
      const batchStart = Date.now();
      logInfo(`📦 Processando bloco ${Math.floor(i / PARALLEL) + 1} (${batch.length} ofertas) — Worker ${WORKER_INDEX}`);

      await Promise.all(batch.map(async (offer) => {
        if (!offer || !offer.adLibraryUrl) return;

        // 1) Pick context: random from pool, occasionally create a temp context for diversity
        let ctxIndex, context;
        let isTempContext = false;
        if (randProb(NEW_CONTEXT_PROB)) {
          try {
            context = await createContext(browser);
            ctxIndex = -1;
            isTempContext = true;
            logInfo(`[${offer.id}] using temporary context for diversification`);
          } catch (e) {
            ctxIndex = Math.floor(Math.random() * CONTEXT_POOL_SIZE);
            context = contexts[ctxIndex];
          }
        } else {
          ctxIndex = Math.floor(Math.random() * CONTEXT_POOL_SIZE);
          context = contexts[ctxIndex];
        }

        const page = await context.newPage();
        try {
          logInfo(`⌛ [${offer.id}] Acessando: ${offer.adLibraryUrl}`);

          // Initial navigation attempt
          let response = null;
          try {
            response = await page.goto(offer.adLibraryUrl, { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT });
          } catch (e) {
            logWarn(`⚠️ [${offer.id}] goto erro: ${String(e?.message || e)}`);
          }

          // Allow more time for JS to render the counter
          await page.waitForTimeout(WAIT_TIME + jitter(500));

          // First: attempt extraction
          let result = await attemptExtractFromPage(page, offer);

          // Gather content and status for block heuristics only if necessary (avoid heavy calls)
          let htmlContent = null;
          let statusCode = response ? response.status() : null;
          if (!result.found) {
            try { htmlContent = await page.content().catch(() => null); } catch (e) { htmlContent = null; }
          }

          // Determine whether initial attempt looks like a block
          const initialBlocked = (statusCode && [401,403,407,429].includes(statusCode)) || contentLooksBlocked(htmlContent);

          // If initialBlocked true, perform a retry with alternate context first (to confirm block)
          let confirmedBlock = false;
          if (initialBlocked) {
            logWarn(`[${offer.id}] possível bloqueio detectado (status=${statusCode}) — tentando retry alternativo para confirmar`);
            // Try retry with an alternate context (if pool > 1)
            let altSucceeded = false;
            if (CONTEXT_POOL_SIZE > 1) {
              const altIdx = (() => {
                let k;
                do { k = Math.floor(Math.random() * CONTEXT_POOL_SIZE); } while (k === ctxIndex && CONTEXT_POOL_SIZE > 1);
                return k;
              })();
              try {
                // close original page to free resources
                try { await page.close(); } catch (e) {}
                const altContext = contexts[altIdx];
                const altPage = await altContext.newPage();
                let resp2 = null;
                try {
                  resp2 = await altPage.goto(offer.adLibraryUrl, { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT });
                } catch (e) { /* ignore */ }
                await altPage.waitForTimeout(WAIT_TIME + jitter(400));
                const evalRes2 = await attemptExtractFromPage(altPage, offer);
                let html2 = null;
                try { html2 = await altPage.content().catch(() => null); } catch (e) { html2 = null; }
                const status2 = resp2 ? resp2.status() : null;
                const blocked2 = (status2 && [401,403,407,429].includes(status2)) || contentLooksBlocked(html2);
                if (evalRes2.found) {
                  // alt succeeded extracting -> not a block
                  result = evalRes2;
                  altSucceeded = true;
                  logInfo(`[${offer.id}] retry alt context extraiu com sucesso: ${result.count}`);
                } else if (blocked2 && initialBlocked) {
                  // both attempts show block signals -> confirm block
                  confirmedBlock = true;
                  logWarn(`[${offer.id}] bloqueio confirmado em ambos contextos (status2=${status2})`);
                } else {
                  // alt didn't extract but not clearly blocked -> leave as no-result (will increment attempts)
                  logInfo(`[${offer.id}] retry alt não extraiu, mas sem confirmação de bloqueio`);
                }
                try { await altPage.close(); } catch (e) {}
              } catch (e) {
                logWarn(`[${offer.id}] retry alternativo falhou: ${String(e?.message || e)}`);
              }
            } else {
              // only one context -> fallback: try reload once on same page
              try {
                await page.reload({ waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT }).catch(() => null);
                await page.waitForTimeout(WAIT_TIME + jitter(400));
                const reloadRes = await attemptExtractFromPage(page, offer);
                if (reloadRes.found) {
                  result = reloadRes;
                  logInfo(`[${offer.id}] reload local extraiu com sucesso: ${result.count}`);
                } else {
                  // check content for block signs
                  const html2 = await page.content().catch(() => null);
                  const status2 = page && page.response ? (page.response().status ? page.response().status() : null) : null;
                  if ((status2 && [401,403,407,429].includes(status2)) || contentLooksBlocked(html2)) {
                    confirmedBlock = true;
                    logWarn(`[${offer.id}] bloqueio possível confirmado após reload (status2=${status2})`);
                  }
                }
              } catch (e) {
                logWarn(`[${offer.id}] reload retry erro: ${String(e?.message || e)}`);
              }
            }
            // If confirm block flagged, mark and skip further retries/extraction attempts
            if (confirmedBlock) {
              blockedCount++;
              if (blockedCount >= 30) {
                logWarn(`⚠️ Worker ${WORKER_INDEX} detectou muitos bloqueios (${blockedCount}). Backoff 5min.`);
                await sleep(5 * 60 * 1000);
                blockedCount = 0;
              }
              try {
                const { error } = await supabase
                  .from("swipe_file_offers")
                  .update({ updated_at: nowIso(), status_updated: "blocked_ip" })
                  .eq("id", offer.id);
                if (error) logWarn(`[${offer.id}] Erro ao marcar blocked_ip no DB: ${error.message || error}`);
                else logInfo(`[${offer.id}] marcado blocked_ip`);
              } catch (e) {
                logWarn(`[${offer.id}] Erro DB ao marcar blocked_ip: ${String(e?.message || e)}`);
              }
              try { if (!page.isClosed()) await page.close(); } catch (e) {}
              if (isTempContext) try { await context.close(); } catch (e) {}
              return;
            }
          } // end initial blocked handling

          // If not found and no confirmed block, perform RETRY_ATTEMPTS (1) by reloading the page once
          let attemptIndex = 0;
          for (; attemptIndex <= RETRY_ATTEMPTS && !result.found; attemptIndex++) {
            if (attemptIndex === 0) {
              // already tried; if not found, we will retry
            } else {
              logInfo(`[${offer.id}] Tentativa de retry ${attemptIndex}/${RETRY_ATTEMPTS}`);
              try {
                await page.reload({ waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT }).catch(() => null);
                await page.waitForTimeout(WAIT_TIME + jitter(400));
                result = await attemptExtractFromPage(page, offer);
                if (result.found) logInfo(`[${offer.id}] retry ${attemptIndex} extraiu: ${result.count}`);
              } catch (e) {
                logWarn(`[${offer.id}] retry ${attemptIndex} erro: ${String(e?.message || e)}`);
              }
            }
          }

          // After retries, if still not found -> mark as erro (X vermelho) and increment attempts
          const updated_at = nowIso();
          if (result.found) {
            const activeAds = result.count;
            try {
              const { error } = await supabase
                .from("swipe_file_offers")
                .update({ activeAds, updated_at, status_updated: "success", attempts: 0 })
                .eq("id", offer.id);
              if (error) logWarn(`[${offer.id}] Erro ao atualizar DB: ${error.message || error}`);
              else logInfo(`✅ [${offer.id}] atualizado activeAds=${activeAds}`);
            } catch (e) {
              logWarn(`[${offer.id}] Supabase update error: ${String(e?.message || e)}`);
            }
            consecutiveSuccess++;
            consecutiveFails = 0;
            blockedCount = 0;
          } else {
            // not found after retry -> mark erro (red X) and increment attempts
            logWarn(`❌ [${offer.id}] contador não encontrado após retry(s) — marcando erro (X vermelho) e incrementando attempts`);
            try {
              const html = DEBUG ? (htmlContent || await page.content().catch(() => null)) : null;
              if (html) {
                const dbg = await saveDebugHtml(html, offer.id, "no-counter-after-retries");
                if (dbg && dbg.htmlPath) logInfo(`[${offer.id}] debug salvo: ${dbg.htmlPath}`);
              }
            } catch (e) {
              logWarn(`[${offer.id}] falha ao salvar debug: ${String(e?.message || e)}`);
            }

            const currentAttempts = Number(offer.attempts ?? 0);
            const newAttempts = currentAttempts + 1;
            try {
              // set status_updated = "erro" so UI shows red X; attempts incremented, activeAds set to null only after MAX_FAILS as before
              if (newAttempts >= MAX_FAILS) {
                const { error } = await supabase
                  .from("swipe_file_offers")
                  .update({ activeAds: null, updated_at, status_updated: "erro", attempts: newAttempts })
                  .eq("id", offer.id);
                if (error) logWarn(`[${offer.id}] DB update (final erro) error: ${error.message || error}`);
                else logInfo(`[${offer.id}] marcado erro final após ${newAttempts} tentativas`);
              } else {
                const { error } = await supabase
                  .from("swipe_file_offers")
                  .update({ updated_at, status_updated: "erro", attempts: newAttempts })
                  .eq("id", offer.id);
                if (error) logWarn(`[${offer.id}] DB update (increment attempts) error: ${error.message || error}`);
                else logInfo(`[${offer.id}] incrementado attempts -> ${newAttempts} e marcado erro`);
              }
            } catch (e) {
              logWarn(`[${offer.id}] Erro ao atualizar attempts/status: ${String(e?.message || e)}`);
            }
            consecutiveFails++;
          }
        } catch (err) {
          logError(`🚫 [${offer.id}] Erro inesperado:`, err?.message || err);
          try {
            const currentAttempts = Number(offer.attempts ?? 0);
            const newAttempts = currentAttempts + 1;
            if (newAttempts >= MAX_FAILS) {
              await supabase.from("swipe_file_offers").update({ activeAds: null, updated_at: nowIso(), status_updated: "error", attempts: newAttempts }).eq("id", offer.id);
            } else {
              await supabase.from("swipe_file_offers").update({ updated_at: nowIso(), status_updated: `error_attempt_${newAttempts}`, attempts: newAttempts }).eq("id", offer.id);
            }
          } catch (e) {
            logWarn(`[${offer.id}] DB update (exception) failed: ${String(e?.message || e)}`);
          }
          consecutiveFails++;
        } finally {
          try { if (!page.isClosed()) await page.close(); } catch (e) {}
          if (ctxIndex === -1 && typeof context?.close === "function") {
            try { await context.close(); } catch (e) {}
          } else if (ctxIndex >= 0) {
            contexts[ctxIndex].__pagesProcessed = (contexts[ctxIndex].__pagesProcessed || 0) + 1;
            if (contexts[ctxIndex].__pagesProcessed >= PROCESS_PER_CONTEXT) {
              recreateContext(browser, contexts, ctxIndex).catch(e => logWarn("recreateContext failed:", e?.message || e));
            }
          }
          await sleep(150 + jitter(300));
        }
      })); // end batch map

      const batchElapsed = (Date.now() - batchStart) / 1000;
      logInfo(`✅ Bloco finalizado em ${batchElapsed.toFixed(2)}s — processed ${batch.length} offers`);

      if (consecutiveFails >= 10) {
        logWarn(`⚠️ Consecutive fails high (${consecutiveFails}) — consider reducing PARALLEL or checking IP`);
      } else if (consecutiveSuccess >= 50) {
        consecutiveSuccess = 0;
      }

      await sleep(200 + jitter(400));
    } // end batches
  } finally {
    await closeContexts(contexts);
    try { await browser.close(); } catch (e) {}
    const totalElapsed = (Date.now() - workerStart) / 1000;
    logInfo(`🏁 Worker ${WORKER_INDEX} finalizado em ${totalElapsed.toFixed(1)}s`);
  }
}

(async () => {
  logInfo("🚀 Scraper otimizado (safer block detect + retry + longer waits) iniciando", { WORKER_INDEX, TOTAL_WORKERS, WORKER_ID, PARALLEL, CONTEXT_POOL_SIZE, PROCESS_PER_CONTEXT, NEW_CONTEXT_PROB });
  if (DEBUG) ensureDebugDir();

  // small stagger between workers to avoid bursting
  await sleep(WORKER_INDEX * 1200 + jitter(800));

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
  const total = shuffledOffers.length;
  const chunkSize = Math.ceil(total / TOTAL_WORKERS);
  let myOffers = shuffledOffers.slice(WORKER_INDEX * chunkSize, Math.min((WORKER_INDEX + 1) * chunkSize, total));
  if (PROCESS_LIMIT) myOffers = myOffers.slice(0, PROCESS_LIMIT);

  logInfo(`🚀 Total ofertas: ${shuffledOffers.length} — Worker ${WORKER_INDEX} processando ${myOffers.length} (chunkSize ${chunkSize})`);
  await processOffers(myOffers);

  logInfo("✅ Worker finalizado");
  process.exit(0);
})();
