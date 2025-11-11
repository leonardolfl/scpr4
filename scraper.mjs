#!/usr/bin/env node

/**
 * scraper.mjs - optimized, fingerprint rotation, context recycling
 *
 * SQL recomendado (execute no Supabase SQL editor se ainda não executou):
 *   ALTER TABLE swipe_file_offers ADD COLUMN IF NOT EXISTS attempts integer DEFAULT 0;
 *
 * Principais características:
 * - selector-first (heading role=3) + fallback evaluate (cópia da extensão)
 * - Pool de contexts para performance (CONTEXT_POOL_SIZE)
 * - Randomização de contexto por página + probabilidade de criar novo contexto (NEW_CONTEXT_PROB)
 * - Reciclagem de contexts após PROCESS_PER_CONTEXT páginas processadas
 * - Detecção de bloqueios (HTTP status / heurística de conteúdo) com retry alternativo e marcação blocked_ip
 * - Não salva screenshots; salva HTML debug apenas se DEBUG=true
 * - RETRY_ATTEMPTS default = 1, PARALLEL default = 5
 *
 * ENV:
 * WORKER_INDEX, TOTAL_WORKERS, PROCESS_LIMIT
 * PARALLEL default 5
 * CONTEXT_POOL_SIZE default PARALLEL
 * PROCESS_PER_CONTEXT default 20
 * NEW_CONTEXT_PROB default 0.05
 * WAIT_TIME default 1200 (ms)
 * NAV_TIMEOUT default 45000 (ms)
 * SELECTOR_TIMEOUT default 5000 (ms)
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

const WAIT_TIME = parseInt(process.env.WAIT_TIME || "1200", 10);
const NAV_TIMEOUT = parseInt(process.env.NAV_TIMEOUT || "45000", 10);
const SELECTOR_TIMEOUT = parseInt(process.env.SELECTOR_TIMEOUT || "5000", 10);
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

function nowIso() { return new Date().toISOString(); }
function nowTs() { return new Date().toISOString().replace(/[:.]/g, "-"); }
function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }
function jitter(ms) { return Math.floor(Math.random() * ms); }
function shuffle(array) { const a = array.slice(); for (let i = a.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [a[i], a[j]] = [a[j], a[i]; } return a; } // note: kept simple
function randProb(p) { return Math.random() < p; }

function logInfo(...args) { if (["info"].includes(LOG_LEVEL)) console.log(...args); }
function logWarn(...args) { if (["info","warn"].includes(LOG_LEVEL)) console.warn(...args); }
function logError(...args) { if (["info","warn","error"].includes(LOG_LEVEL)) console.error(...args); }

function ensureDebugDir() { if (!DEBUG) return; if (!fs.existsSync(DEBUG_DIR)) fs.mkdirSync(DEBUG_DIR, { recursive: true }); }
async function saveDebugHtml(htmlContent, offerId, note = "") { if (!DEBUG) return null; try { ensureDebugDir(); const ts = nowTs(); const safe = `offer-${offerId}-${ts}`; const htmlPath = path.join(DEBUG_DIR, `${safe}.html`); await fs.promises.writeFile(htmlPath, `<!-- ${note} -->\n` + htmlContent, "utf8"); return { htmlPath }; } catch (err) { logWarn("❌ Falha ao salvar debug HTML:", err?.message || err); return null; } }

/** Parser like extension (heading-based) */
function parseCountFromHeadingText(text) {
  if (!text || typeof text !== "string") return null;
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

/** In-page fallback copied from extension (serializable) */
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

/** Heurística de bloqueio leve (palavras-chave no HTML) */
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
  // tracker for recycling
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

      await Promise.all(batch.map(async (offer, idxInBatch) => {
        if (!offer || !offer.adLibraryUrl) return;

        // Choose context randomly; sometimes create new context to increase rotation
        let ctxIndex;
        let context;
        if (randProb(NEW_CONTEXT_PROB)) {
          // temporarily create new context and use it (do not add permanently to pool)
          try {
            const tempCtx = await createContext(browser);
            context = tempCtx;
            ctxIndex = -1; // mark as temporary
            logInfo(`[${offer.id}] using temp context for diversification`);
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
          // goto with response capture
          let response = null;
          try {
            response = await page.goto(offer.adLibraryUrl, { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT });
          } catch (e) {
            logWarn(`⚠️ [${offer.id}] goto erro: ${String(e?.message || e)}`);
          }

          // small wait to let inline JS run
          await page.waitForTimeout(WAIT_TIME + jitter(400));

          // First: try heading selector (fast)
          let foundCount = null;
          let foundRawText = null;
          try {
            const text = await page.locator('div[role="heading"][aria-level="3"]').first().textContent({ timeout: SELECTOR_TIMEOUT }).catch(() => null);
            if (text) {
              const parsed = parseCountFromHeadingText(text);
              if (parsed != null) { foundCount = parsed; foundRawText = text; logInfo(`🔎 [${offer.id}] selector-first heading: ${foundCount}`); }
            }
          } catch (e) { /* ignore */ }

          // If not found, check frames briefly
          if (foundCount === null) {
            try {
              for (const frame of page.frames()) {
                const ft = await frame.locator('div[role="heading"][aria-level="3"]').first().textContent().catch(() => null);
                if (ft) {
                  const parsed = parseCountFromHeadingText(ft);
                  if (parsed != null) { foundCount = parsed; foundRawText = ft; logInfo(`🔎 [${offer.id}] frame heading: ${foundCount}`); break; }
                }
              }
            } catch (e) { /* ignore */ }
          }

          // If still not found, check for blocked signs (only now request content to save time)
          let htmlContent = null;
          let statusCode = response ? response.status() : null;
          let blockedDetected = false;
          if (foundCount === null) {
            try {
              htmlContent = await page.content().catch(() => null);
              const contentBlocked = contentLooksBlocked(htmlContent);
              const statusBlocked = statusCode && [401,403,407,429].includes(statusCode);
              if (statusBlocked || contentBlocked) blockedDetected = true;
            } catch (e) {
              // ignore
            }
          }

          // If blocked detected, try a quick retry with alternate context
          if (blockedDetected) {
            logWarn(`[${offer.id}] possível bloqueio detectado (status=${statusCode}) — tentando retry com contexto alternativo`);
            // try one retry with another context
            let retrySucceeded = false;
            // pick alternative context
            const altIdx = (() => {
              if (CONTEXT_POOL_SIZE === 1) return 0;
              let k; do { k = Math.floor(Math.random() * CONTEXT_POOL_SIZE); } while (k === ctxIndex); return k;
            })();
            try {
              try { await page.close(); } catch (e) {}
              const altContext = contexts[altIdx];
              const altPage = await altContext.newPage();
              let resp2 = null;
              try {
                resp2 = await altPage.goto(offer.adLibraryUrl, { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT });
              } catch (e) { /* ignore */ }
              await altPage.waitForTimeout(WAIT_TIME + jitter(300));
              // quick heading check
              const t2 = await altPage.locator('div[role="heading"][aria-level="3"]').first().textContent({ timeout: Math.min(2000, SELECTOR_TIMEOUT) }).catch(() => null);
              if (t2) {
                const parsed = parseCountFromHeadingText(t2);
                if (parsed != null) {
                  foundCount = parsed;
                  foundRawText = t2;
                  retrySucceeded = true;
                  logInfo(`🔁 [${offer.id}] retry alt context succeeded: ${foundCount}`);
                }
              }
              // cleanup altPage
              try { await altPage.close(); } catch (e) {}
            } catch (e) {
              logWarn(`[${offer.id}] retry alt context erro: ${String(e?.message || e)}`);
            }

            if (!retrySucceeded) {
              // mark as blocked_ip in DB and skip attempts increment
              blockedCount++;
              if (blockedCount >= 30) {
                logWarn(`⚠️ Worker ${WORKER_INDEX} detectou ${blockedCount} bloqueios consecutivos. Backoff 5min.`);
                await sleep(5 * 60 * 1000);
                blockedCount = 0;
              }
              try {
                const { error } = await supabase
                  .from("swipe_file_offers")
                  .update({ updated_at: nowIso(), status_updated: "blocked_ip" })
                  .eq("id", offer.id);
                if (error) logWarn(`[${offer.id}] Erro ao marcar blocked_ip: ${error.message || error}`);
                else logInfo(`[${offer.id}] marcado blocked_ip`);
              } catch (e) {
                logWarn(`[${offer.id}] Erro DB marcar blocked_ip: ${String(e?.message || e)}`);
              }
              try { await page.close(); } catch (e) {}
              if (ctxIndex === -1) try { await context.close(); } catch (e) {}
              return;
            }
          } // end blocked handling

          // If not found yet, fallback to evaluate (extension method)
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
              logWarn(`⚠️ [${offer.id}] evaluate fallback erro: ${String(e?.message || e)}`);
            }
          }

          const updated_at = nowIso();

          if (foundCount != null) {
            // success: reset attempts
            try {
              const { error } = await supabase
                .from("swipe_file_offers")
                .update({ activeAds: foundCount, updated_at, status_updated: "success", attempts: 0 })
                .eq("id", offer.id);
              if (error) logWarn(`[${offer.id}] DB update error: ${error.message || error}`);
              else logInfo(`✅ [${offer.id}] activeAds=${foundCount}`);
            } catch (e) {
              logWarn(`[${offer.id}] Supabase update error: ${String(e?.message || e)}`);
            }
            consecutiveSuccess++;
            consecutiveFails = 0;
            blockedCount = 0;
          } else {
            // not found: increment attempts and save debug only if DEBUG=true
            logWarn(`❌ [${offer.id}] contador não encontrado — incrementando attempts`);
            try {
              const html = DEBUG ? (htmlContent || await page.content().catch(() => null)) : null;
              if (html) {
                const dbg = await saveDebugHtml(html, offer.id, "no-counter");
                if (dbg && dbg.htmlPath) logInfo(`[${offer.id}] debug salvo: ${dbg.htmlPath}`);
              }
            } catch (e) { logWarn(`[${offer.id}] falha ao salvar debug: ${String(e?.message || e)}`); }

            const currentAttempts = Number(offer.attempts ?? 0);
            const newAttempts = currentAttempts + 1;
            try {
              if (newAttempts >= MAX_FAILS) {
                const { error } = await supabase
                  .from("swipe_file_offers")
                  .update({ activeAds: null, updated_at, status_updated: "no_counter", attempts: newAttempts })
                  .eq("id", offer.id);
                if (error) logWarn(`[${offer.id}] DB update (no_counter) error: ${error.message || error}`);
                else logInfo(`[${offer.id}] marcado no_counter após ${newAttempts}`);
              } else {
                const { error } = await supabase
                  .from("swipe_file_offers")
                  .update({ updated_at, status_updated: `no_counter_attempt_${newAttempts}`, attempts: newAttempts })
                  .eq("id", offer.id);
                if (error) logWarn(`[${offer.id}] DB update (increment attempts) error: ${error.message || error}`);
                else logInfo(`[${offer.id}] incrementado attempts -> ${newAttempts}`);
              }
            } catch (e) {
              logWarn(`[${offer.id}] Erro ao atualizar attempts: ${String(e?.message || e)}`);
            }
            consecutiveFails++;
          }
        } catch (err) {
          logError(`🚫 [${offer.id}] Erro geral:`, err?.message || err);
          try {
            const currentAttempts = Number(offer.attempts ?? 0);
            const newAttempts = currentAttempts + 1;
            if (newAttempts >= MAX_FAILS) {
              await supabase.from("swipe_file_offers").update({ activeAds: null, updated_at: nowIso(), status_updated: "error", attempts: newAttempts }).eq("id", offer.id);
            } else {
              await supabase.from("swipe_file_offers").update({ updated_at: nowIso(), status_updated: `error_attempt_${newAttempts}`, attempts: newAttempts }).eq("id", offer.id);
            }
          } catch (e) { logWarn(`[${offer.id}] DB update (exception) failed: ${String(e?.message || e)}`); }
          consecutiveFails++;
        } finally {
          try { await page.close(); } catch (e) {}
          // If used temporary context, close it
          if (ctxIndex === -1) {
            try { await context.close(); } catch (e) {}
          } else {
            // increment counter for recycling
            contexts[ctxIndex].__pagesProcessed = (contexts[ctxIndex].__pagesProcessed || 0) + 1;
            if (contexts[ctxIndex].__pagesProcessed >= PROCESS_PER_CONTEXT) {
              // recycle it asynchronously to not block too long
              recreateContext(browser, contexts, ctxIndex).catch(e => logWarn("recreateContext failed:", e?.message || e));
            }
          }
          // small jittered pause per page
          await sleep(150 + jitter(250));
        }
      })); // end batch map

      // throughput log
      const batchElapsed = (Date.now() - batchStart) / 1000;
      logInfo(`✅ bloco finalizado em ${batchElapsed.toFixed(2)}s — processed ${batch.length} offers`);

      if (consecutiveFails >= 6) {
        logWarn(`⚠️ Consecutive fails >= ${consecutiveFails} — consider reducing PARALLEL or inspecting blocked IPs`);
      } else if (consecutiveSuccess >= 50) {
        consecutiveSuccess = 0;
      }

      // small pause between batches
      await sleep(200 + jitter(400));
    } // end for batches
  } finally {
    await closeContexts(contexts);
    try { await browser.close(); } catch (e) {}
    const totalElapsed = (Date.now() - workerStart) / 1000;
    logInfo(`🏁 Worker ${WORKER_INDEX} finalizado em ${totalElapsed.toFixed(1)}s`);
  }
}

(async () => {
  logInfo("🚀 Scraper otimizado (rotation+recycle) iniciando", { WORKER_INDEX, TOTAL_WORKERS, WORKER_ID, PARALLEL, CONTEXT_POOL_SIZE, PROCESS_PER_CONTEXT, NEW_CONTEXT_PROB });
  if (DEBUG) ensureDebugDir();

  // stagger startup so workers don't burst simultaneously
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

  // shuffle for distribution
  const shuffledOffers = offers.slice();
  for (let i = shuffledOffers.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [shuffledOffers[i], shuffledOffers[j]] = [shuffledOffers[j], shuffledOffers[i]];
  }

  const total = shuffledOffers.length;
  const chunkSize = Math.ceil(total / TOTAL_WORKERS);
  let myOffers = shuffledOffers.slice(WORKER_INDEX * chunkSize, Math.min((WORKER_INDEX + 1) * chunkSize, total));
  if (PROCESS_LIMIT) myOffers = myOffers.slice(0, PROCESS_LIMIT);

  logInfo(`🚀 Total ofertas: ${shuffledOffers.length} — Worker ${WORKER_INDEX} processando ${myOffers.length} (chunkSize ${chunkSize})`);
  await processOffers(myOffers);

  logInfo("✅ Worker finalizado");
  process.exit(0);
})();
