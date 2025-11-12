#!/usr/bin/env node

import fs from "fs";
import { chromium } from "playwright";
import { supabase } from "./supabase.js";

const WAIT_TIME = 7000; // tempo de renderização
const NAV_TIMEOUT = 60000;
const PARALLEL = 3; // processar 3 em paralelo
const DEBUG = String(process.env.DEBUG || "false").toLowerCase() === "true";
const DEBUG_DIR = "./debug_manual";
const TOTAL_WORKERS = parseInt(process.env.TOTAL_WORKERS || "2", 10);
const WORKER_INDEX = parseInt(process.env.WORKER_INDEX ?? "0", 10);

function nowIso() {
  return new Date().toISOString();
}
function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

async function ensureDebugDir() {
  if (!DEBUG) return;
  if (!fs.existsSync(DEBUG_DIR)) fs.mkdirSync(DEBUG_DIR, { recursive: true });
}

async function createFastContext(browser) {
  const context = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    viewport: { width: 430, height: 932 },
    deviceScaleFactor: 2,
    isMobile: true,
    hasTouch: true,
  });

  // Bloquear recursos pesados
  await context.route("**/*", (route) => {
    const type = route.request().resourceType();
    if (["image", "stylesheet", "font", "media"].includes(type)) {
      return route.abort();
    }
    return route.continue();
  });

  return context;
}

function parseCountFromText(text) {
  if (!text) return null;
  const m =
    text.match(/~\s*([0-9.,]+)/) ||
    text.match(/([\d\.,\s\u00A0\u202F]+)\s*(?:resultados|results)/i);
  if (m && m[1]) {
    const cleaned = m[1].replace(/[.,\s\u00A0\u202F]/g, "");
    const n = parseInt(cleaned, 10);
    return isNaN(n) ? null : n;
  }
  return null;
}

async function attemptExtract(page) {
  const selectors = [
    'div[role="heading"][aria-level="3"]',
    'h3',
    'div[data-pagelet="root"] h3',
  ];

  for (const sel of selectors) {
    try {
      const el = await page.locator(sel).first();
      const txt = await el.textContent({ timeout: 3000 });
      const parsed = parseCountFromText(txt);
      if (parsed) return { found: true, count: parsed, selector: sel };
    } catch (e) {}
  }

  // fallback: leitura geral
  try {
    const html = await page.content();
    const parsed = parseCountFromText(html);
    if (parsed) return { found: true, count: parsed, selector: "html-fallback" };
  } catch (e) {}

  return { found: false };
}

async function main() {
  console.log("🚀 Iniciando leitura de ofertas com activeAds = NULL...");
  await ensureDebugDir();

  // Buscar ofertas null no banco
  const { data: offersAll, error } = await supabase
    .from("swipe_file_offers")
    .select("*")
    .is("activeAds", null)
    .is("deleted_at", null)
    .order("id", { ascending: true });

  if (error) {
    console.error("Erro ao buscar ofertas null:", error.message);
    process.exit(1);
  }

  if (!offersAll || offersAll.length === 0) {
    console.log("✅ Nenhuma oferta null encontrada. Encerrando.");
    process.exit(0);
  }

  const total = offersAll.length;
  const chunkSize = Math.ceil(total / TOTAL_WORKERS);
  const myOffers = offersAll.slice(
    WORKER_INDEX * chunkSize,
    (WORKER_INDEX + 1) * chunkSize
  );

  console.log(
    `🧩 Worker ${WORKER_INDEX}/${TOTAL_WORKERS} processando ${myOffers.length}/${total} ofertas`
  );

  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });
  const context = await createFastContext(browser);

  try {
    for (let i = 0; i < myOffers.length; i += PARALLEL) {
      const batch = myOffers.slice(i, i + PARALLEL);

      await Promise.all(
        batch.map(async (offer) => {
          const url = offer.adLibraryUrl;
          if (!url) {
            console.log(`⚠️ [${offer.id}] sem adLibraryUrl`);
            return;
          }

          const page = await context.newPage();
          console.log(`\n🔗 [${offer.id}] Testando: ${url}`);

          try {
            await page.goto(url, {
              waitUntil: "domcontentloaded",
              timeout: NAV_TIMEOUT,
            });
            await page.waitForTimeout(WAIT_TIME);
            const result = await attemptExtract(page);

            if (result.found) {
              console.log(
                `✅ [${offer.id}] ${result.count} anúncios encontrados (${result.selector})`
              );
              const { error: updateError } = await supabase
                .from("swipe_file_offers")
                .update({
                  activeAds: result.count,
                  status_updated: "success",
                  attempts: 0,
                  updated_at: nowIso(),
                })
                .eq("id", offer.id);

              if (updateError)
                console.log(
                  `⚠️ [${offer.id}] erro ao atualizar: ${updateError.message}`
                );
            } else {
              console.log(`⚠️ [${offer.id}] nenhum número encontrado`);
              if (DEBUG) {
                const html = await page.content();
                const file = `${DEBUG_DIR}/debug-${offer.id}.html`;
                await fs.promises.writeFile(file, html, "utf8");
              }
            }
          } catch (e) {
            console.log(`💥 [${offer.id}] erro: ${e.message}`);
          } finally {
            await page.close();
          }
        })
      );
      await sleep(200);
    }
  } finally {
    await context.close();
    await browser.close();
  }

  console.log("🏁 Worker finalizado.");
}

main().catch((e) => {
  console.error("Erro fatal:", e);
  process.exit(1);
});
