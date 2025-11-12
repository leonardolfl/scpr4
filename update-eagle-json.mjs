// update-eagle-json.mjs
import fs from "fs";
import { supabase } from "./supabase.js";

const fetchFn = globalThis.fetch || (await import("node-fetch")).default;

async function main() {
  console.log("🦅 Iniciando atualização do eagle_offers_data.json...");

  const { data: offers, error } = await supabase
    .from("swipe_file_offers")
    .select("*")
    .order("updated_at", { ascending: false });

  if (error) {
    console.error("❌ Erro ao buscar ofertas do Supabase:", error.message);
    process.exit(1);
  }

  if (!offers || offers.length === 0) {
    console.log("⚠️ Nenhuma oferta encontrada no banco. Abortando atualização.");
    process.exit(0);
  }

  // Função para converter strings "null", "", undefined em null literal
  const fix = (v) =>
    v === null || v === undefined || v === "" || v === "null" || v === "NULL"
      ? null
      : v;

  // Gera estrutura formatada corretamente
  const formatted = {
    version: new Date().toISOString().slice(0, 10),
    offers: offers.map((o) => ({
      id: fix(o.id),
      offerName: fix(o.offerName),
      niche: fix(o.niche),
      activeAds:
        typeof o.activeAds === "number"
          ? o.activeAds
          : Number(o.activeAds) || null,
      location: fix(o.location),
      funnel: fix(o.funnel),
      deliverable: fix(o.deliverable),
      ticket: fix(o.ticket),
      dateAdded: fix(o.dateAdded ?? o.created_at),
      adLibraryUrl: fix(o.adLibraryUrl),
      pageUrl: fix(o.pageUrl),
      checkoutUrl: fix(o.checkoutUrl),
    })),
  };

  const json = JSON.stringify(formatted, null, 2);
  fs.writeFileSync("eagle_offers_data.json", json);
  console.log(`✅ Gerado arquivo local com ${offers.length} ofertas.`);

  const uploadUrl = process.env.EAGLE_UPDATE_URL;
  if (!uploadUrl) {
    console.error("❌ Variável EAGLE_UPDATE_URL não definida nos secrets.");
    process.exit(1);
  }

  console.log(`🌐 Enviando JSON atualizado para ${uploadUrl} ...`);

  try {
    const res = await fetchFn(uploadUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: json,
    });

    const text = await res.text();
    if (!res.ok) {
      console.error(`❌ Falha no upload: ${res.status} ${res.statusText}`);
      console.error("Resposta do servidor:", text);
      process.exit(1);
    }

    console.log("✅ Upload concluído com sucesso!");
    console.log("🔍 Resposta do servidor:", text);
  } catch (err) {
    console.error("❌ Erro ao enviar arquivo:", err.message);
    process.exit(1);
  }
}

main();
