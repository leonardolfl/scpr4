import { createClient } from "@supabase/supabase-js";

// As chaves vão vir dos GitHub Secrets, passadas como variáveis de ambiente
export const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);
