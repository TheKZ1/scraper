const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = 'https://zbvibhtopcsqrnecxgim.supabase.co';
const SUPABASE_SERVICE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InpidmliaHRvcGNzcXJuZWN4Z2ltIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MTkyMjI5OCwiZXhwIjoyMDg3NDk4Mjk4fQ.jBvKDo_j0TvZ3FuQ2DpNAQmHXGABuyFcVhfjSc-G42w';

async function run() {
  const failedStageIds = [2294, 2301, 2316, 2392];
  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const { data: stages, error: stagesError } = await supabase
    .from('stages')
    .select('id,race_id,stage_number,url')
    .in('id', failedStageIds)
    .order('id', { ascending: true });

  if (stagesError) throw stagesError;

  const raceIds = Array.from(new Set((stages || []).map((s) => s.race_id)));
  const { data: races, error: racesError } = await supabase
    .from('races')
    .select('id,name,slug,year')
    .in('id', raceIds);

  if (racesError) throw racesError;

  const raceById = new Map((races || []).map((r) => [r.id, r]));

  (stages || []).forEach((stage) => {
    const race = raceById.get(stage.race_id);
    const base = `https://www.procyclingstats.com/race/${race.slug}/${race.year}/stage-${stage.stage_number}/result`;
    const variants = [
      base,
      `${base}/`,
      `${base}/result`,
      `${base}/result/`
    ];

    console.log(`${race.name} ${race.year} - stage ${stage.stage_number} (stage_id=${stage.id})`);
    console.log(`stage.url: ${stage.url || '[null]'}`);
    variants.forEach((v) => console.log(`  - ${v}`));
    console.log('');
  });
}

run().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
