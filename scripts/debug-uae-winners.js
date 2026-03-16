const { createClient } = require('@supabase/supabase-js');

const sb = createClient(
  'https://zbvibhtopcsqrnecxgim.supabase.co',
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InpidmliaHRvcGNzcXJuZWN4Z2ltIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MTkyMjI5OCwiZXhwIjoyMDg3NDk4Mjk4fQ.jBvKDo_j0TvZ3FuQ2DpNAQmHXGABuyFcVhfjSc-G42w'
);

(async () => {
  const { data: races, error: raceErr } = await sb
    .from('races')
    .select('id,name,year')
    .ilike('name', '%uae tour%');

  if (raceErr) throw raceErr;
  const race = (races || []).find((r) => Number(r.year) === 2026) || (races || [])[0];
  if (!race) {
    console.log('No UAE Tour race found.');
    return;
  }

  console.log('Race:', race);

  const { data: stages, error: stageErr } = await sb
    .from('stages')
    .select('id,stage_number,winner,start_date')
    .eq('race_id', race.id)
    .order('stage_number', { ascending: true });

  if (stageErr) throw stageErr;

  (stages || []).forEach((s) => {
    console.log(`stage ${s.stage_number}: ${s.winner || '-'}`);
  });
})();
