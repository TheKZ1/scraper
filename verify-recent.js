const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = 'https://zbvibhtopcsqrnecxgim.supabase.co';
const SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InpidmliaHRvcGNzcXJuZWN4Z2ltIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MTkyMjI5OCwiZXhwIjoyMDg3NDk4Mjk4fQ.jBvKDo_j0TvZ3FuQ2DpNAQmHXGABuyFcVhfjSc-G42w';
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

(async () => {
  // Get the 5 most recently added UCI records
  const { data: recentUci } = await supabase
    .from('rider_uci_database')
    .select('rider_name, uci_points, last_updated')
    .eq('year', 2025)
    .order('last_updated', { ascending: false })
    .limit(5);
  
  console.log('5 Most Recently Updated UCI Records:\n');
  
  for (const uci of recentUci || []) {
    const { data: matches } = await supabase
      .from('riders')
      .select('id, name, race_id, uci_points')
      .eq('name', uci.rider_name)
      .eq('uci_points', uci.uci_points);
    
    console.log(`${uci.rider_name}: ${uci.uci_points} pts`);
    console.log(`  Last updated: ${uci.last_updated}`);
    console.log(`  Found ${matches?.length || 0} matching instances in riders table with correct UCI points`);
    console.log();
  }
})();
