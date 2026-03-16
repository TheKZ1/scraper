const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = 'https://zbvibhtopcsqrnecxgim.supabase.co';
const SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InpidmliaHRvcGNzcXJuZWN4Z2ltIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MTkyMjI5OCwiZXhwIjoyMDg3NDk4Mjk4fQ.jBvKDo_j0TvZ3FuQ2DpNAQmHXGABuyFcVhfjSc-G42w';
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

(async () => {
  // Get all riders in rider_uci_database
  const { data: allUci } = await supabase
    .from('rider_uci_database')
    .select('rider_name, uci_points')
    .eq('year', 2025)
    .order('created_at', { ascending: false })
    .limit(10);
  
  console.log('Latest 10 riders in rider_uci_database:');
  allUci?.forEach(r => console.log(`  [${r.rider_name}] = ${r.uci_points} pts`));
  
  console.log('\nChecking if these exist in riders table with matching UCI points:\n');
  
  for (const uciRider of (allUci || [])) {
    const { data: riderMatch } = await supabase
      .from('riders')
      .select('name, uci_points')
      .eq('name', uciRider.rider_name);
    
    const hasMatchingPoints = riderMatch?.some(r => r.uci_points === uciRider.uci_points);
    const status = hasMatchingPoints ? '✓ SYNCED' : '✗ NOT SYNCED';
    
    console.log(`${status}: [${uciRider.rider_name}]`);
    if (riderMatch && riderMatch.length > 0) {
      riderMatch.forEach(r => {
        console.log(`         In riders table: uci_points = ${r.uci_points}`);
      });
    }
  }
})();
