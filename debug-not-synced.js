const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = 'https://zbvibhtopcsqrnecxgim.supabase.co';
const SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InpidmliaHRvcGNzcXJuZWN4Z2ltIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MTkyMjI5OCwiZXhwIjoyMDg3NDk4Mjk4fQ.jBvKDo_j0TvZ3FuQ2DpNAQmHXGABuyFcVhfjSc-G42w';
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

(async () => {
  // Get all unique riders in rider_uci_database
  const { data: allUci } = await supabase
    .from('rider_uci_database')
    .select('rider_name, uci_points')
    .eq('year', 2025);
  
  console.log(`Total riders in rider_uci_database: ${allUci?.length || 0}\n`);
  
  let notSynced = [];
  let synced = [];
  
  for (const uciRider of (allUci || [])) {
    const { data: riderMatch } = await supabase
      .from('riders')
      .select('uci_points')
      .eq('name', uciRider.rider_name);
    
    // Check if ANY instance of this rider has the matching UCI points
    const hasMatchingPoints = riderMatch?.some(r => r.uci_points === uciRider.uci_points);
    
    if (hasMatchingPoints) {
      synced.push(uciRider.rider_name);
    } else {
      notSynced.push({
        name: uciRider.rider_name,
        expectedPoints: uciRider.uci_points,
        allInstancesInRidersTable: riderMatch?.map(r => r.uci_points) || []
      });
    }
  }
  
  console.log(`✓ Synced: ${synced.length}`);
  console.log(`✗ Not Synced: ${notSynced.length}\n`);
  
  if (notSynced.length > 0) {
    console.log('NOT SYNCED RIDERS:');
    notSynced.slice(0, 20).forEach(r => {
      console.log(`\n  ${r.name}`);
      console.log(`    Expected: ${r.expectedPoints} pts`);
      console.log(`    Found in riders table: ${r.allInstancesInRidersTable.join(', ') || 'NONE'}`);
    });
  }
})();
