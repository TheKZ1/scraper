// scraper-update-uci-points.js - Daily UCI points updater
// Runs after rider updates to fetch UCI points for new riders only

const { createClient } = require('@supabase/supabase-js');
const { scrapeRiderUciPoints } = require('./scraper-cycling-archives');

const getEnvValue = (key, fallback) => {
  const value = process.env[key];
  return value && value.trim() ? value.trim() : fallback;
};

// Supabase config
const SUPABASE_URL = getEnvValue('SUPABASE_URL', 'https://zbvibhtopcsqrnecxgim.supabase.co');
const LEGACY_SUPABASE_SERVICE_ROLE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InpidmliaHRvcGNzcXJuZWN4Z2ltIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MTkyMjI5OCwiZXhwIjoyMDg3NDk4Mjk4fQ.jBvKDo_j0TvZ3FuQ2DpNAQmHXGABuyFcVhfjSc-G42w';
const SUPABASE_KEY = getEnvValue(
  'SUPABASE_SERVICE_ROLE_KEY',
  getEnvValue('SUPABASE_KEY', LEGACY_SUPABASE_SERVICE_ROLE_KEY)
);

if (!SUPABASE_KEY) {
  throw new Error('Missing SUPABASE_SERVICE_ROLE_KEY environment variable.');
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function main() {
  console.log('🔄 Starting daily UCI points update...\n');
  
  try {
    // Get all riders from the riders table (including those without rider_url from past races)
    console.log('📊 Fetching riders from database...');
    const { data: allRiders, error: ridersError } = await supabase
      .from('riders')
      .select('name, rider_url');
    
    if (ridersError) {
      console.error('❌ Error fetching riders:', ridersError);
      process.exit(1);
    }
    
    if (!allRiders || allRiders.length === 0) {
      console.log('✅ No riders to process');
      return;
    }
    
    console.log(`✅ Found ${allRiders.length} rider instances in database`);
    
    // Get unique riders by NAME (not URL)
    const uniqueRiders = [];
    const seenNames = new Set();
    
    for (const rider of allRiders) {
      if (rider.name && !seenNames.has(rider.name)) {
        seenNames.add(rider.name);
        uniqueRiders.push({ rider_name: rider.name, rider_url: rider.rider_url });
      }
    }
    
    console.log(`📋 Unique riders by name: ${uniqueRiders.length}\n`);
    
    // Check which riders already have UCI points in database (match by name)
    const { data: existingUci, error: existingError } = await supabase
      .from('rider_uci_database')
      .select('rider_name')
      .eq('year', 2025);
    
    const existingNames = new Set((existingUci || []).map(r => r.rider_name));
    
    // Filter to only riders not yet in UCI database
    const newRiders = uniqueRiders.filter(r => !existingNames.has(r.rider_name));
    
    if (newRiders.length === 0) {
      console.log('✅ All riders already in UCI database - skipping new scrapes\n');
    } else {
      console.log(`🆕 New riders to process: ${newRiders.length} (skipping ${existingNames.size} existing)\n`);
      
      let processed = 0;
      let successful = 0;
      let failed = 0;
      
      for (const rider of newRiders) {
        processed++;
        console.log(`[${processed}/${newRiders.length}] Processing: ${rider.rider_name}`);
        
        try {
          // Scrape UCI points
          const uciPoints = await scrapeRiderUciPoints(rider.rider_url);
          
          // Add delay to avoid overwhelming the server
          const delay = 500 + Math.random() * 500; // 500-1000ms
          await sleep(delay);
          
          // Insert into UCI database
          const { error: upsertError } = await supabase
            .from('rider_uci_database')
            .upsert({
              rider_name: rider.rider_name,
              rider_url: rider.rider_url,
              uci_points: uciPoints,
              year: 2025,
              last_updated: new Date().toISOString()
            }, {
              onConflict: 'rider_url'
            });
          
          if (upsertError) {
            console.log(`  ⚠️  Error saving to UCI database: ${upsertError.message}`);
            failed++;
          } else {
            // Update all riders in the riders table with this name
            const { error: updateRidersError } = await supabase
              .from('riders')
              .update({ uci_points: uciPoints })
              .eq('name', rider.rider_name);
            
            if (updateRidersError) {
              console.log(`  ⚠️  Error updating riders table: ${updateRidersError.message}`);
            }
            
            successful++;
          }
          
        } catch (error) {
          console.log(`  ❌ Error processing rider: ${error.message}`);
          failed++;
        }
        
        // Progress update every 5 riders
        if (processed % 5 === 0) {
          console.log(`\n📊 Progress: ${processed}/${newRiders.length} | Success: ${successful} | Failed: ${failed}\n`);
        }
      }
      
      console.log('\n' + '='.repeat(60));
      console.log('🏁 Daily UCI points update complete!');
      console.log('='.repeat(60));
      console.log(`New riders processed: ${processed}`);
      console.log(`✅ Successfully updated: ${successful}`);
      console.log(`❌ Failed: ${failed}`);
      console.log('='.repeat(60) + '\n');
    }
    
    // Sync UCI points from database to riders table (always run, even if no new riders)
    console.log('🔄 Syncing UCI points from database to riders table...');
    await syncUciPointsToRiders();
    
  } catch (error) {
    console.error('❌ Fatal error:', error);
    process.exit(1);
  }
}

async function syncUciPointsToRiders() {
  try {
    // Get all riders that have uci_points in rider_uci_database
    const { data: uciData, error: uciError } = await supabase
      .from('rider_uci_database')
      .select('rider_name, uci_points')
      .eq('year', 2025);
    
    if (uciError) {
      console.log('⚠️  Could not read UCI database:', uciError.message);
      return;
    }
    
    if (!uciData || uciData.length === 0) {
      console.log('✅ No UCI data to sync');
      return;
    }
    
    console.log(`📊 Found ${uciData.length} UCI records to sync`);
    
    let synced = 0;
    let failed = 0;
    
    for (const uciRecord of uciData) {
      // Check if this rider exists in riders table by name
      const { data: existingRiders, error: checkError } = await supabase
        .from('riders')
        .select('id')
        .eq('name', uciRecord.rider_name);
      
      if (checkError) {
        console.log(`  ⚠️  Error checking rider ${uciRecord.rider_name}: ${checkError.message}`);
        failed++;
        continue;
      }
      
      if (!existingRiders || existingRiders.length === 0) {
        // No matching riders found for this name
        continue;
      }
      
      // Update all riders with this name
      const { error: updateError } = await supabase
        .from('riders')
        .update({ uci_points: uciRecord.uci_points })
        .eq('name', uciRecord.rider_name);
      
      if (updateError) {
        console.log(`  ❌ Error updating ${uciRecord.rider_name}: ${updateError.message}`);
        failed++;
      } else {
        synced += existingRiders.length;
      }
    }
    
    console.log(`✅ Synced UCI points to ${synced} riders (${failed} failed)\n`);
    
  } catch (error) {
    console.log('⚠️  Error syncing UCI points:', error.message);
  }
}

main();
