// scraper-uci-points-oneoff.js - One-time scraper to build UCI points database
// Run this script once to populate the rider_uci_database table
// Usage: npm run scrape:uci:oneoff

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
  console.log('🏁 Starting one-off UCI points scraper...\n');
  
  try {
    // Get all riders from database
    console.log('📊 Fetching all riders from database...');
    
    const { data: allRiders, error: ridersError } = await supabase
      .from('riders')
      .select('name, rider_url')
      .not('rider_url', 'is', null);
    
    if (ridersError) {
      console.error('❌ Error fetching riders:', ridersError);
      process.exit(1);
    }
    
    if (!allRiders || allRiders.length === 0) {
      console.log('❌ No riders with URLs found in database.');
      console.log('   Run: npm run scrape:riders');
      console.log('   This will populate the rider_url field.');
      process.exit(1);
    }
    
    console.log(`✅ Found ${allRiders.length} riders in database`);
    
    // Get unique riders by URL
    console.log('🔍 Extracting unique riders by URL...');
    const uniqueRiders = [];
    const seenUrls = new Set();
    
    for (const rider of allRiders) {
      if (rider.rider_url && !seenUrls.has(rider.rider_url)) {
        seenUrls.add(rider.rider_url);
        uniqueRiders.push({ rider_name: rider.name, rider_url: rider.rider_url });
      }
    }
    
    console.log(`✅ Found ${uniqueRiders.length} unique riders\n`);
    
    let processed = 0;
    let successful = 0;
    let failed = 0;
    let skipped = 0;
    
    for (const rider of uniqueRiders) {
      processed++;
      console.log(`[${processed}/${uniqueRiders.length}] Processing: ${rider.rider_name}`);
      
      try {
        // Check if rider already exists in UCI database
        const { data: existing, error: checkError } = await supabase
          .from('rider_uci_database')
          .select('id, uci_points')
          .eq('rider_url', rider.rider_url)
          .eq('year', 2025)
          .single();
        
        if (existing && !checkError) {
          console.log(`  ⏭️  Already in database (${existing.uci_points || 'N/A'} pts) - skipping`);
          skipped++;
          continue;
        }
        
        // Scrape UCI points
        console.log(`  🔍 Scraping UCI points...`);
        const uciPoints = await scrapeRiderUciPoints(rider.rider_url);
        
        // Add delay to avoid overwhelming the server
        const delay = 500 + Math.random() * 500; // 500-1000ms
        await sleep(delay);
        
        // Insert/update in UCI database
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
          // Also update riders in the riders table with this URL, but only if they don't have UCI points yet
          const { error: updateRidersError } = await supabase
            .from('riders')
            .update({ uci_points: uciPoints })
            .eq('rider_url', rider.rider_url)
            .is('uci_points', null);
          
          if (updateRidersError) {
            console.log(`  ⚠️  Error updating riders table: ${updateRidersError.message}`);
          }
          
          console.log(`  ✅ Saved: ${uciPoints !== null ? uciPoints + ' pts' : 'No points found'}`);
          successful++;
        }
        
      } catch (error) {
        console.log(`  ❌ Error processing rider: ${error.message}`);
        failed++;
      }
      
      // Progress update every 10 riders
      if (processed % 10 === 0) {
        console.log(`\n📊 Progress: ${processed}/${uniqueRiders.length} | Success: ${successful} | Failed: ${failed} | Skipped: ${skipped}\n`);
      }
    }
    
    console.log('\n' + '='.repeat(60));
    console.log('🏁 One-off UCI scraping complete!');
    console.log('='.repeat(60));
    console.log(`Total riders processed: ${processed}`);
    console.log(`✅ Successfully scraped: ${successful}`);
    console.log(`⏭️  Skipped (already exists): ${skipped}`);
    console.log(`❌ Failed: ${failed}`);
    console.log('='.repeat(60) + '\n');
    
  } catch (error) {
    console.error('❌ Fatal error:', error);
    process.exit(1);
  }
}

main();
