// scraper.js - Scrape PCS.com and insert into Supabase
const axios = require('axios');
const cheerio = require('cheerio');
const { createClient } = require('@supabase/supabase-js');
const http = require('http');
const https = require('https');

// Create HTTP agent with keep-alive and cookie persistence
const httpAgent = new http.Agent({ keepAlive: true });
const httpsAgent = new https.Agent({ keepAlive: true });

const axiosInstance = axios.create({
  httpAgent,
  httpsAgent,
  withCredentials: true,
});

// Simple cookie jar
let cookies = {};

// Intercept responses to capture Set-Cookie headers
axiosInstance.interceptors.response.use(
  response => {
    const setCookie = response.headers['set-cookie'];
    if (setCookie) {
      if (Array.isArray(setCookie)) {
        setCookie.forEach(c => {
          const match = c.match(/([^=]+)=([^;]+)/);
          if (match) cookies[match[1]] = match[2];
        });
      } else {
        const match = setCookie.match(/([^=]+)=([^;]+)/);
        if (match) cookies[match[1]] = match[2];
      }
    }
    return response;
  },
  error => Promise.reject(error)
);

// Supabase config – replace with your values
const SUPABASE_URL = 'https://zbvibhtopcsqrnecxgim.supabase.co';
const SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InpidmliaHRvcGNzcXJuZWN4Z2ltIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE5MjIyOTgsImV4cCI6MjA4NzQ5ODI5OH0.Z4aowdypHAGVHpIjyiHrJzJseXmmu91CEVbPNIc9cQs'; // Use service role for inserts
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

const PCS_BASE = 'https://www.procyclingstats.com';

// User-Agent rotation to avoid detection
const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
];

function getRandomUserAgent() {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

function getHeaders(referer = PCS_BASE) {
  const cookieStr = Object.entries(cookies)
    .map(([k, v]) => `${k}=${v}`)
    .join('; ');

  return {
    'User-Agent': getRandomUserAgent(),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Referer': referer,
    'DNT': '1',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
    ...(cookieStr && { 'Cookie': cookieStr }),
  };
}

// Retry logic with exponential backoff
async function fetchWithRetry(url, maxRetries = 3) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(`    Attempt ${attempt}/${maxRetries}...`);
      const res = await axiosInstance.get(url, {
        headers: getHeaders(url),
        timeout: 10000,
      });
      return res.data;
    } catch (err) {
      const status = err.response?.status;
      console.log(`    Status: ${status} - ${err.message}`);

      if (attempt === maxRetries) {
        throw err;
      }

      // Much longer backoff for 403: 10s, 20s, 30s
      const waitTime = status === 403 ? attempt * 10000 : Math.pow(2, attempt) * 1000;
      console.log(`    ⏳ Waiting ${waitTime / 1000}s...`);
      await sleep(waitTime);
    }
  }
}

// Sleep utility
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

/**
 * Scrape a single race and populate tables
 * Example: scrapeRace('tour-de-france', 2024)
 */
async function scrapeRace(raceSlug, year) {
  console.log(`\n📍 Scraping ${raceSlug} (${year})...`);

  const raceUrl = `${PCS_BASE}/race/${raceSlug}/${year}`;

  try {
    // Fetch race page
    const raceHtml = await fetchWithRetry(raceUrl);
    const $ = cheerio.load(raceHtml);

    // Extract race info
    const raceTitle = $('h1').first().text().trim() || raceSlug;
    console.log(`  Race: ${raceTitle}`);

    // Insert or upsert race into Supabase
    const { data: insertedRace, error: raceError } = await supabase
      .from('races')
      .upsert(
        [{ slug: raceSlug, name: raceTitle, year: year }],
        { onConflict: 'slug' }
      )
      .select();

    if (raceError) {
      console.error('  ❌ Error inserting race:', raceError);
      return;
    }
    const raceId = insertedRace[0].id;
    console.log(`  ✅ Race inserted/updated (ID: ${raceId})`);

    // Scrape stages
    await scrapeStages(raceId, raceSlug, year, $);

    // Scrape riders
    await scrapeRiders(raceId, raceSlug, year);

  } catch (err) {
    console.error(`  ❌ Error scraping ${raceSlug}:`, err.message);
  }
}

/**
 * Scrape stages from race page
 */
async function scrapeStages(raceId, raceSlug, year, $) {
  console.log('  🏔️  Scraping stages...');

  // Example: select stage links. Adjust selector to match PCS structure
  const stageLinks = $('a[href*="/race/' + raceSlug + '/stage"]');

  if (stageLinks.length === 0) {
    console.log('  ⚠️  No stages found, trying stages list page...');
    try {
      const stagesHtml = await fetchWithRetry(
        `${PCS_BASE}/race/${raceSlug}/${year}/stages`
      );
      const $stages = cheerio.load(stagesHtml);
      const links = $stages('a[href*="/stage/"]');
      await parseAndInsertStages(raceId, $stages, links, raceSlug);
    } catch (err) {
      console.error('    Error fetching stages page:', err.message);
    }
  } else {
    await parseAndInsertStages(raceId, $, stageLinks, raceSlug);
  }
}

async function parseAndInsertStages(raceId, $, stageLinks, raceSlug) {
  const stagesToInsert = [];
  let stageNum = 1;

  stageLinks.each((i, el) => {
    const href = $(el).attr('href');
    if (href && href.includes('/stage/')) {
      stagesToInsert.push({
        race_id: raceId,
        stage_number: stageNum,
        slug: `${raceSlug}-stage-${stageNum}`,
        url: href.startsWith('http') ? href : PCS_BASE + href,
      });
      stageNum++;
    }
  });

  if (stagesToInsert.length === 0) return;

  const { error } = await supabase
    .from('stages')
    .upsert(stagesToInsert, { onConflict: 'slug' });

  if (error) {
    console.error(`    ❌ Error inserting stages:`, error);
  } else {
    console.log(`    ✅ Inserted ${stagesToInsert.length} stages`);
  }

  // Scrape individual stage profiles
  for (const stage of stagesToInsert) {
    // Longer delay between stage fetches: 1-2 seconds
    await sleep(1000 + Math.random() * 1000);
    await scrapeStageProfile(raceId, stage);
  }
}

/**
 * Scrape a single stage profile (from /stage/{num})
 */
async function scrapeStageProfile(raceId, stage) {
  try {
    const html = await fetchWithRetry(stage.url);
    const $ = cheerio.load(html);

    const title = $('h1').first().text().trim() || `Stage ${stage.stage_number}`;
    
    // Try to find profile image (usually in main content area)
    let profileImageUrl = null;
    const imgTag = $('img[alt*="profile"], .profile-image img, .stage-profile img').first();
    if (imgTag.length) {
      profileImageUrl = $(imgTag).attr('src');
      if (profileImageUrl && profileImageUrl.startsWith('/')) {
        profileImageUrl = PCS_BASE + profileImageUrl;
      }
      if (profileImageUrl && profileImageUrl.startsWith('//')) {
        profileImageUrl = 'https:' + profileImageUrl;
      }
    }

    // Extract summary/description
    const summary = $('.race-info, .stage-detail, article').first().text().slice(0, 500).trim() || null;

    // Get stage ID from Supabase
    const { data: stageData } = await supabase
      .from('stages')
      .select('id')
      .eq('slug', stage.slug)
      .single();

    if (!stageData) {
      console.log(`    ⚠️  Stage ${stage.stage_number} not found in DB`);
      return;
    }

    const { error } = await supabase
      .from('stage_profiles')
      .upsert(
        [{
          stage_id: stageData.id,
          title,
          profile_image_url: profileImageUrl,
          summary,
          scraped_at: new Date().toISOString(),
        }],
        { onConflict: 'stage_id' }
      );

    if (error) {
      console.log(`    ❌ Error inserting profile for stage ${stage.stage_number}:`, error);
    } else {
      console.log(`    ✅ Stage ${stage.stage_number} profile scraped`);
    }
  } catch (err) {
    console.log(`    ⚠️  Error scraping stage profile:`, err.message);
  }
}

/**
 * Scrape riders for a race
 */
async function scrapeRiders(raceId, raceSlug, year) {
  console.log('  🚴 Scraping riders...');

  try {
    // Try common riders list pages
    const riderUrls = [
      `${PCS_BASE}/race/${raceSlug}/${year}/startlist`,
      `${PCS_BASE}/race/${raceSlug}/${year}/riders`,
      `${PCS_BASE}/race/${raceSlug}/${year}`,
    ];

    let riderHtml = null;
    for (const url of riderUrls) {
      try {
        riderHtml = await fetchWithRetry(url);
        break;
      } catch (e) {
        // Try next URL
      }
    }

    if (!riderHtml) {
      console.log('    ⚠️  Could not find riders page');
      return;
    }

    const $ = cheerio.load(riderHtml);

    // Example: extract from table rows or list. Adjust selector!
    const riderNames = [];
    $('tr td a, .rider-name a, tbody tr td:first-child').each((i, el) => {
      const name = $(el).text().trim();
      if (name && name.length > 2) {
        riderNames.push(name);
      }
    });

    if (riderNames.length === 0) {
      console.log('    ⚠️  No riders found on page');
      return;
    }

    const ridersToInsert = riderNames
      .filter((name, idx, arr) => arr.indexOf(name) === idx) // Remove duplicates
      .slice(0, 200) // Limit to 200 riders
      .map((name, idx) => ({
        race_id: raceId,
        name: name,
        bib_number: idx + 1,
      }));

    const { error } = await supabase
      .from('riders')
      .insert(ridersToInsert)
      .on('*', payload => {}); // Ignore duplicate key errors

    if (error && !error.message.includes('duplicate')) {
      console.log(`    ❌ Error inserting riders:`, error);
    } else {
      console.log(`    ✅ Inserted/found ${ridersToInsert.length} riders`);
    }
  } catch (err) {
    console.error('    ❌ Error scraping riders:', err.message);
  }
}

/**
 * Test if we can reach PCS at all
 */
async function testConnectivity() {
  console.log('🔍 Testing connectivity to PCS...');
  try {
    const res = await axiosInstance.get(PCS_BASE, {
      headers: getHeaders(),
      timeout: 10000,
    });
    console.log('✅ Connected! Status:', res.status);
    return true;
  } catch (err) {
    console.log('❌ Cannot reach PCS:', err.message);
    console.log('\n⚠️  Possible solutions:');
    console.log('  1. Try again later (site may have temp block)');
    console.log('  2. Use a VPN or proxy');
    console.log('  3. Try scraping individual races manually');
    return false;
  }
}

/**
 * Main: scrape multiple races
 */
async function main() {
  console.log('🚀 Starting PCS scraper...\n');

  // Test connectivity first
  const canConnect = await testConnectivity();
  if (!canConnect) return;

  console.log();

  const racesToScrape = [
    { slug: 'tour-de-france', year: 2024 },
    { slug: 'giro-ditalia', year: 2024 },
    { slug: 'vuelta-a-espana', year: 2024 },
  ];

  for (let i = 0; i < racesToScrape.length; i++) {
    const race = racesToScrape[i];
    await scrapeRace(race.slug, race.year);

    // Longer rate limit: 10-15 seconds between races
    if (i < racesToScrape.length - 1) {
      const delay = 10000 + Math.random() * 5000;
      console.log(`⏳ Waiting ${(delay / 1000).toFixed(1)}s before next race...\n`);
      await sleep(delay);
    }
  }

  console.log('\n✅ Scraping complete!');
}

// Run if executed directly
if (require.main === module) {
  main().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
  });
}

module.exports = { scrapeRace };
