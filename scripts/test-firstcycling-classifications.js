const axios = require('axios');
const cheerio = require('cheerio');
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = 'https://zbvibhtopcsqrnecxgim.supabase.co';
const SUPABASE_SERVICE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InpidmliaHRvcGNzcXJuZWN4Z2ltIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MTkyMjI5OCwiZXhwIjoyMDg3NDk4Mjk4fQ.jBvKDo_j0TvZ3FuQ2DpNAQmHXGABuyFcVhfjSc-G42w';

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

function normalizeSlug(value) {
  return String(value || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9-]/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function parseArg(name, fallback = '') {
  const prefix = `--${name}=`;
  const hit = process.argv.find((arg) => arg.startsWith(prefix));
  return hit ? String(hit.slice(prefix.length)).trim() : fallback;
}

function hasFlag(name) {
  return process.argv.includes(`--${name}`);
}

async function getHtml(url) {
  const headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://firstcycling.com/'
  };

  let lastError = null;
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    try {
      const response = await axios.get(url, {
        headers,
        timeout: 20000,
        validateStatus: (status) => status >= 200 && status < 400
      });
      return String(response.data || '');
    } catch (err) {
      lastError = err;
      if (attempt < 3) await new Promise((resolve) => setTimeout(resolve, 600 * attempt));
    }
  }

  throw lastError || new Error(`Failed to fetch ${url}`);
}

function extractRaceNumberFromUrl(value) {
  const match = String(value || '').match(/[?&]r=(\d+)/i);
  return match ? match[1] : '';
}

async function resolveRaceNumber({ raceNumberArg, raceSlugArg, yearArg }) {
  if (raceNumberArg) return raceNumberArg;
  if (!raceSlugArg) return '';

  const slugNeedle = normalizeSlug(raceSlugArg);
  const year = Number(yearArg || 0);

  const { data: races, error: raceErr } = await supabase
    .from('races')
    .select('id,name,slug,year,firstcycling_race_number');

  if (raceErr) throw raceErr;

  const candidates = (races || []).filter((race) => {
    const slug = normalizeSlug(race.slug || race.name || '');
    if (!slug.includes(slugNeedle)) return false;
    if (year && Number(race.year || 0) !== year) return false;
    return true;
  });

  if (!candidates.length) return '';

  const target = candidates[0];
  if (target.firstcycling_race_number) return String(target.firstcycling_race_number);

  const { data: stages, error: stageErr } = await supabase
    .from('stages')
    .select('url')
    .eq('race_id', target.id)
    .limit(100);

  if (stageErr) throw stageErr;

  for (const stage of stages || []) {
    const raceNumber = extractRaceNumberFromUrl(stage.url);
    if (raceNumber) return raceNumber;
  }

  return '';
}

function makeWidgetUrl(raceNumber, year) {
  const qs = new URLSearchParams({ r: String(raceNumber), y: String(year), lang: 'EN' });
  return `https://firstcycling.com/widget/?${qs.toString()}`;
}

function parseRowsFromTable($, tableEl) {
  const rows = [];
  $(tableEl).find('tr').each((_, tr) => {
    const tds = $(tr).find('td');
    if (!tds.length) return;

    const rankText = String($(tds[0]).text() || '').trim();
    const rankMatch = rankText.match(/^(\d{1,3})$/);
    if (!rankMatch) return;

    const riderLink = $(tr).find('a[href*="rider.php"], a[href*="/rider/"]').first();
    const riderName = String(riderLink.text() || '').replace(/\s+/g, ' ').trim();
    const value = String($(tds[tds.length - 1]).text() || '').replace(/\s+/g, ' ').trim();

    if (!riderName) return;

    rows.push({
      rank: Number(rankMatch[1]),
      rider_name: riderName,
      value
    });
  });

  rows.sort((a, b) => a.rank - b.rank);
  return rows;
}

function getTableContextText($, tableEl) {
  const prevBits = $(tableEl)
    .prevAll('h1,h2,h3,h4,h5,strong,b,div')
    .slice(0, 6)
    .toArray()
    .map((el) => String($(el).text() || '').replace(/\s+/g, ' ').trim())
    .filter(Boolean)
    .reverse();

  const parentBits = $(tableEl)
    .parent()
    .find('h1,h2,h3,h4,h5,strong,b')
    .toArray()
    .map((el) => String($(el).text() || '').replace(/\s+/g, ' ').trim())
    .filter(Boolean);

  return `${prevBits.join(' ')} ${parentBits.join(' ')}`.toLowerCase();
}

function parseClassificationFromWidgetPage(html, label) {
  const $ = cheerio.load(html);
  const selectorsByLabel = {
    GC: ['#sta table', '#gc table', '#general table', '[id*="sta"] table'],
    POINTS: ['#point table', '#points table', '[id*="point"] table'],
    YOUTH: ['#youth table', '[id*="youth"] table', '[id*="young"] table'],
    MOUNTAIN: ['#mountain table', '[id*="mountain"] table', '[id*="kom"] table']
  };
  const selectors = selectorsByLabel[label] || selectorsByLabel.GC;

  for (const selector of selectors) {
    const table = $(selector).first();
    if (!table.length) continue;
    const rows = parseRowsFromTable($, table);
    if (rows.length >= 5) return rows;
  }

  // Fallback: if section selectors fail, pick the largest ranking table.
  const tables = $('table').toArray();
  const parsedTables = tables
    .map((tableEl) => parseRowsFromTable($, tableEl))
    .filter((rows) => rows.length >= 5)
    .sort((a, b) => b.length - a.length);

  return parsedTables[0] || [];
}

function parseLargestRankingTableFromPage(html) {
  const $ = cheerio.load(html);
  const parsedTables = $('table').toArray()
    .map((tableEl) => parseRowsFromTable($, tableEl))
    .filter((rows) => rows.length >= 5)
    .sort((a, b) => b.length - a.length);

  return parsedTables[0] || [];
}

function resolveFullGcResultsUrlFromWidget(widgetHtml, widgetUrl, raceNumber, yearArg) {
  const $ = cheerio.load(widgetHtml);
  const section = $('#sta').first();
  if (section.length) {
    const explicit = section.find('a[href]').toArray().map((a) => {
      const text = String($(a).text() || '').replace(/\s+/g, ' ').trim();
      const href = String($(a).attr('href') || '').trim();
      if (!href || !/complete\s+results/i.test(text)) return '';
      try {
        return new URL(href, widgetUrl).toString();
      } catch (err) {
        return '';
      }
    }).find(Boolean);
    if (explicit) return explicit;
  }

  const qs = new URLSearchParams({ r: String(raceNumber), y: String(yearArg) });
  return `https://firstcycling.com/race.php?${qs.toString()}`;
}

async function scrapeClassification(url, label) {
  const html = await getHtml(url);
  const rows = parseClassificationFromWidgetPage(html, label);
  return { label, url, rows };
}

async function run() {
  const raceNumberArg = parseArg('race-number', '');
  const raceSlugArg = parseArg('race-slug', '');
  const yearArg = parseArg('year', String(new Date().getFullYear()));
  const debug = hasFlag('debug');

  const raceNumber = await resolveRaceNumber({ raceNumberArg, raceSlugArg, yearArg });
  if (!raceNumber) {
    throw new Error('No FirstCycling race number found. Pass --race-number=<id> or ensure races.firstcycling_race_number is set.');
  }

  const baseUrl = makeWidgetUrl(raceNumber, yearArg);
  console.log(`Base widget URL: ${baseUrl}`);
  const baseWidgetHtml = await getHtml(baseUrl);

  const targets = [
    { label: 'GC', url: baseUrl },
    { label: 'POINTS', url: baseUrl },
    { label: 'YOUTH', url: baseUrl },
    { label: 'MOUNTAIN', url: baseUrl }
  ];

  if (!targets.length) {
    throw new Error('No GC/POINTS links found on FirstCycling race page.');
  }

  let gcRows = [];
  for (const target of targets) {
    const result = target.url === baseUrl
      ? { label: target.label, url: target.url, rows: parseClassificationFromWidgetPage(baseWidgetHtml, target.label) }
      : await scrapeClassification(target.url, target.label);
    if (target.label === 'GC') {
      gcRows = result.rows.slice();
    }
    console.log(`\n=== ${result.label} (${result.rows.length} rows) ===`);
    console.log(`URL: ${result.url}`);
    result.rows.slice(0, 10).forEach((row) => {
      console.log(`${String(row.rank).padStart(2, '0')} | ${row.rider_name} | ${row.value}`);
    });
    if (result.rows.length > 10) {
      console.log(`... (${result.rows.length - 10} more)`);
    }
  }

  const fullGcUrl = resolveFullGcResultsUrlFromWidget(baseWidgetHtml, baseUrl, raceNumber, yearArg);
  const fullGcHtml = await getHtml(fullGcUrl);
  const fullGcRows = parseLargestRankingTableFromPage(fullGcHtml);

  const gcFinishers = fullGcRows
    .filter((row) => row && Number.isFinite(Number(row.rank)))
    .filter((row) => !/\b(DNF|DNS)\b/i.test(String(row.value || '')));

  if (gcFinishers.length > 0) {
    const lastFinisher = gcFinishers[gcFinishers.length - 1];
    console.log('\n=== LOWEST GC FINISHER (NO DNF/DNS) ===');
    console.log(`Full GC URL: ${fullGcUrl}`);
    console.log(`Full GC rows parsed: ${fullGcRows.length}`);
    console.log(`${String(lastFinisher.rank).padStart(2, '0')} | ${lastFinisher.rider_name} | ${lastFinisher.value}`);
  } else {
    console.log('\n=== LOWEST GC FINISHER (NO DNF/DNS) ===');
    console.log(`Full GC URL: ${fullGcUrl}`);
    console.log(`Full GC rows parsed: ${fullGcRows.length}`);
    console.log('No GC finisher rows found.');
  }
}

run().catch((err) => {
  console.error('FirstCycling classification test failed:', err.message || err);
  process.exit(1);
});
