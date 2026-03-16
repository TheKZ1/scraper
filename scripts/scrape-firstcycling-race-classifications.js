const axios = require('axios');
const cheerio = require('cheerio');
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = 'https://zbvibhtopcsqrnecxgim.supabase.co';
const SUPABASE_SERVICE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InpidmliaHRvcGNzcXJuZWN4Z2ltIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MTkyMjI5OCwiZXhwIjoyMDg3NDk4Mjk4fQ.jBvKDo_j0TvZ3FuQ2DpNAQmHXGABuyFcVhfjSc-G42w';
const SOURCE = 'firstcycling_widget';

const EXPECTED_TYPES = [
  'GC_WINNER',
  'POINTS_WINNER',
  'YOUTH_WINNER',
  'MOUNTAIN_WINNER',
  'LOWEST_GC_FINISHER'
];

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

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

function parseStageStartTimestamp(stage) {
  if (!stage || !stage.start_date) return null;
  const rawTime = String(stage.start_time || '').trim();
  let hhmm = '00:00';
  if (rawTime.includes('T')) {
    hhmm = rawTime.split('T')[1].slice(0, 5);
  } else if (rawTime.includes(' ')) {
    hhmm = rawTime.split(' ')[1].slice(0, 5);
  } else if (rawTime.length >= 5) {
    hhmm = rawTime.slice(0, 5);
  }
  const dt = new Date(`${stage.start_date}T${hhmm}:00`);
  return Number.isFinite(dt.getTime()) ? dt.getTime() : null;
}

async function getHtml(url) {
  const headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://firstcycling.com/'
  };

  let lastError = null;
  for (let attempt = 1; attempt <= 4; attempt += 1) {
    try {
      const response = await axios.get(url, {
        headers,
        timeout: 20000,
        validateStatus: (status) => status >= 200 && status < 500
      });
      if (response.status >= 400) {
        throw new Error(`HTTP ${response.status}`);
      }
      return String(response.data || '');
    } catch (err) {
      lastError = err;
      if (attempt < 4) {
        await sleep(600 * attempt);
      }
    }
  }

  throw lastError || new Error(`Failed to fetch ${url}`);
}

function extractRaceNumberFromUrl(value) {
  const match = String(value || '').match(/[?&]r=(\d+)/i);
  return match ? match[1] : '';
}

function makeWidgetUrl(raceNumber, year) {
  const qs = new URLSearchParams({ r: String(raceNumber), y: String(year), lang: 'EN' });
  return `https://firstcycling.com/widget/?${qs.toString()}`;
}

function makeFullRaceUrl(raceNumber, year) {
  const qs = new URLSearchParams({ r: String(raceNumber), y: String(year) });
  return `https://firstcycling.com/race.php?${qs.toString()}`;
}

function parseRowsFromTable($, tableEl) {
  const rows = [];
  $(tableEl).find('tr').each((_, tr) => {
    const tds = $(tr).find('td');
    if (!tds.length) return;

    const rankText = String($(tds[0]).text() || '').trim();
    const rankMatch = rankText.match(/^(\d{1,4})$/);
    if (!rankMatch) return;

    const riderLink = $(tr).find('a[href*="rider.php"], a[href*="/rider/"]').first();
    const riderName = String(riderLink.text() || '').replace(/\s+/g, ' ').trim();
    const value = String($(tds[tds.length - 1]).text() || '').replace(/\s+/g, ' ').trim();
    if (!riderName) return;

    rows.push({
      rank: Number(rankMatch[1]),
      rider_name: riderName,
      value,
    });
  });

  rows.sort((a, b) => a.rank - b.rank);
  return rows;
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
    if (rows.length >= 1) return rows;
  }

  return [];
}

function parseLargestRankingTableFromPage(html) {
  const $ = cheerio.load(html);
  const parsedTables = $('table').toArray()
    .map((tableEl) => parseRowsFromTable($, tableEl))
    .filter((rows) => rows.length >= 5)
    .sort((a, b) => b.length - a.length);

  return parsedTables[0] || [];
}

function buildRowsForRace(race, widgetUrl, byLabelRows, lowestGcFinisher, fullGcUrl) {
  const nowIso = new Date().toISOString();
  const typeMap = [
    { label: 'GC', result_type: 'GC_WINNER', category_label: 'GC' },
    { label: 'POINTS', result_type: 'POINTS_WINNER', category_label: 'Sprint' },
    { label: 'YOUTH', result_type: 'YOUTH_WINNER', category_label: 'Pogi Trui' },
    { label: 'MOUNTAIN', result_type: 'MOUNTAIN_WINNER', category_label: 'KOM' }
  ];

  const rows = [];
  typeMap.forEach((entry) => {
    const first = (byLabelRows[entry.label] || [])[0];
    if (!first || !first.rider_name) return;
    rows.push({
      race_id: race.id,
      result_type: entry.result_type,
      category_label: entry.category_label,
      rank: first.rank,
      rider_name: first.rider_name,
      value: first.value || null,
      source: SOURCE,
      source_url: widgetUrl,
      scraped_at: nowIso,
      updated_at: nowIso
    });
  });

  if (lowestGcFinisher && lowestGcFinisher.rider_name) {
    rows.push({
      race_id: race.id,
      result_type: 'LOWEST_GC_FINISHER',
      category_label: 'Rode Lantaarn',
      rank: lowestGcFinisher.rank,
      rider_name: lowestGcFinisher.rider_name,
      value: lowestGcFinisher.value || null,
      source: SOURCE,
      source_url: fullGcUrl,
      scraped_at: nowIso,
      updated_at: nowIso
    });
  }

  return rows;
}

async function loadFinishedStageRaces() {
  const [{ data: races, error: racesError }, { data: stages, error: stagesError }] = await Promise.all([
    supabase.from('races').select('id,name,slug,year,firstcycling_race_number'),
    supabase.from('stages').select('id,race_id,url,start_date,start_time,is_rest_day')
  ]);

  if (racesError) throw racesError;
  if (stagesError) throw stagesError;

  const stagesByRaceId = new Map();
  (stages || []).forEach((stage) => {
    const key = String(stage.race_id);
    if (!stagesByRaceId.has(key)) stagesByRaceId.set(key, []);
    stagesByRaceId.get(key).push(stage);
  });

  const nowTs = Date.now();
  const result = [];

  (races || []).forEach((race) => {
    const raceStages = (stagesByRaceId.get(String(race.id)) || []).filter((stage) => !stage.is_rest_day);
    if (raceStages.length <= 1) return;

    const maxStageStartTs = raceStages
      .map((stage) => parseStageStartTimestamp(stage))
      .filter((ts) => Number.isFinite(ts))
      .reduce((max, ts) => Math.max(max, ts), 0);

    if (!maxStageStartTs || maxStageStartTs > nowTs) return;

    let raceNumber = String(race.firstcycling_race_number || '').trim();
    if (!raceNumber) {
      for (const stage of raceStages) {
        const extracted = extractRaceNumberFromUrl(stage.url);
        if (extracted) {
          raceNumber = extracted;
          break;
        }
      }
    }

    if (!raceNumber) return;

    result.push({
      ...race,
      race_number: raceNumber,
      stage_count: raceStages.length
    });
  });

  return result;
}

async function loadExistingTypesByRace(raceIds) {
  if (!raceIds.length) return new Map();

  const { data, error } = await supabase
    .from('race_classification_results')
    .select('race_id,result_type')
    .eq('source', SOURCE)
    .in('race_id', raceIds);

  if (error) {
    if (String(error.message || '').toLowerCase().includes('relation') && String(error.message || '').toLowerCase().includes('does not exist')) {
      throw new Error('Table race_classification_results not found. Run database/create-race-classifications-results-table.sql first.');
    }
    throw error;
  }

  const map = new Map();
  (data || []).forEach((row) => {
    const key = String(row.race_id);
    if (!map.has(key)) map.set(key, new Set());
    map.get(key).add(String(row.result_type || ''));
  });
  return map;
}

async function saveRows(rows) {
  if (!rows.length) return 0;

  const { error } = await supabase
    .from('race_classification_results')
    .upsert(rows, { onConflict: 'race_id,result_type,source' });

  if (error) throw error;
  return rows.length;
}

async function run() {
  const limitArg = parseArg('limit', '0');
  const limit = Number(limitArg || 0);
  const onlyMissing = process.argv.includes('--only-missing');
  const raceSlugFilter = normalizeSlug(parseArg('race-slug', ''));

  const finishedRaces = await loadFinishedStageRaces();
  const filteredBySlug = raceSlugFilter
    ? finishedRaces.filter((race) => normalizeSlug(race.slug || race.name || '').includes(raceSlugFilter))
    : finishedRaces;

  const existingTypesByRace = await loadExistingTypesByRace(filteredBySlug.map((race) => race.id));

  const pending = filteredBySlug.filter((race) => {
    if (!onlyMissing) return true;
    const existing = existingTypesByRace.get(String(race.id));
    if (!existing) return true;
    return EXPECTED_TYPES.some((resultType) => !existing.has(resultType));
  });

  const targets = limit > 0 ? pending.slice(0, limit) : pending;

  console.log(`Found ${finishedRaces.length} finished stage race(s).`);
  if (raceSlugFilter) console.log(`Race filter enabled: ${raceSlugFilter}`);
  if (onlyMissing) console.log('Incremental mode enabled: skipping races that already have all classification results.');
  console.log(`Processing ${targets.length} race(s)${limit > 0 ? ` (limit=${limit})` : ''}...`);

  let processed = 0;
  let upsertedRows = 0;
  let skippedNoRows = 0;
  const failures = [];

  for (const race of targets) {
    processed += 1;
    const label = `${race.name} ${race.year}`;
    console.log(`\n[${processed}/${targets.length}] ${label} (race_id=${race.id}, r=${race.race_number})`);

    try {
      const widgetUrl = makeWidgetUrl(race.race_number, race.year || new Date().getFullYear());
      const widgetHtml = await getHtml(widgetUrl);

      const byLabelRows = {
        GC: parseClassificationFromWidgetPage(widgetHtml, 'GC'),
        POINTS: parseClassificationFromWidgetPage(widgetHtml, 'POINTS'),
        YOUTH: parseClassificationFromWidgetPage(widgetHtml, 'YOUTH'),
        MOUNTAIN: parseClassificationFromWidgetPage(widgetHtml, 'MOUNTAIN')
      };

      const fullGcUrl = makeFullRaceUrl(race.race_number, race.year || new Date().getFullYear());
      const fullGcHtml = await getHtml(fullGcUrl);
      const fullGcRows = parseLargestRankingTableFromPage(fullGcHtml);
      const gcFinishers = fullGcRows
        .filter((row) => Number.isFinite(Number(row.rank)))
        .filter((row) => !/\b(DNF|DNS)\b/i.test(String(row.value || '')));
      const lowestGcFinisher = gcFinishers.length ? gcFinishers[gcFinishers.length - 1] : null;

      const baseRows = buildRowsForRace(race, widgetUrl, byLabelRows, lowestGcFinisher, fullGcUrl);
      if (!baseRows.length) {
        skippedNoRows += 1;
        console.log('  No classification rows parsed.');
        continue;
      }

      const existingTypes = existingTypesByRace.get(String(race.id)) || new Set();
      const rowsToWrite = onlyMissing
        ? baseRows.filter((row) => !existingTypes.has(row.result_type))
        : baseRows;

      if (!rowsToWrite.length) {
        console.log('  All classification result types already present, nothing to save.');
        continue;
      }

      const wrote = await saveRows(rowsToWrite);
      upsertedRows += wrote;
      rowsToWrite.forEach((row) => existingTypes.add(row.result_type));
      existingTypesByRace.set(String(race.id), existingTypes);
      console.log(`  Saved ${wrote} classification row(s).`);
    } catch (err) {
      failures.push({ race_id: race.id, race: label, message: err.message || String(err) });
      console.warn(`  Failed: ${err.message || err}`);
    }

    await sleep(250);
  }

  console.log('\nClassification scrape complete.');
  console.log(`Races processed: ${processed}`);
  console.log(`Rows upserted: ${upsertedRows}`);
  console.log(`Races with no rows parsed: ${skippedNoRows}`);
  console.log(`Failures: ${failures.length}`);
  failures.slice(0, 20).forEach((item) => {
    console.log(`  - race_id=${item.race_id} (${item.race}): ${item.message}`);
  });
}

run().catch((err) => {
  console.error('Classification scrape failed:', err.message || err);
  process.exit(1);
});
