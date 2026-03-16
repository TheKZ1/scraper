const axios = require('axios');
const cheerio = require('cheerio');
const { supabase } = require('./scraper-cycling-archives');

const TARGET_YEAR = Number(process.env.RESULTS_YEAR || String(new Date().getFullYear() - 1));
const DRY_RUN = String(process.env.DRY_RUN || '') === '1';
const RACE_WIDGET_COLUMN = `results_widget_url_${TARGET_YEAR}`;

const headers = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'Accept-Language': 'en-US,en;q=0.5',
  'Connection': 'keep-alive',
};

const firstCyclingCookie = process.env.FIRSTCYCLING_COOKIE;
if (firstCyclingCookie && firstCyclingCookie.trim()) {
  headers.Cookie = firstCyclingCookie.trim();
}

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

function normalizeName(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function appendQueryParam(url, key, value) {
  try {
    const parsed = new URL(url);
    parsed.searchParams.set(key, value);
    return parsed.toString();
  } catch (err) {
    return url;
  }
}

function uniqueUrls(urls) {
  const seen = new Set();
  const out = [];
  for (const url of urls) {
    if (!url) continue;
    const clean = String(url).trim();
    if (!clean || seen.has(clean)) continue;
    seen.add(clean);
    out.push(clean);
  }
  return out;
}

function extractRiderNameFromRow($row) {
  const riderLink = $row.find('a[href*="/rider/"], a[href*="rider.php"]').first();
  if (!riderLink.length) return null;
  return normalizeName(riderLink.text());
}

function extractFirstRiderFromTable(html) {
  const $ = cheerio.load(html);

  for (const table of $('table').toArray()) {
    const $table = $(table);
    const rows = $table.find('tbody tr').length ? $table.find('tbody tr') : $table.find('tr');

    for (const row of rows.toArray()) {
      const rider = extractRiderNameFromRow($(row));
      if (rider) {
        return rider;
      }
    }
  }

  return null;
}

function extractStageWinnerFromHtml(html) {
  return extractFirstRiderFromTable(html);
}

function mergeClassification(current, next) {
  return {
    raceWinner: current.raceWinner || next.raceWinner || null,
    gcWinner: current.gcWinner || next.gcWinner || null,
    pointsWinner: current.pointsWinner || next.pointsWinner || null,
    komWinner: current.komWinner || next.komWinner || null,
    youthWinner: current.youthWinner || next.youthWinner || null,
  };
}

function buildStageResultUrl(raceNumber, stageNumber) {
  return `https://firstcycling.com/race.php?r=${raceNumber}&y=${TARGET_YEAR}&s=${stageNumber}`;
}

function buildRaceBaseUrl(raceNumber) {
  return `https://firstcycling.com/race.php?r=${raceNumber}&y=${TARGET_YEAR}`;
}

function normalizeWidgetBaseUrl(url) {
  const raw = String(url || '').trim();
  if (!raw) return null;

  try {
    const parsed = new URL(raw);
    parsed.searchParams.set('y', String(TARGET_YEAR));
    parsed.searchParams.set('lang', 'EN');
    parsed.searchParams.delete('s');
    parsed.searchParams.delete('k');
    return parsed.toString();
  } catch (err) {
    return null;
  }
}

function buildStageWidgetUrlFromBase(widgetBaseUrl, stageNumber) {
  const normalized = normalizeWidgetBaseUrl(widgetBaseUrl);
  if (!normalized) return null;

  try {
    const parsed = new URL(normalized);
    parsed.searchParams.set('s', String(stageNumber));
    return parsed.toString();
  } catch (err) {
    return null;
  }
}

function buildWidgetUrl(raceNumber, options = {}) {
  const params = new URLSearchParams({
    r: String(raceNumber),
    y: String(TARGET_YEAR),
    lang: 'EN',
  });

  if (options.stageNumber !== undefined && options.stageNumber !== null) {
    params.set('s', String(options.stageNumber));
  }

  if (options.k !== undefined && options.k !== null) {
    params.set('k', String(options.k));
  }

  return `https://firstcycling.com/widget/?${params.toString()}`;
}

function buildCategoryUrls(raceNumber, category) {
  const base = buildRaceBaseUrl(raceNumber);
  if (category === 'gc') {
    return uniqueUrls([base, `${base}&k=1`]);
  }
  if (category === 'points') {
    return uniqueUrls([`${base}&k=2`, `${base}&k=5`, `${base}&k=1`]);
  }
  if (category === 'kom') {
    return uniqueUrls([`${base}&k=3`, `${base}&k=6`, `${base}&k=1`]);
  }
  if (category === 'youth') {
    return uniqueUrls([`${base}&k=4`, `${base}&k=7`, `${base}&k=1`]);
  }
  return [base];
}

function buildWidgetCategoryUrls(raceNumber, category) {
  if (category === 'gc') {
    return uniqueUrls([
      buildWidgetUrl(raceNumber),
      buildWidgetUrl(raceNumber, { k: 1 }),
    ]);
  }

  if (category === 'points') {
    return uniqueUrls([
      buildWidgetUrl(raceNumber, { k: 2 }),
      buildWidgetUrl(raceNumber, { k: 5 }),
      buildWidgetUrl(raceNumber),
    ]);
  }

  if (category === 'kom') {
    return uniqueUrls([
      buildWidgetUrl(raceNumber, { k: 3 }),
      buildWidgetUrl(raceNumber, { k: 6 }),
      buildWidgetUrl(raceNumber),
    ]);
  }

  if (category === 'youth') {
    return uniqueUrls([
      buildWidgetUrl(raceNumber, { k: 4 }),
      buildWidgetUrl(raceNumber, { k: 7 }),
      buildWidgetUrl(raceNumber),
    ]);
  }

  return [buildWidgetUrl(raceNumber)];
}

function extractRaceNumberFromUrl(url) {
  const raw = String(url || '').trim();
  if (!raw) return null;

  const queryMatch = raw.match(/[?&]r=(\d+)/i);
  if (queryMatch) return queryMatch[1];

  return null;
}

async function resolveRaceNumber(race, raceStages) {
  for (const stage of raceStages || []) {
    const raceNumber = extractRaceNumberFromUrl(stage.url);
    if (raceNumber) {
      return raceNumber;
    }
  }

  const slug = String(race.slug || '').trim();
  if (!slug) return null;

  const candidateUrls = uniqueUrls([
    `https://firstcycling.com/race/${slug}/${TARGET_YEAR}`,
    `https://www.firstcycling.com/race/${slug}/${TARGET_YEAR}`,
    race.year ? `https://firstcycling.com/race/${slug}/${race.year}` : null,
    race.year ? `https://www.firstcycling.com/race/${slug}/${race.year}` : null,
  ]);

  for (const url of candidateUrls) {
    try {
      const html = await fetchHtml(url);
      const match = html.match(/race\.php\?r=(\d+)&amp;y=(?:19|20)\d{2}/i)
        || html.match(/race\.php\?r=(\d+)&y=(?:19|20)\d{2}/i);
      if (match && match[1]) {
        return match[1];
      }
    } catch (err) {
    }
  }

  return null;
}

async function fetchHtml(url) {
  const response = await axios.get(url, { headers, timeout: 20000 });
  return response.data;
}

async function scrapeStageWinner(stage, raceNumber, widgetBaseUrl = null) {
  const preferredWidgetUrl = buildStageWidgetUrlFromBase(widgetBaseUrl, stage.stage_number);
  const widgetCandidates = uniqueUrls([
    preferredWidgetUrl,
    raceNumber ? buildWidgetUrl(raceNumber, { stageNumber: stage.stage_number }) : null,
  ]);

  for (const widgetUrl of widgetCandidates) {
    try {
      const widgetHtml = await fetchHtml(widgetUrl);
      const widgetWinner = extractStageWinnerFromHtml(widgetHtml);
      if (widgetWinner) {
        return { winner: widgetWinner, sourceUrl: widgetUrl };
      }
    } catch (err) {
    }
  }

  if (!raceNumber) {
    return { winner: null, sourceUrl: preferredWidgetUrl || null };
  }

  const url = buildStageResultUrl(raceNumber, stage.stage_number);
  try {
    const html = await fetchHtml(url);
    const winner = extractStageWinnerFromHtml(html);
    return { winner: winner || null, sourceUrl: url };
  } catch (err) {
    return { winner: null, sourceUrl: url };
  }
}

async function scrapeOneDayRaceWinner(raceNumber) {
  const url = buildRaceBaseUrl(raceNumber);
  try {
    const html = await fetchHtml(url);
    const winner = extractFirstRiderFromTable(html);
    return { winner: winner || null, sourceUrl: url };
  } catch (err) {
    return { winner: null, sourceUrl: url };
  }
}

async function scrapeCategoryWinner(raceNumber, category) {
  const widgetUrls = buildWidgetCategoryUrls(raceNumber, category);
  for (const url of widgetUrls) {
    try {
      const html = await fetchHtml(url);
      const rider = extractFirstRiderFromTable(html);
      if (rider) {
        return { winner: rider, sourceUrl: url };
      }
    } catch (err) {
    }
  }

  const urls = buildCategoryUrls(raceNumber, category);
  for (const url of urls) {
    try {
      const html = await fetchHtml(url);
      const rider = extractFirstRiderFromTable(html);
      if (rider) {
        return { winner: rider, sourceUrl: url };
      }
    } catch (err) {
    }
  }
  return { winner: null, sourceUrl: urls[0] || null };
}

async function scrapeRaceClassifications(raceNumber) {
  let collected = {
    raceWinner: null,
    gcWinner: null,
    pointsWinner: null,
    komWinner: null,
    youthWinner: null,
  };

  const gc = await scrapeCategoryWinner(raceNumber, 'gc');
  const points = await scrapeCategoryWinner(raceNumber, 'points');
  const kom = await scrapeCategoryWinner(raceNumber, 'kom');
  const youth = await scrapeCategoryWinner(raceNumber, 'youth');

  collected.gcWinner = gc.winner;
  collected.pointsWinner = points.winner;
  collected.komWinner = kom.winner;
  collected.youthWinner = youth.winner;
  collected.raceWinner = gc.winner;

  return {
    ...collected,
    sourceUrl: gc.sourceUrl || buildRaceBaseUrl(raceNumber),
  };
}

async function loadDatabaseRacesAndStages() {
  const { data: races, error: racesError } = await supabase
    .from('races')
    .select(`id, slug, name, year, Monument, firstcycling_race_number, ${RACE_WIDGET_COLUMN}`)
    .order('name', { ascending: true });

  if (racesError) throw racesError;

  const { data: stages, error: stagesError } = await supabase
    .from('stages')
    .select('id, race_id, stage_number, url, is_rest_day')
    .order('stage_number', { ascending: true });

  if (stagesError) throw stagesError;

  const stagesByRace = new Map();
  for (const stage of stages || []) {
    const key = String(stage.race_id);
    if (!stagesByRace.has(key)) {
      stagesByRace.set(key, []);
    }
    stagesByRace.get(key).push(stage);
  }

  return {
    races: races || [],
    stagesByRace,
  };
}

async function updateStageResult(stageId, winnerName) {
  if (DRY_RUN) return;

  const payload = {
    last_year_winner: winnerName || null,
    last_year_results_year: TARGET_YEAR,
    last_year_results_scraped_at: new Date().toISOString(),
  };

  const { error } = await supabase
    .from('stages')
    .update(payload)
    .eq('id', stageId);

  if (error) throw error;
}

async function updateRaceResults(raceId, values) {
  if (DRY_RUN) return;

  const payload = {
    last_year_winner: values.raceWinner || null,
    last_year_gc_winner: values.gcWinner || null,
    last_year_points_winner: values.pointsWinner || null,
    last_year_kom_winner: values.komWinner || null,
    last_year_youth_winner: values.youthWinner || null,
    last_year_results_year: TARGET_YEAR,
    last_year_results_scraped_at: new Date().toISOString(),
  };

  const { error } = await supabase
    .from('races')
    .update(payload)
    .eq('id', raceId);

  if (error) throw error;
}

async function main() {
  console.log(`🚀 Backfill last-year results started (target year: ${TARGET_YEAR})`);
  if (DRY_RUN) {
    console.log('🧪 DRY_RUN=1 enabled, no database writes will happen.');
  }

  const { races, stagesByRace } = await loadDatabaseRacesAndStages();

  if (!races.length) {
    console.log('⚠️ No races found in database.');
    return;
  }

  let updatedStages = 0;
  let updatedRaces = 0;

  for (let raceIndex = 0; raceIndex < races.length; raceIndex++) {
    const race = races[raceIndex];
    const raceStages = (stagesByRace.get(String(race.id)) || [])
      .slice()
      .sort((a, b) => Number(a.stage_number) - Number(b.stage_number));

    if (!raceStages.length) {
      console.log(`\n⏭️  [${raceIndex + 1}/${races.length}] ${race.name}: no stages in database, skipped`);
      continue;
    }

    const nonRestStages = raceStages.filter(stage => !stage.is_rest_day);
    const isOneDayRace = nonRestStages.length <= 1;

    console.log(`\n${'='.repeat(60)}`);
    console.log(`[${raceIndex + 1}/${races.length}] ${race.name}`);
    console.log(`Stages: ${raceStages.length} (non-rest: ${nonRestStages.length}) | one-day: ${isOneDayRace}`);
    console.log(`${'='.repeat(60)}`);

    const raceWidgetBaseUrl = normalizeWidgetBaseUrl(race[RACE_WIDGET_COLUMN]);
    let raceNumber = extractRaceNumberFromUrl(raceWidgetBaseUrl)
      || (race.firstcycling_race_number ? String(race.firstcycling_race_number) : null);

    if (!raceNumber) {
      raceNumber = await resolveRaceNumber(race, raceStages);
    }

    if (!raceNumber) {
      console.log('  ⚠️  Could not resolve FirstCycling race number (r=...), skipping');
      continue;
    }
    console.log(`  FirstCycling race number: ${raceNumber}`);
    if (raceWidgetBaseUrl) {
      console.log(`  Widget base from race column (${RACE_WIDGET_COLUMN}): ${raceWidgetBaseUrl}`);
    }

    let fallbackRaceWinner = null;

    for (const stage of nonRestStages) {
      const { winner, sourceUrl } = await scrapeStageWinner(stage, raceNumber, raceWidgetBaseUrl);
      await updateStageResult(stage.id, winner);

      if (winner) {
        updatedStages += 1;
      }
      if (!fallbackRaceWinner && winner) {
        fallbackRaceWinner = winner;
      }

      console.log(`  Stage ${stage.stage_number}: ${winner || 'not found'}${sourceUrl ? ` (${sourceUrl})` : ''}`);
      await sleep(450);
    }

    if (isOneDayRace) {
      const oneDay = await scrapeOneDayRaceWinner(raceNumber);
      const raceResults = {
        raceWinner: oneDay.winner || fallbackRaceWinner || null,
        gcWinner: null,
        pointsWinner: null,
        komWinner: null,
        youthWinner: null,
        sourceUrl: oneDay.sourceUrl || buildRaceBaseUrl(raceNumber),
      };

      await updateRaceResults(race.id, raceResults);
      updatedRaces += 1;

      const raceSummary = `winner=${raceResults.raceWinner || 'not found'}`;
      console.log(`  Race result (one-day): ${raceSummary}${raceResults.sourceUrl ? ` (${raceResults.sourceUrl})` : ''}`);
    } else {
      console.log('  Race result: skipped for stage race (this scraper only applies race-level last-year winner to one-day races)');
    }

    await sleep(700);
  }

  console.log('\n✅ Backfill finished.');
  console.log(`   Updated stage winners: ${updatedStages}`);
  console.log(`   Processed races: ${updatedRaces}`);
}

if (require.main === module) {
  main().catch(err => {
    console.error('❌ Backfill failed:', err.message || err);
    process.exit(1);
  });
}
