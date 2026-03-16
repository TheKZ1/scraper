const axios = require('axios');
const cheerio = require('cheerio');
const { supabase } = require('./scraper-cycling-archives');

const TARGET_YEAR = Number(process.env.RESULTS_YEAR || String(new Date().getFullYear()));
const DRY_RUN = String(process.env.DRY_RUN || '') === '1';
const FORCE = String(process.env.FORCE || '') === '1';
const RACE_FILTER = String(process.env.RESULTS_RACE_FILTER || '').trim().toLowerCase();
const RACE_WIDGET_COLUMN = `results_widget_url_${TARGET_YEAR}`;

const getEnvValue = (key, fallback = '') => {
  const value = process.env[key];
  return value && value.trim() ? value.trim() : fallback;
};

const headers = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'Accept-Language': 'en-US,en;q=0.5',
  'Connection': 'keep-alive',
};

const firstCyclingProxyUrl = getEnvValue(
  'FIRSTCYCLING_PROXY_URL',
  getEnvValue('HTTPS_PROXY', getEnvValue('https_proxy', ''))
);

const parseAxiosProxy = (proxyUrl) => {
  const raw = String(proxyUrl || '').trim();
  if (!raw) {
    return null;
  }

  try {
    const parsed = new URL(raw);
    const protocol = parsed.protocol.replace(':', '');
    const port = parsed.port
      ? Number(parsed.port)
      : (protocol === 'https' ? 443 : 80);

    const proxy = {
      protocol,
      host: parsed.hostname,
      port,
    };

    if (parsed.username) {
      proxy.auth = {
        username: decodeURIComponent(parsed.username),
        password: decodeURIComponent(parsed.password || ''),
      };
    }

    return proxy;
  } catch (err) {
    console.warn(`⚠️  Invalid FIRSTCYCLING_PROXY_URL: ${err.message}`);
    return null;
  }
};

const firstCyclingAxiosProxy = parseAxiosProxy(firstCyclingProxyUrl);

const buildRequestOptions = (timeout) => {
  const options = { headers, timeout };
  if (firstCyclingAxiosProxy) {
    options.proxy = firstCyclingAxiosProxy;
  }
  return options;
};

const firstCyclingCookie = process.env.FIRSTCYCLING_COOKIE;
if (firstCyclingCookie && firstCyclingCookie.trim()) {
  headers.Cookie = firstCyclingCookie.trim();
}

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

function normalizeName(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function normalizeCompareName(value) {
  return normalizeName(value)
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase();
}

function extractRiderNameFromRow($row) {
  const riderLink = $row.find('a[href*="/rider/"], a[href*="rider.php"]').first();
  if (!riderLink.length) return null;
  return normalizeName(riderLink.text());
}

function extractTeamNameFromRow($row) {
  const teamLink = $row.find('a[href*="/team/"], a[href*="team.php"]').first();
  if (!teamLink.length) return null;
  return normalizeName(teamLink.text());
}

function isRankedResultRow($row) {
  const firstCellText = normalizeName($row.find('td').first().text()).replace(/\.$/, '');
  return /^\d{1,3}$/.test(firstCellText);
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

function extractFirstTeamFromTable(html) {
  const $ = cheerio.load(html);

  for (const table of $('table').toArray()) {
    const $table = $(table);
    const rows = $table.find('tbody tr').length ? $table.find('tbody tr') : $table.find('tr');

    for (const row of rows.toArray()) {
      const $row = $(row);
      if (!isRankedResultRow($row)) continue;

      const team = extractTeamNameFromRow($row);
      if (team) {
        return team;
      }
    }
  }

  return null;
}

function isTeamResultTable(html) {
  const $ = cheerio.load(html);
  for (const table of $('table').toArray()) {
    const $table = $(table);
    const rows = $table.find('tbody tr').length ? $table.find('tbody tr') : $table.find('tr');
    for (const row of rows.toArray()) {
      const $row = $(row);
      if (!isRankedResultRow($row)) continue;
      const hasTeam = Boolean(extractTeamNameFromRow($row));
      const hasRider = Boolean(extractRiderNameFromRow($row));
      if (hasTeam && !hasRider) {
        return true;
      }
      return false;
    }
  }
  return false;
}

function getTodayUtcDateOnly() {
  const now = new Date();
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
}

function parseDateOnlyUtc(dateStr) {
  if (!dateStr) return null;
  const match = String(dateStr).match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;
  return new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])));
}

function hasStageStarted(stage, todayUtc) {
  const stageDate = parseDateOnlyUtc(stage.start_date);
  if (!stageDate) return false;
  return stageDate <= todayUtc;
}

function hasRaceStarted(stages, todayUtc) {
  return stages.some(stage => hasStageStarted(stage, todayUtc));
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

function extractRaceNumberFromUrl(url) {
  const raw = String(url || '').trim();
  if (!raw) return null;
  const queryMatch = raw.match(/[?&]r=(\d+)/i);
  return queryMatch ? queryMatch[1] : null;
}

function buildWidgetUrl(raceNumber, stageNumber) {
  const params = new URLSearchParams({
    r: String(raceNumber),
    y: String(TARGET_YEAR),
    lang: 'EN',
    s: String(stageNumber),
  });
  return `https://firstcycling.com/widget/?${params.toString()}`;
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

async function detectStageNumberOffset(nonRestStages, raceNumber, widgetBaseUrl) {
  if (!Array.isArray(nonRestStages) || nonRestStages.length < 2) {
    return 0;
  }

  const sorted = nonRestStages
    .slice()
    .sort((a, b) => Number(a.stage_number) - Number(b.stage_number));

  const firstStage = sorted[0];
  if (!firstStage || Number(firstStage.stage_number) !== 1) {
    return 0;
  }

  const stageZeroCandidates = [
    buildStageWidgetUrlFromBase(widgetBaseUrl, 0),
    raceNumber ? buildWidgetUrl(raceNumber, 0) : null,
  ].filter(Boolean);

  for (const url of stageZeroCandidates) {
    try {
      const html = await fetchHtml(url);
      const text = String(html || '').toLowerCase();
      if (text.includes('prologue')) {
        return -1;
      }
    } catch (err) {
    }
  }

  return 0;
}

function buildRaceBaseUrl(raceNumber) {
  return `https://firstcycling.com/race.php?r=${raceNumber}&y=${TARGET_YEAR}`;
}

async function fetchHtml(url) {
  const response = await axios.get(url, buildRequestOptions(20000));
  return response.data;
}

async function scrapeStageWinner(stage, raceNumber, widgetBaseUrl, scrapedStageNumber) {
  const preferredWidgetUrl = buildStageWidgetUrlFromBase(widgetBaseUrl, scrapedStageNumber);
  const widgetCandidates = [
    preferredWidgetUrl,
    raceNumber ? buildWidgetUrl(raceNumber, scrapedStageNumber) : null,
  ].filter(Boolean);

  for (const widgetUrl of widgetCandidates) {
    try {
      const widgetHtml = await fetchHtml(widgetUrl);
      const winner = isTeamResultTable(widgetHtml)
        ? extractFirstTeamFromTable(widgetHtml)
        : extractFirstRiderFromTable(widgetHtml);
      if (winner) {
        return { winner, sourceUrl: widgetUrl };
      }
    } catch (err) {
    }
  }

  return { winner: null, sourceUrl: preferredWidgetUrl || null };
}

async function scrapeOneDayRaceWinner(raceNumber) {
  if (!raceNumber) return { winner: null, sourceUrl: null };

  const url = buildRaceBaseUrl(raceNumber);
  try {
    const html = await fetchHtml(url);
    const winner = extractFirstRiderFromTable(html);
    return { winner: winner || null, sourceUrl: url };
  } catch (err) {
    return { winner: null, sourceUrl: url };
  }
}

async function loadRacesAndStages() {
  const { data: races, error: racesError } = await supabase
    .from('races')
    .select(`id, name, year, firstcycling_race_number, full_results, ${RACE_WIDGET_COLUMN}`)
    .eq('year', TARGET_YEAR)
    .order('name', { ascending: true });

  if (racesError) throw racesError;

  const { data: stages, error: stagesError } = await supabase
    .from('stages')
    .select('id, race_id, stage_number, start_date, is_rest_day, winner')
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

async function updateStageWinner(stageId, winnerName) {
  if (DRY_RUN) return;

  const payload = {
    winner: winnerName || null,
    results_year: TARGET_YEAR,
    results_scraped_at: new Date().toISOString(),
  };

  const { error } = await supabase
    .from('stages')
    .update(payload)
    .eq('id', stageId);

  if (error) throw error;
}

async function updateRaceResult(raceId, winnerName, fullResults) {
  if (DRY_RUN) return;

  const payload = {
    winner: winnerName || null,
    full_results: Boolean(fullResults),
    results_year: TARGET_YEAR,
    results_scraped_at: new Date().toISOString(),
  };

  const { error } = await supabase
    .from('races')
    .update(payload)
    .eq('id', raceId);

  if (error) throw error;
}

async function main() {
  console.log(`🚀 Daily current-year results scraper started (target year: ${TARGET_YEAR})`);
  if (DRY_RUN) {
    console.log('🧪 DRY_RUN=1 enabled, no database writes will happen.');
  }
  if (FORCE) {
    console.log('⚠️ FORCE=1 enabled: races with full_results=true will also be processed.');
  }

  const { races, stagesByRace } = await loadRacesAndStages();
  const todayUtc = getTodayUtcDateOnly();

  if (!races.length) {
    console.log('⚠️ No races found for target year.');
    return;
  }

  let updatedStages = 0;
  let updatedRaces = 0;

  for (let raceIndex = 0; raceIndex < races.length; raceIndex++) {
    const race = races[raceIndex];

    if (RACE_FILTER && !String(race.name || '').toLowerCase().includes(RACE_FILTER)) {
      continue;
    }

    const raceStages = (stagesByRace.get(String(race.id)) || [])
      .slice()
      .sort((a, b) => Number(a.stage_number) - Number(b.stage_number));

    if (!raceStages.length) {
      continue;
    }

    if (!FORCE && race.full_results) {
      console.log(`\n⏭️  [${raceIndex + 1}/${races.length}] ${race.name}: full_results=true, skipped`);
      continue;
    }

    if (!hasRaceStarted(raceStages, todayUtc)) {
      console.log(`\n⏭️  [${raceIndex + 1}/${races.length}] ${race.name}: race has not started yet, skipped`);
      continue;
    }

    const nonRestStages = raceStages.filter(stage => !stage.is_rest_day);
    const startedStages = nonRestStages.filter(stage => hasStageStarted(stage, todayUtc));
    const isOneDayRace = nonRestStages.length <= 1;

    if (!startedStages.length) {
      console.log(`\n⏭️  [${raceIndex + 1}/${races.length}] ${race.name}: no started stages yet, skipped`);
      continue;
    }

    console.log(`\n${'='.repeat(60)}`);
    console.log(`[${raceIndex + 1}/${races.length}] ${race.name}`);
    console.log(`Stages: ${nonRestStages.length} | started stages: ${startedStages.length} | one-day: ${isOneDayRace}`);
    console.log(`${'='.repeat(60)}`);

    const raceWidgetBaseUrl = normalizeWidgetBaseUrl(race[RACE_WIDGET_COLUMN]);
    const raceNumber = extractRaceNumberFromUrl(raceWidgetBaseUrl)
      || (race.firstcycling_race_number ? String(race.firstcycling_race_number) : null);

    if (!raceNumber) {
      console.log('  ⚠️  Could not resolve FirstCycling race number, skipping race');
      continue;
    }

    const stageNumberOffset = await detectStageNumberOffset(nonRestStages, raceNumber, raceWidgetBaseUrl);
    if (stageNumberOffset !== 0) {
      console.log('  Prologue mapping detected: stage 0 -> stage 1 (offset -1)');
    }

    let fallbackRaceWinner = null;
    for (const stage of startedStages) {
      const scrapedStageNumber = Math.max(0, Number(stage.stage_number) + stageNumberOffset);
      const { winner, sourceUrl } = await scrapeStageWinner(stage, raceNumber, raceWidgetBaseUrl, scrapedStageNumber);
      const existingWinner = normalizeCompareName(stage.winner);
      const scrapedWinner = normalizeCompareName(winner);

      if (winner && existingWinner !== scrapedWinner) {
        await updateStageWinner(stage.id, winner);
        updatedStages += 1;
      }

      if (!fallbackRaceWinner && winner) {
        fallbackRaceWinner = winner;
      }

      const mappingLabel = scrapedStageNumber !== Number(stage.stage_number)
        ? ` [from s=${scrapedStageNumber}]`
        : '';
      console.log(`  Stage ${stage.stage_number}${mappingLabel}: ${winner || 'not found'}${sourceUrl ? ` (${sourceUrl})` : ''}`);
      await sleep(250);
    }

    const allStageWinnersKnown = nonRestStages.length > 0
      && nonRestStages.every(stage => {
        const started = hasStageStarted(stage, todayUtc);
        if (!started) return false;
        const alreadyWinner = startedStages.find(s => s.id === stage.id)?.winner;
        return normalizeName(alreadyWinner || stage.winner);
      });

    if (isOneDayRace) {
      const oneDay = await scrapeOneDayRaceWinner(raceNumber);
      const raceWinner = oneDay.winner || fallbackRaceWinner || null;
      await updateRaceResult(race.id, raceWinner, Boolean(raceWinner));
      updatedRaces += 1;
      console.log(`  Race result (one-day): ${raceWinner || 'not found'}${oneDay.sourceUrl ? ` (${oneDay.sourceUrl})` : ''}`);
    } else {
      await updateRaceResult(race.id, race.winner || null, allStageWinnersKnown);
      updatedRaces += 1;
      console.log(`  Race result: stage race, winner skipped | full_results=${allStageWinnersKnown}`);
    }

    await sleep(400);
  }

  console.log('\n✅ Daily current-year results scraping finished.');
  console.log(`   Updated stages: ${updatedStages}`);
  console.log(`   Updated races: ${updatedRaces}`);
}

if (require.main === module) {
  main().catch(err => {
    console.error('❌ Daily results scraper failed:', err.message || err);
    process.exit(1);
  });
}
