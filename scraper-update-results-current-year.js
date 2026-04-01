const axios = require('axios');
const cheerio = require('cheerio');
const { supabase, resolvePcsRaceSlug } = require('./scraper-cycling-archives');

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

const buildRequestOptions = (timeout, config = {}) => {
  const options = { headers, timeout };
  if (firstCyclingAxiosProxy && !config.disableProxy) {
    options.proxy = firstCyclingAxiosProxy;
  }
  return options;
};

const toggleFirstCyclingHost = (url) => {
  const input = String(url || '').trim();
  if (!input.includes('firstcycling.com')) {
    return input;
  }

  if (input.includes('://www.firstcycling.com')) {
    return input.replace('://www.firstcycling.com', '://firstcycling.com');
  }

  if (input.includes('://firstcycling.com')) {
    return input.replace('://firstcycling.com', '://www.firstcycling.com');
  }

  return input;
};

const firstCyclingCookie = 'KU819ojPaRYw95HZeHl_48sR2WrDdhQe_mS4tw7fl0s-1775023953-1.2.1.1-36bEbSpg_6Fcp0Kf0raEA2A8oTiv9GaKyISBdy.1wXMFZ2PkIR3iGVSz4UnM37QY5G9HSiWXhWTu5av9O69TskqjbRjwW3t_HyWitIjWUJ1iQSEbYkgqwN7i5jySZQb6lF4fnOjIDhR0lj0fMzNhqlGaTXmFEZ8XtRbVlaIo_MEZDCaZYRNjPkMpGv0n1lAVuVFc6J8_kr7Pv0.xldKKHEbkpKIH0mWshi2OnDSXtimtcopQdd0F0x5gEJ0ggJyl';
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
  for (const url of urls || []) {
    const clean = String(url || '').trim();
    if (!clean || seen.has(clean)) continue;
    seen.add(clean);
    out.push(clean);
  }
  return out;
}

async function fetchHtml(url) {
  try {
    const response = await axios.get(url, buildRequestOptions(20000));
    return response.data;
  } catch (err) {
    const status = Number(err && err.response && err.response.status);
    const isFirstCycling = String(url || '').includes('firstcycling.com');
    if (status !== 403 || !isFirstCycling) {
      throw err;
    }

    const hostVariantUrl = toggleFirstCyclingHost(url);
    const fallbackCandidates = [url, hostVariantUrl]
      .map((value) => String(value || '').trim())
      .filter((value, idx, arr) => value && arr.indexOf(value) === idx);

    for (const candidateUrl of fallbackCandidates) {
      try {
        const fallbackResponse = await axios.get(candidateUrl, buildRequestOptions(20000, { disableProxy: true }));
        if (candidateUrl !== url) {
          console.log(`  ℹ️  FirstCycling 403 recovered via host fallback: ${candidateUrl}`);
        } else {
          console.log('  ℹ️  FirstCycling 403 recovered by bypassing configured proxy.');
        }
        return fallbackResponse.data;
      } catch (fallbackErr) {
        // Try next fallback candidate.
      }
    }

    throw err;
  }
}

function extractLastRiderFromTable(html) {
  const $ = cheerio.load(html);

  for (const table of $('table').toArray()) {
    const $table = $(table);
    const rows = $table.find('tbody tr').length ? $table.find('tbody tr') : $table.find('tr');
    const rankedRiders = [];

    for (const row of rows.toArray()) {
      const $row = $(row);
      if (!isRankedResultRow($row)) continue;
      const rider = extractRiderNameFromRow($row);
      if (rider) rankedRiders.push(rider);
    }

    if (rankedRiders.length > 0) {
      return rankedRiders[rankedRiders.length - 1];
    }
  }

  return null;
}

function extractRankedRidersByTable(html) {
  const $ = cheerio.load(html);
  const rankedTables = [];

  for (const table of $('table').toArray()) {
    const $table = $(table);
    const rows = $table.find('tbody tr').length ? $table.find('tbody tr') : $table.find('tr');
    const riders = [];

    for (const row of rows.toArray()) {
      const $row = $(row);
      if (!isRankedResultRow($row)) continue;
      const rider = extractRiderNameFromRow($row);
      if (rider) riders.push(rider);
    }

    if (riders.length > 0) {
      rankedTables.push(riders);
    }
  }

  return rankedTables;
}

const pcsHeaders = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'Accept-Language': 'en-US,en;q=0.5',
  'Connection': 'keep-alive',
};

async function fetchPcsHtml(url) {
  try {
    const response = await axios.get(url, {
      headers: pcsHeaders,
      timeout: 15000,
    });
    return response.data;
  } catch (err) {
    throw err;
  }
}

function extractRiderNameFromPcsRow($row) {
  const riderLink = $row.find('a[href*="/rider/"]').first();
  if (!riderLink.length) return null;
  return normalizeName(riderLink.text());
}

function extractRankedRidersFromPcsTable(html) {
  const $ = cheerio.load(html);
  const riders = [];

  for (const table of $('table').toArray()) {
    const $table = $(table);
    const rows = $table.find('tbody tr').length ? $table.find('tbody tr') : $table.find('tr');

    for (const row of rows.toArray()) {
      const $row = $(row);
      if (!isRankedResultRow($row)) continue;
      const rider = extractRiderNameFromPcsRow($row);
      if (rider) riders.push(rider);
    }

    if (riders.length > 0) break; // Use first table with ranked riders
  }

  return riders;
}

async function scrapePcsRaceClassificationResults(race, isOneDayRace) {
  if (!race || !race.id) {
    return {
      GC_WINNER: null,
      POINTS_WINNER: null,
      MOUNTAIN_WINNER: null,
      YOUTH_WINNER: null,
      LOWEST_GC_FINISHER: null,
      sourceUrls: {
        GC_WINNER: null,
        POINTS_WINNER: null,
        MOUNTAIN_WINNER: null,
        YOUTH_WINNER: null,
        LOWEST_GC_FINISHER: null,
      }
    };
  }

  const slug = await resolvePcsRaceSlug(race);
  const year = Number(race.year || TARGET_YEAR);
  if (!slug || !Number.isFinite(year)) {
    return {
      GC_WINNER: null,
      POINTS_WINNER: null,
      MOUNTAIN_WINNER: null,
      YOUTH_WINNER: null,
      LOWEST_GC_FINISHER: null,
      sourceUrls: {
        GC_WINNER: null,
        POINTS_WINNER: null,
        MOUNTAIN_WINNER: null,
        YOUTH_WINNER: null,
        LOWEST_GC_FINISHER: null,
      }
    };
  }

  const baseUrl = `https://www.procyclingstats.com/race/${slug}/${year}`;
  const resultUrls = [
    `${baseUrl}/results`,
    `${baseUrl}/results/gc`, // GC results
  ];

  const results = {
    GC_WINNER: null,
    POINTS_WINNER: null,
    MOUNTAIN_WINNER: null,
    YOUTH_WINNER: null,
    LOWEST_GC_FINISHER: null,
    sourceUrls: {
      GC_WINNER: null,
      POINTS_WINNER: null,
      MOUNTAIN_WINNER: null,
      YOUTH_WINNER: null,
      LOWEST_GC_FINISHER: null,
    }
  };

  let gcRiders = [];
  for (const url of resultUrls) {
    try {
      const html = await fetchPcsHtml(url);
      gcRiders = extractRankedRidersFromPcsTable(html);
      if (gcRiders.length > 0) {
        results.GC_WINNER = gcRiders[0] || null;
        results.LOWEST_GC_FINISHER = gcRiders.length > 0 ? gcRiders[gcRiders.length - 1] : null;
        results.sourceUrls.GC_WINNER = url;
        results.sourceUrls.LOWEST_GC_FINISHER = url;
        break;
      }
    } catch (err) {
      // Try next URL
    }
  }

  if (!isOneDayRace && gcRiders.length > 0) {
    const pointsUrl = `${baseUrl}/results/points`;
    const mountainUrl = `${baseUrl}/results/mountain`;
    const youthUrl = `${baseUrl}/results/youth`;

    try {
      const pointsHtml = await fetchPcsHtml(pointsUrl);
      const pointsRiders = extractRankedRidersFromPcsTable(pointsHtml);
      if (pointsRiders.length > 0) {
        results.POINTS_WINNER = pointsRiders[0];
        results.sourceUrls.POINTS_WINNER = pointsUrl;
      }
    } catch (err) {
      // Continue without points winner
    }

    try {
      const mountainHtml = await fetchPcsHtml(mountainUrl);
      const mountainRiders = extractRankedRidersFromPcsTable(mountainHtml);
      if (mountainRiders.length > 0) {
        results.MOUNTAIN_WINNER = mountainRiders[0];
        results.sourceUrls.MOUNTAIN_WINNER = mountainUrl;
      }
    } catch (err) {
      // Continue without mountain winner
    }

    try {
      const youthHtml = await fetchPcsHtml(youthUrl);
      const youthRiders = extractRankedRidersFromPcsTable(youthHtml);
      if (youthRiders.length > 0) {
        results.YOUTH_WINNER = youthRiders[0];
        results.sourceUrls.YOUTH_WINNER = youthUrl;
      }
    } catch (err) {
      // Continue without youth winner
    }
  }

  return results;
}

async function scrapePcsStageWinner(race, stageNumber) {
  if (!race || !race.id || !Number.isFinite(Number(stageNumber))) {
    return { winner: null, sourceUrl: null };
  }

  const slug = await resolvePcsRaceSlug(race);
  const year = Number(race.year || TARGET_YEAR);
  const stage = Number(stageNumber);
  if (!slug || !Number.isFinite(year)) {
    return { winner: null, sourceUrl: null };
  }

  const url = `https://www.procyclingstats.com/race/${slug}/${year}/stage-${stage}`;
  try {
    const html = await fetchPcsHtml(url);
    const winner = extractFirstRiderFromTable(html);
    return { winner: winner || null, sourceUrl: url };
  } catch (err) {
    return { winner: null, sourceUrl: url };
  }
}

async function scrapePcsOneDayRaceWinner(race) {
  if (!race || !race.id) {
    return { winner: null, sourceUrl: null };
  }

  const slug = await resolvePcsRaceSlug(race);
  const year = Number(race.year || TARGET_YEAR);
  if (!slug || !Number.isFinite(year)) {
    return { winner: null, sourceUrl: null };
  }

  const url = `https://www.procyclingstats.com/race/${slug}/${year}`;
  try {
    const html = await fetchPcsHtml(url);
    const winner = extractFirstRiderFromTable(html);
    return { winner: winner || null, sourceUrl: url };
  } catch (err) {
    return { winner: null, sourceUrl: url };
  }
}

async function scrapeRaceClassificationResults(raceNumber, _widgetBaseUrl, isOneDayRace, race = null) {
  if (!raceNumber) {
    return {
      GC_WINNER: null,
      POINTS_WINNER: null,
      MOUNTAIN_WINNER: null,
      YOUTH_WINNER: null,
      LOWEST_GC_FINISHER: null,
      sourceUrls: {
        GC_WINNER: null,
        POINTS_WINNER: null,
        MOUNTAIN_WINNER: null,
        YOUTH_WINNER: null,
        LOWEST_GC_FINISHER: null,
      }
    };
  }

  const sourceUrl = buildRaceBaseUrl(raceNumber);
  let html = '';
  try {
    html = await fetchHtml(sourceUrl);
  } catch (err) {
    // FirstCycling fetch failed, try PCS fallback
    if (race && race.id) {
      try {
        console.log(`    ℹ️  FirstCycling unavailable, trying PCS...`);
        const pcsResults = await scrapePcsRaceClassificationResults(race, isOneDayRace);
        if (pcsResults.GC_WINNER) {
          console.log(`    ✓ Scraped GC winner from PCS`);
          return pcsResults;
        }
      } catch (pcsErr) {
        // PCS fallback also failed
      }
    }
    
    return {
      GC_WINNER: null,
      POINTS_WINNER: null,
      MOUNTAIN_WINNER: null,
      YOUTH_WINNER: null,
      LOWEST_GC_FINISHER: null,
      sourceUrls: {
        GC_WINNER: sourceUrl,
        POINTS_WINNER: sourceUrl,
        MOUNTAIN_WINNER: sourceUrl,
        YOUTH_WINNER: sourceUrl,
        LOWEST_GC_FINISHER: sourceUrl,
      }
    };
  }

  const rankedTables = extractRankedRidersByTable(html);
  const gcTable = rankedTables[0] || [];
  const youthTable = rankedTables[1] || [];
  const pointsTable = rankedTables[2] || [];
  const mountainTable = rankedTables[3] || [];

  const results = {
    GC_WINNER: gcTable[0] || null,
    POINTS_WINNER: null,
    MOUNTAIN_WINNER: null,
    YOUTH_WINNER: null,
    LOWEST_GC_FINISHER: gcTable.length > 0 ? gcTable[gcTable.length - 1] : null,
    sourceUrls: {
      GC_WINNER: sourceUrl,
      POINTS_WINNER: sourceUrl,
      MOUNTAIN_WINNER: sourceUrl,
      YOUTH_WINNER: sourceUrl,
      LOWEST_GC_FINISHER: sourceUrl,
    }
  };

  if (!isOneDayRace) {
    results.YOUTH_WINNER = youthTable[0] || null;
    results.POINTS_WINNER = pointsTable[0] || null;
    results.MOUNTAIN_WINNER = mountainTable[0] || null;
  }

  return results;
}

const REQUIRED_ONE_DAY_RESULT_TYPES = ['GC_WINNER'];
const REQUIRED_STAGE_RACE_RESULT_TYPES = ['GC_WINNER', 'POINTS_WINNER', 'MOUNTAIN_WINNER', 'YOUTH_WINNER', 'LOWEST_GC_FINISHER'];
const CATEGORY_LABEL_BY_RESULT_TYPE = {
  GC_WINNER: 'GC',
  POINTS_WINNER: 'Sprint',
  MOUNTAIN_WINNER: 'KOM',
  YOUTH_WINNER: 'Pogi Trui',
  LOWEST_GC_FINISHER: 'Rode Lantaarn',
};

function getRequiredResultTypes(isOneDayRace) {
  return isOneDayRace ? REQUIRED_ONE_DAY_RESULT_TYPES : REQUIRED_STAGE_RACE_RESULT_TYPES;
}

function hasStoredClassificationResult(classificationByRaceId, raceId, resultType) {
  const byType = classificationByRaceId.get(String(raceId));
  if (!byType) return false;
  return Boolean(String(byType[resultType] || '').trim());
}

function hasAllClassificationResults(classificationByRaceId, raceId, isOneDayRace) {
  const required = getRequiredResultTypes(isOneDayRace);
  return required.every((resultType) => hasStoredClassificationResult(classificationByRaceId, raceId, resultType));
}

async function loadRaceClassificationByRaceId() {
  const { data, error } = await supabase
    .from('race_classification_results')
    .select('race_id,result_type,rider_name,source')
    .eq('source', 'firstcycling_widget');

  if (error) {
    console.warn(`⚠️  Could not load race_classification_results: ${error.message}`);
    return new Map();
  }

  const byRace = new Map();
  for (const row of data || []) {
    const raceId = String(row && row.race_id || '').trim();
    const resultType = String(row && row.result_type || '').trim();
    const riderName = String(row && row.rider_name || '').trim();
    if (!raceId || !resultType || !riderName) continue;
    if (!byRace.has(raceId)) byRace.set(raceId, {});
    const byType = byRace.get(raceId);
    if (!byType[resultType]) byType[resultType] = riderName;
  }

  return byRace;
}

async function upsertRaceClassificationResult(raceId, resultType, riderName, classificationByRaceId) {
  const cleanName = normalizeName(riderName);
  if (!cleanName) return false;
  if (DRY_RUN) return false;
  const categoryLabel = CATEGORY_LABEL_BY_RESULT_TYPE[resultType] || resultType;

  const { data: existingRows, error: existingError } = await supabase
    .from('race_classification_results')
    .select('id,rider_name')
    .eq('race_id', raceId)
    .eq('result_type', resultType)
    .eq('source', 'firstcycling_widget')
    .limit(1);

  if (existingError) throw existingError;

  const existing = (existingRows || [])[0] || null;
  if (existing) {
    const existingName = normalizeCompareName(existing.rider_name);
    const nextName = normalizeCompareName(cleanName);
    if (existingName !== nextName) {
      const { error: updateError } = await supabase
        .from('race_classification_results')
        .update({ rider_name: cleanName, category_label: categoryLabel })
        .eq('id', existing.id);
      if (updateError) throw updateError;
    }
  } else {
    const { error: insertError } = await supabase
      .from('race_classification_results')
      .insert([{
        race_id: raceId,
        result_type: resultType,
        category_label: categoryLabel,
        rider_name: cleanName,
        source: 'firstcycling_widget',
      }]);
    if (insertError) throw insertError;
  }

  const raceKey = String(raceId);
  if (!classificationByRaceId.has(raceKey)) classificationByRaceId.set(raceKey, {});
  classificationByRaceId.get(raceKey)[resultType] = cleanName;
  return true;
}

async function scrapeStageWinner(stage, raceNumber, widgetBaseUrl, scrapedStageNumber, race = null) {
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

  // Fall back to PCS stage pages when FirstCycling is unavailable.
  if (race && race.id) {
    const pcsResult = await scrapePcsStageWinner(race, stage.stage_number);
    if (pcsResult.winner) {
      return pcsResult;
    }
  }

  return { winner: null, sourceUrl: preferredWidgetUrl || null };
}

async function scrapeOneDayRaceWinner(raceNumber, race = null) {
  if (!raceNumber) return { winner: null, sourceUrl: null };

  const url = buildRaceBaseUrl(raceNumber);
  try {
    const html = await fetchHtml(url);
    const winner = extractFirstRiderFromTable(html);
    if (!winner && race && race.id) {
      const pcsResult = await scrapePcsOneDayRaceWinner(race);
      if (pcsResult.winner) {
        return pcsResult;
      }
    }
    return { winner: winner || null, sourceUrl: url };
  } catch (err) {
    if (race && race.id) {
      const pcsResult = await scrapePcsOneDayRaceWinner(race);
      if (pcsResult.winner) {
        return pcsResult;
      }
    }
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
  const classificationByRaceId = await loadRaceClassificationByRaceId();
  const todayUtc = getTodayUtcDateOnly();

  if (!races.length) {
    console.log('⚠️ No races found for target year.');
    return;
  }

  let updatedStages = 0;
  let updatedRaces = 0;
  let updatedRaceClassifications = 0;

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

    if (!hasRaceStarted(raceStages, todayUtc)) {
      console.log(`\n⏭️  [${raceIndex + 1}/${races.length}] ${race.name}: race has not started yet, skipped`);
      continue;
    }

    const nonRestStages = raceStages.filter(stage => !stage.is_rest_day);
    const startedStages = nonRestStages.filter(stage => hasStageStarted(stage, todayUtc));
    const isOneDayRace = nonRestStages.length <= 1;
    const hasClassifications = hasAllClassificationResults(classificationByRaceId, race.id, isOneDayRace);

    if (!FORCE && race.full_results && hasClassifications) {
      console.log(`\n⏭️  [${raceIndex + 1}/${races.length}] ${race.name}: full_results + classifications already present, skipped`);
      continue;
    }

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
      const { winner, sourceUrl } = await scrapeStageWinner(stage, raceNumber, raceWidgetBaseUrl, scrapedStageNumber, race);
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
      const oneDay = await scrapeOneDayRaceWinner(raceNumber, race);
      const raceWinner = oneDay.winner || fallbackRaceWinner || null;
      await updateRaceResult(race.id, raceWinner, Boolean(raceWinner));
      updatedRaces += 1;
      console.log(`  Race result (one-day): ${raceWinner || 'not found'}${oneDay.sourceUrl ? ` (${oneDay.sourceUrl})` : ''}`);
    } else {
      await updateRaceResult(race.id, race.winner || null, allStageWinnersKnown);
      updatedRaces += 1;
      console.log(`  Race result: stage race, winner skipped | full_results=${allStageWinnersKnown}`);
    }

    const raceIsFinished = Boolean(allStageWinnersKnown);
    const classificationsAlreadyStored = hasAllClassificationResults(classificationByRaceId, race.id, isOneDayRace);
    if (raceIsFinished && (!classificationsAlreadyStored || FORCE)) {
      const classificationResults = await scrapeRaceClassificationResults(raceNumber, raceWidgetBaseUrl, isOneDayRace, race);
      const requiredTypes = getRequiredResultTypes(isOneDayRace);
      for (const resultType of requiredTypes) {
        const winner = classificationResults[resultType] || null;
        if (!winner) continue;
        const changed = await upsertRaceClassificationResult(
          race.id,
          resultType,
          winner,
          classificationByRaceId
        );
        if (changed) {
          updatedRaceClassifications += 1;
        }
      }

      const summary = requiredTypes
        .map((resultType) => `${resultType}=${classificationResults[resultType] || 'not found'}`)
        .join(' | ');
      console.log(`  Race classifications: ${summary}`);
    } else if (!raceIsFinished) {
      console.log('  Race classifications: skipped (race not fully finished yet)');
    } else {
      console.log('  Race classifications: already present, skipped');
    }

    await sleep(400);
  }

  console.log('\n✅ Daily current-year results scraping finished.');
  console.log(`   Updated stages: ${updatedStages}`);
  console.log(`   Updated races: ${updatedRaces}`);
  console.log(`   Updated race classifications: ${updatedRaceClassifications}`);
}

if (require.main === module) {
  main().catch(err => {
    console.error('❌ Daily results scraper failed:', err.message || err);
    process.exit(1);
  });
}
