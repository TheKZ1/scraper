const axios = require('axios');
const { supabase, fetchFirstCyclingWorldTourRaces } = require('./scraper-cycling-archives');

const TARGET_YEAR = Number(process.env.WIDGET_YEAR || String(new Date().getFullYear()));
const DRY_RUN = String(process.env.DRY_RUN || '') === '1';
const FORCE_OVERWRITE = String(process.env.FORCE_OVERWRITE || '') === '1';
const WIDGET_URL_COLUMN = `results_widget_url_${TARGET_YEAR}`;

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
const calendarCache = new Map();

function normalizeName(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/&/g, ' and ')
    .replace(/[^a-zA-Z0-9]+/g, ' ')
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .trim();
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

function extractRaceNumberFromUrl(url) {
  const raw = String(url || '').trim();
  if (!raw) return null;
  const match = raw.match(/[?&]r=(\d+)/i);
  return match ? match[1] : null;
}

function buildWidgetUrl(raceNumber) {
  return `https://firstcycling.com/widget/?r=${encodeURIComponent(String(raceNumber))}&y=${TARGET_YEAR}&lang=EN`;
}

async function fetchHtml(url) {
  const response = await axios.get(url, { headers, timeout: 20000 });
  return response.data;
}

async function fetchPage(url) {
  const response = await axios.get(url, {
    headers,
    timeout: 20000,
    maxRedirects: 10,
  });

  const finalUrl = response && response.request && response.request.res
    ? response.request.res.responseUrl
    : url;

  return {
    html: response.data,
    finalUrl: finalUrl || url,
  };
}

async function getCalendarRacesForYear(year) {
  const key = Number(year);
  if (!Number.isFinite(key)) return [];
  if (calendarCache.has(key)) return calendarCache.get(key);

  const calendarUrl = `https://firstcycling.com/race.php?y=${key}&t=1`;
  const races = await fetchFirstCyclingWorldTourRaces(key, calendarUrl);
  calendarCache.set(key, races || []);
  return races || [];
}

function buildCandidateYears(race) {
  const years = [];

  if (Number.isFinite(TARGET_YEAR)) years.push(Number(TARGET_YEAR));

  const raceYear = Number(race && race.year);
  if (Number.isFinite(raceYear) && !years.includes(raceYear)) {
    years.push(raceYear);
  }

  return years;
}

function extractRaceNumberFromRacePhp(htmlOrUrl, years) {
  const source = String(htmlOrUrl || '');
  if (!source) return null;

  for (const year of years) {
    const pattern = new RegExp(`race\\.php\\?r=(\\d+)&(?:amp;)?y=${year}(?:[^0-9]|$)`, 'i');
    const match = source.match(pattern);
    if (match && match[1]) return match[1];
  }

  return null;
}

function extractRaceNumberFromCanonicalHtml(html, years) {
  const source = String(html || '');
  if (!source) return null;

  const canonicalMatch = source.match(/<link[^>]+rel=["']canonical["'][^>]+href=["']([^"']+)["']/i)
    || source.match(/<meta[^>]+property=["']og:url["'][^>]+content=["']([^"']+)["']/i);

  if (!canonicalMatch || !canonicalMatch[1]) return null;
  return extractRaceNumberFromRacePhp(canonicalMatch[1], years);
}

async function resolveRaceNumberFromCalendar(race) {
  const candidateYears = buildCandidateYears(race);
  const targetName = normalizeName(race.name);
  if (!targetName) return null;

  for (const year of candidateYears) {
    let calendarRaces = [];
    try {
      calendarRaces = await getCalendarRacesForYear(year);
    } catch (err) {
      continue;
    }
    if (!calendarRaces.length) continue;

    const exact = calendarRaces.filter(item => normalizeName(item.name) === targetName);
    if (exact.length === 1) {
      return extractRaceNumberFromUrl(exact[0].url);
    }

    const targetSlug = String(race.slug || '').trim().toLowerCase();
    if (targetSlug) {
      const bySlug = calendarRaces.filter(item => String(item.slug || '').trim().toLowerCase() === targetSlug);
      if (bySlug.length === 1) {
        return extractRaceNumberFromUrl(bySlug[0].url);
      }
    }

  }

  return null;
}

async function resolveRaceNumber(race, raceStages) {
  if (!FORCE_OVERWRITE) {
    const existingNumber = extractRaceNumberFromUrl(race[WIDGET_URL_COLUMN]) || String(race.firstcycling_race_number || '').trim();
    if (existingNumber) {
      return existingNumber;
    }
  }

  for (const stage of raceStages || []) {
    const raceNumber = extractRaceNumberFromUrl(stage.url);
    if (raceNumber) {
      return raceNumber;
    }
  }

  const fromCalendar = await resolveRaceNumberFromCalendar(race);
  if (fromCalendar) {
    return fromCalendar;
  }

  const slug = String(race.slug || '').trim();
  if (!slug) return null;
  const candidateYears = buildCandidateYears(race);

  const candidateUrls = uniqueUrls([
    `https://firstcycling.com/race/${slug}/${TARGET_YEAR}`,
    `https://www.firstcycling.com/race/${slug}/${TARGET_YEAR}`,
    race.year ? `https://firstcycling.com/race/${slug}/${race.year}` : null,
    race.year ? `https://www.firstcycling.com/race/${slug}/${race.year}` : null,
  ]);

  for (const url of candidateUrls) {
    try {
      const { html, finalUrl } = await fetchPage(url);
      const fromFinalUrl = extractRaceNumberFromRacePhp(finalUrl, candidateYears);
      if (fromFinalUrl) return fromFinalUrl;

      const fromCanonical = extractRaceNumberFromCanonicalHtml(html, candidateYears);
      if (fromCanonical) return fromCanonical;
    } catch (err) {
    }
  }

  return null;
}

async function loadRacesAndStages() {
  const selectColumns = ['id', 'slug', 'name', 'year', 'firstcycling_race_number', WIDGET_URL_COLUMN];
  const { data: races, error: racesError } = await supabase
    .from('races')
    .select(selectColumns.join(', '))
    .order('name', { ascending: true });

  if (racesError) throw racesError;

  const { data: stages, error: stagesError } = await supabase
    .from('stages')
    .select('id, race_id, stage_number, url')
    .order('stage_number', { ascending: true });

  if (stagesError) throw stagesError;

  const stagesByRace = new Map();
  for (const stage of stages || []) {
    const key = String(stage.race_id);
    if (!stagesByRace.has(key)) stagesByRace.set(key, []);
    stagesByRace.get(key).push(stage);
  }

  return { races: races || [], stagesByRace };
}

async function updateRaceWidgetFields(raceId, raceNumber) {
  const payload = {
    firstcycling_race_number: String(raceNumber),
    [WIDGET_URL_COLUMN]: buildWidgetUrl(raceNumber),
  };

  if (DRY_RUN) return payload;

  const { error } = await supabase
    .from('races')
    .update(payload)
    .eq('id', raceId);

  if (error) throw error;
  return payload;
}

function shouldSkipRace(race) {
  if (FORCE_OVERWRITE) return false;
  const hasRaceNumber = String(race.firstcycling_race_number || '').trim().length > 0;
  const hasWidgetUrl = String(race[WIDGET_URL_COLUMN] || '').trim().length > 0;
  return hasRaceNumber && hasWidgetUrl;
}

async function main() {
  console.log(`🚀 Fill FirstCycling widget columns started (WIDGET_YEAR=${TARGET_YEAR}, column=${WIDGET_URL_COLUMN})`);
  if (DRY_RUN) console.log('🧪 DRY_RUN=1 enabled, no database writes will happen.');
  if (FORCE_OVERWRITE) console.log('♻️ FORCE_OVERWRITE=1 enabled, existing values will be overwritten.');

  const { races, stagesByRace } = await loadRacesAndStages();
  if (!races.length) {
    console.log('⚠️ No races found in database.');
    return;
  }

  let updated = 0;
  let skipped = 0;
  let unresolved = 0;

  for (let index = 0; index < races.length; index++) {
    const race = races[index];
    const raceStages = (stagesByRace.get(String(race.id)) || [])
      .slice()
      .sort((a, b) => Number(a.stage_number) - Number(b.stage_number));

    if (shouldSkipRace(race)) {
      skipped += 1;
      console.log(`[${index + 1}/${races.length}] ${race.name}: skipped (already filled)`);
      continue;
    }

    const raceNumber = await resolveRaceNumber(race, raceStages);
    if (!raceNumber) {
      unresolved += 1;
      console.log(`[${index + 1}/${races.length}] ${race.name}: ⚠️ could not resolve race number`);
      continue;
    }

    const payload = await updateRaceWidgetFields(race.id, raceNumber);
    updated += 1;
    console.log(`[${index + 1}/${races.length}] ${race.name}: ✅ r=${raceNumber} | ${payload[WIDGET_URL_COLUMN]}`);

    await sleep(300);
  }

  console.log('\n✅ Fill finished.');
  console.log(`   Updated races: ${updated}`);
  console.log(`   Skipped races: ${skipped}`);
  console.log(`   Unresolved races: ${unresolved}`);
}

if (require.main === module) {
  main().catch(err => {
    const msg = err && err.message ? err.message : String(err);
    if (/column .* does not exist/i.test(msg)) {
      console.error(`❌ Missing column on races table (${WIDGET_URL_COLUMN}). Add it first with SQL migration.`);
    }
    console.error('❌ Fill failed:', msg);
    process.exit(1);
  });
}
