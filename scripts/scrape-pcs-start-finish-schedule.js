const axios = require('axios');
const cheerio = require('cheerio');
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = 'https://zbvibhtopcsqrnecxgim.supabase.co';
const SUPABASE_SERVICE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InpidmliaHRvcGNzcXJuZWN4Z2ltIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MTkyMjI5OCwiZXhwIjoyMDg3NDk4Mjk4fQ.jBvKDo_j0TvZ3FuQ2DpNAQmHXGABuyFcVhfjSc-G42w';
const PCS_SCHEDULE_URL = 'https://www.procyclingstats.com/calendar/uci/start-finish-schedule';
const USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36';

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

function normalizeWhitespace(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function normalizeNameForCompare(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, ' ')
    .replace(/-/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function nameTokens(value) {
  return normalizeNameForCompare(value)
    .split(' ')
    .map((token) => token.trim())
    .filter(Boolean);
}

function sortedTokenKey(tokens) {
  return tokens.slice().sort((a, b) => a.localeCompare(b)).join('|');
}

function isWomenRaceText(value) {
  const norm = normalizeNameForCompare(value);
  if (!norm) return false;
  return /\b(women|woman|femmes?|donne|ladies|elite women|wj|wu|we)\b/i.test(norm);
}

function isMenRaceText(value) {
  const norm = normalizeNameForCompare(value);
  if (!norm) return false;
  return /\b(men|man|elite men|mj|mu|me)\b/i.test(norm);
}

function isValidTimeToken(value) {
  return /^\d{1,2}:\d{2}$/.test(String(value || '').trim());
}

function normalizeTime(value) {
  const text = normalizeWhitespace(value);
  if (!isValidTimeToken(text)) return null;
  const [hh, mm] = text.split(':');
  return `${hh.padStart(2, '0')}:${mm}`;
}

function parseDateDdMmToIso(dateText, year) {
  const match = String(dateText || '').match(/^(\d{1,2})\/(\d{1,2})$/);
  if (!match || !Number.isFinite(year)) return null;

  const day = Number(match[1]);
  const month = Number(match[2]);
  if (day < 1 || day > 31 || month < 1 || month > 12) return null;

  const mm = String(month).padStart(2, '0');
  const dd = String(day).padStart(2, '0');
  return `${year}-${mm}-${dd}`;
}

function buildStageStartTimestamp(startDate, startTime) {
  if (!startDate || !startTime) return null;
  return `${startDate}T${startTime}:00`;
}

function buildStageExpectedFinishTimestamp(startDate, startTime, finishTime) {
  if (!startDate || !finishTime) return null;

  // PCS times are local race-day times; if finish appears earlier than start, assume next day.
  if (startTime && isValidTimeToken(startTime) && isValidTimeToken(finishTime)) {
    const [startH, startM] = startTime.split(':').map(Number);
    const [finishH, finishM] = finishTime.split(':').map(Number);
    if (Number.isFinite(startH) && Number.isFinite(startM) && Number.isFinite(finishH) && Number.isFinite(finishM)) {
      const startMinutes = (startH * 60) + startM;
      const finishMinutes = (finishH * 60) + finishM;
      if (finishMinutes < startMinutes) {
        const nextDay = new Date(`${startDate}T00:00:00`);
        if (!Number.isNaN(nextDay.getTime())) {
          nextDay.setDate(nextDay.getDate() + 1);
          const yyyy = nextDay.getFullYear();
          const mm = String(nextDay.getMonth() + 1).padStart(2, '0');
          const dd = String(nextDay.getDate()).padStart(2, '0');
          return `${yyyy}-${mm}-${dd}T${finishTime}:00`;
        }
      }
    }
  }

  return `${startDate}T${finishTime}:00`;
}

function parseRaceHref(href) {
  const clean = String(href || '').replace(/^\//, '');
  const match = clean.match(/^race\/([^/]+)\/(\d{4})(?:\/(stage-(\d+)|prologue))?/i);
  if (!match) return null;

  const slug = normalizeWhitespace(match[1]).toLowerCase();
  const year = Number(match[2]);
  const detail = (match[3] || '').toLowerCase();
  const stageNumberFromStage = Number(match[4] || 0);

  let stageNumber = 1;
  if (detail.startsWith('stage-') && Number.isFinite(stageNumberFromStage) && stageNumberFromStage > 0) {
    stageNumber = stageNumberFromStage;
  } else if (detail === 'prologue') {
    stageNumber = 1;
  }

  return { slug, year, stageNumber };
}

async function fetchScheduleRows() {
  const response = await axios.get(PCS_SCHEDULE_URL, {
    headers: {
      'User-Agent': USER_AGENT,
      'Accept-Language': 'en-US,en;q=0.9',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    },
    timeout: 20000
  });

  const $ = cheerio.load(response.data);
  const rows = [];

  $('table tr').each((_, tr) => {
    const tds = $(tr).find('td');
    if (tds.length < 5) return;

    const dateText = normalizeWhitespace($(tds[0]).text());
    const localStartText = normalizeWhitespace($(tds[1]).text());
    const raceCell = $(tds[2]);
    const stageStartText = normalizeWhitespace($(tds[3]).text());
    const finishText = normalizeWhitespace($(tds[4]).text());

    const raceHref = raceCell.find('a[href]').first().attr('href') || '';
    const parsed = parseRaceHref(raceHref);
    if (!parsed) return;

    const stageLabel = normalizeWhitespace(raceCell.text());
    const raceName = normalizeWhitespace((stageLabel.split('|')[0] || stageLabel));
    const stageStart = normalizeTime(stageStartText);
    const localStart = normalizeTime(localStartText);
    const startTime = stageStart || localStart;
    const startDate = parseDateDdMmToIso(dateText, parsed.year);

    rows.push({
      ...parsed,
      raceName,
      stageLabel,
      startDate,
      startTime,
      finishTime: normalizeTime(finishText),
      sourceUrl: `https://www.procyclingstats.com/${raceHref.replace(/^\//, '')}`
    });
  });

  return rows;
}

async function loadRaceMap(rows) {
  const years = Array.from(new Set(rows.map((r) => r.year).filter(Number.isFinite)));
  if (!years.length) {
    return {
      exactBySlugYear: new Map(),
      racesByYear: new Map(),
      byNormNameYear: new Map(),
      byNormSlugYear: new Map()
    };
  }

  const { data, error } = await supabase
    .from('races')
    .select('id,slug,year,name')
    .in('year', years);

  if (error) throw error;

  const exactBySlugYear = new Map();
  const racesByYear = new Map();
  const byNormNameYear = new Map();
  const byNormSlugYear = new Map();

  (data || [])
    .filter((race) => !isWomenRaceText(`${race.name || ''} ${race.slug || ''}`))
    .forEach((race) => {
    const slug = String(race.slug || '').toLowerCase();
    const year = Number(race.year);
    exactBySlugYear.set(`${slug}|${year}`, race);

    const yearKey = String(year);
    if (!racesByYear.has(yearKey)) racesByYear.set(yearKey, []);
    racesByYear.get(yearKey).push(race);

    const normName = normalizeNameForCompare(race.name || '');
    if (normName && !byNormNameYear.has(`${normName}|${year}`)) {
      byNormNameYear.set(`${normName}|${year}`, race);
    }

    const normSlug = normalizeNameForCompare(slug);
    if (normSlug && !byNormSlugYear.has(`${normSlug}|${year}`)) {
      byNormSlugYear.set(`${normSlug}|${year}`, race);
    }
  });

  return { exactBySlugYear, racesByYear, byNormNameYear, byNormSlugYear };
}

function resolveRaceForRow(row, context) {
  const { exactBySlugYear, racesByYear, byNormNameYear, byNormSlugYear } = context;

  const rowLooksWomen = isWomenRaceText(`${row.raceName || ''} ${row.slug || ''}`);
  const rowLooksMen = isMenRaceText(`${row.raceName || ''} ${row.slug || ''}`);

  const exact = exactBySlugYear.get(`${row.slug}|${row.year}`);
  if (exact) return { race: exact, method: 'exact-slug' };

  const normSlug = normalizeNameForCompare(row.slug || '');
  const slugNormMatch = normSlug ? byNormSlugYear.get(`${normSlug}|${row.year}`) : null;
  if (slugNormMatch) return { race: slugNormMatch, method: 'norm-slug' };

  const normRaceName = normalizeNameForCompare(row.raceName || '');
  const nameNormMatch = normRaceName ? byNormNameYear.get(`${normRaceName}|${row.year}`) : null;
  if (nameNormMatch) return { race: nameNormMatch, method: 'norm-name' };

  let yearCandidates = racesByYear.get(String(row.year)) || [];
  if (rowLooksWomen) {
    // Men-only mode: never map women schedule rows to men's race records.
    return { race: null, method: 'women-row-skipped' };
  }
  if (rowLooksMen) {
    yearCandidates = yearCandidates.filter((candidate) => isMenRaceText(`${candidate.name || ''} ${candidate.slug || ''}`) || !isWomenRaceText(`${candidate.name || ''} ${candidate.slug || ''}`));
  }
  if (!yearCandidates.length) return { race: null, method: 'none' };

  const rowTokens = nameTokens(row.raceName || row.slug || '');
  if (rowTokens.length >= 2) {
    const rowTokenKey = sortedTokenKey(rowTokens);
    const permutationMatches = yearCandidates.filter((candidate) => {
      const candTokens = nameTokens(candidate.name || candidate.slug || '');
      if (candTokens.length !== rowTokens.length) return false;
      return sortedTokenKey(candTokens) === rowTokenKey;
    });

    if (permutationMatches.length === 1) {
      return { race: permutationMatches[0], method: 'token-permutation' };
    }
  }

  let best = { score: -1, race: null };
  let secondBest = { score: -1, race: null };
  yearCandidates.forEach((candidate) => {
    const candName = normalizeNameForCompare(candidate.name || '');
    const candSlug = normalizeNameForCompare(candidate.slug || '');
    const candTokens = nameTokens(candidate.name || candidate.slug || '');

    let score = 0;
    if (candName && normRaceName && candName === normRaceName) score += 100;
    if (candSlug && normSlug && candSlug === normSlug) score += 90;
    if (candName && normRaceName && (candName.includes(normRaceName) || normRaceName.includes(candName))) score += 30;
    if (candSlug && normSlug && (candSlug.includes(normSlug) || normSlug.includes(candSlug))) score += 20;

    const overlap = candTokens.filter((token) => rowTokens.includes(token)).length;
    score += overlap * 8;

    if (score > best.score) {
      secondBest = best;
      best = { score, race: candidate };
    } else if (score > secondBest.score) {
      secondBest = { score, race: candidate };
    }
  });

  if (best.race && best.score >= 36 && best.score - secondBest.score >= 10) {
    return { race: best.race, method: 'fuzzy' };
  }

  return { race: null, method: 'none' };
}

async function loadStagesByRaceIds(raceIds) {
  if (!raceIds.length) return [];
  const { data, error } = await supabase
    .from('stages')
    .select('id,race_id,stage_number,start_time,start_date,expected_finish_time,is_rest_day')
    .in('race_id', raceIds);

  if (error) throw error;
  return data || [];
}

async function run() {
  const dryRun = process.argv.includes('--dry-run');
  const onlyMissing = process.argv.includes('--only-missing');
  console.log(`Fetching PCS schedule: ${PCS_SCHEDULE_URL}`);
  if (onlyMissing) {
    console.log('Incremental mode enabled: only filling missing stage start_date/start_time.');
  }

  const rows = await fetchScheduleRows();
  console.log(`Parsed schedule rows: ${rows.length}`);

  const raceContext = await loadRaceMap(rows);

  let matchedByExact = 0;
  let matchedByFallback = 0;
  const resolvedRows = rows.map((row) => {
    const resolved = resolveRaceForRow(row, raceContext);
    if (resolved.race) {
      if (resolved.method === 'exact-slug') matchedByExact += 1;
      else matchedByFallback += 1;
    }
    return { ...row, resolvedRace: resolved.race, matchMethod: resolved.method };
  });

  const matchedRaceIds = Array.from(new Set(
    resolvedRows
      .map((row) => (row.resolvedRace ? row.resolvedRace.id : null))
      .filter(Boolean)
  ));

  const stages = await loadStagesByRaceIds(matchedRaceIds);
  const stageMap = new Map(
    stages.map((stage) => [`${stage.race_id}|${Number(stage.stage_number || 0)}`, stage])
  );

  let matchedRows = 0;
  let missingRace = 0;
  let missingStage = 0;
  const updates = [];

  resolvedRows.forEach((row) => {
    const race = row.resolvedRace;
    if (!race) {
      missingRace += 1;
      return;
    }

    const stage = stageMap.get(`${race.id}|${row.stageNumber}`);
    if (!stage) {
      missingStage += 1;
      return;
    }

    matchedRows += 1;
    if (!row.startTime && !row.startDate) return;

    const nextStartDate = row.startDate || null;
    const nextStartTime = buildStageStartTimestamp(nextStartDate, row.startTime);
    const nextExpectedFinishTime = buildStageExpectedFinishTimestamp(nextStartDate, row.startTime, row.finishTime);
    const stageHasStartTime = Boolean(String(stage.start_time || '').trim());
    const stageHasStartDate = Boolean(String(stage.start_date || '').trim());
    const stageHasExpectedFinishTime = Boolean(String(stage.expected_finish_time || '').trim());

    if (onlyMissing) {
      const shouldFillTime = !stageHasStartTime && nextStartTime !== null;
      const shouldFillDate = !stageHasStartDate && nextStartDate !== null;
      const shouldFillExpectedFinish = !stageHasExpectedFinishTime && nextExpectedFinishTime !== null;
      if (!shouldFillTime && !shouldFillDate && !shouldFillExpectedFinish) return;

      updates.push({
        id: stage.id,
        start_time: shouldFillTime ? nextStartTime : stage.start_time,
        start_date: shouldFillDate ? nextStartDate : stage.start_date,
        expected_finish_time: shouldFillExpectedFinish ? nextExpectedFinishTime : stage.expected_finish_time
      });
      return;
    }

    const changed = (nextStartTime !== null && String(stage.start_time || '') !== String(nextStartTime || ''))
      || (String(stage.start_date || '') !== String(nextStartDate || ''))
      || (nextExpectedFinishTime !== null && String(stage.expected_finish_time || '') !== String(nextExpectedFinishTime || ''));

    if (!changed) return;

    updates.push({
      id: stage.id,
      start_time: nextStartTime !== null ? nextStartTime : stage.start_time,
      start_date: nextStartDate,
      expected_finish_time: nextExpectedFinishTime !== null ? nextExpectedFinishTime : stage.expected_finish_time
    });
  });

  console.log(`Matched schedule rows to DB stages: ${matchedRows}`);
  console.log(`Race match methods: exact=${matchedByExact}, fallback=${matchedByFallback}`);
  console.log(`Unmatched rows (race missing in DB): ${missingRace}`);
  console.log(`Unmatched rows (stage missing in DB): ${missingStage}`);
  console.log(`Stage updates prepared: ${updates.length}`);

  if (dryRun) {
    console.log('Dry run enabled; no updates written.');
    return;
  }

  if (!updates.length) {
    console.log('No changes to write.');
    return;
  }

  let written = 0;
  for (const row of updates) {
    const payload = {
      start_time: row.start_time,
      start_date: row.start_date,
      expected_finish_time: row.expected_finish_time
    };

    const { error } = await supabase
      .from('stages')
      .update(payload)
      .eq('id', row.id);

    if (error) throw error;
    written += 1;
  }

  console.log(`Updated stage rows: ${written}`);
}

run().catch((err) => {
  console.error('PCS schedule scrape failed:', err.message || err);
  process.exit(1);
});
