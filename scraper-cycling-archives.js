const normalizeStageTime = (value) => {
  if (!value) return '';
  if (value.includes('T')) return value.split('T')[1].slice(0, 5);
  if (value.includes(' ')) return value.split(' ')[1].slice(0, 5);
  return value.slice(0, 5);
};
const getStageStartDateTime = (stage) => {
  if (!stage || !stage.start_date) return null;
  const timeValue = normalizeStageTime(stage.start_time || '');
  const timePart = timeValue ? `${timeValue}:00` : '00:00:00';
  return new Date(`${stage.start_date}T${timePart}`);
};
const isDnsDnfStatus = (status) => {
  const s = String(status || '').toUpperCase();
  return s.includes('DNF') || s.includes('DNS');
};
const normalizeRiderLookupName = (value) => String(value || '')
  .normalize('NFD')
  .replace(/[\u0300-\u036f]/g, '')
  .replace(/\s+/g, ' ')
  .trim()
  .toLowerCase();
const hasRiderDnfStageColumns = async () => {
  const { error } = await supabase
    .from('riders')
    .select('id,dnf_stage_number,dnf_detected_at')
    .limit(1);
  return !error;
};
const hasRiderYouthColumn = async () => {
  const { error } = await supabase
    .from('riders')
    .select('id,youth_eligible')
    .limit(1);
  return !error;
};
const getLatestDnfStageNumberForRace = async (raceId, detectedAtIso) => {
  const { data: stages, error } = await supabase
    .from('stages')
    .select('stage_number,start_date,is_rest_day')
    .eq('race_id', raceId)
    .order('stage_number', { ascending: true });
  if (error || !stages || stages.length === 0) return null;
  const activeStages = stages
    .filter((stage) => !stage.is_rest_day)
    .map((stage) => ({
      stage_number: Number(stage.stage_number),
      start_date: String(stage.start_date || '').trim()
    }))
    .filter((stage) => Number.isFinite(stage.stage_number))
    .sort((a, b) => a.stage_number - b.stage_number);
  if (activeStages.length === 0) return null;

  const detectedDate = new Date(detectedAtIso || Date.now());
  const yyyy = detectedDate.getFullYear();
  const mm = String(detectedDate.getMonth() + 1).padStart(2, '0');
  const dd = String(detectedDate.getDate()).padStart(2, '0');
  const detectedYmd = `${yyyy}-${mm}-${dd}`;

  const boundaryIndex = activeStages.findIndex((stage) => stage.start_date && stage.start_date >= detectedYmd);
  if (boundaryIndex > 0) return activeStages[boundaryIndex - 1].stage_number;
  if (boundaryIndex === 0) return activeStages[0].stage_number;
  return activeStages[activeStages.length - 1].stage_number;
};
// scraper-cycling-archives.js - Scrape cycling data from cycling archive websites
const axios = require('axios');
const fs = require('fs');
const cheerio = require('cheerio');
const { createClient } = require('@supabase/supabase-js');

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
  throw new Error('Missing SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY) environment variable.');
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

const headers = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'Accept-Language': 'en-US,en;q=0.5',
  'Connection': 'keep-alive',
};

const pcsHeaders = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'Accept-Language': 'en-US,en;q=0.5',
  'Referer': 'https://www.procyclingstats.com/',
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

const FIRSTCYCLING_BASE_URL = 'https://www.firstcycling.com';
const DEFAULT_WORLDTOUR_YEAR = 2026;
const DEFAULT_WORLDTOUR_URL = 'https://firstcycling.com/race.php?y=2026&t=1';
const EXTRA_INCLUDED_RACE_NAMES = new Set([
  'world championship itt',
  'world championship rr',
]);
const EXTRA_INCLUDED_RACE_SEARCH_TERMS = [
  'World Championship ITT',
  'World Championship RR',
];
const EXTRA_INCLUDED_RACE_ID_MAP = {
  26: 'World Championship RR',
  27: 'World Championship ITT',
};

const normalizeRaceNameForIncludeCheck = (value) => String(value || '')
  .toLowerCase()
  .normalize('NFD')
  .replace(/[\u0300-\u036f]/g, '')
  .replace(/[^a-z0-9\s]/g, ' ')
  .replace(/\s+/g, ' ')
  .trim();

const shouldIncludeExtraRace = (value) => {
  const normalized = normalizeRaceNameForIncludeCheck(value);
  if (EXTRA_INCLUDED_RACE_NAMES.has(normalized)) {
    return true;
  }

  // Be tolerant to common naming variants such as elite/men/women suffixes.
  return normalized.includes('world championship')
    && (normalized.includes(' itt') || normalized.includes(' rr'));
};

const collectExtraFirstCyclingRacesFromSearch = async (year) => {
  const races = [];
  const seenUrls = new Set();

  for (const term of EXTRA_INCLUDED_RACE_SEARCH_TERMS) {
    try {
      const searchUrl = `https://firstcycling.com/index.php?search=${encodeURIComponent(term)}`;
      const html = await fetchHtml(searchUrl);
      const $ = cheerio.load(html);

      $('a[href*="race.php"]').each((idx, link) => {
        const $link = $(link);
        const href = String($link.attr('href') || '').trim();
        const name = String($link.text() || '').trim();
        if (!href || !name) return;
        if (!shouldIncludeExtraRace(name)) return;

        const absoluteUrl = buildAbsoluteUrl(href);
        if (!absoluteUrl || !absoluteUrl.includes('race.php')) return;

        const raceYearMatch = absoluteUrl.match(/[?&]y=(\d{4})/);
        if (raceYearMatch && Number(raceYearMatch[1]) !== Number(year)) return;

        if (seenUrls.has(absoluteUrl)) return;
        seenUrls.add(absoluteUrl);

        races.push({
          name,
          url: absoluteUrl,
          year,
          slug: buildRaceSlugFromUrl(absoluteUrl, name),
          start_date: null,
        });
      });
    } catch (err) {
      console.log(`  ⚠️  Could not resolve extra race via search (${term}): ${err.message.split('\n')[0]}`);
    }
  }

  return races;
};

const collectExtraFirstCyclingRacesById = (year) => {
  return Object.entries(EXTRA_INCLUDED_RACE_ID_MAP).map(([raceId, raceName]) => {
    const id = Number(raceId);
    const url = `https://firstcycling.com/race.php?r=${id}&y=${year}`;
    return {
      name: raceName,
      url,
      year,
      slug: buildRaceSlugFromUrl(url, raceName),
      start_date: null,
    };
  });
};

const getTodayUtcDateOnly = () => {
  const now = new Date();
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
};

const parseDateOnlyUtc = (dateStr) => {
  if (!dateStr) {
    return null;
  }

  const match = dateStr.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) {
    return null;
  }

  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) {
    return null;
  }

  return new Date(Date.UTC(year, month - 1, day));
};

const isPastDate = (dateStr) => {
  const date = parseDateOnlyUtc(dateStr);
  if (!date) {
    return false;
  }

  return date < getTodayUtcDateOnly();
};

const firstCyclingCookie = getEnvValue('FIRSTCYCLING_COOKIE', '');
if (firstCyclingCookie) {
  headers.Cookie = firstCyclingCookie;
}

const pcsCookie = getEnvValue('PCS_COOKIE', '');
if (pcsCookie) {
  pcsHeaders.Cookie = pcsCookie;
}

const PCS_SLUG_OVERRIDES = {
  'omloop nieuwsblad': 'omloop-het-nieuwsblad',
  'cadel evans great ocean road race': 'great-ocean-road-race',
  'volta ciclista a catalunya': 'volta-a-catalunya',
  'ronde van brugge tour of bruges': 'classic-brugge-de-panne',
  'e3 saxo classic': 'e3-harelbeke',
  'in flanders fields': 'gent-wevelgem',
  'tour auvergne rhone alpes': 'dauphine',
  'donostia san sebastian klasikoa': 'san-sebastian',
  'adac cyclassics': 'cyclassics-hamburg'
};
const pcsResolvedSlugCache = new Map();
const pcsCalendarCache = new Map();

const normalizeSlug = (value) => value
  .toLowerCase()
  .normalize('NFD')
  .replace(/[\u0300-\u036f]/g, '')
  .replace(/[^a-z0-9]+/g, '-')
  .replace(/(^-|-$)/g, '');

const normalizeRaceNameForMatch = (value) => String(value || '')
  .toLowerCase()
  .normalize('NFD')
  .replace(/[\u0300-\u036f]/g, '')
  .replace(/[^a-z0-9\s]/g, ' ')
  .replace(/\s+/g, ' ')
  .trim();

const tokenizeRaceForMatch = (value) => {
  const stop = new Set(['the', 'de', 'la', 'le', 'a', 'of', 'tour', 'race', 'men', 'me', 'elite', 'road']);
  return normalizeRaceNameForMatch(value)
    .split(' ')
    .map((token) => token.trim())
    .filter((token) => token.length >= 3 && !stop.has(token));
};

const buildPcsSlugAliasCandidates = (raceName, raceSlug) => {
  const slug = normalizeSlug(raceSlug || raceName || '');
  const aliases = [];
  if (slug.includes('tour-down-under')) aliases.push('tour-down-under');
  if (slug.includes('omloop')) aliases.push('omloop-het-nieuwsblad');
  if (slug.includes('uae-tour')) aliases.push('uae-tour');
  if (slug.includes('paris-nice')) aliases.push('paris-nice');
  if (slug.includes('strade-bianche')) aliases.push('strade-bianche');
  if (slug.includes('cadel-evans')) aliases.push('great-ocean-road-race');
  if (slug.includes('volta-ciclista-a-catalunya')) aliases.push('volta-a-catalunya');
  if (slug.includes('ronde-van-brugge') || slug.includes('tour-of-bruges')) aliases.push('classic-brugge-de-panne');
  if (slug.includes('e3-saxo-classic')) aliases.push('e3-harelbeke');
  if (slug.includes('in-flanders-fields')) aliases.push('gent-wevelgem');
  if (slug.includes('tour-auvergne')) aliases.push('dauphine');
  if (slug.includes('san-sebastian-klasikoa') || slug.includes('donostia')) aliases.push('san-sebastian');
  if (slug.includes('adac-cyclassics')) aliases.push('cyclassics-hamburg');
  return aliases;
};

const extractPcsSlugFromHref = (href) => {
  const value = String(href || '').trim();
  const match = value.match(/\/race\/([^/?#]+)/i);
  return match ? normalizeSlug(match[1]) : '';
};

const parsePcsDateToIso = (ddmm, year) => {
  const m = String(ddmm || '').match(/^(\d{1,2})\.(\d{1,2})$/);
  if (!m) return '';
  const day = String(Number(m[1])).padStart(2, '0');
  const month = String(Number(m[2])).padStart(2, '0');
  return `${year}-${month}-${day}`;
};

const getPcsUrlStatus = async (url) => {
  try {
    const res = await axios.get(url, {
      headers: pcsHeaders,
      timeout: 15000,
      maxRedirects: 2,
      validateStatus: () => true,
    });
    return Number(res.status || 0);
  } catch (err) {
    return 0;
  }
};

const loadPcsYearCalendar = async (year) => {
  const cacheKey = String(year || '');
  if (pcsCalendarCache.has(cacheKey)) {
    return pcsCalendarCache.get(cacheKey);
  }

  const entries = [];
  try {
    const html = await fetchPcsHtml(`https://www.procyclingstats.com/races.php?year=${year}`);
    const $ = cheerio.load(html);
    $('table.basic tbody tr').each((_, tr) => {
      const $row = $(tr);
      const dateStartRaw = String($row.find('td.hide.cs500').first().text() || '').trim();
      const raceAnchor = $row.find('td').eq(2).find('a[href*="race/"]').first();
      if (!raceAnchor.length) return;

      const href = String(raceAnchor.attr('href') || '').trim();
      const raceName = String(raceAnchor.text() || '').trim();
      if (!href || !raceName || !dateStartRaw) return;

      const slug = extractPcsSlugFromHref(href);
      if (!slug) return;

      const dateIso = parsePcsDateToIso(dateStartRaw, Number(year));
      if (!dateIso) return;

      entries.push({ dateIso, raceName, slug });
    });
  } catch (err) {
    pcsCalendarCache.set(cacheKey, []);
    return [];
  }

  pcsCalendarCache.set(cacheKey, entries);
  return entries;
};

const resolvePcsRaceSlug = async (race) => {
  const cacheKey = `${race && race.id ? race.id : race && race.slug ? race.slug : race && race.name ? race.name : ''}-${race && race.year ? race.year : ''}`;
  if (pcsResolvedSlugCache.has(cacheKey)) {
    return pcsResolvedSlugCache.get(cacheKey);
  }

  const raceName = String(race && race.name || '').trim();
  const raceSlug = String(race && race.slug || '').trim();
  const raceYear = Number(race && race.year);
  const raceStartDate = String(race && race.start_date || '').trim();
  const raceNameKey = normalizeRaceNameForMatch(raceName);

  const candidates = [];
  if (PCS_SLUG_OVERRIDES[raceNameKey]) candidates.push(PCS_SLUG_OVERRIDES[raceNameKey]);
  candidates.push(...buildPcsSlugAliasCandidates(raceName, raceSlug));
  if (raceSlug) candidates.push(normalizeSlug(raceSlug));
  if (raceName) candidates.push(normalizeSlug(raceName));

  // Search fallback, mirroring breakaway script behavior.
  const searchTerm = raceName || raceSlug;
  if (searchTerm) {
    try {
      const searchUrl = `https://www.procyclingstats.com/search.php?term=${encodeURIComponent(searchTerm)}`;
      const html = await fetchPcsHtml(searchUrl);
      const $ = cheerio.load(html);
      $('a[href]').each((_, el) => {
        const href = String($(el).attr('href') || '');
        const slug = extractPcsSlugFromHref(href);
        if (slug) candidates.push(slug);
      });
    } catch (err) {
      // Search-based enrichment is best-effort only.
    }
  }

  if (Number.isFinite(raceYear) && raceStartDate) {
    try {
      const calendarEntries = await loadPcsYearCalendar(raceYear);
      const dbTokens = tokenizeRaceForMatch(`${raceName} ${raceSlug}`);
      let best = { score: -1, slug: '' };
      calendarEntries.forEach((entry) => {
        let score = 0;
        if (entry.dateIso === raceStartDate) score += 30;

        const pcsTokens = tokenizeRaceForMatch(`${entry.raceName} ${entry.slug}`);
        const pcsSet = new Set(pcsTokens);
        const overlap = dbTokens.filter((token) => pcsSet.has(token)).length;
        score += overlap * 4;

        const normalizedRaceSlug = normalizeSlug(raceSlug || raceName);
        if (normalizedRaceSlug && entry.slug === normalizedRaceSlug) score += 20;
        if (normalizedRaceSlug && entry.slug.includes(normalizedRaceSlug)) score += 5;
        if (normalizedRaceSlug && normalizedRaceSlug.includes(entry.slug)) score += 5;

        if (score > best.score) {
          best = { score, slug: entry.slug };
        }
      });
      if (best.slug) candidates.unshift(best.slug);
    } catch (err) {
      // Calendar scoring fallback is best-effort only.
    }
  }

  const deduped = Array.from(new Set(candidates.map((value) => normalizeSlug(value)).filter(Boolean)));
  for (const slug of deduped) {
    if (!Number.isFinite(raceYear)) {
      pcsResolvedSlugCache.set(cacheKey, slug);
      return slug;
    }

    // Match breakaway resolver behavior: validate race-year URL existence first.
    const status = await getPcsUrlStatus(`https://www.procyclingstats.com/race/${slug}/${raceYear}`);
    if (status >= 200 && status < 400) {
      pcsResolvedSlugCache.set(cacheKey, slug);
      return slug;
    }
  }

  const fallback = deduped[0] || (raceSlug ? normalizeSlug(raceSlug) : normalizeSlug(raceName));
  pcsResolvedSlugCache.set(cacheKey, fallback);
  return fallback;
};

const buildRaceSlugFromUrl = (url, name) => {
  const match = url.match(/\/race\/([^/]+)\/(\d{4})/i);
  if (!match) {
    return normalizeSlug(name || url);
  }

  return normalizeSlug(match[1]);
};

const detectTimeTrialLabel = (value) => {
  const text = String(value || '').toLowerCase();
  if (!text) return null;
  if (/\bttt\b|team\s*time\s*trial|ploegen\s*tijdrit/.test(text)) return 'TTT';
  if (/\bitt\b|individual\s*time\s*trial|prologue|\btijdrit\b/.test(text)) return 'ITT';
  return null;
};

const detectTimeTrialLabelFromRow = ($row) => {
  const iconSources = [];
  $row.find('img').each((idx, img) => {
    const $img = $row.find(img);
    const src = String($img.attr('src') || '').toLowerCase();
    const alt = String($img.attr('alt') || '').toLowerCase();
    const title = String($img.attr('title') || '').toLowerCase();
    iconSources.push(`${src} ${alt} ${title}`.trim());
  });

  const blob = iconSources.join(' | ');
  if (!blob) return null;

  // FirstCycling stage profile icons: Icon_TTT is explicit TTT.
  // Icon_TTR appears on non-TT stages as a generic profile icon, so do not map it to ITT.
  if (blob.includes('icon_ttt')) return 'TTT';

  if (/team\s*time\s*trial/.test(blob)) return 'TTT';

  return null;
};

const getStageMiniIconUrlFromRow = ($row) => {
  let found = '';
  $row.find('img').each((idx, img) => {
    const src = String($row.find(img).attr('src') || '').trim();
    if (!src) return;
    if (/img\/mini\/icon_/i.test(src)) {
      if (src.startsWith('http://') || src.startsWith('https://')) {
        found = src;
      } else if (src.startsWith('//')) {
        found = `https:${src}`;
      } else if (src.startsWith('/')) {
        found = `${FIRSTCYCLING_BASE_URL}${src}`;
      } else {
        found = `${FIRSTCYCLING_BASE_URL}/${src.replace(/^\.?\//, '')}`;
      }
    }
  });
  return found || null;
};

const getRaceNumberFromUrl = (url) => {
  const match = url.match(/[?&]r=(\d+)/);
  return match ? match[1] : null;
};

const resolveStageProfileImageUrls = async (race, stageNumber, totalStages = null) => {
  const raceNumber = getRaceNumberFromUrl(race.url);
  if (!raceNumber) {
    return { jpgUrl: null, pngUrl: null, chosenUrl: null };
  }

  const year = race.year || DEFAULT_WORLDTOUR_YEAR;
  const isOneDay = totalStages === 1;
  let candidates = [];

  if (isOneDay && stageNumber === 1) {
    // One-day race: only try <racenumber>_<year> (no stage number)
    const baseNoStage = `https://firstcycling.com/img/rittaar/${raceNumber}_${year}`;
    candidates = [`${baseNoStage}.jpg`, `${baseNoStage}.png`];
  } else {
    // Multi-stage: Tour de France may use zero-padded stage filenames (01, 02, ...).
    const raceSlug = String(race && race.slug || '').trim().toLowerCase();
    const raceName = String(race && race.name || '').trim().toLowerCase();
    const isTourDeFrance = raceSlug === 'tour-de-france' || raceName.includes('tour de france');
    const paddedStage = String(stageNumber).padStart(2, '0');
    const baseStandard = `https://firstcycling.com/img/ritt_etapper/${year}_${raceNumber}_${stageNumber}`;
    const basePadded = `https://firstcycling.com/img/ritt_etapper/${year}_${raceNumber}_${paddedStage}`;

    if (isTourDeFrance) {
      candidates = [
        `${basePadded}.jpg`, `${basePadded}.png`,
        `${baseStandard}.jpg`, `${baseStandard}.png`,
      ];
    } else {
      candidates = [`${baseStandard}.jpg`, `${baseStandard}.png`];
    }
  }

  const jpgUrl = candidates.find((url) => String(url).toLowerCase().endsWith('.jpg')) || null;
  const pngUrl = candidates.find((url) => String(url).toLowerCase().endsWith('.png')) || null;

  for (const url of candidates) {
    try {
      await axios.head(url, buildRequestOptions(10000));
      return { jpgUrl, pngUrl, chosenUrl: url };
    } catch (err) {
      // try next extension
    }
  }

  return { jpgUrl, pngUrl, chosenUrl: null };
};

const buildAbsoluteUrl = (href) => {
  if (!href) {
    return null;
  }

  if (href.startsWith('//')) {
    return `https:${href}`;
  }

  if (href.startsWith('http://') || href.startsWith('https://')) {
    return href;
  }

  if (href.startsWith('/')) {
    return `${FIRSTCYCLING_BASE_URL}${href}`;
  }

  return `${FIRSTCYCLING_BASE_URL}/${href}`;
};

const fetchWithRetry = async (fetchFn, url, maxRetries = 3) => {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fetchFn();
    } catch (err) {
      const status = err.response?.status;
      const isRetryable = !err.response || status >= 500; // Retry on network errors or 5xx
      const isLastAttempt = attempt === maxRetries;

      if (isRetryable && !isLastAttempt) {
        const delay = Math.pow(2, attempt - 1) * 1000; // Exponential backoff: 1s, 2s, 4s
        console.log(`  ⚠️  Attempt ${attempt}/${maxRetries} failed (${status || 'network error'}), retrying in ${delay}ms...`);
        await sleep(delay);
        continue;
      }

      throw err;
    }
  }
};

const fetchHtml = async (url) => {
  const res = await fetchWithRetry(
    () => axios.get(url, buildRequestOptions(15000)),
    url
  );
  return res.data;
};

const fetchPcsHtml = async (url) => {
  const options = {
    headers: pcsHeaders,
    timeout: 15000,
  };
  const res = await fetchWithRetry(
    () => axios.get(url, options),
    url
  );
  return res.data;
};

const scrapeFirstCyclingStageImages = async (race) => {
  const stageImages = new Map();
  let raceUrl = race.url.replace('https://www.', 'https://');

  if (!raceUrl.includes('race.php')) {
    return stageImages;
  }

  const separator = raceUrl.includes('?') ? '&' : '?';
  const eAllUrl = `${raceUrl}${separator}e=all`;

  try {
    const html = await fetchHtml(eAllUrl);
    const $ = cheerio.load(html);

    const stageLinkSelector = 'a[href*="/stage/"], a[href*="stage.php"]';

    const imageMatches = Array.from(html.matchAll(/\/img\/ritt_etapper\/\d{4}_(\d+)_([0-9]+)\.(?:jpg|png)/gi));
    imageMatches.forEach((match) => {
      let stageNumber = Number(match[2]);
      if (!Number.isFinite(stageNumber)) {
        return;
      }

      if (stageNumber === 0) {
        stageNumber = 1;
      }

      if (stageImages.has(stageNumber)) {
        return;
      }

      const imageUrl = buildAbsoluteUrl(match[0]);
      if (imageUrl) {
        stageImages.set(stageNumber, imageUrl);
      }
    });

    $('img').each((idx, img) => {
      const $img = $(img);
      const imgSrc = $img.attr('data-src') || $img.attr('src') || '';
      if (!imgSrc.includes('/img/ritt_etapper/')) {
        return;
      }

      const imageUrl = buildAbsoluteUrl(imgSrc);
      if (!imageUrl) {
        return;
      }

      const match = imageUrl.match(/\/img\/ritt_etapper\/\d{4}_(\d+)_([0-9]+)\./i);
      if (!match) {
        return;
      }

      let stageNumber = Number(match[2]);
      if (!Number.isFinite(stageNumber)) {
        return;
      }

      if (stageNumber === 0) {
        stageNumber = 1;
      }

      if (!stageImages.has(stageNumber)) {
        stageImages.set(stageNumber, imageUrl);
      }
    });

    $(stageLinkSelector).each((idx, link) => {
      const $link = $(link);
      const href = $link.attr('href') || '';
      let stageNumber = null;

      const matchDirect = href.match(/\/stage\/[^/]+\/\d{4}\/(\d+)/i);
      if (matchDirect) {
        stageNumber = Number(matchDirect[1]);
      }

      if (!stageNumber) {
        const rowText = $link.closest('tr, div, li').text();
        const matchText = rowText.match(/\b(?:Stage|Etappe)\s*(\d+)\b/i);
        if (matchText) {
          stageNumber = Number(matchText[1]);
        }
      }

      if (!stageNumber || !Number.isFinite(stageNumber)) {
        return;
      }

      if (stageImages.has(stageNumber)) {
        return;
      }

      const container = $link.closest('tr, div, li');
      const img = container.find('img').first();
      const imgSrc = img.attr('data-src') || img.attr('src');
      const imageUrl = buildAbsoluteUrl(imgSrc);

      if (imageUrl) {
        stageImages.set(stageNumber, imageUrl);
      }
    });

    $('table tr').each((idx, row) => {
      const $row = $(row);
      let stageNumber = null;

      const firstCellText = $row.find('td, th').first().text().trim();
      if (/^\d+$/.test(firstCellText)) {
        stageNumber = Number(firstCellText);
      }

      if (!stageNumber) {
        const rowText = $row.text();
        const matchText = rowText.match(/\b(?:Stage|Etappe)\s*(\d+)\b/i);
        if (matchText) {
          stageNumber = Number(matchText[1]);
        }
      }

      if (!stageNumber || !Number.isFinite(stageNumber)) {
        return;
      }

      if (stageImages.has(stageNumber)) {
        return;
      }

      let imageUrl = null;
      const img = $row.find('img').first();
      const imgSrc = img.attr('data-src') || img.attr('src');
      if (imgSrc) {
        imageUrl = buildAbsoluteUrl(imgSrc);
      }

      if (!imageUrl) {
        const styleAttr = $row.find('[style*="background"]').attr('style') || '';
        const bgMatch = styleAttr.match(/url\(['"]?([^'")]+)['"]?\)/i);
        if (bgMatch) {
          imageUrl = buildAbsoluteUrl(bgMatch[1]);
        }
      }

      if (imageUrl) {
        stageImages.set(stageNumber, imageUrl);
      }
    });
  } catch (err) {
    console.log(`    Could not fetch stage images: ${err.message.split('\n')[0]}`);
  }

  return stageImages;
};

const scrapeFirstCyclingStageImageFromStagePage = async (stageUrl) => {
  try {
    const html = await fetchHtml(stageUrl);

    const imageMatch = html.match(/\/img\/ritt_etapper\/\d{4}_\d+_[0-9]+\.(?:jpg|png)/i);
    if (imageMatch) {
      return buildAbsoluteUrl(imageMatch[0]);
    }

    const $ = cheerio.load(html);
    const ogImage = $('meta[property="og:image"]').attr('content')
      || $('meta[name="og:image"]').attr('content');
    if (ogImage) {
      return buildAbsoluteUrl(ogImage);
    }

    const img = $('img').filter((idx, el) => {
      const src = $(el).attr('data-src') || $(el).attr('src') || '';
      return src.includes('/img/ritt_etapper/');
    }).first();
    const imgSrc = img.attr('data-src') || img.attr('src');
    if (imgSrc) {
      return buildAbsoluteUrl(imgSrc);
    }

    const lastJpgImg = $('img').filter((idx, el) => {
      const src = $(el).attr('data-src') || $(el).attr('src') || '';
      return src.toLowerCase().includes('.jpg');
    }).last();
    const lastJpgSrc = lastJpgImg.attr('data-src') || lastJpgImg.attr('src');
    if (lastJpgSrc) {
      return buildAbsoluteUrl(lastJpgSrc);
    }
  } catch (err) {
    console.log(`    Could not fetch stage page image: ${err.message.split('\n')[0]}`);
  }

  return null;
};

const fetchFirstCyclingWorldTourRaces = async (year, calendarUrl) => {
  const html = await fetchHtml(calendarUrl);
  if (html.includes('Consent to Cookies') || html.includes('Toon betting advertenties')) {
    console.log('❌ FirstCycling returned the consent page. Set FIRSTCYCLING_COOKIE and try again.');
    return [];
  }
  const $ = cheerio.load(html);
  const races = new Map();

  $('table tr').each((idx, row) => {
    const $row = $(row);
    const rowText = $row.text().trim();
    const rowHasExtraTargetRace = shouldIncludeExtraRace(rowText);

    // Keep UWT rows plus explicitly requested World Championship races.
    if (!rowText.includes('UWT') && !rowHasExtraTargetRace) {
      return;
    }

    // Try to extract race start date from the row (usually first cell with a date pattern)
    let raceStartDate = null;
    const datePattern = /\b(\d{1,2})\.(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|\d{1,2})\b/i;
    const dateMatch = rowText.match(datePattern);
    if (dateMatch) {
      const dayNum = parseInt(dateMatch[1], 10);
      // Validate day is between 1-31
      if (dayNum >= 1 && dayNum <= 31) {
        const monthMap = {
          jan: '01', feb: '02', mar: '03', apr: '04', may: '05', jun: '06',
          jul: '07', aug: '08', sep: '09', oct: '10', nov: '11', dec: '12',
          '01': '01', '02': '02', '03': '03', '04': '04', '05': '05', '06': '06',
          '07': '07', '08': '08', '09': '09', '10': '10', '11': '11', '12': '12',
        };
        const day = String(dayNum).padStart(2, '0');
        const monthRaw = dateMatch[2].toLowerCase();
        const month = monthMap[monthRaw] || monthRaw.padStart(2, '0');
        raceStartDate = `${year}-${month}-${day}`;
      }
    }

    // Find all race links in this row
    $row.find('a[href]').each((linkIdx, link) => {
      const $link = $(link);
      const href = $link.attr('href');
      const name = $link.text().trim();

      if (!href || !name) {
        return;
      }

      const isExtraTargetRace = shouldIncludeExtraRace(name);

      // Look for race.php links with r parameter
      if (href.includes('race.php')) {
        const rMatch = href.match(/[?&]r=(\d+)/);
        if (rMatch) {
          if (!rowText.includes('UWT') && !isExtraTargetRace) {
            return;
          }

          const url = buildAbsoluteUrl(href);
          if (url && url.includes('race.php')) {
            const slug = buildRaceSlugFromUrl(href, name);
            if (!races.has(url)) {
              races.set(url, {
                name,
                url,
                year,
                slug,
                start_date: raceStartDate,
              });
            }
          }
        }
      }
    });
  });

  // Ensure World Championship ITT/RR are still included even when absent from UWT rows.
  const extraRacesById = collectExtraFirstCyclingRacesById(year);
  extraRacesById.forEach((race) => {
    if (!races.has(race.url)) {
      races.set(race.url, race);
    }
  });

  const extraRaces = await collectExtraFirstCyclingRacesFromSearch(year);
  extraRaces.forEach((race) => {
    if (!races.has(race.url)) {
      races.set(race.url, race);
    }
  });

  return Array.from(races.values());
};

/**
 * Scrape firstcycling.com (more permissive)
 */
async function scrapeFirstCyclingRace(race) {
  console.log(`\n📍 Trying FirstCycling for ${race.name}...`);
  
  // Remove www from URL if present, to match the working format
  let raceUrl = race.url.replace('https://www.', 'https://');
  console.log(`  Race URL: ${raceUrl}`);

  try {
    const html = await fetchHtml(raceUrl);
    const $ = cheerio.load(html);

    const title = $('h1').first().text().trim() || race.name;
    console.log(`  ✅ Found: ${title}`);

    // Build k=2 URL - just append &k=2 to the race URL
    const k2Url = `${raceUrl}&k=2`;
    
    console.log(`  Fetching k=2 page: ${k2Url}`);
    const k2Html = await fetchHtml(k2Url);
    const $k2 = cheerio.load(k2Html);
    
    // Find rows that contain a date (stages have dates like "20.Jan")
    const stages = [];
    const datePattern = /\b(\d{1,2})\.(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|\d{1,2})\b/i;
    
    let stageCount = 0;
    let rowCount = 0;
    
    $k2('table tbody tr').each((idx, row) => {
      const $row = $k2(row);
      const rowText = $row.text().trim();
      rowCount++;
      
      // Check if this row contains a date
      if (datePattern.test(rowText)) {
        stageCount++;
        console.log(`    Stage ${stageCount}: ${rowText.substring(0, 60)}`);
      }
    });
    
    console.log(`  Scanned ${rowCount} table rows, found ${stageCount} stage rows with dates`);
    
    // Extract stage information (number, distance, start time, start date)
    const stageInfo = [];
    const timePatterns = [
      /(\d{1,2}):(\d{2})/, // HH:MM format
      /(\d{1,2})\.(\d{2})/, // HH.MM format (European)
      /(\d{1,2})h(\d{2})/, // HHhMM format
    ];
    const monthMap = {
      jan: '01', feb: '02', mar: '03', apr: '04', may: '05', jun: '06',
      jul: '07', aug: '08', sep: '09', oct: '10', nov: '11', dec: '12',
      '01': '01', '02': '02', '03': '03', '04': '04', '05': '05', '06': '06',
      '07': '07', '08': '08', '09': '09', '10': '10', '11': '11', '12': '12',
    };
    let fallbackStageNum = 0;
    
    $k2('table tbody tr').each((idx, row) => {
      const $row = $k2(row);
      const rowText = $row.text().trim();
      
      // Check if this row contains a date (stage line)
      if (datePattern.test(rowText)) {
        const firstCellText = String($row.find('td, th').first().text() || '').trim();
        let parsedStageNumber = null;
        if (/^\d{1,2}$/.test(firstCellText)) {
          parsedStageNumber = Number(firstCellText);
        }
        
        // Try to extract start date (e.g., 20.Jan or 20.2)
        let startDate = null;
        const dateMatch = rowText.match(datePattern);
        if (dateMatch) {
          const dayNum = parseInt(dateMatch[1], 10);
          // Validate day is between 1-31
          if (dayNum >= 1 && dayNum <= 31) {
            const day = String(dayNum).padStart(2, '0');
            const monthRaw = dateMatch[2].toLowerCase();
            const month = monthMap[monthRaw] || monthRaw.padStart(2, '0');
            const year = race.year || DEFAULT_WORLDTOUR_YEAR;
            startDate = `${year}-${month}-${day}`;
          }
        }

        // Try to extract start time - try multiple patterns
        let startTime = null;
        for (const pattern of timePatterns) {
          const match = rowText.match(pattern);
          if (match) {
            startTime = `${match[1].padStart(2, '0')}:${match[2]}`;
            break;
          }
        }
        
        // Try to extract distance (usually contains 'km')
        let distance = '';
        const distanceMatch = rowText.match(/(\d+(?:\.\d+)?)\s*km/i);
        if (distanceMatch) {
          distance = distanceMatch[1];
        }
        
        // Rest day rows on FirstCycling k=2 typically have a date but no stage number and no km distance.
        const hasExplicitStageNumber = Number.isFinite(parsedStageNumber) && parsedStageNumber > 0;
        const hasRestMarker = /\b(rest(?:\s*day)?|rest\s*jour|jour(?:n|née)?\s*de\s*repos|repos|ruhetag|rustdag|giorno\s+di\s+riposo|descanso|d[ií]a\s+de\s+descanso)\b/i.test(rowText);
        const isRestDay = distance === '' && (!hasExplicitStageNumber || hasRestMarker);

        let stageNum = null;
        if (hasExplicitStageNumber) {
          stageNum = parsedStageNumber;
          fallbackStageNum = Math.max(fallbackStageNum, parsedStageNumber);
        } else if (!isRestDay) {
          fallbackStageNum += 1;
          stageNum = fallbackStageNum;
        } else {
          // Keep rest-day rows distinct from numbered race stages.
          stageNum = 0;
        }
        const ttLabelFromIcon = detectTimeTrialLabelFromRow($row);
        const ttLabel = ttLabelFromIcon || detectTimeTrialLabel(rowText);
        const stageIconUrl = getStageMiniIconUrlFromRow($row);
        
        stageInfo.push({
          stage_number: stageNum,
          distance: distance,
          start_time: startTime,
          start_date: startDate,
          is_rest_day: isRestDay,
          tt_label: ttLabel,
          stage_icon_url: stageIconUrl,
        });
        
        const timeDisplay = startTime ? ` | Time: ${startTime}` : '';
        const distDisplay = distance ? ` | Distance: ${distance}km` : '';
        const restDisplay = isRestDay ? ' | 🛌 REST DAY' : '';
        const stageLogLabel = isRestDay ? 'Rest day' : `Stage ${stageNum}`;
        console.log(`    ${stageLogLabel}: ${rowText.substring(0, 80)}${timeDisplay}${distDisplay}${restDisplay}`);
      }
    });
    
    // Use extracted stage info if available
    if (stageInfo.length > 0) {
      stages.push(...stageInfo);
    } else {
      // Fallback: create stage records with placeholder
      for (let i = 0; i < stageCount; i++) {
        stages.push({
          stage_number: i + 1,
          distance: '',
          start_time: null,
          start_date: null,
          is_rest_day: false,
          tt_label: null,
          stage_icon_url: null,
        });
      }
    }
    
    if (stages.length === 0) {
      stages.push({ 
        stage_number: 1, 
        distance: '',
        start_time: null,
        start_date: null,
        is_rest_day: false,
        tt_label: null,
        stage_icon_url: null,
      });
      console.log(`  ℹ️  No stages detected; defaulting to 1 stage`);
    }

    // For one-day races (single stage) or any stage missing a date, use race-level start_date as fallback
    if (race.start_date) {
      if (stages.length === 1 && !stages[0].start_date) {
        // Single-stage race with no stage date: use race date
        stages[0].start_date = race.start_date;
        console.log(`  ℹ️  Applied race-level start_date to single-stage race: ${race.start_date}`);
      } else {
        // For any stage missing a date, use race start_date
        let updatedCount = 0;
        for (const stage of stages) {
          if (!stage.start_date) {
            stage.start_date = race.start_date;
            updatedCount++;
          }
        }
        if (updatedCount > 0) {
          console.log(`  ℹ️  Applied race-level start_date to ${updatedCount} stage(s)`);
        }
      }
    }

    console.log(`  ✅ Found ${stages.length} stages total`);
    return { title, stages, source: 'firstcycling' };
  } catch (err) {
    console.log(`  ⚠️  FirstCycling failed: ${err.message.split('\n')[0]}`);
    return null;
  }
}

const scrapeFirstCyclingStartlist = async (race) => {
  const startlistUrls = [];
  
  // Remove www from URL to match working format
  let raceUrl = race.url.replace('https://www.', 'https://');

  if (raceUrl.includes('race.php')) {
    const separator = raceUrl.includes('?') ? '&' : '?';
    startlistUrls.push(`${raceUrl}${separator}k=8`);
  }

  startlistUrls.push(
    `${raceUrl}/startlist`,
    `${raceUrl}/startlist/`
  );

  for (const url of startlistUrls) {
    try {
      const html = await fetchHtml(url);
      if (String(html || '').includes('Consent to Cookies') || String(html || '').includes('Toon betting advertenties')) {
        console.log(`    FirstCycling consent page at ${url}; set FIRSTCYCLING_COOKIE to improve access.`);
        continue;
      }
      const $ = cheerio.load(html);
      
      // Check if this looks like a valid page
      const pageText = $('body').text().toLowerCase();
      const hasRiderContent = pageText.includes('rider') || pageText.includes('startlist') || pageText.includes('results') || pageText.includes('stage');
      
      if (!hasRiderContent) {
        console.log(`    Skipping ${url}: not a rider page`);
        continue;
      }
      
      const riders = [];

      // Process tables looking for team headers followed by rider rows
      $('table').each((tableIdx, table) => {
        const $table = $(table);
        let currentTeam = '';
        
        // Look for team headers within the table
        $table.find('tr').each((rowIdx, row) => {
          const $row = $(row);
          const rowText = $row.text().trim();
          
          // Check if this row is a team header (usually has colspan or is just text)
          const headerCells = $row.find('th, td[colspan]');
          if (headerCells.length > 0 && rowText.length > 2 && rowText.length < 100) {
            const headerText = headerCells.first().text().trim();
            // Could be a team header if it's not "Rider", "Team", "Bib", etc.
            if (headerText && !['rider', 'team', 'bib', 'number', 'pos', 'position', 'rank'].includes(headerText.toLowerCase())) {
              currentTeam = headerText;
            }
          }
          
          // Look for rider links in this row
          const riderLink = $row.find('a[href*="/rider/"], a[href*="rider.php"]').first();
          if (riderLink.length > 0) {
            const name = riderLink.text().trim();
            const riderUrl = buildAbsoluteUrl(riderLink.attr('href'));
            if (name && name.length > 2 && name.length < 100) {
              // Check if rider has strikethrough styling (DNF/DNS indicator)
              const style = riderLink.attr('style') || '';
              const hasStrikethrough = style.includes('text-decoration:line-through') || style.includes('text-decoration: line-through');
              const status = hasStrikethrough ? 'DNF/DNS' : null;
              riders.push({ name, team: currentTeam, status, rider_url: riderUrl });
            }
          }
        });
      });

      if (riders.length > 0) {
        console.log(`  ✅ Found ${riders.length} riders at ${url}`);
        if (riders.length > 0) {
          const statusStr = riders[0].status ? ` [${riders[0].status}]` : '';
          console.log(`    Sample: ${riders[0].name} - Team: ${riders[0].team || '(no team)'}${statusStr}`);
        }
        return riders;
      }

      // Fallback: extract just from table rows with no team
      const ridersSet = new Set();
      $('table tbody tr').each((idx, row) => {
        const $row = $(row);
        const riderLink = $row.find('a[href*="/rider/"], a[href*="rider.php"]').first();
        if (riderLink.length > 0) {
          const name = riderLink.text().trim();
          const riderUrl = buildAbsoluteUrl(riderLink.attr('href'));
          if (name && name.length > 2) {
            const style = riderLink.attr('style') || '';
            const hasStrikethrough = style.includes('text-decoration:line-through') || style.includes('text-decoration: line-through');
            ridersSet.add(JSON.stringify({ name, hasStrikethrough, rider_url: riderUrl }));
          }
        }
      });

      if (ridersSet.size > 0) {
        console.log(`  ✅ Found ${ridersSet.size} riders at ${url} (no team info)`);
        return Array.from(ridersSet.values()).map(riderJson => {
          const { name, hasStrikethrough, rider_url } = JSON.parse(riderJson);
          return { name, team: '', status: hasStrikethrough ? 'DNF/DNS' : null, rider_url };
        });
      }
    } catch (err) {
      console.log(`    Error fetching ${url}: ${err.message.split('\n')[0]}`);
    }
  }

  console.log('  ⚠️  No riders found for startlist');
  return [];
};

const scrapeFirstCyclingRiderStatus = async (race) => {
  // Scrape DNF/DNS status from race results
  const riderStatus = new Map(); // { riderName: 'DNF' | 'DNS' | null }
  
  // Remove www from URL to match working format
  let raceUrl = race.url.replace('https://www.', 'https://');
  const k2Url = `${raceUrl}&k=2`;
  
  try {
    const html = await fetchHtml(k2Url);
    const $ = cheerio.load(html);
    
    // Look for rows with DNF/DNS status
    $('table tbody tr').each((idx, row) => {
      const $row = $(row);
      const rowText = $row.text();
      
      // Check for DNF (Did Not Finish) or DNS (Did Not Start)
      const hasDNF = rowText.toLowerCase().includes('dnf') || rowText.toLowerCase().includes('did not finish');
      const hasDNS = rowText.toLowerCase().includes('dns') || rowText.toLowerCase().includes('did not start');
      
      if (hasDNF || hasDNS) {
        // Try to extract rider name from the row
        const nameLink = $row.find('a[href*="/rider/"], a[href*="rider.php"]').first();
        if (nameLink.length > 0) {
          const riderName = nameLink.text().trim();
          if (riderName && riderName.length > 2) {
            riderStatus.set(riderName, hasDNF ? 'DNF' : 'DNS');
          }
        }
      }
    });
  } catch (err) {
    console.log(`    Could not fetch rider status: ${err.message.split('\n')[0]}`);
  }
  
  return riderStatus;
};

const scrapePcsRiderStatusWithStage = async (race) => {
  const statusByNormalizedName = new Map();
  const slug = await resolvePcsRaceSlug(race);
  const year = Number(race && race.year);
  if (!slug || !Number.isFinite(year)) {
    return statusByNormalizedName;
  }

  const urls = [
    `https://www.procyclingstats.com/race/${slug}/${year}/startlist`,
    `https://www.procyclingstats.com/race/${slug}/${year}/startlist/startlist`
  ];

  for (const url of urls) {
    try {
      const html = await fetchPcsHtml(url);
      const htmlText = String(html || '');
      if (/cf-mitigated|just a moment/i.test(htmlText)) {
        console.log(`    PCS challenge page detected at ${url}; set PCS_COOKIE to improve access.`);
        continue;
      }

      const $ = cheerio.load(html);

      // Parse rider status in a DOM-scoped way so a DNF/DNS marker from one rider
      // cannot bleed into another rider in nearby HTML.
      $('li').each((idx, el) => {
        const $container = $(el);
        const containerText = $container.text();
        const statusWithStageMatch = containerText.match(/\((DNF|DNS)\s*#\s*(\d+)\)/i);
        const statusOnlyMatch = containerText.match(/\((DNF|DNS)\)/i);

        const detectedStatus = statusWithStageMatch
          ? statusWithStageMatch[1].toUpperCase()
          : (statusOnlyMatch ? statusOnlyMatch[1].toUpperCase() : null);

        const detectedYouthEligible = containerText.includes('*');

        if (!detectedStatus && !detectedYouthEligible) return;

        const detectedStageNumber = statusWithStageMatch
          ? Number(statusWithStageMatch[2])
          : null;

        const riderLinks = $container.find('a[href*="rider/"]');
        if (riderLinks.length !== 1) return;

        const $riderLink = riderLinks.first();
        const riderName = $riderLink.text().trim();
        const key = normalizeRiderLookupName(riderName);
        if (!key) return;

        const existing = statusByNormalizedName.get(key);
        if (!existing) {
          statusByNormalizedName.set(key, {
            name: riderName,
            status: detectedStatus,
            stage_number: Number.isFinite(detectedStageNumber) ? detectedStageNumber : null,
            youth_eligible: detectedYouthEligible,
            source: 'pcs'
          });
          return;
        }

        if (!Number.isFinite(existing.stage_number) && Number.isFinite(detectedStageNumber)) {
          existing.stage_number = detectedStageNumber;
        }
        if (detectedYouthEligible) {
          existing.youth_eligible = true;
        }
        statusByNormalizedName.set(key, existing);
      });

      $('a[href*="rider/"]').each((idx, link) => {
        const $link = $(link);
        const riderName = $link.text().trim();
        if (!riderName || riderName.length < 3) return;

        const contextText = [
          $link.parent().text(),
          $link.closest('tr').text(),
          $link.closest('li').text()
        ].join(' ');

        const statusWithStageMatch = contextText.match(/\((DNF|DNS)\s*#\s*(\d+)\)/i);
        const statusOnlyMatch = contextText.match(/\((DNF|DNS)\)/i);

        const detectedStatus = statusWithStageMatch
          ? statusWithStageMatch[1].toUpperCase()
          : (statusOnlyMatch ? statusOnlyMatch[1].toUpperCase() : null);

        const detectedYouthEligible = contextText.includes('*');

        if (!detectedStatus && !detectedYouthEligible) return;

        const detectedStageNumber = statusWithStageMatch
          ? Number(statusWithStageMatch[2])
          : null;

        const key = normalizeRiderLookupName(riderName);
        if (!key) return;

        const existing = statusByNormalizedName.get(key);
        if (!existing) {
          statusByNormalizedName.set(key, {
            name: riderName,
            status: detectedStatus,
            stage_number: Number.isFinite(detectedStageNumber) ? detectedStageNumber : null,
            youth_eligible: detectedYouthEligible,
            source: 'pcs'
          });
          return;
        }

        if (!Number.isFinite(existing.stage_number) && Number.isFinite(detectedStageNumber)) {
          existing.stage_number = detectedStageNumber;
        }
        if (detectedYouthEligible) {
          existing.youth_eligible = true;
        }
        statusByNormalizedName.set(key, existing);
      });

      if (statusByNormalizedName.size > 0) {
        console.log(`  ✅ Found ${statusByNormalizedName.size} riders with PCS DNF/DNS status at ${url}`);
        return statusByNormalizedName;
      }
    } catch (err) {
      console.log(`    Could not fetch PCS rider status from ${url}: ${err.message.split('\n')[0]}`);
    }
  }

  return statusByNormalizedName;
};

const scrapeRiderUciPoints = async (riderUrl) => {
  try {
    // Use the stats page which has a cleaner UCI points list
    let url = riderUrl.replace('https://www.', 'https://');
    
    // Add stats=1 parameter to get the UCI points ranking table
    if (!url.includes('&stats=1')) {
      const separator = url.includes('?') ? '&' : '?';
      url = `${url}${separator}stats=1`;
    }
    
    const html = await fetchHtml(url);
    const $ = cheerio.load(html);
    
    // Look for UCI points in the stats table
    // Format is typically: Ranking (XXXpts) where points are in parentheses
    let points2025 = null;
    
    // Strategy 1: Find table rows that contain "2025" and extract points from (XXX pts) pattern
    $('table tr').each((idx, row) => {
      const $row = $(row);
      const rowText = $row.text();
      
      // Check if this row contains 2025
      if (rowText.includes('2025')) {
        // Look for pattern like "123 (456 pts)" or "123 (1.234 pts)"
        // Handle thousand separators: 1.234 or 1,234
        const pointsMatch = rowText.match(/\((\d{1,3}[.,]?\d{0,3})\s*pts?\)/i);
        if (pointsMatch) {
          // Remove dots/commas from the number (1.234 -> 1234)
          const cleanNumber = pointsMatch[1].replace(/[.,]/g, '');
          const points = parseInt(cleanNumber, 10);
          if (points >= 0 && points <= 10000) {
            points2025 = points;
            return false; // break - we found it
          }
        }
      }
    });
    
    if (points2025 !== null) {
      return points2025;
    }
    
    // Strategy 2: Look for 2025 in any text and find nearby (XXX pts) pattern
    const allText = $('body').text();
    const lines = allText.split('\n');
    
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].trim();
      
      // Check if line contains both 2025 and pts pattern
      if (line.includes('2025')) {
        const pointsMatch = line.match(/\((\d{1,3}[.,]?\d{0,3})\s*pts?\)/i);
        if (pointsMatch) {
          // Remove dots/commas from the number (1.234 -> 1234)
          const cleanNumber = pointsMatch[1].replace(/[.,]/g, '');
          const points = parseInt(cleanNumber, 10);
          if (points > 0 && points <= 10000) {
            return points;
          }
        }
      }
    }
    
    return null;
  } catch (err) {
    return null;
  }
};

/**
 * Parse race data and insert into Supabase
 */
async function insertRaceData(race, raceData, riders) {
  try {
    console.log(`\n💾 Inserting ${race.name} data into Supabase...`);

    const safeRiders = Array.isArray(riders) ? riders : [];
    const { data: insertedRace, error: raceError } = await supabase
      .from('races')
      .upsert([{
        slug: race.slug,
        name: raceData.title || race.name,
        year: race.year,
      }], { onConflict: 'slug' })
      .select();

    if (raceError) {
      console.error('  ❌ Error inserting race:', raceError.message);
      return;
    }

    const raceId = insertedRace[0].id;
    console.log(`  ✅ Race inserted (ID: ${raceId})`);

    // Insert stages
    const stagesSource = raceData.stages && raceData.stages.length > 0
      ? raceData.stages
      : [{ stage_number: 1, distance: '' }];

    const upcomingStages = stagesSource.filter(stage => !isPastDate(stage.start_date));

    if (upcomingStages.length > 0) {
      const stageMap = new Map();
      
      for (const stage of upcomingStages) {
        const stageNumber = Number(stage.stage_number);
        if (!Number.isFinite(stageNumber)) {
          continue;
        }

        const stageSlug = `${race.slug}-stage-${stageNumber}`;
        if (!stageMap.has(stageSlug)) {
          const ttLabel = stage.tt_label === 'TTT' || stage.tt_label === 'ITT' ? stage.tt_label : null;
          const stageIconUrl = String(stage.stage_icon_url || '').trim() || null;
          const stageTitle = ttLabel
            ? `Stage ${stageNumber} (${ttLabel})`
            : `Stage ${stageNumber}`;
          const stageSummary = ttLabel === 'TTT'
            ? `${race.slug.replace(/-/g, ' ')} - Stage ${stageNumber}. Team time trial stage.`
            : (ttLabel === 'ITT'
              ? `${race.slug.replace(/-/g, ' ')} - Stage ${stageNumber}. Individual time trial stage.`
              : `${race.slug.replace(/-/g, ' ')} - Stage ${stageNumber}. Challenging mountain stage.`);

          stageMap.set(stageSlug, {
            race_id: raceId,
            stage_number: stageNumber,
            slug: stageSlug,
            url: `https://www.firstcycling.com/stage/${race.slug}/${race.year}/${stageNumber}`,
            start_time: stage.start_time || null,
            start_date: stage.start_date || null,
            is_rest_day: stage.is_rest_day || false,
            tt_label: ttLabel,
            stage_icon_url: stageIconUrl,
            __profile_title: stageTitle,
            __profile_summary: stageSummary,
          });
        }
      }

      const stagesToInsert = Array.from(stageMap.values());
      const stageRowsToUpsert = stagesToInsert.map((stage) => ({
        race_id: stage.race_id,
        stage_number: stage.stage_number,
        slug: stage.slug,
        url: stage.url,
        start_time: stage.start_time,
        start_date: stage.start_date,
        is_rest_day: stage.is_rest_day,
      }));

      const { error: stageError } = await supabase
        .from('stages')
        .upsert(stageRowsToUpsert, { onConflict: 'slug' });

      if (stageError) {
        console.log(`  ⚠️  Error inserting stages: ${stageError.message}`);
      } else {
        console.log(`  ✅ Inserted ${stagesToInsert.length} stages`);

        // Create stage profiles with stage images
        for (const stageToInsert of stagesToInsert) {
          await sleep(100);

          const { data: stageData } = await supabase
            .from('stages')
            .select('id')
            .eq('slug', stageToInsert.slug)
            .single();

          if (stageData) {
            const { jpgUrl, pngUrl, chosenUrl } = await resolveStageProfileImageUrls(race, stageToInsert.stage_number, stagesSource.length);
            const stageProfile = {
              stage_id: stageData.id,
              title: stageToInsert.__profile_title || `Stage ${stageToInsert.stage_number}`,
              summary: stageToInsert.__profile_summary || `${race.slug.replace(/-/g, ' ')} - Stage ${stageToInsert.stage_number}. Challenging mountain stage.`,
              stage_icon_url: stageToInsert.stage_icon_url || null,
              profile_image_url_jpg: jpgUrl,
              profile_image_url_png: pngUrl,
              scraped_at: new Date().toISOString(),
            };

            const { error: stageProfileError } = await supabase
              .from('stage_profiles')
              .upsert([stageProfile], { onConflict: 'stage_id' });

            if (stageProfileError) {
              console.log(`  ⚠️  Error inserting stage profile for stage ${stageToInsert.stage_number}: ${stageProfileError.message}`);
            }
          } else {
            console.log(`  ⚠️  No stage found for slug ${stageToInsert.slug}; skipping profile`);
          }
        }

        console.log(`  ✅ Stage profiles created`);
      }
    }

    if (safeRiders.length === 0) {
      console.log('  ⚠️  No riders scraped from startlist; skipping riders insert');
      return;
    }

    const { data: existingRiders, error: existingRidersError } = await supabase
      .from('riders')
      .select('name')
      .eq('race_id', raceId);

    if (existingRidersError) {
      console.log(`  ⚠️  Error checking existing riders: ${existingRidersError.message}`);
      return;
    }

    // Scrape rider status (DNF/DNS) from PCS only.
    const pcsRiderStatusByName = await scrapePcsRiderStatusWithStage(race);
    console.log(`  Found ${pcsRiderStatusByName.size} riders with PCS DNF/DNS status`);
    const riderDnfStageColumnsSupported = await hasRiderDnfStageColumns();
    const riderYouthColumnSupported = await hasRiderYouthColumn();
    const scrapeDetectedAt = new Date().toISOString();
    const latestDnfStageNumber = riderDnfStageColumnsSupported
      ? await getLatestDnfStageNumberForRace(raceId, scrapeDetectedAt)
      : null;

    const existingNames = new Set((existingRiders || []).map(rider => rider.name));
    const ridersToInsert = safeRiders
      .filter(riderObj => !existingNames.has(riderObj.name || riderObj))
      .map((riderObj, idx) => {
        const name = typeof riderObj === 'string' ? riderObj : riderObj.name;
        const team = typeof riderObj === 'string' ? '' : (riderObj.team || '');
        const normalizedName = normalizeRiderLookupName(name);
        const pcsEntry = pcsRiderStatusByName.get(normalizedName);
        const status = pcsEntry && pcsEntry.status ? pcsEntry.status : null;
        const insertRow = {
          race_id: raceId,
          name: name,
          bib_number: idx + 1,
          team: team,
          status: status,
        };
        if (riderDnfStageColumnsSupported && isDnsDnfStatus(status)) {
          const pcsStageNumber = Number(pcsEntry && pcsEntry.stage_number);
          insertRow.dnf_stage_number = Number.isFinite(pcsStageNumber) ? pcsStageNumber : latestDnfStageNumber;
          insertRow.dnf_detected_at = scrapeDetectedAt;
        }
        if (riderYouthColumnSupported) {
          insertRow.youth_eligible = !!(pcsEntry && pcsEntry.youth_eligible);
        }
        return insertRow;
      });

    if (ridersToInsert.length > 0) {
      const { error: ridersInsertError } = await supabase
        .from('riders')
        .insert(ridersToInsert);

      if (ridersInsertError) {
        console.log(`  ⚠️  Error inserting riders: ${ridersInsertError.message}`);
      } else {
        console.log(`  ✅ Inserted ${ridersToInsert.length} riders`);
      }
    } else {
      console.log('  ℹ️  No new riders to insert');
    }

  } catch (err) {
    console.error('  ❌ Fatal error:', err.message);
  }
}

/**
 * Main orchestration
 */
async function main() {
  console.log('🚀 Starting alternative cycling data scraper...\n');

  console.log('📍 Fetching all 2026 UWT races from FirstCycling...');
  const races = await fetchFirstCyclingWorldTourRaces(2026, DEFAULT_WORLDTOUR_URL);
  
  if (races.length === 0) {
    console.log('❌ No UWT races found. Check FIRSTCYCLING_COOKIE or calendar URL.');
    process.exit(1);
  }
  
  console.log(`✅ Found ${races.length} UWT races for 2026\n`);

  // Get existing races from database to check which ones have passed
  const { data: existingRaces } = await supabase
    .from('races')
    .select('id, slug, name');

  const existingRaceMap = new Map((existingRaces || []).map(r => [r.slug, r]));

  for (let i = 0; i < races.length; i++) {
    const race = races[i];
    const raceExists = existingRaceMap.has(race.slug);

    if (raceExists) {
      console.log(`\n⏭️  Skipping ${race.name} (already exists in database)`);
      continue;
    }

    if (isPastDate(race.start_date)) {
      console.log(`\n⏭️  Skipping ${race.name} (already started: ${race.start_date})`);
      continue;
    }

    console.log(`\n${'='.repeat(50)}`);
    console.log(`[${i + 1}/${races.length}] Processing: ${race.name}`);
    console.log(`${'='.repeat(50)}`);

    let raceData = null;

    // Try FirstCycling
    raceData = await scrapeFirstCyclingRace(race);

    // If FirstCycling fails, use synthetic data
    if (!raceData) {
      console.log(`  ℹ️  Using synthetic data for ${race.name}`);
      raceData = {
        title: race.name,
        stages: [],
        source: 'synthetic',
      };
    }

    // Insert/update data
    const riders = await scrapeFirstCyclingStartlist(race);
    await insertRaceData(race, raceData, riders);

    if (i < races.length - 1) {
      const delay = 3000 + Math.random() * 3000;
      console.log(`\n⏳ Waiting ${(delay / 1000).toFixed(1)}s before next race...\n`);
      await sleep(delay);
    }
  }

  console.log('\n' + '='.repeat(50));
  console.log('✅ Data insertion complete!');
  console.log('='.repeat(50));
  console.log('\n📊 Open index.html to test the race predictor');
}

if (require.main === module) {
  main().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
  });
}

module.exports = {
  scrapeFirstCyclingRace,
  insertRaceData,
  fetchFirstCyclingWorldTourRaces,
  scrapeFirstCyclingStartlist,
  scrapeFirstCyclingRiderStatus,
  scrapePcsRiderStatusWithStage,
  resolvePcsRaceSlug,
  scrapeRiderUciPoints,
  resolveStageProfileImageUrls,
  supabase,
};
