const axios = require('axios');
const cheerio = require('cheerio');
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = 'https://zbvibhtopcsqrnecxgim.supabase.co';
const SUPABASE_SERVICE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InpidmliaHRvcGNzcXJuZWN4Z2ltIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MTkyMjI5OCwiZXhwIjoyMDg3NDk4Mjk4fQ.jBvKDo_j0TvZ3FuQ2DpNAQmHXGABuyFcVhfjSc-G42w';

const SOURCE = 'pcs_shield';
const WIELERFLITS_CATEGORY_URL = 'https://www.wielerflits.nl/nieuws/category/wedstrijdverslag/';
const WIELERFLITS_MAX_PAGES = 40;
const WIELERFLITS_DATE_WINDOW_DAYS = 3;
const USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36';
const REQUEST_TIMEOUT_MS = 20000;
const HTTP_RETRY_ATTEMPTS = 4;
const HTTP_RETRY_BASE_DELAY_MS = 700;
let cachedArchiveArticles = null;
const pcsSlugCache = new Map();
const pcsRaceLinksCache = new Map();
const pcsUrlStatusCache = new Map();
const pcsCalendarCache = new Map();
const raceRiderNameCache = new Map();
const PCS_SLUG_OVERRIDES = {
  'omloop nieuwsblad': 'omloop-het-nieuwsblad'
};

const BREAKAWAY_CONTEXT_REGEX = /(break(?:away)?|front group|went clear|escaped early|up the road|slipped away|off the front|kopgroep|vlucht|vluchters|ontsnapping|op kop|reed weg|reden weg|aanval|aanvallen|demarrage)/i;
const DUTCH_PRIORITY_REGEX = /(aanval|aanvallen|kopgroep|ontsnapping|vluchters?|demarrage)/i;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

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

async function getRaceRiderNameContext(raceId) {
  const key = String(raceId || '');
  if (!key) return { riders: [], byNorm: new Map() };
  if (raceRiderNameCache.has(key)) return raceRiderNameCache.get(key);

  const { data, error } = await supabase
    .from('riders')
    .select('name')
    .eq('race_id', raceId);

  if (error) throw error;

  const riders = unique((data || [])
    .map((row) => normalizeWhitespace(row && row.name ? row.name : ''))
    .filter(Boolean));

  const byNorm = new Map();
  riders.forEach((name) => {
    const norm = normalizeNameForCompare(name);
    if (norm && !byNorm.has(norm)) byNorm.set(norm, name);
  });

  const context = { riders, byNorm };
  raceRiderNameCache.set(key, context);
  return context;
}

async function translatePcsRiderNameToDbName(pcsName, raceId) {
  const clean = normalizeWhitespace(pcsName);
  if (!clean) return clean;

  const context = await getRaceRiderNameContext(raceId);
  if (!context.riders.length) return clean;

  const norm = normalizeNameForCompare(clean);
  if (context.byNorm.has(norm)) return context.byNorm.get(norm);

  const pcsTokens = nameTokens(clean);
  if (pcsTokens.length < 2) return clean;

  // Handle reordered multi-part names (e.g. "Urianstad Bugge Martin" -> "Bugge Martin Urianstad").
  const pcsTokenKey = sortedTokenKey(pcsTokens);
  const permutationMatches = context.riders.filter((candidate) => {
    const candTokens = nameTokens(candidate);
    if (candTokens.length !== pcsTokens.length) return false;
    return sortedTokenKey(candTokens) === pcsTokenKey;
  });

  if (permutationMatches.length === 1) {
    return permutationMatches[0];
  }

  const pcsFirst = pcsTokens[0];
  const pcsLast = pcsTokens[pcsTokens.length - 1];
  const pcsInitial = pcsFirst ? pcsFirst[0] : '';

  let best = { score: -1, name: clean };
  context.riders.forEach((candidate) => {
    const candTokens = nameTokens(candidate);
    if (candTokens.length < 2) return;

    const candFirst = candTokens[0];
    const candLast = candTokens[candTokens.length - 1];
    const candInitial = candFirst ? candFirst[0] : '';

    let score = 0;
    if (candLast === pcsLast) score += 50;
    if (candFirst === pcsFirst) score += 25;
    if (candInitial && pcsInitial && candInitial === pcsInitial) score += 12;

    const overlap = candTokens.filter((token) => pcsTokens.includes(token)).length;
    score += overlap * 6;

    if (score > best.score) {
      best = { score, name: candidate };
    }
  });

  // Guardrail: fuzzy mapping only when surname matches and score is clearly strong.
  if (best.score >= 62 && normalizeNameForCompare(best.name).split(' ').slice(-1)[0] === pcsLast) {
    return best.name;
  }

  return clean;
}

function cleanRiderToken(value) {
  return normalizeWhitespace(value)
    .replace(/\([^)]*\)/g, '')
    .replace(/^[-,:;\s]+|[-,:;\s]+$/g, '');
}

function looksLikeRiderName(value) {
  const token = cleanRiderToken(value);
  if (!token) return false;

  const parts = token.split(' ').filter(Boolean);
  if (parts.length < 2 || parts.length > 4) return false;
  const allowedParticles = new Set(['van', 'de', 'der', 'den', 'ten', 'ter', 'la', 'le', 'di', 'da', 'del', 'dos']);
  return parts.every((part) => {
    const lower = part.toLowerCase();
    if (allowedParticles.has(lower)) return true;
    return /^[A-Z][A-Za-z'`.-]+$/.test(part);
  });
}

function splitPotentialNames(segment) {
  const normalized = normalizeWhitespace(segment)
    .replace(/\s+and\s+/gi, ', ')
    .replace(/\s+en\s+/gi, ', ')
    .replace(/\s+&\s+/g, ', ');

  return normalized
    .split(',')
    .map((item) => cleanRiderToken(item))
    .filter((item) => item && looksLikeRiderName(item));
}

function extractNamesFromSentence(sentence) {
  const patterns = [
    /(?:break(?:away)?(?: group)?\s+(?:featuring|including|of|with)|front group\s+(?:including|with))\s+([^\.]+?)(?:\s+(?:escaped|went clear|went up the road|built|opened|would|was|were)|\.|$)/i,
    /([A-Z][^\.]{20,260}?)\s+(?:escaped|went clear|went up the road|slipped away|attacked early)/i,
    /(?:part of|in)\s+(?:a|the)?\s*\d{1,2}-?(?:man|rider|strong)?\s+(?:group|break(?:away)?)\s+(?:that|which)?\s*(?:also\s+)?(?:included|featuring|including)\s+([^\.]+?)(?:\.|$)/i
  ];

  for (const pattern of patterns) {
    const match = sentence.match(pattern);
    if (!match) continue;

    const segment = match[1] || '';
    const names = splitPotentialNames(segment);
    if (names.length >= 2) {
      return names;
    }
  }

  return [];
}

function getCandidateSentences(text) {
  const sentences = normalizeWhitespace(text)
    .split(/(?<=[.!?])\s+/)
    .map((line) => normalizeWhitespace(line))
    .filter(Boolean);

  return sentences.filter((line) => BREAKAWAY_CONTEXT_REGEX.test(line));
}

function extractBreakawayTextWindows(text) {
  const windows = [];
  const patterns = [
    /(.{0,300}breakaway.{0,300})/ig,
    /(.{0,300}front group.{0,300})/ig,
    /(.{0,300}\bwent clear\b.{0,300})/ig,
    /(.{0,300}\boff the front\b.{0,300})/ig,
    /(.{0,300}\bkopgroep\b.{0,300})/ig,
    /(.{0,300}\bvlucht\b.{0,300})/ig,
    /(.{0,300}\bontsnapping\b.{0,300})/ig,
    /(.{0,300}\baanval(?:len)?\b.{0,300})/ig,
    /(.{0,300}\bdemarrage\b.{0,300})/ig
  ];

  patterns.forEach((pattern) => {
    let match;
    while ((match = pattern.exec(text))) {
      windows.push(normalizeWhitespace(match[1] || ''));
    }
  });

  return Array.from(new Set(windows)).filter(Boolean);
}

function extractNamesFromTeamParentheses(text) {
  const names = [];
  const regex = /([A-Z][A-Za-z'`.-]+(?:\s+[A-Z][A-Za-z'`.-]+){1,3})\s*\([^)]{2,80}\)/g;
  let match;
  while ((match = regex.exec(text))) {
    const name = cleanRiderToken(match[1]);
    if (looksLikeRiderName(name)) {
      names.push(name);
    }
  }
  return Array.from(new Set(names));
}

function extractHowItUnfoldedText($) {
  const heading = $('h2, h3').filter((_, el) => {
    const txt = normalizeWhitespace($(el).text()).toUpperCase();
    return txt.includes('HOW IT UNFOLDED') || txt.includes('HOW IN UNFOLDED');
  }).first();

  if (!heading.length) return '';

  const chunks = [];
  let node = heading.next();
  while (node && node.length) {
    const tag = String(node.prop('tagName') || '').toLowerCase();
    if (tag === 'h1' || tag === 'h2' || tag === 'h3') break;

    if (tag === 'p') {
      const text = normalizeWhitespace(node.text());
      if (text) chunks.push(text);
    }

    node = node.next();
  }

  return chunks.join(' ');
}

async function getHtml(url) {
  let lastError = null;

  for (let attempt = 1; attempt <= HTTP_RETRY_ATTEMPTS; attempt += 1) {
    try {
      const response = await axios.get(url, {
        headers: {
          'User-Agent': USER_AGENT,
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        },
        timeout: REQUEST_TIMEOUT_MS,
        validateStatus: (status) => status >= 200 && status < 300
      });
      return response.data;
    } catch (err) {
      lastError = err;
      const status = err && err.response ? Number(err.response.status) : 0;
      const retryable = !status || status === 408 || status === 425 || status === 429 || status >= 500;
      if (!retryable || attempt >= HTTP_RETRY_ATTEMPTS) break;

      const waitMs = HTTP_RETRY_BASE_DELAY_MS * attempt + Math.floor(Math.random() * 200);
      await sleep(waitMs);
    }
  }

  throw lastError || new Error(`Failed to fetch ${url}`);
}

function stageMentionsInText(stageNumber, text) {
  const normalized = normalizeWhitespace(text).toLowerCase();
  const ordinalMap = {
    1: ['eerste'],
    2: ['tweede'],
    3: ['derde'],
    4: ['vierde'],
    5: ['vijfde'],
    6: ['zesde'],
    7: ['zevende'],
    8: ['achtste'],
    9: ['negende'],
    10: ['tiende']
  };

  const directPattern = new RegExp(`\\b(stage|etappe|rit)\\s*${stageNumber}\\b`, 'i');
  if (directPattern.test(normalized)) return true;

  const ordinals = ordinalMap[stageNumber] || [];
  return ordinals.some((word) => normalized.includes(`${word} etappe`) || normalized.includes(`${word} rit`));
}

function normalizedRaceTokens(race) {
  const base = normalizeWhitespace(race.name || race.slug || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9 ]/g, ' ')
    .split(/\s+/)
    .filter((token) => token.length >= 2);

  const slug = normalizeSlug(race.slug || '');
  const aliases = [];
  if (slug.includes('tour-down-under')) aliases.push('down', 'under', 'adelaide');
  if (slug.includes('omloop')) aliases.push('omloop', 'nieuwsblad');
  if (slug.includes('uae-tour')) aliases.push('uae', 'hafeet');
  if (slug.includes('paris-nice')) aliases.push('paris', 'nice');
  if (slug.includes('strade-bianche')) aliases.push('strade', 'bianche');
  if (slug.includes('cadel-evans')) aliases.push('cadel', 'evans');

  return unique([...base, ...aliases]);
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

function isWomenRace(race) {
  const text = normalizeWhitespace(`${race && race.name ? race.name : ''} ${race && race.slug ? race.slug : ''}`).toLowerCase();
  return text.includes('women') || text.includes('feminine') || text.includes('femmes') || text.includes('donne') || text.includes('ladies') || text.includes('femenina');
}

function isUrlRelevantToRace(url, stage, race, raceTokens) {
  const lower = String(url || '').toLowerCase();
  const stageToken = `stage-${stage.stage_number}`;
  const etappeToken = `etappe-${stage.stage_number}`;
  const ritToken = `rit-${stage.stage_number}`;
  const raceNameLower = normalizeWhitespace(race.name || '').toLowerCase();
  const slugToken = normalizeSlug(race.slug);
  const previousYear = race.year ? String(race.year - 1) : '';
  const nextYear = race.year ? String(race.year + 1) : '';

  if (!/wielerflits\.nl/i.test(lower)) return false;
  if (!lower.includes('/nieuws/')) return false;
  if (lower.includes('#comments')) return false;

  if (stage.stage_number > 1 && !(lower.includes(stageToken) || lower.includes(etappeToken) || lower.includes(ritToken))) {
    return false;
  }

  if (!raceNameLower.includes('women') && (lower.includes('-vrouwen-') || lower.includes('-women-') || lower.includes('-donne-') || lower.includes('-ladies-'))) return false;

  // Reject obvious wrong years early. Yearless URLs are allowed and validated on page text later.
  if (previousYear && lower.includes(previousYear)) return false;
  if (nextYear && lower.includes(nextYear)) return false;

  if (slugToken && slugToken.length >= 5) {
    const relaxedSlug = slugToken
      .replace('a-espana', 'espana')
      .replace('qu-bec', 'quebec');
    if (!lower.includes(slugToken) && !lower.includes(relaxedSlug)) return false;
  }

  const tokenHits = raceTokens.filter((token) => token.length >= 4 && lower.includes(token)).length;
  if (tokenHits === 0) return false;

  return true;
}

function scoreUrl(url, stage, race, raceTokens) {
  let score = 0;
  const lower = url.toLowerCase();
  const stageToken = `stage-${stage.stage_number}`;

  if (lower.includes(stageToken)) score += 6;
  if (lower.includes(String(race.year || ''))) score += 2;
  if (lower.includes('/pro-cycling/races/')) score += 2;
  if (lower.includes('/races/')) score += 1;

  raceTokens.forEach((token) => {
    if (lower.includes(token)) score += 1;
  });

  if (race.year && lower.includes(String((race.year || 0) - 1))) score -= 2;
  if (race.year && lower.includes(String((race.year || 0) + 1))) score -= 1;

  return score;
}

function sourcePassesRaceYearValidation(url, pageText, stage, race, raceTokens) {
  const lowerUrl = String(url || '').toLowerCase();
  const normalizedText = normalizeWhitespace(pageText).toLowerCase();
  const stagePattern = new RegExp(`\\bstage\\s*${stage.stage_number}\\b`, 'i');
  const raceYear = String(race.year || '');
  const previousYear = race.year ? String(race.year - 1) : '';
  const nextYear = race.year ? String(race.year + 1) : '';

  if (previousYear && lowerUrl.includes(previousYear)) return false;
  if (nextYear && lowerUrl.includes(nextYear)) return false;
  if (!isWomenRace(race) && (lowerUrl.includes('-vrouwen-') || lowerUrl.includes('-women-') || lowerUrl.includes('-donne-') || lowerUrl.includes('-ladies-') || /women'?s\s+tour/i.test(normalizedText))) return false;

  const tokenHitsInText = raceTokens.filter((token) => token.length >= 4 && normalizedText.includes(token)).length;
  const tokenHitsInUrl = raceTokens.filter((token) => token.length >= 4 && lowerUrl.includes(token)).length;
  const hasStageContext = stage.stage_number <= 1
    ? true
    : stagePattern.test(normalizedText)
      || stageMentionsInText(stage.stage_number, normalizedText)
      || lowerUrl.includes(`stage-${stage.stage_number}`)
      || lowerUrl.includes(`etappe-${stage.stage_number}`)
      || lowerUrl.includes(`rit-${stage.stage_number}`);
  if (!hasStageContext) return false;

  if (raceYear && lowerUrl.includes(raceYear)) {
    return tokenHitsInText >= 1 || tokenHitsInUrl >= 1;
  }

  // Yearless URL path: accept strong race+stage context even when article text omits the year.
  if (tokenHitsInText >= 1 && tokenHitsInUrl >= 1) return true;
  if (tokenHitsInUrl >= 2) return true;

  // Fallback: explicit year mention in content with at least one race token.
  return Boolean(raceYear && normalizedText.includes(raceYear) && (tokenHitsInText >= 1 || tokenHitsInUrl >= 1));
}

function unique(values) {
  return Array.from(new Set(values.filter(Boolean)));
}

function normalizeForMatch(value) {
  return normalizeWhitespace(value)
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9 ]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function tokenizeForMatch(value) {
  const stop = new Set(['the', 'de', 'la', 'le', 'a', 'of', 'tour', 'race', 'men', 'me', 'elite', 'road']);
  return normalizeForMatch(value)
    .split(' ')
    .map((t) => t.trim())
    .filter((t) => t.length >= 3 && !stop.has(t));
}

function parsePcsDateToIso(ddmm, year) {
  const m = String(ddmm || '').match(/^(\d{1,2})\.(\d{1,2})$/);
  if (!m) return '';
  const day = String(Number(m[1])).padStart(2, '0');
  const month = String(Number(m[2])).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function extractPcsSlugFromHref(href) {
  const value = String(href || '').trim();
  const match = value.match(/\/race\/([^\/?#]+)/i);
  if (!match) return '';
  return normalizeSlug(match[1]);
}

async function getHttpStatus(url) {
  const cacheKey = canonicalizePcsUrl(url);
  if (pcsUrlStatusCache.has(cacheKey)) return pcsUrlStatusCache.get(cacheKey);

  try {
    const response = await axios.get(url, {
      headers: {
        'User-Agent': USER_AGENT,
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
      },
      timeout: REQUEST_TIMEOUT_MS,
      maxRedirects: 2,
      validateStatus: () => true
    });
    const status = Number(response.status || 0);
    pcsUrlStatusCache.set(cacheKey, status);
    return status;
  } catch (err) {
    pcsUrlStatusCache.set(cacheKey, 0);
    return 0;
  }
}

async function resolvePcsRaceSlug(race) {
  const cacheKey = `${race.id || race.slug || race.name || ''}-${race.year || ''}`;
  if (pcsSlugCache.has(cacheKey)) return pcsSlugCache.get(cacheKey);

  const raceNameKey = normalizeForMatch(race.name || '');
  const candidates = [];
  if (PCS_SLUG_OVERRIDES[raceNameKey]) candidates.push(PCS_SLUG_OVERRIDES[raceNameKey]);
  if (race.slug) candidates.push(normalizeSlug(race.slug));
  if (race.name) candidates.push(normalizeSlug(race.name));

  try {
    const searchUrl = `https://www.procyclingstats.com/search.php?term=${encodeURIComponent(race.name || race.slug || '')}`;
    const html = await getHtml(searchUrl);
    const $ = cheerio.load(html);
    $('a[href]').each((_, el) => {
      const href = String($(el).attr('href') || '');
      const slug = extractPcsSlugFromHref(href);
      if (slug) candidates.push(slug);
    });
  } catch (err) {
    // Search fallback is opportunistic; URL construction continues without it.
  }

  const deduped = unique(candidates.map((v) => normalizeSlug(v)).filter(Boolean));
  for (const slug of deduped) {
    const raceYearUrl = `https://www.procyclingstats.com/race/${slug}/${race.year}`;
    const status = await getHttpStatus(raceYearUrl);
    if (status >= 200 && status < 400) {
      pcsSlugCache.set(cacheKey, slug);
      return slug;
    }
  }

  const fallback = deduped[0] || normalizeSlug(race.slug || race.name || '');
  pcsSlugCache.set(cacheKey, fallback);
  return fallback;
}

async function loadPcsYearCalendar(year) {
  const cacheKey = String(year || '');
  if (pcsCalendarCache.has(cacheKey)) return pcsCalendarCache.get(cacheKey);

  const entries = [];
  try {
    const html = await getHtml(`https://www.procyclingstats.com/races.php?year=${year}`);
    const $ = cheerio.load(html);
    $('table.basic tbody tr').each((_, tr) => {
      const row = $(tr);
      const dateStartRaw = normalizeWhitespace(row.find('td.hide.cs500').first().text());
      const raceAnchor = row.find('td').eq(2).find('a[href*="race/"]').first();
      if (!raceAnchor.length) return;

      const href = String(raceAnchor.attr('href') || '').trim();
      const raceName = normalizeWhitespace(raceAnchor.text());
      if (!href || !raceName || !dateStartRaw) return;

      const absoluteHref = href.startsWith('http') ? href : `https://www.procyclingstats.com/${href.replace(/^\//, '')}`;
      const slug = extractPcsSlugFromHref(absoluteHref);
      if (!slug) return;

      const dateIso = parsePcsDateToIso(dateStartRaw, Number(year));
      if (!dateIso) return;

      entries.push({
        dateIso,
        raceName,
        slug,
        href: canonicalizePcsUrl(absoluteHref)
      });
    });
  } catch (err) {
    pcsCalendarCache.set(cacheKey, []);
    return [];
  }

  const deduped = [];
  const seen = new Set();
  entries.forEach((entry) => {
    const key = `${entry.dateIso}|${entry.slug}|${entry.href}`;
    if (seen.has(key)) return;
    seen.add(key);
    deduped.push(entry);
  });

  pcsCalendarCache.set(cacheKey, deduped);
  return deduped;
}

function scorePcsRaceCalendarEntry(race, entry, raceStartDate) {
  let score = 0;
  if (entry.dateIso === raceStartDate) score += 30;

  const dbTokens = tokenizeForMatch(`${race.name || ''} ${race.slug || ''}`);
  const pcsTokens = tokenizeForMatch(`${entry.raceName || ''} ${entry.slug || ''}`);
  const pcsSet = new Set(pcsTokens);
  const overlap = dbTokens.filter((t) => pcsSet.has(t)).length;
  score += overlap * 4;

  const dbSlug = normalizeSlug(race.slug || race.name || '');
  if (dbSlug && entry.slug === dbSlug) score += 20;
  if (dbSlug && entry.slug.includes(dbSlug)) score += 5;
  if (dbSlug && dbSlug.includes(entry.slug)) score += 5;

  return score;
}

async function resolvePcsRaceBaseFromCalendar(race, raceStartDate) {
  const year = Number(race.year || 0);
  if (!year || !raceStartDate) return '';

  const entries = await loadPcsYearCalendar(year);
  if (!entries.length) return '';

  const scored = entries
    .map((entry) => ({ entry, score: scorePcsRaceCalendarEntry(race, entry, raceStartDate) }))
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score);

  if (!scored.length) return '';

  const best = scored[0].entry;
  return `https://www.procyclingstats.com/race/${best.slug}/${year}`;
}

function canonicalizePcsUrl(url) {
  return String(url || '').trim().replace(/\/$/, '');
}

async function loadPcsRaceResultLinks(slug, year) {
  const key = `${slug}-${year}`;
  if (pcsRaceLinksCache.has(key)) return pcsRaceLinksCache.get(key);

  const raceYearUrl = `https://www.procyclingstats.com/race/${slug}/${year}`;
  const found = [];

  try {
    const html = await getHtml(raceYearUrl);
    const $ = cheerio.load(html);
    $('a[href]').each((_, el) => {
      let href = String($(el).attr('href') || '').trim();
      if (!href) return;
      if (href.startsWith('/')) href = `https://www.procyclingstats.com${href}`;
      if (!/^https?:\/\//i.test(href)) return;
      const normalized = canonicalizePcsUrl(href);
      if (!normalized.includes(`/race/${slug}/${year}`)) return;

      // Race pages often link to /prologue or /stage-n without /result.
      if (/\/(prologue|stage-\d+)$/i.test(normalized)) {
        found.push(`${normalized}/result`);
        return;
      }

      if (/\/result$/i.test(normalized)) {
        found.push(normalized);
      }
    });
  } catch (err) {
    pcsRaceLinksCache.set(key, []);
    return [];
  }

  const deduped = unique(found);
  pcsRaceLinksCache.set(key, deduped);
  return deduped;
}

function normalizePcsStageResultUrl(url) {
  let value = String(url || '').trim();
  if (!value) return '';
  value = value.split('#')[0].split('?')[0].replace(/\/$/, '');

  if (!/procyclingstats\.com/i.test(value)) return '';
  // Collapse any duplicated trailing /result segments (e.g. /result/result/result).
  value = value.replace(/(?:\/result)+$/i, '/result');

  if (/\/stage-\d+$/i.test(value)) {
    value = `${value}/result`;
  } else if (/\/prologue$/i.test(value)) {
    value = `${value}/result`;
  } else if (/\/stage-\d+\/(info|startlist|live|statistics|game|history)$/i.test(value)) {
    value = value.replace(/\/(info|startlist|live|statistics|game|history)$/i, '/result');
  } else if (/\/prologue\/(info|startlist|live|statistics|game|history)$/i.test(value)) {
    value = value.replace(/\/(info|startlist|live|statistics|game|history)$/i, '/result');
  }

  if (!(/\/stage-\d+\/result$/i.test(value) || /\/prologue\/result$/i.test(value) || /\/\d{4}\/result$/i.test(value))) return '';
  return value;
}

async function buildPcsStageResultUrls(stage, race, raceStageCount, raceStartDate) {
  const candidates = [];
  const count = Number(raceStageCount || 0);
  const isOneDay = count <= 1;
  const raceSlugNormalized = normalizeSlug((race && race.slug) || (race && race.name) || '');

  const fromStage = normalizePcsStageResultUrl(stage.url || '');

  if (race && race.year && stage.stage_number) {
    let baseRaceUrl = await resolvePcsRaceBaseFromCalendar(race, raceStartDate);
    if (!baseRaceUrl) {
      const resolvedSlug = await resolvePcsRaceSlug(race);
      if (resolvedSlug) {
        baseRaceUrl = `https://www.procyclingstats.com/race/${resolvedSlug}/${race.year}`;
      }
    }

    if (baseRaceUrl) {
      const prologueUrl = `${baseRaceUrl}/prologue/result`;
      const oneDayUrl = `${baseRaceUrl}/result`;
      if (isOneDay) {
        candidates.push(oneDayUrl);
      } else {
        const resolvedSlug = extractPcsSlugFromHref(baseRaceUrl);
        const availableLinks = resolvedSlug ? await loadPcsRaceResultLinks(resolvedSlug, race.year) : [];
        const stageLinks = availableLinks
          .filter((url) => /\/stage-\d+\/result$/i.test(url))
          .sort((a, b) => {
            const an = Number((a.match(/\/stage-(\d+)\/result$/i) || [])[1] || 0);
            const bn = Number((b.match(/\/stage-(\d+)\/result$/i) || [])[1] || 0);
            return an - bn;
          });
        const prologueLinks = availableLinks.filter((url) => /\/prologue\/result$/i.test(url));

        // Only apply prologue offset when the race numbering is actually shifted in DB.
        // Some races expose a prologue link but still keep stage numbering aligned (stage 1 is stage-1).
        const hasStageOneLink = stageLinks.some((url) => /\/stage-1\/result$/i.test(url));
        const hasPrologueShift = raceSlugNormalized.includes('tour-down-under')
          || (prologueLinks.length > 0 && !hasStageOneLink);
        const requestedStageNo = Number(stage.stage_number || 0);
        const mappedStageNo = hasPrologueShift ? Math.max(0, requestedStageNo - 1) : requestedStageNo;

        if (hasPrologueShift && requestedStageNo === 1) {
          // Explicitly prefer prologue for stage 1.
          candidates.push(...prologueLinks);
          candidates.push(prologueUrl);
        } else {
          const mappedStageLink = mappedStageNo > 0
            ? stageLinks.find((url) => new RegExp(`/stage-${mappedStageNo}/result$`, 'i').test(url))
            : '';
          const mappedStageUrl = mappedStageNo > 0
            ? normalizePcsStageResultUrl(`${baseRaceUrl}/stage-${mappedStageNo}/result`)
            : '';
          if (mappedStageLink) candidates.push(mappedStageLink);
          if (mappedStageUrl) candidates.push(mappedStageUrl);

          // Keep same-number stage as fallback only when no prologue shift applies.
          if (!hasPrologueShift) {
            const requestedStageUrl = requestedStageNo > 0
              ? normalizePcsStageResultUrl(`${baseRaceUrl}/stage-${requestedStageNo}/result`)
              : '';
            if (requestedStageUrl) candidates.push(requestedStageUrl);
          }
        }

        // One more safety net when stage links are broken: keep prologue as fallback.
        if (prologueLinks.length) {
          candidates.push(...prologueLinks);
        }
      }
    }
  }

  // Use original stage URL only as final fallback, after PCS canonical mapping.
  if (fromStage) candidates.push(fromStage);

  // Hard override requested: TDU stage 1 must use prologue URL first.
  if (raceSlugNormalized.includes('tour-down-under') && Number(stage.stage_number || 0) === 1) {
    return [
      'https://www.procyclingstats.com/race/tour-down-under/2026/prologue/result'
    ];
  }

  const expanded = [];
  candidates.forEach((url) => {
    const clean = normalizePcsStageResultUrl(url);
    if (!clean) return;
    expanded.push(clean);
    expanded.push(`${clean}/`);
  });

  return unique(expanded).filter((url) => /procyclingstats\.com/i.test(url));
}

function pickPcsRiderNameFromRow($, row) {
  const tds = row.find('td').toArray().map((td) => $(td).text().replace(/\s+/g, ' ').trim());
  for (let i = 0; i < tds.length - 1; i += 1) {
    const riderAndTeam = tds[i] || '';
    const team = tds[i + 1] || '';
    if (!riderAndTeam || !team) continue;
    if (team.length < 3 || riderAndTeam.length <= team.length) continue;
    if (!riderAndTeam.endsWith(team)) continue;

    const candidate = riderAndTeam.slice(0, riderAndTeam.length - team.length).trim();
    if (candidate && /[A-Za-zÀ-ÿ]/.test(candidate) && !/^\d+$/.test(candidate)) {
      return candidate;
    }
  }

  const links = row.find('a[href*="rider/"]');
  for (const a of links.toArray()) {
    const text = $(a).text().replace(/\s+/g, ' ').trim();
    if (text && !/^\d+$/.test(text) && text.length >= 3) return text;
  }

  return '';
}

function buildPcsTimeSplitsUrl(resultUrl) {
  const clean = String(resultUrl || '').trim().replace(/\/$/, '');
  if (!clean) return '';
  const base = clean.replace(/\/result$/i, '');
  if (!/\/prologue$|\/stage-\d+$/i.test(base)) return '';
  return `${base}/live/time-splits`;
}

function parseRankToken(value) {
  const text = normalizeWhitespace(value);
  if (!text) return null;
  const match = text.match(/^(\d{1,4})(?:\s*\(|$)/);
  if (!match) return null;
  const rank = Number(match[1]);
  return Number.isFinite(rank) && rank > 0 ? rank : null;
}

function parseTimeToken(value) {
  const text = normalizeWhitespace(value);
  if (!text || text === '-') return false;
  return /^\d{1,2}[\.,:]\d{1,2}(?:[\.,:]\d+)?$/.test(text);
}

function normalizePcsDisplayName(value) {
  const raw = normalizeWhitespace(value);
  if (!raw) return '';
  return raw
    .split(' ')
    .filter(Boolean)
    .map((part) => {
      const lower = part.toLowerCase();
      return lower.charAt(0).toUpperCase() + lower.slice(1);
    })
    .join(' ');
}

function buildPcsSortedT1Url(raceId) {
  const id = String(raceId || '').trim();
  if (!/^\d+$/.test(id)) return '';
  return `https://www.procyclingstats.com/race.php?id=${id}&p=live&s=time-splits&t1c=1&t9c=1&timesc=1&rnksc=1&avgsc=1&sortby=t1time&filter2=Apply+filter`;
}

async function scrapePcsFastestT1Rider(resultUrl) {
  const resultHtml = await getHtml(resultUrl);
  const $result = cheerio.load(resultHtml);
  const resultText = normalizeWhitespace($result('body').text() || '');

  // Limit this path to TT stages to avoid accidental use on non-TT stages.
  const resultTitle = normalizeWhitespace($result('title').text() || '');
  const ttSignal = /time\s*trial|\(itt\)|\(ttt\)|\bitt\b|\bttt\b/i.test(`${resultTitle} ${resultText}`);
  if (!ttSignal) return { rider: '', rank: null, isTimeTrial: false };

  const raceId = normalizeWhitespace($result('input[name="race_id"]').first().attr('value') || '');
  const sortedUrl = buildPcsSortedT1Url(raceId);
  if (!sortedUrl) return { rider: '', rank: null, isTimeTrial: true };

  const sortedHtml = await getHtml(sortedUrl);
  const $ = cheerio.load(sortedHtml);

  let topRider = '';
  $('tr').each((_, tr) => {
    if (topRider) return;
    const riderLink = $(tr).find('a[href*="rider/"]').first();
    const riderName = normalizePcsDisplayName(riderLink.text());
    if (riderName) topRider = riderName;
  });

  if (!topRider) return { rider: '', rank: null, isTimeTrial: true, url: sortedUrl };
  return { rider: topRider, rank: 1, isTimeTrial: true, url: sortedUrl };
}

async function scrapePcsShieldRiders(url) {
  const html = await getHtml(url);
  const $ = cheerio.load(html);
  const pageText = $('body').text() || '';
  const ttSignal = /time\s*trial|\(itt\)|\(ttt\)|individual\s*time\s*trial/i.test(normalizeWhitespace(pageText));
  const riders = [];
  const titles = [];
  let winnerRider = '';

  $('tr').each((_, tr) => {
    const row = $(tr);
    if (!winnerRider) {
      const winnerCandidate = pickPcsRiderNameFromRow($, row);
      if (winnerCandidate) winnerRider = winnerCandidate;
    }

    const shield = row.find('div.svg_shield');
    if (!shield.length) return;

    const rider = pickPcsRiderNameFromRow($, row);
    if (rider) riders.push(rider);

    const title = normalizeWhitespace(shield.attr('title') || '');
    if (title) titles.push(title);
  });

  return {
    riders: unique(riders),
    titles: unique(titles),
    pageText,
    isTimeTrial: ttSignal,
    winnerRider
  };
}

function parseDateOnly(value) {
  if (!value) return null;
  const m = String(value).match(/(\d{4})-(\d{2})-(\d{2})/);
  if (!m) return null;
  const dt = new Date(`${m[1]}-${m[2]}-${m[3]}T00:00:00Z`);
  return Number.isNaN(dt.getTime()) ? null : dt;
}

function parsePublishedDate(value) {
  if (!value) return null;
  const isoMatch = String(value).match(/\d{4}-\d{2}-\d{2}/);
  if (!isoMatch) return null;
  return parseDateOnly(isoMatch[0]);
}

function parseDutchDateNearStage(text, stageDateValue) {
  const stageDate = parseDateOnly(stageDateValue);
  if (!stageDate) return null;

  const monthMap = {
    jan: 1,
    feb: 2,
    mrt: 3,
    apr: 4,
    mei: 5,
    jun: 6,
    jul: 7,
    aug: 8,
    sep: 9,
    okt: 10,
    nov: 11,
    dec: 12
  };

  const normalized = normalizeWhitespace(text).toLowerCase();
  const match = normalized.match(/\b(\d{1,2})\s+(jan|feb|mrt|apr|mei|jun|jul|aug|sep|okt|nov|dec)\b/i);
  if (!match) return null;

  const day = Number(match[1]);
  const month = monthMap[match[2].toLowerCase()];
  if (!day || !month) return null;

  const stageYear = stageDate.getUTCFullYear();
  const candidates = [stageYear - 1, stageYear, stageYear + 1]
    .map((year) => new Date(Date.UTC(year, month - 1, day)))
    .filter((date) => !Number.isNaN(date.getTime()));

  candidates.sort((a, b) => Math.abs(a.getTime() - stageDate.getTime()) - Math.abs(b.getTime() - stageDate.getTime()));
  return candidates[0] || null;
}

function withinDateWindow(stageDateValue, publishedDate, windowDays) {
  const stageDate = parseDateOnly(stageDateValue);
  if (!stageDate || !publishedDate) return false;
  const diffMs = Math.abs(stageDate.getTime() - publishedDate.getTime());
  return diffMs <= windowDays * 24 * 60 * 60 * 1000;
}

async function loadWielerflitsArchiveArticles() {
  if (cachedArchiveArticles) return cachedArchiveArticles;

  const found = [];
  let unchangedPages = 0;

  for (let page = 1; page <= WIELERFLITS_MAX_PAGES; page += 1) {
    const pageUrl = page === 1
      ? WIELERFLITS_CATEGORY_URL
      : `${WIELERFLITS_CATEGORY_URL}page/${page}/`;

    try {
      const html = await getHtml(pageUrl);
      const $ = cheerio.load(html);
      const before = found.length;
      $('a[href]').each((_, el) => {
        let href = String($(el).attr('href') || '').trim();
        if (!href) return;
        if (href.startsWith('/')) href = `https://www.wielerflits.nl${href}`;
        if (!/^https?:\/\//i.test(href)) return;
        const lower = href.toLowerCase();
        if (!lower.includes('wielerflits.nl/nieuws/')) return;
        if (lower.includes('/category/')) return;
        if (lower.includes('/tag/')) return;
        if (lower.includes('/privacy') || lower.includes('/cookie')) return;

        const normalizedUrl = href.replace(/\/$/, '');
        const container = $(el).closest('article, li, .post, .news-item, .entry').first();
        const dateAttr =
          (container.length ? container.find('time').first().attr('datetime') : '')
          || $(el).siblings('time').first().attr('datetime')
          || $(el).parent().find('time').first().attr('datetime')
          || '';
        const title = normalizeWhitespace(
          (container.length ? container.find('h1,h2,h3').first().text() : '') || $(el).text() || ''
        );
        const metaText = normalizeWhitespace(container.length ? container.text() : '');

        found.push({
          url: normalizedUrl,
          publishedAt: parsePublishedDate(dateAttr),
          title,
          metaText
        });
      });

      const after = found.length;
      if (after === before) {
        unchangedPages += 1;
      } else {
        unchangedPages = 0;
      }
      if (unchangedPages >= 4) break;
    } catch (err) {
      console.warn(`  Could not read WielerFlits page ${pageUrl}: ${err.message}`);
    }

    await sleep(120);
  }

  const seen = new Set();
  const deduped = [];
  found.forEach((item) => {
    if (!item || !item.url || seen.has(item.url)) return;
    seen.add(item.url);
    deduped.push(item);
  });

  cachedArchiveArticles = deduped;
  return cachedArchiveArticles;
}

async function discoverWielerflitsUrls(stage, race) {
  const candidates = [];
  const raceTokens = normalizedRaceTokens(race);
  const allArticles = await loadWielerflitsArchiveArticles();

  allArticles.forEach((article) => {
    const url = article.url;
    const lower = url.toLowerCase();
    const titleLower = normalizeWhitespace(article.title || '').toLowerCase();
    const inferredDate = parseDutchDateNearStage(`${article.metaText || ''} ${article.title || ''}`, stage.start_date);
    const publishedAt = article.publishedAt || inferredDate;

    const hasDateMatch = withinDateWindow(stage.start_date, publishedAt, WIELERFLITS_DATE_WINDOW_DAYS);
    if (!hasDateMatch) return;

    const tokenHits = raceTokens.filter((token) => token.length >= 4 && lower.includes(token)).length;
    const tokenHitsInTitle = raceTokens.filter((token) => token.length >= 4 && titleLower.includes(token)).length;
    if (tokenHits === 0 && tokenHitsInTitle === 0) return;
    candidates.push(url);
  });

  const deduped = unique(candidates)
    .filter((url) => isUrlRelevantToRace(url, stage, race, raceTokens))
    .map((url) => ({
      url,
      score: scoreUrl(url, stage, race, raceTokens)
    }))
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score)
    .map((item) => item.url);

  return deduped.slice(0, 16);
}

async function scrapeBreakawayFromUrl(url) {
  const html = await getHtml(url);
  const $ = cheerio.load(html);

  const unfoldedText = extractHowItUnfoldedText($);
  const articleParagraphText = $('article p')
    .toArray()
    .map((el) => normalizeWhitespace($(el).text()))
    .filter(Boolean)
    .join(' ');

  const articleText = unfoldedText || articleParagraphText || $('main').text() || $('body').text();
  const candidates = getCandidateSentences(articleText);

  const prioritized = [
    ...candidates.filter((line) => DUTCH_PRIORITY_REGEX.test(line)),
    ...candidates.filter((line) => !DUTCH_PRIORITY_REGEX.test(line))
  ];

  for (const sentence of prioritized) {
    const names = extractNamesFromSentence(sentence);
    if (names.length >= 2) {
      return {
        detected: true,
        sentence,
        riders: unique(names),
        pageText: articleText
      };
    }
  }

  const windows = extractBreakawayTextWindows(articleText);
  const prioritizedWindows = [
    ...windows.filter((line) => DUTCH_PRIORITY_REGEX.test(line)),
    ...windows.filter((line) => !DUTCH_PRIORITY_REGEX.test(line))
  ];
  for (const windowText of prioritizedWindows) {
    const namesFromParens = extractNamesFromTeamParentheses(windowText);
    if (namesFromParens.length >= 2) {
      return {
        detected: true,
        sentence: windowText,
        riders: unique(namesFromParens),
        pageText: articleText
      };
    }

    const namesFromSentence = extractNamesFromSentence(windowText);
    if (namesFromSentence.length >= 2) {
      return {
        detected: true,
        sentence: windowText,
        riders: unique(namesFromSentence),
        pageText: articleText
      };
    }
  }

  return {
    detected: false,
    sentence: candidates[0] || '',
    riders: [],
    pageText: articleText
  };
}

function hasStagePassed(stage, now) {
  if (stage.is_rest_day) return false;
  if (!stage.start_date) return false;

  const datePart = String(stage.start_date).trim();
  const timeRaw = stage.start_time ? String(stage.start_time).trim() : '';

  const nowDateLocal = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;

  if (datePart < nowDateLocal) return true;
  if (datePart > nowDateLocal) return false;
  if (!timeRaw) return false;

  let timeNorm = '';
  if (/^\d{1,2}:\d{2}/.test(timeRaw)) {
    timeNorm = timeRaw.slice(0, 5);
  } else {
    const match = timeRaw.match(/T(\d{2}:\d{2})/);
    if (match) timeNorm = match[1];
  }
  if (!timeNorm) return false;

  const nowTime = `${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}`;
  return timeNorm <= nowTime;
}

async function loadPastStages(options = {}) {
  const onlyMissing = Boolean(options.onlyMissing);

  const [{ data: stages, error: stagesError }, { data: races, error: racesError }] = await Promise.all([
    supabase.from('stages').select('id,race_id,stage_number,url,start_date,start_time,is_rest_day').order('id', { ascending: true }),
    supabase.from('races').select('id,name,slug,year')
  ]);

  if (stagesError) throw stagesError;
  if (racesError) throw racesError;

  const raceById = new Map((races || []).map((race) => [String(race.id), race]));
  const stageCountByRaceId = new Map();
  const raceStartDateByRaceId = new Map();
  (stages || []).forEach((stage) => {
    if (stage.is_rest_day) return;
    const key = String(stage.race_id);
    stageCountByRaceId.set(key, (stageCountByRaceId.get(key) || 0) + 1);
    const current = raceStartDateByRaceId.get(key);
    if (!current || String(stage.start_date || '') < current) {
      raceStartDateByRaceId.set(key, String(stage.start_date || ''));
    }
  });
  const now = new Date();

  let filteredStages = (stages || [])
    .filter((stage) => hasStagePassed(stage, now))
    .map((stage) => ({
      ...stage,
      race: raceById.get(String(stage.race_id)) || null,
      race_stage_count_total: stageCountByRaceId.get(String(stage.race_id)) || 1,
      race_start_date_total: raceStartDateByRaceId.get(String(stage.race_id)) || String(stage.start_date || '')
    }))
    .filter((stage) => Boolean(stage.race))
    .filter((stage) => !isWomenRace(stage.race));

  if (onlyMissing && filteredStages.length > 0) {
    const stageIds = filteredStages.map((stage) => stage.id);
    const { data: existingRows, error: existingError } = await supabase
      .from('stage_breakaways')
      .select('stage_id,rider_name')
      .eq('source', SOURCE)
      .in('stage_id', stageIds);

    if (existingError) throw existingError;

    const existingStageIds = new Set(
      (existingRows || [])
        .filter((row) => String(row.rider_name || '').trim().length > 0)
        .map((row) => String(row.stage_id))
    );
    filteredStages = filteredStages.filter((stage) => !existingStageIds.has(String(stage.id)));
  }

  return filteredStages;
}

async function saveBreakaways(stageId, sourceUrl, sentence, riders, confidence, options = {}) {
  if (!riders.length) return 0;
  const rewriteExisting = Boolean(options.rewriteExisting);

  if (rewriteExisting) {
    const { error: deleteError } = await supabase
      .from('stage_breakaways')
      .delete()
      .eq('stage_id', stageId)
      .eq('source', SOURCE);
    if (deleteError) throw deleteError;
  }

  const rows = riders.map((riderName) => ({
    stage_id: stageId,
    rider_name: riderName,
    source: SOURCE,
    source_url: sourceUrl,
    confidence,
    detected_sentence: sentence || null,
    scraped_at: new Date().toISOString()
  }));

  const { error } = await supabase
    .from('stage_breakaways')
    .upsert(rows, { onConflict: 'stage_id,rider_name,source' });

  if (error) throw error;
  return rows.length;
}

async function clearBreakawaysForStage(stageId) {
  const { error } = await supabase
    .from('stage_breakaways')
    .delete()
    .eq('stage_id', stageId)
    .eq('source', SOURCE);

  if (error) throw error;
}

async function run() {
  const arg = process.argv.find((v) => v.startsWith('--limit='));
  const limit = arg ? Number(arg.split('=')[1]) : 0;
  const onlyMissing = process.argv.includes('--only-missing');
  const raceSlugArg = process.argv.find((v) => v.startsWith('--race-slug='));
  const raceSlugFilter = raceSlugArg ? normalizeSlug(raceSlugArg.split('=')[1] || '') : '';
  const rewriteExisting = process.argv.includes('--rewrite-existing');

  const stages = await loadPastStages({ onlyMissing });
  const filteredStages = raceSlugFilter
    ? stages.filter((stage) => {
      const raceSlug = normalizeSlug((stage.race && stage.race.slug) || (stage.race && stage.race.name) || '');
      return raceSlug.includes(raceSlugFilter);
    })
    : stages;
  const targets = limit > 0 ? filteredStages.slice(0, limit) : filteredStages;

  console.log(`Found ${stages.length} passed stages in database.`);
  if (raceSlugFilter) {
    console.log(`Race filter enabled: ${raceSlugFilter}`);
  }
  console.log(`Processing ${targets.length} stage(s)${limit > 0 ? ` (limit=${limit})` : ''}...`);
  if (onlyMissing) {
    console.log('Incremental mode enabled: skipping stages that already have PCS breakaway rows.');
  }
  if (rewriteExisting) {
    console.log('Rewrite mode enabled: existing PCS breakaway rows per processed stage will be replaced.');
  }

  let processed = 0;
  let withHits = 0;
  let insertedRows = 0;
  const failures = [];

  for (const stage of targets) {
    processed += 1;
    const race = stage.race;
    console.log(`\n[${processed}/${targets.length}] ${race.name} ${race.year} - stage ${stage.stage_number} (stage_id=${stage.id})`);

    try {
      const urls = await buildPcsStageResultUrls(stage, race, stage.race_stage_count_total || 1, stage.race_start_date_total || stage.start_date);
      if (!urls.length) {
        console.log('  No candidate PCS stage result URLs found.');
        continue;
      }

      let ttT1 = null;
      for (const url of urls) {
        if (!/\/prologue\/result$|\/stage-\d+\/result$/i.test(String(url || ''))) continue;

        try {
          const ttResult = await scrapePcsFastestT1Rider(url);
          if (ttResult && ttResult.isTimeTrial && ttResult.rider) {
            ttT1 = { ...ttResult, url: ttResult.url || url };
            break;
          }
        } catch (err) {
          console.warn(`  Time-splits scrape failed for ${url}: ${err.message}`);
        }

        await sleep(150);
      }

      if (ttT1 && ttT1.rider) {
        const translatedT1Rider = await translatePcsRiderNameToDbName(ttT1.rider, stage.race_id);
        if (normalizeWhitespace(translatedT1Rider) !== normalizeWhitespace(ttT1.rider)) {
          console.log(`  Name translated: ${ttT1.rider} -> ${translatedT1Rider}`);
        }

        const sentence = `T1_TIME_SPLIT rank=${ttT1.rank || 1}`;
        const wrote = await saveBreakaways(stage.id, ttT1.url, sentence, [translatedT1Rider], 'high', { rewriteExisting: true });
        withHits += 1;
        insertedRows += wrote;
        console.log(`  TT T1 saved ${wrote} rider(s) from ${ttT1.url}`);
        continue;
      }

      let best = null;
      for (const url of urls) {
        try {
          const result = await scrapePcsShieldRiders(url);
          if (!best || result.riders.length > best.riders.length) {
            best = { ...result, url };
          }
          if (result.riders.length >= 2) {
            best = { ...result, url };
            break;
          }
        } catch (err) {
          console.warn(`  Scrape failed for ${url}: ${err.message}`);
        }

        await sleep(250);
      }

      if (!best || !best.riders.length) {
        if (rewriteExisting) {
          await clearBreakawaysForStage(stage.id);
          console.log('  Cleared existing PCS breakaway rows (rewrite mode, no riders detected).');
        }
        console.log('  No PCS shield breakaway riders detected.');
        continue;
      }

      const translatedRidersRaw = await Promise.all(
        best.riders.map((name) => translatePcsRiderNameToDbName(name, stage.race_id))
      );
      const translatedRiders = unique(translatedRidersRaw.filter(Boolean));
      const translations = best.riders
        .map((original, index) => ({ original, translated: translatedRidersRaw[index] }))
        .filter((item) => normalizeWhitespace(item.original) !== normalizeWhitespace(item.translated));

      if (translations.length) {
        translations.forEach((item) => {
          console.log(`  Name translated: ${item.original} -> ${item.translated}`);
        });
      }

      const confidence = best.riders.length >= 3 ? 'high' : 'medium';
      const sentence = best.titles && best.titles.length ? best.titles.join(' | ') : null;
      const wrote = await saveBreakaways(stage.id, best.url, sentence, translatedRiders, confidence, { rewriteExisting });
      withHits += 1;
      insertedRows += wrote;
      console.log(`  Saved ${wrote} rider(s) from ${best.url}`);
    } catch (err) {
      failures.push({ stage_id: stage.id, message: err.message });
      console.error(`  Failed stage ${stage.id}: ${err.message}`);
    }

    await sleep(200);
  }

  console.log('\nBackfill complete.');
  console.log(`Stages processed: ${processed}`);
  console.log(`Stages with detected breakaway riders: ${withHits}`);
  console.log(`Rows upserted into stage_breakaways: ${insertedRows}`);
  console.log(`Failures: ${failures.length}`);
  if (failures.length) {
    failures.slice(0, 20).forEach((f) => {
      console.log(`  - stage_id=${f.stage_id}: ${f.message}`);
    });
  }
}

run().catch((err) => {
  console.error('Backfill failed:', err.message || err);
  process.exit(1);
});
