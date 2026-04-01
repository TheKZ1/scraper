const axios = require('axios');
const cheerio = require('cheerio');
const { createClient } = require('@supabase/supabase-js');

const getEnvValue = (key, fallback) => {
  const value = process.env[key];
  return value && value.trim() ? value.trim() : fallback;
};

const SUPABASE_URL = getEnvValue('SUPABASE_URL', 'https://zbvibhtopcsqrnecxgim.supabase.co');
const LEGACY_SUPABASE_SERVICE_ROLE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InpidmliaHRvcGNzcXJuZWN4Z2ltIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MTkyMjI5OCwiZXhwIjoyMDg3NDk4Mjk4fQ.jBvKDo_j0TvZ3FuQ2DpNAQmHXGABuyFcVhfjSc-G42w';
const SUPABASE_KEY = getEnvValue(
  'SUPABASE_SERVICE_ROLE_KEY',
  getEnvValue('SUPABASE_KEY', LEGACY_SUPABASE_SERVICE_ROLE_KEY)
);
const TARGET_POINTS_YEAR = Number(getEnvValue('UCI_POINTS_YEAR', '2025'));
const FORCE_UCI_REFRESH_ALL = String(getEnvValue('FORCE_UCI_REFRESH_ALL', 'false')).toLowerCase() === 'true';

if (!SUPABASE_KEY) {
  throw new Error('Missing SUPABASE_SERVICE_ROLE_KEY environment variable.');
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const PAGE_SIZE = 1000;

const pcsHeaders = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'Accept-Language': 'en-US,en;q=0.5',
  'Connection': 'keep-alive',
};

const pcsCookie = getEnvValue('PCS_COOKIE', '');
if (pcsCookie) {
  pcsHeaders.Cookie = pcsCookie;
}

function normalizeName(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function normalizeCompareName(value) {
  return normalizeName(value)
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase();
}

function tokenizeName(value) {
  const normalized = normalizeCompareName(value);
  if (!normalized) return [];
  return normalized
    .split(/[^a-z0-9]+/)
    .map((token) => token.trim())
    .filter(Boolean);
}

function nameSimilarity(left, right) {
  const a = new Set(tokenizeName(left));
  const b = new Set(tokenizeName(right));
  if (!a.size || !b.size) return 0;

  let intersection = 0;
  a.forEach((token) => {
    if (b.has(token)) intersection += 1;
  });

  const union = new Set([...a, ...b]).size;
  if (!union) return 0;
  return intersection / union;
}

function buildNameTokenKey(value) {
  const normalized = normalizeCompareName(value);
  if (!normalized) return '';
  return normalized
    .split(/[^a-z0-9]+/)
    .map((token) => token.trim())
    .filter(Boolean)
    .sort()
    .join('|');
}

function normalizeRiderUrl(input) {
  const raw = String(input || '').trim();
  if (!raw) return null;

  const ensureAbsolute = (pathPart) => `https://www.procyclingstats.com/${pathPart.replace(/^\/+/, '')}`;

  let url = raw;
  if (url.startsWith('/rider/')) {
    url = ensureAbsolute(url);
  } else if (url.startsWith('rider/')) {
    url = ensureAbsolute(url);
  }

  try {
    const parsed = new URL(url);
    if (!/procyclingstats\.com$/i.test(parsed.hostname)) {
      return null;
    }

    const path = parsed.pathname.replace(/\/$/, '');
    if (!path.startsWith('/rider/')) {
      return null;
    }

    return `https://www.procyclingstats.com${path}`;
  } catch (err) {
    return null;
  }
}

function extractSeasonPointsFromTable($, $table, year) {
  for (const row of $table.find('tr').toArray()) {
    const $row = $(row);
    const cells = $row.find('th,td').toArray().map((cell) => normalizeName($(cell).text()));
    if (cells.length < 2) continue;

    const yearCell = String(cells[0] || '').replace(/\D/g, '');
    if (yearCell !== String(year)) continue;

    const pointsCell = String(cells[1] || '');
    const pointsMatch = pointsCell.match(/\d[\d.,]*/);
    if (!pointsMatch) return null;

    const parsed = Number(pointsMatch[0].replace(/[.,]/g, ''));
    if (!Number.isFinite(parsed)) return null;
    return parsed;
  }

  return null;
}

function findSeasonPoints($, year) {
  let points = null;

  $('h1,h2,h3,h4,strong').each((_, heading) => {
    if (points !== null) return;
    const headingText = normalizeName($(heading).text()).toLowerCase();
    if (!headingText.includes('pcs ranking position per season')) return;

    const $table = $(heading).nextAll('table').first();
    if (!$table.length) return;

    const found = extractSeasonPointsFromTable($, $table, year);
    if (Number.isFinite(found)) {
      points = found;
    }
  });

  if (points !== null) return points;

  $('table').each((_, table) => {
    if (points !== null) return;
    const $table = $(table);
    const headerText = normalizeName($table.find('tr').first().text()).toLowerCase();
    if (!/(points|ranking|season|\#)/.test(headerText)) return;

    const found = extractSeasonPointsFromTable($, $table, year);
    if (Number.isFinite(found)) {
      points = found;
    }
  });

  return points;
}

function extractRiderDisplayName($) {
  const h1Name = normalizeName($('h1').first().text());
  if (h1Name) return h1Name;

  const title = normalizeName($('title').text());
  if (!title) return null;
  return title.split('|')[0].trim() || null;
}

function hasLetter(token) {
  return /[A-Za-zÀ-ÖØ-öø-ÿ]/.test(String(token || ''));
}

function isUppercaseToken(token) {
  const t = normalizeName(token);
  if (!t || !hasLetter(t)) return false;
  return t === t.toUpperCase();
}

function isLikelyPcsCapsName(name) {
  const clean = normalizeName(name);
  if (!clean) return false;
  const tokens = clean.split(' ').filter(Boolean);
  if (tokens.length < 2) return false;
  return isUppercaseToken(tokens[0]);
}

function toPcsCapsName(name) {
  const clean = normalizeName(name);
  if (!clean) return null;
  if (isLikelyPcsCapsName(clean)) return clean;

  const tokens = clean.split(' ').filter(Boolean);
  if (tokens.length < 2) return clean;

  let surnameStart = -1;
  for (let i = 1; i < tokens.length; i += 1) {
    const token = tokens[i];
    if (hasLetter(token) && token === token.toLowerCase()) {
      surnameStart = i;
      break;
    }
  }

  if (surnameStart < 0) {
    surnameStart = tokens.length - 1;
  }

  const firstPart = tokens.slice(0, surnameStart).join(' ');
  const surnamePart = tokens.slice(surnameStart).join(' ').toUpperCase();

  if (!firstPart || !surnamePart) {
    return clean;
  }

  return `${surnamePart} ${firstPart}`;
}

function chooseCanonicalPcsName(scrapedName, candidateName, existingNamesForUrl) {
  const existingNames = Array.isArray(existingNamesForUrl) ? existingNamesForUrl : [];
  const existingPcs = existingNames.find((n) => isLikelyPcsCapsName(n));
  if (existingPcs) return normalizeName(existingPcs);

  if (isLikelyPcsCapsName(candidateName)) return normalizeName(candidateName);
  if (isLikelyPcsCapsName(scrapedName)) return normalizeName(scrapedName);

  const source = normalizeName(scrapedName) || normalizeName(candidateName);
  return toPcsCapsName(source);
}

async function fetchPcsHtml(url) {
  const response = await axios.get(url, {
    headers: pcsHeaders,
    timeout: 20000,
  });
  return response.data;
}

async function resolveRiderUrlFromSearch(riderName) {
  const term = normalizeName(riderName);
  if (!term) return null;

  try {
    const searchUrl = `https://www.procyclingstats.com/search.php?term=${encodeURIComponent(term)}`;
    const html = await fetchPcsHtml(searchUrl);
    const $ = cheerio.load(html);

    let firstMatch = null;
    $('a[href]').each((_, anchor) => {
      if (firstMatch) return;
      const href = String($(anchor).attr('href') || '').trim();
      const normalized = normalizeRiderUrl(href);
      if (!normalized) return;

      const text = normalizeName($(anchor).text());
      if (!text) return;
      firstMatch = normalized;
    });

    return firstMatch;
  } catch (err) {
    return null;
  }
}

async function scrapePcsSeasonPoints(riderUrl, year) {
  const url = normalizeRiderUrl(riderUrl);
  if (!url) {
    return { points: null, riderName: null, sourceUrl: null, error: 'invalid rider url' };
  }

  try {
    const html = await fetchPcsHtml(url);
    const $ = cheerio.load(html);
    const points = findSeasonPoints($, year);
    const riderName = extractRiderDisplayName($);

    return {
      points: Number.isFinite(points) ? points : null,
      riderName: riderName || null,
      sourceUrl: url,
      error: null,
    };
  } catch (err) {
    const status = err && err.response && err.response.status;
    return {
      points: null,
      riderName: null,
      sourceUrl: url,
      error: status ? `http ${status}` : (err.message || 'request failed'),
    };
  }
}

async function fetchAllRows(tableName, columns) {
  const rows = [];
  let from = 0;

  while (true) {
    const to = from + PAGE_SIZE - 1;
    const { data, error } = await supabase
      .from(tableName)
      .select(columns)
      .range(from, to);

    if (error) {
      return { data: null, error };
    }

    const batch = Array.isArray(data) ? data : [];
    rows.push(...batch);

    if (batch.length < PAGE_SIZE) {
      break;
    }

    from += PAGE_SIZE;
  }

  return { data: rows, error: null };
}

async function fetchAllUciRowsForYear(year) {
  const rows = [];
  let from = 0;

  while (true) {
    const to = from + PAGE_SIZE - 1;
    const { data, error } = await supabase
      .from('rider_uci_database')
      .select('rider_name,rider_url,uci_points,year')
      .eq('year', year)
      .range(from, to);

    if (error) {
      return { data: null, error };
    }

    const batch = Array.isArray(data) ? data : [];
    rows.push(...batch);

    if (batch.length < PAGE_SIZE) {
      break;
    }

    from += PAGE_SIZE;
  }

  return { data: rows, error: null };
}

function groupRiderRows(ridersRows) {
  const byUrl = new Map();
  const byName = new Map();

  for (const row of ridersRows || []) {
    const normalizedUrl = normalizeRiderUrl(row.rider_url);
    const normalizedName = normalizeCompareName(row.name);

    if (normalizedUrl) {
      if (!byUrl.has(normalizedUrl)) byUrl.set(normalizedUrl, []);
      byUrl.get(normalizedUrl).push(row);
    }

    if (normalizedName) {
      if (!byName.has(normalizedName)) byName.set(normalizedName, []);
      byName.get(normalizedName).push(row);
    }
  }

  return { byUrl, byName };
}

function buildIncrementalCandidates(ridersRows, existingUciRows, forceRefreshAll) {
  const uciByUrl = new Map();
  const uciByToken = new Map();

  (existingUciRows || []).forEach((row) => {
    const cleanUrl = normalizeRiderUrl(row.rider_url);
    const cleanName = normalizeName(row.rider_name);
    const tokenKey = buildNameTokenKey(cleanName);

    if (cleanUrl && !uciByUrl.has(cleanUrl)) uciByUrl.set(cleanUrl, row);
    if (tokenKey && !uciByToken.has(tokenKey)) uciByToken.set(tokenKey, row);
  });

  const urlMap = new Map();
  const nameOnlyMap = new Map();

  for (const row of ridersRows || []) {
    const cleanName = normalizeName(row.name);
    const tokenKey = buildNameTokenKey(cleanName);
    const normalizedUrl = normalizeRiderUrl(row.rider_url);

    if (normalizedUrl) {
      const cached = uciByUrl.get(normalizedUrl);
      const shouldScrape = forceRefreshAll || !cached || !Number.isFinite(Number(cached.uci_points));
      if (!shouldScrape) continue;

      if (!urlMap.has(normalizedUrl)) {
        urlMap.set(normalizedUrl, {
          rider_url: normalizedUrl,
          rider_name: cleanName || null,
          seen_names: cleanName ? [cleanName] : [],
        });
      } else if (cleanName) {
        const existing = urlMap.get(normalizedUrl);
        existing.seen_names.push(cleanName);
        if (!existing.rider_name) existing.rider_name = cleanName;
        urlMap.set(normalizedUrl, existing);
      }
      continue;
    }

    if (!cleanName || !tokenKey) continue;
    const cachedByName = uciByToken.get(tokenKey);
    const shouldResolve = forceRefreshAll || !cachedByName || !Number.isFinite(Number(cachedByName.uci_points));
    if (!shouldResolve) continue;

    if (!nameOnlyMap.has(tokenKey)) {
      nameOnlyMap.set(tokenKey, {
        rider_name: cleanName,
        seen_names: [cleanName],
      });
    } else {
      const existing = nameOnlyMap.get(tokenKey);
      existing.seen_names.push(cleanName);
      nameOnlyMap.set(tokenKey, existing);
    }
  }

  return {
    urlCandidates: Array.from(urlMap.values()),
    nameOnlyCandidates: Array.from(nameOnlyMap.values()),
  };
}

async function updateRidersByIds(riderRows, payload) {
  if (!Array.isArray(riderRows) || riderRows.length === 0) return 0;
  const ids = riderRows.map((r) => r.id).filter(Boolean);
  if (ids.length === 0) return 0;

  const { error } = await supabase
    .from('riders')
    .update(payload)
    .in('id', ids);

  if (error) {
    console.log(`  ⚠️  Error updating riders by ids: ${error.message}`);
    return 0;
  }

  return ids.length;
}

async function processRiderCandidate(candidate, riderRowsByUrl, riderRowsByName, options = {}) {
  const requireNameMatch = Boolean(options.requireNameMatch);
  const expectedName = normalizeName(candidate && candidate.rider_name);

  const scraped = await scrapePcsSeasonPoints(candidate.rider_url, TARGET_POINTS_YEAR);
  if (!Number.isFinite(scraped.points)) {
    return {
      ok: false,
      reason: scraped.error || 'not found',
      updatedRows: 0,
      upserted: 0,
      resolvedUrl: normalizeRiderUrl(candidate.rider_url),
    };
  }

  if (requireNameMatch && expectedName) {
    const scrapedName = normalizeName(scraped.riderName);
    const similarity = nameSimilarity(expectedName, scrapedName);
    if (!scrapedName || similarity < 0.45) {
      return {
        ok: false,
        reason: `name mismatch (expected="${expectedName}", got="${scrapedName || 'unknown'}")`,
        updatedRows: 0,
        upserted: 0,
        resolvedUrl: normalizeRiderUrl(scraped.sourceUrl || candidate.rider_url),
      };
    }
  }

  const targetUrl = normalizeRiderUrl(scraped.sourceUrl || candidate.rider_url);
  const byUrlMatches = targetUrl ? (riderRowsByUrl.get(targetUrl) || []) : [];

  const fallbackName = normalizeName(scraped.riderName) || normalizeName(candidate.rider_name);
  const byNameMatches = fallbackName ? (riderRowsByName.get(normalizeCompareName(fallbackName)) || []) : [];

  const merged = new Map();
  byUrlMatches.forEach((row) => merged.set(row.id, row));
  byNameMatches.forEach((row) => merged.set(row.id, row));
  const matchedRows = Array.from(merged.values());

  const existingNamesForUrl = byUrlMatches.map((row) => row.name);
  const canonicalName = chooseCanonicalPcsName(scraped.riderName, candidate.rider_name, existingNamesForUrl);

  const { error: upsertError } = await supabase
    .from('rider_uci_database')
    .upsert({
      rider_name: canonicalName || null,
      rider_url: targetUrl,
      uci_points: scraped.points,
      year: TARGET_POINTS_YEAR,
      last_updated: new Date().toISOString(),
    }, { onConflict: 'rider_url' });

  if (upsertError) {
    return {
      ok: false,
      reason: `upsert failed: ${upsertError.message}`,
      updatedRows: 0,
      upserted: 0,
      resolvedUrl: targetUrl,
    };
  }

  const payload = { uci_points: scraped.points };
  if (canonicalName) payload.name = canonicalName;
  if (targetUrl) payload.rider_url = targetUrl;

  const updatedRows = await updateRidersByIds(matchedRows, payload);

  return {
    ok: true,
    reason: null,
    points: scraped.points,
    canonicalName: canonicalName || null,
    updatedRows,
    upserted: 1,
    resolvedUrl: targetUrl,
  };
}

async function syncUciPointsToAllRiderEntries() {
  console.log('🔁 Running final points sync across all rider entries...');

  const { data: uciRows, error: uciError } = await fetchAllUciRowsForYear(TARGET_POINTS_YEAR);

  if (uciError) {
    console.log(`  ⚠️  Could not load rider_uci_database for final sync: ${uciError.message}`);
    return { synced: 0, unmatched: 0, failed: 0 };
  }

  const { data: riderRows, error: ridersError } = await fetchAllRows('riders', 'id,name,rider_url,uci_points');

  if (ridersError) {
    console.log(`  ⚠️  Could not load riders for final sync: ${ridersError.message}`);
    return { synced: 0, unmatched: 0, failed: 0 };
  }

  const byUrl = new Map();
  const byName = new Map();
  const byToken = new Map();

  for (const row of uciRows || []) {
    const cleanUrl = normalizeRiderUrl(row.rider_url);
    const cleanName = normalizeName(row.rider_name);
    const tokenKey = buildNameTokenKey(cleanName);

    if (cleanUrl && !byUrl.has(cleanUrl)) byUrl.set(cleanUrl, row);
    if (cleanName && !byName.has(normalizeCompareName(cleanName))) {
      byName.set(normalizeCompareName(cleanName), row);
    }
    if (tokenKey && !byToken.has(tokenKey)) byToken.set(tokenKey, row);
  }

  let synced = 0;
  let unmatched = 0;
  let failed = 0;

  for (const rider of riderRows || []) {
    const riderUrl = normalizeRiderUrl(rider.rider_url);
    const riderName = normalizeName(rider.name);
    const riderNameKey = normalizeCompareName(riderName);
    const riderTokenKey = buildNameTokenKey(riderName);

    let match = null;
    if (riderUrl && byUrl.has(riderUrl)) {
      match = byUrl.get(riderUrl);
    } else if (riderNameKey && byName.has(riderNameKey)) {
      match = byName.get(riderNameKey);
    } else if (riderTokenKey && byToken.has(riderTokenKey)) {
      match = byToken.get(riderTokenKey);
    }

    if (!match || !Number.isFinite(Number(match.uci_points))) {
      unmatched += 1;
      continue;
    }

    const payload = {
      uci_points: Number(match.uci_points),
    };

    const canonicalName = normalizeName(match.rider_name);
    const canonicalUrl = normalizeRiderUrl(match.rider_url);
    if (canonicalName) payload.name = canonicalName;
    if (canonicalUrl) payload.rider_url = canonicalUrl;

    const { error: updateError } = await supabase
      .from('riders')
      .update(payload)
      .eq('id', rider.id);

    if (updateError) {
      failed += 1;
      continue;
    }

    synced += 1;
  }

  console.log(`  ✅ Final sync complete: synced=${synced}, unmatched=${unmatched}, failed=${failed}`);
  return { synced, unmatched, failed };
}

async function main() {
  console.log(`🔄 Starting PCS points update for ${TARGET_POINTS_YEAR}...\n`);
  if (FORCE_UCI_REFRESH_ALL) {
    console.log('⚠️ FORCE_UCI_REFRESH_ALL=true: all riders will be re-scraped.\n');
  }

  try {
    console.log('📊 Fetching riders and existing points from database...');

    const { data: ridersRows, error: ridersError } = await fetchAllRows('riders', 'id,name,rider_url,uci_points');
    if (ridersError) {
      console.log(`❌ Error fetching riders: ${ridersError.message}`);
      process.exit(1);
    }

    const { data: existingUciRows, error: existingUciError } = await fetchAllUciRowsForYear(TARGET_POINTS_YEAR);
    if (existingUciError) {
      console.log(`❌ Error fetching rider_uci_database: ${existingUciError.message}`);
      process.exit(1);
    }

    const { byUrl: ridersByUrl, byName: ridersByName } = groupRiderRows(ridersRows || []);
    const { urlCandidates, nameOnlyCandidates } = buildIncrementalCandidates(
      ridersRows || [],
      existingUciRows || [],
      FORCE_UCI_REFRESH_ALL
    );

    console.log(`✅ Riders rows: ${(ridersRows || []).length}`);
    console.log(`✅ Existing ${TARGET_POINTS_YEAR} points rows: ${(existingUciRows || []).length}`);
    console.log(`📋 Unique rider URLs to process: ${urlCandidates.length}`);
    console.log(`🔎 Name-only riders to resolve via search: ${nameOnlyCandidates.length}\n`);

    if (urlCandidates.length === 0 && nameOnlyCandidates.length === 0) {
      console.log('ℹ️ No new riders to scrape; running cache sync only.\n');
      const finalSyncOnly = await syncUciPointsToAllRiderEntries();
      console.log('='.repeat(60));
      console.log(`🏁 PCS points sync complete (${TARGET_POINTS_YEAR})`);
      console.log('='.repeat(60));
      console.log('Processed unique riders: 0');
      console.log('✅ Success: 0');
      console.log('❌ Failed: 0');
      console.log('🔎 Resolved via search: 0');
      console.log('💾 Upserted rider_uci_database rows: 0');
      console.log('🔄 Updated riders table rows: 0');
      console.log(`🔁 Final sync updated rider rows: ${finalSyncOnly.synced}`);
      console.log(`🔁 Final sync unmatched riders: ${finalSyncOnly.unmatched}`);
      console.log(`🔁 Final sync failed updates: ${finalSyncOnly.failed}`);
      console.log('='.repeat(60) + '\n');
      return;
    }

    let processed = 0;
    let success = 0;
    let failed = 0;
    let resolvedFromSearch = 0;
    let upsertedUciRows = 0;
    let updatedRidersRows = 0;

    const processedUrls = new Set();

    for (const candidate of urlCandidates) {
      const normalizedUrl = normalizeRiderUrl(candidate.rider_url);
      if (!normalizedUrl || processedUrls.has(normalizedUrl)) continue;
      processedUrls.add(normalizedUrl);

      processed += 1;
      const logLabel = candidate.rider_name || normalizedUrl;
      console.log(`[${processed}] ${logLabel}`);

      const result = await processRiderCandidate(candidate, ridersByUrl, ridersByName);
      if (!result.ok) {
        failed += 1;
        console.log(`  ⚠️  Could not process (${result.reason})`);
      } else {
        success += 1;
        upsertedUciRows += result.upserted;
        updatedRidersRows += result.updatedRows;
        console.log(`  ✅ ${TARGET_POINTS_YEAR} points=${result.points} | name=${result.canonicalName || '(none)'} | rider rows updated=${result.updatedRows}`);
      }

      await sleep(450 + Math.random() * 450);
    }

    for (const candidate of nameOnlyCandidates) {
      const resolvedUrl = await resolveRiderUrlFromSearch(candidate.rider_name);
      if (!resolvedUrl || processedUrls.has(resolvedUrl)) {
        if (!resolvedUrl) {
          failed += 1;
          console.log(`[search] ${candidate.rider_name}: ⚠️ no rider URL found`);
        }
        continue;
      }

      processedUrls.add(resolvedUrl);
      resolvedFromSearch += 1;
      processed += 1;
      console.log(`[${processed}] ${candidate.rider_name} (resolved via search)`);

      const syntheticCandidate = {
        rider_url: resolvedUrl,
        rider_name: candidate.rider_name,
        seen_names: candidate.seen_names || [candidate.rider_name],
      };

      const result = await processRiderCandidate(syntheticCandidate, ridersByUrl, ridersByName, { requireNameMatch: true });
      if (!result.ok) {
        failed += 1;
        console.log(`  ⚠️  Could not process (${result.reason})`);
      } else {
        success += 1;
        upsertedUciRows += result.upserted;
        updatedRidersRows += result.updatedRows;
        console.log(`  ✅ ${TARGET_POINTS_YEAR} points=${result.points} | name=${result.canonicalName || '(none)'} | rider rows updated=${result.updatedRows}`);
      }

      await sleep(500 + Math.random() * 500);
    }

    const finalSync = await syncUciPointsToAllRiderEntries();

    console.log('\n' + '='.repeat(60));
    console.log(`🏁 PCS points update complete (${TARGET_POINTS_YEAR})`);
    console.log('='.repeat(60));
    console.log(`Processed unique riders: ${processed}`);
    console.log(`✅ Success: ${success}`);
    console.log(`❌ Failed: ${failed}`);
    console.log(`🔎 Resolved via search: ${resolvedFromSearch}`);
    console.log(`💾 Upserted rider_uci_database rows: ${upsertedUciRows}`);
    console.log(`🔄 Updated riders table rows: ${updatedRidersRows}`);
    console.log(`🔁 Final sync updated rider rows: ${finalSync.synced}`);
    console.log(`🔁 Final sync unmatched riders: ${finalSync.unmatched}`);
    console.log(`🔁 Final sync failed updates: ${finalSync.failed}`);
    console.log('='.repeat(60) + '\n');

  } catch (error) {
    console.log(`❌ Fatal error: ${error.message || error}`);
    process.exit(1);
  }
}

main();
