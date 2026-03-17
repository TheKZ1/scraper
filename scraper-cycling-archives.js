import { parse } from 'https://esm.sh/linkedom';

const FIRSTCYCLING_BASE_URL = 'https://www.firstcycling.com';

const getHeaders = (env) => ({
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'Accept-Language': 'en-US,en;q=0.5',
  'Connection': 'keep-alive',
  ...(env.FIRSTCYCLING_COOKIE ? { 'Cookie': env.FIRSTCYCLING_COOKIE } : {}),
});

const buildAbsoluteUrl = (href) => {
  if (!href) return null;
  if (href.startsWith('//')) return `https:${href}`;
  if (href.startsWith('http://') || href.startsWith('https://')) return href;
  if (href.startsWith('/')) return `${FIRSTCYCLING_BASE_URL}${href}`;
  return `${FIRSTCYCLING_BASE_URL}/${href}`;
};

const normalizeSlug = (value) =>
  String(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/(^-|-$)/g, '');

const buildRaceSlugFromUrl = (url, name) => {
  const match = String(url).match(/\/race\/([^/]+)\/(\d{4})/i);
  if (!match) return normalizeSlug(name || url);
  return normalizeSlug(match[1]);
};

const fetchHtml = async (url, headers) => {
  const res = await fetch(url, { headers });
  if (!res.ok) throw new Error(`Failed to fetch ${url}: ${res.statusText}`);
  return res.text();
};

const getRaceNumberFromUrl = (url) => {
  const match = String(url).match(/[?&]r=(\d+)/);
  return match ? match[1] : null;
};

/**
 * Fetches World Tour races from firstcycling.com
 * @param {number} year - The year to fetch races for
 * @param {string} url - The URL to scrape
 * @param {object} env - Cloudflare Worker environment variables
 * @returns {Promise<Array>} Array of race objects with slug, name, start_date
 */
export async function fetchFirstCyclingWorldTourRaces(year, url, env) {
  const headers = getHeaders(env);

  try {
    const html = await fetchHtml(url, headers);

    if (html.includes('Consent to Cookies') || html.includes('Toon betting advertenties')) {
      console.log('❌ FirstCycling returned consent page. Set FIRSTCYCLING_COOKIE and try again.');
      return [];
    }

    const { document } = parse(html);
    const races = new Map();

    const monthMap = {
      jan: '01', feb: '02', mar: '03', apr: '04', may: '05', jun: '06',
      jul: '07', aug: '08', sep: '09', oct: '10', nov: '11', dec: '12',
      '01': '01', '02': '02', '03': '03', '04': '04', '05': '05', '06': '06',
      '07': '07', '08': '08', '09': '09', '10': '10', '11': '11', '12': '12',
    };

    const datePattern = /\b(\d{1,2})\.(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|\d{1,2})\b/i;

    const rows = document.querySelectorAll('table tr');
    for (const row of rows) {
      const rowText = row.textContent.trim();

      // Skip rows without UWT
      if (!rowText.includes('UWT')) continue;

      // Extract race start date
      let raceStartDate = null;
      const dateMatch = rowText.match(datePattern);
      if (dateMatch) {
        const dayNum = parseInt(dateMatch[1], 10);
        if (dayNum >= 1 && dayNum <= 31) {
          const day = String(dayNum).padStart(2, '0');
          const monthRaw = dateMatch[2].toLowerCase();
          const month = monthMap[monthRaw] || monthRaw.padStart(2, '0');
          raceStartDate = `${year}-${month}-${day}`;
        }
      }

      // Find race links
      const links = row.querySelectorAll('a[href]');
      for (const link of links) {
        const href = link.getAttribute('href');
        const name = link.textContent.trim();

        if (!href || !name) continue;

        if (href.includes('race.php')) {
          const rMatch = href.match(/[?&]r=(\d+)/);
          if (rMatch) {
            const raceUrl = buildAbsoluteUrl(href);
            if (raceUrl && raceUrl.includes('race.php')) {
              const slug = buildRaceSlugFromUrl(href, name);
              if (!races.has(raceUrl)) {
                races.set(raceUrl, {
                  name,
                  url: raceUrl,
                  year,
                  slug,
                  start_date: raceStartDate,
                });
              }
            }
          }
        }
      }
    }

    console.log(`Found ${races.size} UWT races for ${year}`);
    return Array.from(races.values());
  } catch (error) {
    console.error('Error fetching races:', error.message);
    throw error;
  }
}

/**
 * Resolves stage profile image URLs
 * @param {object} race - Race object with url property
 * @param {number} stageNumber - Stage number
 * @returns {Promise<{jpgUrl: string, pngUrl: string}>}
 */
export async function resolveStageProfileImageUrls(race, stageNumber) {
  const raceNumber = getRaceNumberFromUrl(race.url);
  if (!raceNumber) {
    return { jpgUrl: null, pngUrl: null, chosenUrl: null };
  }

  const year = race.year || 2026;
  const raceSlug = String(race && race.slug || '').trim().toLowerCase();
  const raceName = String(race && race.name || '').trim().toLowerCase();
  const isTourDeFrance = raceSlug === 'tour-de-france' || raceName.includes('tour de france');
  const paddedStage = String(stageNumber).padStart(2, '0');
  const baseStandard = `https://firstcycling.com/img/ritt_etappen/${year}_${raceNumber}_${stageNumber}`;
  const basePadded = `https://firstcycling.com/img/ritt_etappen/${year}_${raceNumber}_${paddedStage}`;
  const candidates = isTourDeFrance
    ? [`${basePadded}.jpg`, `${basePadded}.png`, `${baseStandard}.jpg`, `${baseStandard}.png`]
    : [`${baseStandard}.jpg`, `${baseStandard}.png`];

  const jpgUrl = candidates.find((url) => String(url).toLowerCase().endsWith('.jpg')) || null;
  const pngUrl = candidates.find((url) => String(url).toLowerCase().endsWith('.png')) || null;

  for (const url of candidates) {
    try {
      const response = await fetch(url, { method: 'HEAD' });
      if (response.ok) {
        return { jpgUrl, pngUrl, chosenUrl: url };
      }
    } catch (err) {
      // try next extension
    }
  }

  return { jpgUrl, pngUrl, chosenUrl: null };
}
