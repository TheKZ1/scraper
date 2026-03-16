const axios = require('axios');
const cheerio = require('cheerio');

function pickRiderNameFromRow($, row) {
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

  const selectors = [
    'a[href*="/rider/"]',
    'a[href*="rider.php"]',
    'td.rider a',
    'td.cu600 a',
    'td a'
  ];

  for (const selector of selectors) {
    const anchors = row.find(selector).toArray();
    for (const a of anchors) {
      const text = $(a).text().replace(/\s+/g, ' ').trim();
      if (!text) continue;
      // Skip obvious non-name tokens.
      if (/^\d+$/.test(text)) continue;
      if (text.length < 3) continue;
      return text;
    }
  }

  // Fallback: find any cell that looks like a rider name (letters + spaces).
  const cells = row.find('td').toArray().map((td) => $(td).text().replace(/\s+/g, ' ').trim()).filter(Boolean);
  const candidate = cells.find((c) => /[A-Za-zÀ-ÿ]/.test(c) && c.length >= 4 && !/^\d+$/.test(c));
  return candidate || '';
}

async function getShieldRiders(url, debug) {
  const response = await axios.get(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      'Accept-Language': 'en-US,en;q=0.9',
      Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    },
    timeout: 20000
  });

  const $ = cheerio.load(response.data);
  const rows = [];

  // Try common PCS row selectors and keep rows that contain the shield icon.
  $('tr').each((idx, tr) => {
    const row = $(tr);
    const shield = row.find('div.svg_shield');
    if (!shield.length) return;

    const title = shield.attr('title') || '';

    const riderName = pickRiderNameFromRow($, row);
    if (debug) {
      const compactRow = row.text().replace(/\s+/g, ' ').trim();
      console.log(`DEBUG row ${idx}: ${compactRow.slice(0, 220)}`);
      const tdValues = row.find('td').toArray().map((td, tdIdx) => `${tdIdx}:${$(td).text().replace(/\s+/g, ' ').trim()}`);
      console.log(`DEBUG td ${idx}: ${tdValues.join(' | ')}`);
    }

    rows.push({ riderName, title });
  });

  // De-duplicate by rider name + title.
  const seen = new Set();
  const unique = [];
  for (const item of rows) {
    const key = `${item.riderName}||${item.title}`;
    if (seen.has(key)) continue;
    seen.add(key);
    unique.push(item);
  }

  return unique;
}

async function main() {
  const url = process.argv[2] || 'https://www.procyclingstats.com/race/paris-nice/2026/stage-1/result';
  const debug = process.argv.includes('--debug');
  const riders = await getShieldRiders(url, debug);
  console.log(`Found ${riders.length} rider(s) with .svg_shield`);
  riders.forEach((r, idx) => {
    console.log(`${idx + 1}. ${r.riderName || '[unknown]'} | ${r.title}`);
  });
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
