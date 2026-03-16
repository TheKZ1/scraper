const axios = require('axios');
const cheerio = require('cheerio');

function normalizeWhitespace(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function cleanRiderToken(value) {
  return normalizeWhitespace(value)
    .replace(/\([^)]*\)/g, '')
    .replace(/^[-,:;\s]+|[-,:;\s]+$/g, '');
}

function looksLikeRiderName(value) {
  const token = cleanRiderToken(value);
  if (!token) return false;

  // Common rider format: at least two words with capitalized starts.
  const parts = token.split(' ').filter(Boolean);
  if (parts.length < 2 || parts.length > 4) return false;
  return parts.every((part) => /^[A-Z][A-Za-z'`.-]+$/.test(part));
}

function splitPotentialNames(segment) {
  const normalized = normalizeWhitespace(segment)
    .replace(/\s+and\s+/gi, ', ')
    .replace(/\s+&\s+/g, ', ');

  return normalized
    .split(',')
    .map((item) => cleanRiderToken(item))
    .filter((item) => item && looksLikeRiderName(item));
}

function extractNamesFromSentence(sentence) {
  const patterns = [
    /(?:break(?:away)?(?: group)?\s+(?:featuring|including|of|with)|front group\s+(?:including|with))\s+([^\.]+?)(?:\s+(?:escaped|went clear|went up the road|built|opened|would|was|were)|\.|$)/i,
    /([A-Z][^\.]{20,220}?)\s+(?:escaped|went clear|went up the road|slipped away|attacked early)/i,
    /(?:part of|in)\s+(?:a|the)?\s*\d{1,2}-?(?:man|rider|strong)?\s+(?:group|break(?:away)?)\s+(?:that|which)?\s*(?:also\s+)?(?:included|featuring|including)\s+([^\.]+?)(?:\.|$)/i,
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

  return sentences.filter((line) =>
    /(break(?:away)?|front group|went clear|escaped early|up the road|slipped away)/i.test(line)
  );
}

function extractBreakawayTextWindows(text) {
  const windows = [];
  const patterns = [
    /(.{0,280}breakaway.{0,280})/ig,
    /(.{0,280}front group.{0,280})/ig,
    /(.{0,280}\bwent clear\b.{0,280})/ig,
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
    if (tag === 'h1' || tag === 'h2' || tag === 'h3') {
      break;
    }

    if (tag === 'p') {
      const text = normalizeWhitespace(node.text());
      if (text) chunks.push(text);
    }

    node = node.next();
  }

  return chunks.join(' ');
}

async function scrapeBreakaway(url) {
  const response = await axios.get(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      'Accept-Language': 'en-US,en;q=0.9',
    },
    timeout: 20000,
  });

  const $ = cheerio.load(response.data);
  const unfoldedText = extractHowItUnfoldedText($);
  const articleParagraphText = $('article p').toArray().map((el) => normalizeWhitespace($(el).text())).filter(Boolean).join(' ');
  const articleText = unfoldedText || articleParagraphText || $('main').text() || $('body').text();
  const candidates = getCandidateSentences(articleText);

  for (const sentence of candidates) {
    const names = extractNamesFromSentence(sentence);
    if (names.length >= 2) {
      return {
        url,
        detected: true,
        sentence,
        riders: Array.from(new Set(names)),
      };
    }
  }

  // Fallback for pages where breakaway riders are mainly in image captions/gallery text.
  const windows = extractBreakawayTextWindows(articleText);
  for (const windowText of windows) {
    const namesFromParens = extractNamesFromTeamParentheses(windowText);
    if (namesFromParens.length >= 2) {
      return {
        url,
        detected: true,
        sentence: windowText,
        riders: namesFromParens,
      };
    }

    const namesFromSentence = extractNamesFromSentence(windowText);
    if (namesFromSentence.length >= 2) {
      return {
        url,
        detected: true,
        sentence: windowText,
        riders: Array.from(new Set(namesFromSentence)),
      };
    }
  }

  return {
    url,
    detected: false,
    sentence: candidates[0] || '',
    riders: [],
  };
}

async function main() {
  const url = process.argv[2];
  if (!url) {
    console.error('Usage: node scripts/scrape-cyclingnews-breakaway.js <cyclingnews-stage-url>');
    process.exit(1);
  }

  try {
    const result = await scrapeBreakaway(url);
    console.log(JSON.stringify(result, null, 2));
  } catch (err) {
    console.error('Failed to scrape Cyclingnews breakaway:', err.message || err);
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}
