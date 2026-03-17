const { createClient } = require('@supabase/supabase-js');
const nodemailer = require('nodemailer');
const dns = require('dns');

const EMAIL_SCRIPT_VERSION = '2026-03-16-email-v1';

const SUPABASE_URL = 'https://zbvibhtopcsqrnecxgim.supabase.co';
const SUPABASE_SERVICE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InpidmliaHRvcGNzcXJuZWN4Z2ltIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MTkyMjI5OCwiZXhwIjoyMDg3NDk4Mjk4fQ.jBvKDo_j0TvZ3FuQ2DpNAQmHXGABuyFcVhfjSc-G42w';

const runStatus = process.env.SCRAPER_REPORT_STATUS || 'unknown';
const runMessage = process.env.SCRAPER_REPORT_MESSAGE || '';
const PLAYER_IDS = { A: 'A', B: 'B' };
const PLAYER_DISPLAY_NAMES = {
  [PLAYER_IDS.A]: 'Ole',
  [PLAYER_IDS.B]: 'Koen'
};
const POINTS = {
  oneDay: { head: 10, t1: 6, grill: 40, rode_lantaarn: 20 },
  monumentOneDay: { head: 20, t1: 12, grill: 80, rode_lantaarn: 40 },
  stage: { head: 5, t1: 3, grill: 20 },
  race: { GC: 10, Sprint: 10, KOM: 20, 'Pogi Trui': 10, 'Rode Lantaarn': 20 }
};
const RACE_CATEGORY_LABELS = {
  GC: 'GC',
  SPRINT: 'Sprint',
  KOM: 'KOM',
  'POGI TRUI': 'Pogi Trui',
  'RODE LANTAARN': 'Rode Lantaarn'
};

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

async function fetchAllTableRows(tableName, selectClause = '*', orderColumn = 'id', pageSize = 1000) {
  const allRows = [];
  let from = 0;

  while (true) {
    const to = from + pageSize - 1;
    let data, error;
    try {
      ({ data, error } = await supabase
        .from(tableName)
        .select(selectClause)
        .order(orderColumn, { ascending: true })
        .range(from, to));
    } catch (e) {
      console.error(`[fetchAllTableRows] Exception for table ${tableName}:`, e);
      throw e;
    }
    if (error) {
      console.error(`[fetchAllTableRows] Supabase error for table ${tableName}:`, error);
      throw error;
    }
    const rows = data || [];
    allRows.push(...rows);
    if (rows.length < pageSize) break;
    from += pageSize;
  }
  return allRows;
}

function parseTimestamp(value) {
  const ts = new Date(value || '').getTime();
  return Number.isFinite(ts) ? ts : null;
}

function isInWindow(ts, fromTs, toTs) {
  if (!Number.isFinite(ts) || !Number.isFinite(toTs)) return false;
  if (Number.isFinite(fromTs)) return ts > fromTs && ts <= toTs;
  return ts <= toTs;
}

function normalizeStageTime(value) {
  if (!value) return '';
  if (value.includes('T')) return value.split('T')[1].slice(0, 5);
  if (value.includes(' ')) return value.split(' ')[1].slice(0, 5);
  return value.slice(0, 5);
}

function getStageStartDateTime(stage) {
  if (!stage || !stage.start_date) return null;
  const timeValue = normalizeStageTime(stage.start_time || '');
  const timePart = timeValue ? `${timeValue}:00` : '00:00:00';
  return new Date(`${stage.start_date}T${timePart}`);
}

function isDnsDnfStatus(status) {
  const s = String(status || '').toUpperCase();
  return s.includes('DNF') || s.includes('DNS');
}

function getStageWinnerName(stage) {
  return String(stage?.winner || stage?.last_year_winner || '').trim();
}

function formatDateTime(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value || '');
  const yyyy = date.getFullYear();
  const mm = String(date.getMonth() + 1).padStart(2, '0');
  const dd = String(date.getDate()).padStart(2, '0');
  const hh = String(date.getHours()).padStart(2, '0');
  const min = String(date.getMinutes()).padStart(2, '0');
  return `${dd}-${mm}-${yyyy} ${hh}:${min}`;
}

function formatDisplayDate(value) {
  if (!value) return '';
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return String(value || '');
  const yyyy = date.getFullYear();
  const mm = String(date.getMonth() + 1).padStart(2, '0');
  const dd = String(date.getDate()).padStart(2, '0');
  return `${dd}-${mm}-${yyyy}`;
}

function formatTimeHM(value) {
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return '';
  const hh = String(date.getHours()).padStart(2, '0');
  const mm = String(date.getMinutes()).padStart(2, '0');
  return `${hh}:${mm}`;
}

function formatDateYMD(value) {
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return '';
  const yyyy = date.getFullYear();
  const mm = String(date.getMonth() + 1).padStart(2, '0');
  const dd = String(date.getDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

function formatCountdown(ms) {
  const totalSeconds = Math.max(0, Math.floor(Number(ms || 0) / 1000));
  const days = Math.floor(totalSeconds / 86400);
  const hours = Math.floor((totalSeconds % 86400) / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  const parts = [];
  if (days > 0) parts.push(`${days}d`);
  parts.push(String(hours).padStart(2, '0'));
  parts.push(String(minutes).padStart(2, '0'));
  parts.push(String(seconds).padStart(2, '0'));
  return days > 0 ? `${parts[0]} ${parts.slice(1).join(':')}` : parts.join(':');
}

function normalizeRaceCategory(value) {
  return String(value || '').toUpperCase().replace(/\s+/g, ' ').trim();
}

function getDisplayNameByPlayerId(playerId) {
  return PLAYER_DISPLAY_NAMES[String(playerId)] || `Player ${playerId}`;
}

function normalizePlayerId(value) {
  const normalized = String(value || '').trim().toUpperCase();
  return normalized === PLAYER_IDS.A || normalized === PLAYER_IDS.B ? normalized : '';
}

function escapeHtml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function getRaceCategoryLabel(value) {
  const key = normalizeRaceCategory(value);
  return RACE_CATEGORY_LABELS[key] || String(value || '');
}

function normalizeRiderName(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function isRiderNameMatch(leftName, rightName) {
  const left = normalizeRiderName(leftName);
  const right = normalizeRiderName(rightName);
  if (!left || !right) return false;
  if (left === right) return true;

  const leftParts = left.split(' ').filter(Boolean);
  const rightParts = right.split(' ').filter(Boolean);
  const minParts = Math.min(leftParts.length, rightParts.length);

  if (minParts >= 2) {
    let partsMatch = true;
    for (let i = 0; i < minParts; i += 1) {
      if (leftParts[i] !== rightParts[i]) {
        partsMatch = false;
        break;
      }
    }
    if (partsMatch) return true;
  }

  return false;
}

function isTeamTimeTrialStage(stage) {
  if (!stage) return false;
  const markers = [
    stage.stage_type,
    stage.type,
    stage.name,
    stage.stage_name,
    stage.title,
    stage.profile_type,
    stage.url
  ]
    .filter(Boolean)
    .map((value) => String(value).toLowerCase())
    .join(' | ');

  if (!markers) return false;
  if (markers.includes('itt')) return false;
  return /\bttt\b|team\s*time\s*trial|ploegen\s*tijdrit/.test(markers);
}

function isIndividualTimeTrialStage(stage) {
  if (!stage) return false;
  const markers = [
    stage.stage_type,
    stage.type,
    stage.name,
    stage.stage_name,
    stage.title,
    stage.profile_type,
    stage.url
  ]
    .filter(Boolean)
    .map((value) => String(value).toLowerCase())
    .join(' | ');

  if (!markers) return false;
  return /\bitt\b|individual\s*time\s*trial|tijdrit|prologue/.test(markers) && !isTeamTimeTrialStage(stage);
}

function getTimeTrialStageLabel(stage) {
  if (!stage) return '';
  if (stage.__ttLabel === 'TTT' || stage.__ttLabel === 'ITT') return stage.__ttLabel;
  if (isTeamTimeTrialStage(stage)) return 'TTT';
  if (isIndividualTimeTrialStage(stage)) return 'ITT';
  return '';
}

function mapRaceCategoryToResultType(category) {
  const normalized = normalizeRaceCategory(category);
  if (normalized === 'GC') return 'GC_WINNER';
  if (normalized === 'SPRINT') return 'POINTS_WINNER';
  if (normalized === 'POGI TRUI') return 'YOUTH_WINNER';
  if (normalized === 'KOM') return 'MOUNTAIN_WINNER';
  if (normalized === 'RODE LANTAARN') return 'LOWEST_GC_FINISHER';
  return '';
}

function getPredictionPoints(prediction, raceInfo) {
  if (!prediction || !prediction.category) return 0;
  if (POINTS.race[prediction.category] !== undefined) {
    return POINTS.race[prediction.category];
  }
  if (raceInfo && raceInfo.isOneDay) {
    const table = raceInfo.isMonument ? POINTS.monumentOneDay : POINTS.oneDay;
    return table[prediction.category] || 0;
  }
  return POINTS.stage[prediction.category] || 0;
}

function pickLatestPredictionRows(rows) {
  const latestByKey = new Map();

  (rows || []).forEach((row) => {
    const raceId = String(row && row.race_id || '').trim();
    const playerId = String(row && row.player || '').trim();
    const categoryKey = normalizeRaceCategory(row && row.category);
    if (!raceId || !playerId || !categoryKey) return;

    const key = `${raceId}::${playerId}::${categoryKey}`;
    const existing = latestByKey.get(key);
    if (!existing) {
      latestByKey.set(key, row);
      return;
    }

    const rowUpdatedTs = parseTimestamp(row && (row.updated_at || row.created_at));
    const existingUpdatedTs = parseTimestamp(existing && (existing.updated_at || existing.created_at));

    if (Number.isFinite(rowUpdatedTs) && Number.isFinite(existingUpdatedTs)) {
      if (rowUpdatedTs > existingUpdatedTs) latestByKey.set(key, row);
      return;
    }

    if (Number.isFinite(rowUpdatedTs) && !Number.isFinite(existingUpdatedTs)) {
      latestByKey.set(key, row);
      return;
    }

    const rowId = Number(row && row.id || 0);
    const existingId = Number(existing && existing.id || 0);
    if (rowId > existingId) latestByKey.set(key, row);
  });

  return Array.from(latestByKey.values());
}

function formatSignedPoints(value) {
  const number = Number(value || 0);
  if (!Number.isFinite(number)) return '0';
  if (number > 0) return `+${number}`;
  return String(number);
}

function parseSettingsJsonValue(rawValue, fallbackValue) {
  if (rawValue === null || rawValue === undefined) return fallbackValue;
  if (typeof rawValue === 'string') {
    try {
      return JSON.parse(rawValue);
    } catch (err) {
      return fallbackValue;
    }
  }
  if (typeof rawValue === 'object') return rawValue;
  return fallbackValue;
}

function normalizeSnapshotEntries(entries) {
  if (!Array.isArray(entries)) return [];
  return entries
    .map((entry) => ({
      race_id: String(entry.race_id || ''),
      race_name: String(entry.race_name || ''),
      rider_id: String(entry.rider_id || ''),
      rider_name: String(entry.rider_name || '').trim()
    }))
    .filter((entry) => entry.race_id && entry.rider_id);
}

function normalizeRiderStatusSnapshotEntries(entries) {
  if (!Array.isArray(entries)) return [];
  return entries
    .map((entry) => ({
      race_id: String(entry.race_id || ''),
      race_name: String(entry.race_name || ''),
      rider_id: String(entry.rider_id || ''),
      rider_name: String(entry.rider_name || '').trim(),
      status: String(entry.status || '').trim()
    }))
    .filter((entry) => entry.race_id && entry.rider_id);
}

function normalizeStageStartSnapshotEntries(entries) {
  if (!Array.isArray(entries)) return [];
  return entries
    .map((entry) => ({
      race_id: String(entry.race_id || ''),
      race_name: String(entry.race_name || ''),
      stage_id: String(entry.stage_id || ''),
      stage_number: Number(entry.stage_number || 0),
      start_date: String(entry.start_date || '').trim(),
      start_time: String(entry.start_time || '').trim()
    }))
    .filter((entry) => entry.race_id && entry.stage_id);
}

function buildSnapshotDiff(currentEntries, previousEntries) {
  const previousMap = new Map(previousEntries.map((entry) => [`${entry.race_id}::${entry.rider_id}`, entry]));
  const currentMap = new Map(currentEntries.map((entry) => [`${entry.race_id}::${entry.rider_id}`, entry]));

  const added = currentEntries.filter((entry) => !previousMap.has(`${entry.race_id}::${entry.rider_id}`));
  const removed = previousEntries.filter((entry) => !currentMap.has(`${entry.race_id}::${entry.rider_id}`));
  return { added, removed };
}

function buildCurrentStartListSnapshot({ riders, futureRaceIds, raceNameById }) {
  const entries = (riders || [])
    .filter((rider) => futureRaceIds.has(String(rider.race_id)))
    .map((rider) => ({
      race_id: String(rider.race_id),
      race_name: raceNameById.get(String(rider.race_id)) || String(rider.race_id),
      rider_id: String(rider.id),
      rider_name: String(rider.name || '').trim()
    }))
    .filter((entry) => entry.rider_id)
    .sort((a, b) => a.race_name.localeCompare(b.race_name) || a.rider_name.localeCompare(b.rider_name));

  return {
    captured_at: new Date().toISOString(),
    entries
  };
}

async function getStartListSnapshotPair() {
  const { data, error } = await supabase
    .from('settings')
    .select('value')
    .eq('id', 'startlist_snapshot_pair')
    .limit(1);

  if (error) throw error;
  const rawValue = data && data[0] ? data[0].value : null;
  const parsed = parseSettingsJsonValue(rawValue, {});

  const latest = parsed && parsed.latest ? {
    captured_at: String(parsed.latest.captured_at || ''),
    entries: normalizeSnapshotEntries(parsed.latest.entries)
  } : null;

  const previous = parsed && parsed.previous ? {
    captured_at: String(parsed.previous.captured_at || ''),
    entries: normalizeSnapshotEntries(parsed.previous.entries)
  } : null;

  return { latest, previous };
}

async function saveStartListSnapshotPair(pairValue) {
  const { error } = await supabase
    .from('settings')
    .upsert({
      id: 'startlist_snapshot_pair',
      value: pairValue,
      updated_at: new Date().toISOString()
    });

  if (error) throw error;
}

function buildCurrentRiderStatusSnapshot({ riders, currentRaceIds, raceNameById }) {
  const entries = (riders || [])
    .filter((rider) => currentRaceIds.has(String(rider.race_id)))
    .map((rider) => ({
      race_id: String(rider.race_id),
      race_name: raceNameById.get(String(rider.race_id)) || String(rider.race_id),
      rider_id: String(rider.id),
      rider_name: String(rider.name || '').trim(),
      status: String(rider.status || '').trim()
    }))
    .filter((entry) => entry.rider_id)
    .sort((a, b) => a.race_name.localeCompare(b.race_name) || a.rider_name.localeCompare(b.rider_name));

  return {
    captured_at: new Date().toISOString(),
    entries
  };
}

async function getRiderStatusSnapshotPair() {
  const { data, error } = await supabase
    .from('settings')
    .select('value')
    .eq('id', 'rider_status_snapshot_pair')
    .limit(1);

  if (error) throw error;
  const rawValue = data && data[0] ? data[0].value : null;
  const parsed = parseSettingsJsonValue(rawValue, {});

  const latest = parsed && parsed.latest ? {
    captured_at: String(parsed.latest.captured_at || ''),
    entries: normalizeRiderStatusSnapshotEntries(parsed.latest.entries)
  } : null;

  const previous = parsed && parsed.previous ? {
    captured_at: String(parsed.previous.captured_at || ''),
    entries: normalizeRiderStatusSnapshotEntries(parsed.previous.entries)
  } : null;

  return { latest, previous };
}

async function saveRiderStatusSnapshotPair(pairValue) {
  const { error } = await supabase
    .from('settings')
    .upsert({
      id: 'rider_status_snapshot_pair',
      value: pairValue,
      updated_at: new Date().toISOString()
    });

  if (error) throw error;
}

function buildCurrentStageStartSnapshot({ stages, relevantRaceIds, raceNameById }) {
  const entries = (stages || [])
    .filter((stage) => !stage.is_rest_day)
    .filter((stage) => relevantRaceIds.has(String(stage.race_id)))
    .map((stage) => ({
      race_id: String(stage.race_id),
      race_name: raceNameById.get(String(stage.race_id)) || String(stage.race_id),
      stage_id: String(stage.id),
      stage_number: Number(stage.stage_number || 0),
      start_date: String(stage.start_date || '').trim(),
      start_time: normalizeStageTime(String(stage.start_time || '').trim())
    }))
    .sort((a, b) => a.race_name.localeCompare(b.race_name) || a.stage_number - b.stage_number);

  return {
    captured_at: new Date().toISOString(),
    entries
  };
}

async function getStageStartSnapshotPair() {
  const { data, error } = await supabase
    .from('settings')
    .select('value')
    .eq('id', 'stage_start_snapshot_pair')
    .limit(1);

  if (error) throw error;
  const rawValue = data && data[0] ? data[0].value : null;
  const parsed = parseSettingsJsonValue(rawValue, {});

  const latest = parsed && parsed.latest ? {
    captured_at: String(parsed.latest.captured_at || ''),
    entries: normalizeStageStartSnapshotEntries(parsed.latest.entries)
  } : null;

  const previous = parsed && parsed.previous ? {
    captured_at: String(parsed.previous.captured_at || ''),
    entries: normalizeStageStartSnapshotEntries(parsed.previous.entries)
  } : null;

  return { latest, previous };
}

async function saveStageStartSnapshotPair(pairValue) {
  const { error } = await supabase
    .from('settings')
    .upsert({
      id: 'stage_start_snapshot_pair',
      value: pairValue,
      updated_at: new Date().toISOString()
    });

  if (error) throw error;
}

function buildStageStartAddedDiff(currentEntries, previousEntries) {
  const previousMap = new Map(previousEntries.map((entry) => [entry.stage_id, entry]));
  const added = [];

  currentEntries.forEach((entry) => {
    const previous = previousMap.get(entry.stage_id);
    const hadStartDate = Boolean(previous && previous.start_date);
    const hadStartTime = Boolean(previous && previous.start_time);
    const hasStartDate = Boolean(entry.start_date);
    const hasStartTime = Boolean(entry.start_time);

    const addedDate = hasStartDate && !hadStartDate;
    const addedTime = hasStartTime && !hadStartTime;
    if (!addedDate && !addedTime) return;

    added.push({
      ...entry,
      added_date: addedDate,
      added_time: addedTime
    });
  });

  return added;
}

function buildRiderStatusDiff(currentEntries, previousEntries, detectedAtIso) {
  const previousMap = new Map(previousEntries.map((entry) => [`${entry.race_id}::${entry.rider_id}`, entry]));
  const changedToDnfDns = [];

  currentEntries.forEach((entry) => {
    const key = `${entry.race_id}::${entry.rider_id}`;
    const previous = previousMap.get(key);
    const currentIsDnfDns = isDnsDnfStatus(entry.status);
    const previousIsDnfDns = previous ? isDnsDnfStatus(previous.status) : false;
    if (currentIsDnfDns && !previousIsDnfDns) {
      changedToDnfDns.push({
        race_id: entry.race_id,
        race_name: entry.race_name,
        rider_id: entry.rider_id,
        rider_name: entry.rider_name,
        status: entry.status,
        detected_at: detectedAtIso
      });
    }
  });

  return changedToDnfDns;
}

function isTlsModeMismatchError(err) {
  const message = String((err && err.message) || err || '').toLowerCase();
  return (
    message.includes('wrong version number') ||
    message.includes('tls_validate_record_header') ||
    message.includes('ssl routines')
  );
}

function isConnectionError(err) {
  const code = String((err && err.code) || '').toUpperCase();
  const message = String((err && err.message) || err || '').toLowerCase();
  return (
    code === 'ETIMEDOUT' ||
    code === 'ECONNREFUSED' ||
    code === 'ECONNRESET' ||
    code === 'EHOSTUNREACH' ||
    code === 'ESOCKET' ||
    message.includes('connection timeout') ||
    message.includes('connect timed out') ||
    message.includes('etimedout')
  );
}

async function sendViaResendIfConfigured(mailPayloads) {
  const resendApiKey = String(process.env.RESEND_API_KEY || '').trim();
  if (!resendApiKey) return false;

  const overrideFrom = String(process.env.RESEND_FROM || '').trim();
  console.log('[EMAIL DEBUG] SMTP failed; attempting Resend HTTPS fallback.');

  for (const payload of mailPayloads) {
    const body = {
      from: overrideFrom || payload.from,
      to: [payload.to],
      subject: payload.subject,
      html: payload.html || undefined,
      text: payload.text || undefined
    };

    const response = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${resendApiKey}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(body)
    });

    if (!response.ok) {
      const details = await response.text();
      throw new Error(`Resend API error ${response.status}: ${details}`);
    }
  }

  console.log(`Scraper report email sent via Resend to: ${mailPayloads.map(payload => payload.to).join(', ')}`);
  return true;
}

function parseFromAddress(fromValue) {
  const raw = String(fromValue || '').trim();
  if (!raw) return { email: '', name: '' };

  const match = raw.match(/^([^<]+)<([^>]+)>$/);
  if (!match) return { email: raw, name: '' };

  return {
    name: String(match[1] || '').trim().replace(/^"|"$/g, ''),
    email: String(match[2] || '').trim()
  };
}

async function sendViaBrevoIfConfigured(mailPayloads) {
  const brevoApiKey = String(process.env.BREVO_API_KEY || '').trim();
  if (!brevoApiKey) return false;

  const overrideFrom = String(process.env.BREVO_FROM || '').trim();
  console.log('[EMAIL DEBUG] SMTP/Resend unavailable; attempting Brevo HTTPS fallback.');

  for (const payload of mailPayloads) {
    const senderParsed = parseFromAddress(overrideFrom || payload.from);
    if (!senderParsed.email) {
      throw new Error('Brevo fallback requires a valid sender email (set BREVO_FROM or SMTP from).');
    }

    const body = {
      sender: {
        email: senderParsed.email,
        ...(senderParsed.name ? { name: senderParsed.name } : {})
      },
      to: [{ email: String(payload.to || '').trim() }],
      subject: payload.subject,
      htmlContent: payload.html || undefined,
      textContent: payload.text || undefined
    };

    const response = await fetch('https://api.brevo.com/v3/smtp/email', {
      method: 'POST',
      headers: {
        'api-key': brevoApiKey,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(body)
    });

    if (!response.ok) {
      const details = await response.text();
      throw new Error(`Brevo API error ${response.status}: ${details}`);
    }
  }

  console.log(`Scraper report email sent via Brevo to: ${mailPayloads.map(payload => payload.to).join(', ')}`);
  return true;
}

function buildTransportConfig(smtp, secureValue) {
  const hostForConnection = smtp.connectHost || smtp.host;
  return {
    host: hostForConnection,
    port: smtp.port,
    secure: secureValue,
    family: 4,
    connectionTimeout: 20000,
    greetingTimeout: 15000,
    socketTimeout: 30000,
    // Force IPv4 lookup because some Windows networks have no working IPv6 route.
    lookup: (hostname, options, callback) => dns.lookup(hostname, { family: 4, all: false }, callback),
    tls: {
      // Keep TLS validation/SNI on the original hostname even if we connect via IPv4 literal.
      servername: smtp.host
    },
    auth: {
      user: smtp.user,
      pass: smtp.pass
    }
  };
}

async function resolveIpv4Host(hostname) {
  try {
    const result = await dns.promises.lookup(hostname, { family: 4, all: false });
    return result && result.address ? result.address : hostname;
  } catch (err) {
    console.warn(`IPv4 DNS lookup failed for ${hostname}; using hostname directly.`);
    return hostname;
  }
}

async function getRecipientMapping() {
  const { data, error } = await supabase
    .from('settings')
    .select('value')
    .eq('id', 'scraper_report_recipients')
    .limit(1);

  if (error) throw error;
  if (!data || data.length === 0) return [];

  const value = data[0].value;
  let emails = [];
  if (value && Array.isArray(value.emails)) {
    emails = value.emails;
  } else if (Array.isArray(value)) {
    emails = value;
  } else if (typeof value === 'string') {
    emails = value.split(',');
  }

  const normalized = emails
    .map((email) => String(email || '').trim())
    .filter(Boolean)
    .filter((email, index, arr) => arr.indexOf(email) === index)
    .slice(0, 2);

  return {
    koenEmail: normalized[0] || '',
    oleEmail: normalized[1] || ''
  };
}

async function getReportResultsRetentionDaysFromSettings() {
  const defaultDays = 2;

  const { data, error } = await supabase
    .from('settings')
    .select('value')
    .eq('id', 'scraper_report_results_retention_days')
    .limit(1);

  if (error) throw error;
  if (!data || data.length === 0) return defaultDays;

  const rawValue = data[0].value;
  const parsedValue = parseSettingsJsonValue(rawValue, null);

  let days = defaultDays;
  if (typeof parsedValue === 'number') {
    days = parsedValue;
  } else if (typeof parsedValue === 'string') {
    days = Number(parsedValue);
  } else if (parsedValue && typeof parsedValue === 'object' && parsedValue.days !== undefined) {
    days = Number(parsedValue.days);
  }

  if (!Number.isFinite(days)) return defaultDays;
  return Math.max(1, Math.min(14, Math.floor(days)));
}

async function getStartlistUpcomingRaceCountFromSettings() {
  const defaultCount = 5;

  const { data, error } = await supabase
    .from('settings')
    .select('value')
    .eq('id', 'scraper_report_startlist_upcoming_race_count')
    .limit(1);

  if (error) throw error;
  if (!data || data.length === 0) return defaultCount;

  const rawValue = data[0].value;
  const parsedValue = parseSettingsJsonValue(rawValue, null);

  let count = defaultCount;
  if (typeof parsedValue === 'number') {
    count = parsedValue;
  } else if (typeof parsedValue === 'string') {
    count = Number(parsedValue);
  } else if (parsedValue && typeof parsedValue === 'object' && parsedValue.count !== undefined) {
    count = Number(parsedValue.count);
  }

  if (!Number.isFinite(count)) return defaultCount;
  return Math.max(1, Math.min(20, Math.floor(count)));
}

function buildDeadlineEvents(races, stages) {
  const raceNameById = new Map((races || []).map(race => [String(race.id), race.name || `Race ${race.id}`]));
  const stagesByRace = new Map();

  (stages || []).forEach((stage) => {
    const raceKey = String(stage.race_id || '');
    if (!raceKey) return;
    if (!stagesByRace.has(raceKey)) stagesByRace.set(raceKey, []);
    stagesByRace.get(raceKey).push(stage);
  });

  const events = [];

  (stages || []).forEach((stage) => {
    const start = getStageStartDateTime(stage);
    if (!start) return;
    events.push({
      type: 'stage',
      time: start.getTime(),
      stageId: String(stage.id),
      raceId: String(stage.race_id),
      raceName: raceNameById.get(String(stage.race_id)) || `Race ${stage.race_id}`,
      stageNumber: stage.stage_number
    });
  });

  (races || []).forEach((race) => {
    const raceStages = (stagesByRace.get(String(race.id)) || [])
      .filter(stage => !stage.is_rest_day)
      .sort((a, b) => Number(a.stage_number || 0) - Number(b.stage_number || 0));

    if (raceStages.length <= 1) return;
    const firstStage = raceStages[0];
    const start = getStageStartDateTime(firstStage);
    if (!start) return;

    events.push({
      type: 'race',
      time: start.getTime(),
      raceId: String(race.id),
      raceName: raceNameById.get(String(race.id)) || `Race ${race.id}`,
      stageNumber: firstStage.stage_number
    });
  });

  return events;
}

function getRequiredStageCategoryKeysForRaceId(raceId, raceIsOneDayById) {
  return raceIsOneDayById && raceIsOneDayById.get(String(raceId))
    ? ['head', 't1', 'grill', 'rode_lantaarn']
    : ['head', 't1', 'grill'];
}

function computeCompletedPredictionSetsForPlayerRows(playerRows, stageById, raceIsOneDayById) {
  const completedStages = new Set();
  const completedRaces = new Set();
  const stageCategorySets = new Map();
  const raceCategorySets = new Map();

  (playerRows || []).forEach((row) => {
    const stageId = String(row.stage_id || '');
    const raceId = String(row.race_id || '');
    if (!stageId || !raceId) return;

    const normalizedStageCategory = String(row.category || '').trim().toLowerCase();
    const requiredStageKeys = getRequiredStageCategoryKeysForRaceId(raceId, raceIsOneDayById);
    if (requiredStageKeys.includes(normalizedStageCategory)) {
      if (!stageCategorySets.has(stageId)) stageCategorySets.set(stageId, new Set());
      stageCategorySets.get(stageId).add(normalizedStageCategory);
    }

    const normalizedRaceCategory = normalizeRaceCategory(row.category || '');
    if (!RACE_CATEGORY_LABELS[normalizedRaceCategory]) return;
    if (!raceCategorySets.has(raceId)) raceCategorySets.set(raceId, new Set());
    raceCategorySets.get(raceId).add(normalizedRaceCategory);
  });

  stageCategorySets.forEach((categories, stageId) => {
    const stage = stageById.get(String(stageId));
    if (!stage) return;
    const requiredStageKeys = getRequiredStageCategoryKeysForRaceId(stage.race_id, raceIsOneDayById);
    const hasAllRequired = requiredStageKeys.every((key) => categories.has(key));
    if (hasAllRequired) completedStages.add(String(stageId));
  });

  raceCategorySets.forEach((categories, raceId) => {
    const requiredRaceKeys = Object.keys(RACE_CATEGORY_LABELS);
    const hasAllRequired = requiredRaceKeys.every((key) => categories.has(key));
    if (hasAllRequired) completedRaces.add(String(raceId));
  });

  return { completedStages, completedRaces };
}

function getUpcomingDeadlineDisplayForPlayer(playerId, referenceTs, events, predictionsRows, stageById, raceIsOneDayById) {
  const rows = (predictionsRows || []).filter(row => String(row.player || '') === String(playerId));
  const sets = computeCompletedPredictionSetsForPlayerRows(rows, stageById, raceIsOneDayById);

  const futureEvents = (events || [])
    .filter(event => event && Number(event.time) > Number(referenceTs || 0))
    .filter((event) => {
      if (event.type === 'stage') return !sets.completedStages.has(String(event.stageId));
      if (event.type === 'race') return !sets.completedRaces.has(String(event.raceId));
      return false;
    })
    .sort((a, b) => a.time - b.time);

  if (futureEvents.length === 0) return [];

  const buildLine = (targetTime) => {
    const sameTimeEvents = futureEvents.filter(event => Math.abs(event.time - targetTime) < 1000);
    const types = new Set(sameTimeEvents.map(event => event.type));
    const typeLabel = types.size > 1
      ? 'Race + Stage predictions'
      : (types.has('race') ? 'Race predictions' : 'Stage predictions');

    const details = sameTimeEvents
      .map(event => event.type === 'race'
        ? `${event.raceName} (race)`
        : `${event.raceName} - Stage ${event.stageNumber}`)
      .join(' | ');

    return {
      typeLabel,
      dateLabel: formatDisplayDate(new Date(targetTime)),
      timeLabel: formatTimeHM(new Date(targetTime)),
      countdown: formatCountdown(targetTime - Number(referenceTs || 0)),
      details
    };
  };

  const nextTime = futureEvents[0].time;
  const lines = [buildLine(nextTime)];
  const nextDateLabel = formatDateYMD(new Date(nextTime));
  const secondCandidate = futureEvents.find(event => event.time > nextTime && formatDateYMD(new Date(event.time)) === nextDateLabel);
  if (secondCandidate) lines.push(buildLine(secondCandidate.time));

  return lines;
}

async function getSmtpConfigFromSettings() {
  const { data, error } = await supabase
    .from('settings')
    .select('value')
    .eq('id', 'scraper_report_smtp')
    .limit(1);

  if (error) throw error;
  const rawValue = data && data[0] ? data[0].value : {};
  let value = rawValue || {};
  if (typeof value === 'string') {
    try {
      value = JSON.parse(value);
    } catch (err) {
      value = {};
    }
  }

  const fromSettings = {
    host: String(value.host || '').trim(),
    port: Number(value.port || 587),
    secure: Boolean(value.secure),
    user: String(value.user || '').trim(),
    pass: String(value.pass || ''),
    from: String(value.from || '').trim()
  };

  // Environment variables still override settings when provided.
  const host = process.env.SCRAPER_SMTP_HOST || fromSettings.host;
  const port = Number(process.env.SCRAPER_SMTP_PORT || fromSettings.port || 587);
  const secure = process.env.SCRAPER_SMTP_SECURE !== undefined
    ? String(process.env.SCRAPER_SMTP_SECURE).toLowerCase() === 'true'
    : fromSettings.secure;
  const user = process.env.SCRAPER_SMTP_USER || fromSettings.user;
  const pass = process.env.SCRAPER_SMTP_PASS || fromSettings.pass;
  const from = process.env.SCRAPER_MAIL_FROM || fromSettings.from || user;

  return { host, port, secure, user, pass, from };
}

async function buildReportText() {
  const { data: runs, error: runsError } = await supabase
    .from('scraper_runs')
    .select('finished_at,status,message')
    .order('finished_at', { ascending: false })
    .limit(2);

  if (runsError) throw runsError;
  const latest = runs && runs[0] ? runs[0] : null;
  const previous = runs && runs[1] ? runs[1] : null;

  if (!latest || !latest.finished_at) {
    return {
      subject: `Scraper report (${runStatus})`,
      body: `Run status: ${runStatus}\nMessage: ${runMessage || '-'}\n\nNo scraper run found in database.`
    };
  }

  const toTs = parseTimestamp(latest.finished_at);
  const fromTs = previous ? parseTimestamp(previous.finished_at) : null;
  const reportResultsRetentionDays = await getReportResultsRetentionDaysFromSettings();
  const upcomingRaceCount = await getStartlistUpcomingRaceCountFromSettings();
  const reportResultsRetentionLabel = `${reportResultsRetentionDays} day${reportResultsRetentionDays === 1 ? '' : 's'}`;

  const [allRaces, allRiders, allStages, allPredictions, allBreakaways, allRaceClassificationResults] = await Promise.all([
    fetchAllTableRows('races', '*', 'id'),
    fetchAllTableRows('riders', '*', 'id'),
    fetchAllTableRows('stages', '*', 'id'),
    fetchAllTableRows('predictions', '*', 'id'),
    fetchAllTableRows('stage_breakaways', '*', 'id'),
    fetchAllTableRows('race_classification_results', '*', 'id')
  ]);
  const deadlineEvents = buildDeadlineEvents(allRaces, allStages);

  const raceNameById = new Map(allRaces.map((race) => [String(race.id), race.name || `Race ${race.id}`]));
  const stageById = new Map(allStages.map((stage) => [String(stage.id), stage]));
  const riderById = new Map(allRiders.map((rider) => [String(rider.id), rider]));
  const raceIsOneDayById = new Map();
  const stagesByRaceForType = new Map();
  allStages.forEach((stage) => {
    const raceKey = String(stage.race_id || '');
    if (!raceKey) return;
    if (!stagesByRaceForType.has(raceKey)) stagesByRaceForType.set(raceKey, []);
    stagesByRaceForType.get(raceKey).push(stage);
  });
  stagesByRaceForType.forEach((raceStages, raceId) => {
    const nonRestCount = raceStages.filter(stage => !stage.is_rest_day).length;
    raceIsOneDayById.set(String(raceId), nonRestCount <= 1);
  });

  const nowTs = Date.now();
  const oneDayMs = 24 * 60 * 60 * 1000;
  const resultRetentionMs = reportResultsRetentionDays * oneDayMs;
  const stagesByRace = new Map();
  allStages.forEach((stage) => {
    const key = String(stage.race_id);
    if (!stagesByRace.has(key)) stagesByRace.set(key, []);
    stagesByRace.get(key).push(stage);
  });

  const futureRaceIds = new Set();
  const currentRaceIds = new Set();
  const resultVisibleRaceIds = new Set();
  const currentStageRaceIds = new Set();
  const upcomingStageIds = new Set();
  const completedCurrentStageIds = new Set();
  const completedResultVisibleStageIds = new Set();
  const stageTimelineByRace = new Map();
  const resultTimelineByRace = new Map();
  const upcomingRaceCandidates = [];

  stagesByRace.forEach((raceStages, raceId) => {
    const activeStages = raceStages
      .filter((stage) => !stage.is_rest_day)
      .sort((a, b) => (a.stage_number || 0) - (b.stage_number || 0));
    if (!activeStages.length) return;

    const stageStarts = activeStages
      .map((stage) => ({ stage, start: getStageStartDateTime(stage) }))
      .filter((item) => Boolean(item.start))
      .sort((a, b) => a.start.getTime() - b.start.getTime());
    if (!stageStarts.length) return;

    stageTimelineByRace.set(
      raceId,
      stageStarts.map((item) => ({
        stageId: String(item.stage.id),
        stageNumber: item.stage.stage_number,
        startTs: item.start.getTime()
      }))
    );

    const stageResultTimeline = stageStarts
      .map((item) => ({
        stageId: String(item.stage.id),
        stageNumber: item.stage.stage_number,
        updatedTs: parseTimestamp(item.stage.updated_at),
        hasWinner: Boolean(getStageWinnerName(item.stage))
      }))
      .filter((item) => item.hasWinner && Number.isFinite(item.updatedTs))
      .sort((a, b) => a.updatedTs - b.updatedTs);

    if (stageResultTimeline.length > 0) {
      resultTimelineByRace.set(raceId, stageResultTimeline);
    }

    const firstStartTs = stageStarts[0].start.getTime();
    const lastStartTs = stageStarts[stageStarts.length - 1].start.getTime();
    const hasStarted = firstStartTs <= nowTs;
    const hasUpcoming = stageStarts.some((item) => item.start.getTime() > nowTs);
    const isRecentRace = lastStartTs <= nowTs && (nowTs - lastStartTs) <= oneDayMs;
    const isVisibleForResults = lastStartTs <= nowTs && (nowTs - lastStartTs) <= resultRetentionMs;

    if (firstStartTs > nowTs) {
      futureRaceIds.add(raceId);
      upcomingRaceCandidates.push({ raceId, firstStartTs });
    }
    if (hasStarted && (hasUpcoming || isRecentRace)) currentRaceIds.add(raceId);
    if (hasStarted && (hasUpcoming || isVisibleForResults)) resultVisibleRaceIds.add(raceId);
    if (hasStarted && hasUpcoming && activeStages.length > 1) currentStageRaceIds.add(raceId);

    if (currentRaceIds.has(raceId)) {
      stageStarts.forEach((item) => {
        const ts = item.start.getTime();
        if (ts <= nowTs) completedCurrentStageIds.add(String(item.stage.id));
        if (ts > nowTs && currentStageRaceIds.has(raceId)) upcomingStageIds.add(String(item.stage.id));
      });
    }

    if (resultVisibleRaceIds.has(raceId)) {
      stageStarts.forEach((item) => {
        const ts = item.start.getTime();
        if (ts <= nowTs) completedResultVisibleStageIds.add(String(item.stage.id));
      });
    }
  });

  function isRemovedFromStartListStatus(status) {
    const s = String(status || '').toUpperCase();
    return s.includes('WITHDRAW') || s.includes('OUT') || s.includes('SCRATCH') || s.includes('ABANDON');
  }

  const selectedUpcomingRaces = upcomingRaceCandidates
    .slice()
    .sort((a, b) => a.firstStartTs - b.firstStartTs)
    .slice(0, upcomingRaceCount)
    .map((item) => ({
      race_id: String(item.raceId),
      race_name: raceNameById.get(String(item.raceId)) || String(item.raceId),
      first_start_ts: item.firstStartTs
    }));

  const selectedUpcomingRaceIds = new Set(selectedUpcomingRaces.map((item) => item.race_id));

  const startlistCountsByRace = new Map();
  selectedUpcomingRaces.forEach((item) => {
    startlistCountsByRace.set(item.race_id, 0);
  });

  allRiders.forEach((rider) => {
    const raceId = String(rider.race_id);
    if (!selectedUpcomingRaceIds.has(raceId)) return;
    if (isRemovedFromStartListStatus(rider.status)) return;
    startlistCountsByRace.set(raceId, (startlistCountsByRace.get(raceId) || 0) + 1);
  });

  const startlistOverviewEntries = selectedUpcomingRaces.map((item) => ({
    race_id: item.race_id,
    race_name: item.race_name,
    start_date: formatDisplayDate(formatDateYMD(new Date(item.first_start_ts))),
    start_time: formatTimeHM(new Date(item.first_start_ts)),
    rider_count: startlistCountsByRace.get(item.race_id) || 0
  }));

  const startlistDeltaByRace = new Map();
  function ensureStartlistDeltaRace(raceId) {
    if (!startlistDeltaByRace.has(raceId)) {
      startlistDeltaByRace.set(raceId, { added: 0, removed: 0 });
    }
    return startlistDeltaByRace.get(raceId);
  }

  allRiders.forEach((rider) => {
    const raceId = String(rider.race_id);
    if (!selectedUpcomingRaceIds.has(raceId)) return;

    const createdTs = parseTimestamp(rider.created_at);
    if (isInWindow(createdTs, fromTs, toTs) && !isRemovedFromStartListStatus(rider.status)) {
      ensureStartlistDeltaRace(raceId).added += 1;
    }

    const updatedTs = parseTimestamp(rider.updated_at);
    if (isInWindow(updatedTs, fromTs, toTs) && isRemovedFromStartListStatus(rider.status)) {
      ensureStartlistDeltaRace(raceId).removed += 1;
    }
  });

  const startlistDeltaEntries = selectedUpcomingRaces
    .map((item) => {
      const delta = startlistDeltaByRace.get(item.race_id) || { added: 0, removed: 0 };
      return {
        race_id: item.race_id,
        race_name: item.race_name,
        added: delta.added,
        removed: delta.removed,
        net: delta.added - delta.removed
      };
    })
    .filter((item) => item.added !== 0 || item.removed !== 0);

  const removedEntries = allRiders
    .filter((rider) => selectedUpcomingRaceIds.has(String(rider.race_id)) && isRemovedFromStartListStatus(rider.status))
    .map((rider) => ({
      race_id: String(rider.race_id),
      name: String(rider.name || rider.rider_name || 'Unknown rider'),
      status: String(rider.status || 'removed'),
      id: String(rider.id || '')
    }));

  // Exact add/remove detection via rolling snapshot diff.
  let snapshotAddedEntries = [];
  let snapshotRemovedEntries = [];
  let hasExactSnapshotDiff = false;
  try {
    const snapshotPair = await getStartListSnapshotPair();
    const currentSnapshot = buildCurrentStartListSnapshot({
      riders: allRiders,
      futureRaceIds,
      raceNameById
    });

    const previousSnapshotEntries = snapshotPair && snapshotPair.latest
      ? normalizeSnapshotEntries(snapshotPair.latest.entries)
      : [];

    if (previousSnapshotEntries.length > 0) {
      const diff = buildSnapshotDiff(currentSnapshot.entries, previousSnapshotEntries);
      snapshotAddedEntries = diff.added;
      snapshotRemovedEntries = diff.removed;
      hasExactSnapshotDiff = true;
    }

    await saveStartListSnapshotPair({
      latest: currentSnapshot,
      previous: snapshotPair && snapshotPair.latest ? snapshotPair.latest : null
    });
  } catch (err) {
    console.warn('Could not load/save start list snapshots, using fallback add/remove logic.');
  }

  // DNF/DNS transition detection via rolling rider-status snapshots.
  let snapshotDnfDnsTransitions = [];
  let hasExactDnfDnsSnapshotDiff = false;
  try {
    const statusSnapshotPair = await getRiderStatusSnapshotPair();
    const currentStatusSnapshot = buildCurrentRiderStatusSnapshot({
      riders: allRiders,
      currentRaceIds: resultVisibleRaceIds,
      raceNameById
    });

    const previousStatusEntries = statusSnapshotPair && statusSnapshotPair.latest
      ? normalizeRiderStatusSnapshotEntries(statusSnapshotPair.latest.entries)
      : [];

    if (previousStatusEntries.length > 0) {
      snapshotDnfDnsTransitions = buildRiderStatusDiff(
        currentStatusSnapshot.entries,
        previousStatusEntries,
        currentStatusSnapshot.captured_at
      );
      hasExactDnfDnsSnapshotDiff = true;
    }

    await saveRiderStatusSnapshotPair({
      latest: currentStatusSnapshot,
      previous: statusSnapshotPair && statusSnapshotPair.latest ? statusSnapshotPair.latest : null
    });
  } catch (err) {
    console.warn('Could not load/save rider status snapshots, using fallback DNF/DNS logic.');
  }

  // Stage start date/time additions via rolling snapshot diff.
  let stageStartAddedEntries = [];
  let hasExactStageStartSnapshotDiff = false;
  try {
    const stageStartSnapshotPair = await getStageStartSnapshotPair();
    const relevantRaceIds = new Set([...futureRaceIds, ...currentStageRaceIds]);
    const currentStageStartSnapshot = buildCurrentStageStartSnapshot({
      stages: allStages,
      relevantRaceIds,
      raceNameById
    });

    const previousStageEntries = stageStartSnapshotPair && stageStartSnapshotPair.latest
      ? normalizeStageStartSnapshotEntries(stageStartSnapshotPair.latest.entries)
      : [];

    if (previousStageEntries.length > 0) {
      stageStartAddedEntries = buildStageStartAddedDiff(
        currentStageStartSnapshot.entries,
        previousStageEntries
      );
      hasExactStageStartSnapshotDiff = true;
    }

    await saveStageStartSnapshotPair({
      latest: currentStageStartSnapshot,
      previous: stageStartSnapshotPair && stageStartSnapshotPair.latest ? stageStartSnapshotPair.latest : null
    });
  } catch (err) {
    console.warn('Could not load/save stage start snapshots, using fallback stage timing logic.');
  }

  const dnfDnsChanged = allRiders.filter((rider) => {
    if (!resultVisibleRaceIds.has(String(rider.race_id))) return false;
    if (!isDnsDnfStatus(rider.status)) return false;
    return isInWindow(parseTimestamp(rider.updated_at), fromTs, toTs);
  });

  function getDnfDnsStageLabel(rider) {
    const persistedStageNumberDirect = Number(rider && rider.dnf_stage_number);
    if (Number.isFinite(persistedStageNumberDirect) && persistedStageNumberDirect > 0) {
      return `Stage ${persistedStageNumberDirect}`;
    }

    const linkedRider = riderById.get(String(rider && rider.id));
    const persistedStageNumberById = Number(linkedRider && linkedRider.dnf_stage_number);
    if (Number.isFinite(persistedStageNumberById) && persistedStageNumberById > 0) {
      return `Stage ${persistedStageNumberById}`;
    }

    const raceId = String(rider.race_id);
    const timeline = stageTimelineByRace.get(raceId) || [];
    if (!timeline.length) return null;

    const statusTs = parseTimestamp(rider.__snapshotDetectedAt || rider.detected_at || rider.updated_at);
    const targetTs = Number.isFinite(statusTs) ? statusTs : nowTs;

    const raceResultTimeline = resultTimelineByRace.get(raceId) || [];
    if (raceResultTimeline.length > 0) {
      let selectedByResult = null;
      raceResultTimeline.forEach((item) => {
        if (item.updatedTs <= targetTs) selectedByResult = item;
      });

      if (selectedByResult && selectedByResult.stageNumber != null) {
        return `Stage ${selectedByResult.stageNumber}`;
      }
    }

    let selected = null;
    timeline.forEach((item) => {
      if (item.startTs <= targetTs) selected = item;
    });

    // If status changed while the next stage has already started but result is not scraped yet,
    // keep the label on the most recent completed stage start.
    if (selected) {
      const selectedIdx = timeline.findIndex((item) => String(item.stageId) === String(selected.stageId));
      if (selectedIdx > 0) {
        const selectedStage = stageById.get(String(selected.stageId));
        const selectedHasWinner = Boolean(selectedStage && getStageWinnerName(selectedStage));
        if (!selectedHasWinner) {
          for (let i = selectedIdx - 1; i >= 0; i -= 1) {
            const prevStage = stageById.get(String(timeline[i].stageId));
            if (prevStage && getStageWinnerName(prevStage)) {
              selected = timeline[i];
              break;
            }
          }
        }
      }
    }

    if (!selected && Number.isFinite(statusTs)) {
      selected = timeline[0];
    }

    return selected && selected.stageNumber != null
      ? `Stage ${selected.stageNumber}`
      : null;
  }

  const scrapedResults = allStages.filter((stage) => {
    if (!resultVisibleRaceIds.has(String(stage.race_id))) return false;
    if (!completedResultVisibleStageIds.has(String(stage.id))) return false;
    if (!getStageWinnerName(stage)) return false;
    return isInWindow(parseTimestamp(stage.updated_at), fromTs, toTs);
  });

  const hasRiderUpdatedAt = allRiders.some((rider) => Boolean(rider.updated_at));
  const hasStageUpdatedAt = allStages.some((stage) => Boolean(stage.updated_at));

  const currentRaceDnfDnsSnapshot = allRiders.filter(
    (rider) => isDnsDnfStatus(rider.status) && resultVisibleRaceIds.has(String(rider.race_id))
  );

  const transitionDnfDnsEntries = snapshotDnfDnsTransitions
    .filter((entry) => resultVisibleRaceIds.has(String(entry.race_id)))
    .map((entry) => ({
      race_id: entry.race_id,
      name: entry.rider_name,
      status: entry.status,
      id: entry.rider_id,
      __snapshotDetectedAt: entry.detected_at
    }));

  // Keep snapshot diff support, but always include the current-race DNF/DNS snapshot so
  // all active race statuses are visible in report/email.
  const dnfDnsEntriesMap = new Map();
  if (hasRiderUpdatedAt && hasExactDnfDnsSnapshotDiff) {
    transitionDnfDnsEntries.forEach((entry) => {
      const key = `${String(entry.race_id)}::${String(entry.id || entry.name || '').toLowerCase()}`;
      dnfDnsEntriesMap.set(key, entry);
    });
  }

  const fallbackDnfDnsEntries = hasRiderUpdatedAt ? dnfDnsChanged : currentRaceDnfDnsSnapshot;
  const baseDnfDnsEntries = hasRiderUpdatedAt
    ? (hasExactDnfDnsSnapshotDiff ? currentRaceDnfDnsSnapshot : fallbackDnfDnsEntries)
    : currentRaceDnfDnsSnapshot;

  baseDnfDnsEntries.forEach((entry) => {
    const key = `${String(entry.race_id)}::${String(entry.id || entry.name || '').toLowerCase()}`;
    if (!dnfDnsEntriesMap.has(key)) dnfDnsEntriesMap.set(key, entry);
  });

  const dnfDnsEntries = Array.from(dnfDnsEntriesMap.values());

  function parseStageOrder(stageLabel) {
    const match = String(stageLabel || '').match(/(\d+)/);
    return match ? Number(match[1]) : Number.MAX_SAFE_INTEGER;
  }

  function buildGroupedDnfDnsText(entries) {
    if (!entries.length) return '';

    const groupedByRace = new Map();
    entries.forEach((rider) => {
      const raceId = String(rider.race_id);
      const raceName = raceNameById.get(raceId) || raceId;
      const stageLabel = getDnfDnsStageLabel(rider) || 'Unspecified stage';

      if (!groupedByRace.has(raceId)) {
        groupedByRace.set(raceId, { raceName, stages: new Map() });
      }

      const raceEntry = groupedByRace.get(raceId);
      if (!raceEntry.stages.has(stageLabel)) {
        raceEntry.stages.set(stageLabel, []);
      }
      raceEntry.stages.get(stageLabel).push(rider);
    });

    const lines = [];
    const raceItems = Array.from(groupedByRace.values())
      .sort((a, b) => a.raceName.localeCompare(b.raceName));

    raceItems.forEach((raceEntry) => {
      lines.push(`- ${raceEntry.raceName}`);

      const stageItems = Array.from(raceEntry.stages.entries())
        .sort((a, b) => parseStageOrder(a[0]) - parseStageOrder(b[0]) || a[0].localeCompare(b[0]));

      stageItems.forEach(([stageLabel, ridersInStage]) => {
        lines.push(`  ${stageLabel}`);
        ridersInStage
          .slice()
          .sort((a, b) => String(a.name || '').localeCompare(String(b.name || '')))
          .forEach((rider) => lines.push(`  - ${rider.name} (${rider.status})`));
      });
    });

    return lines.join('\n');
  }

  function appendPredictionMarker(name, correctA, correctB) {
    const base = String(name || '').trim() || '—';
    if (correctA && correctB) return `${base} [O+K]`;
    if (correctA) return `${base} [O]`;
    if (correctB) return `${base} [K]`;
    return base;
  }

  function buildGroupedResultsText(entries) {
    if (!entries.length) return '';

    const groupedByRace = new Map();
    entries.forEach((stage) => {
      const raceId = String(stage.race_id);
      const raceName = raceNameById.get(raceId) || raceId;
      if (!groupedByRace.has(raceId)) {
        groupedByRace.set(raceId, { raceName, stages: [] });
      }
      groupedByRace.get(raceId).stages.push(stage);
    });

    const lines = [];
    const raceItems = Array.from(groupedByRace.values())
      .sort((a, b) => a.raceName.localeCompare(b.raceName));

    raceItems.forEach((raceEntry) => {
      lines.push(`- ${raceEntry.raceName}`);
      raceEntry.stages
        .slice()
        .sort((a, b) => (a.stage_number || 0) - (b.stage_number || 0))
        .forEach((stage) => {
          const winner = getStageWinnerName(stage);
          const stageCorrect = stageWinnerCorrectByStagePlayer.get(String(stage.id)) || { A: false, B: false };
          lines.push(`  - Stage ${stage.stage_number}: ${appendPredictionMarker(winner, Boolean(stageCorrect.A), Boolean(stageCorrect.B))}`);
        });
    });

    return lines.join('\n');
  }

  function buildGroupedBreakawaysText(entries, stageById, raceNameById) {
    if (!entries.length) return '';

    const groupedByRace = new Map();
    entries.forEach((item) => {
      const stage = stageById.get(String(item.stage_id));
      if (!stage) return;
      const raceId = String(stage.race_id);
      const raceName = raceNameById.get(raceId) || raceId;
      const stageLabel = `Stage ${stage.stage_number}`;

      if (!groupedByRace.has(raceId)) {
        groupedByRace.set(raceId, { raceName, stages: new Map() });
      }
      const raceEntry = groupedByRace.get(raceId);
      if (!raceEntry.stages.has(stageLabel)) {
        raceEntry.stages.set(stageLabel, {
          stageId: String(stage.id),
          names: []
        });
      }
      const names = Array.isArray(item.rider_names)
        ? item.rider_names
        : [String(item.rider_name || '').trim()];
      raceEntry.stages.get(stageLabel).names.push(...names.filter(Boolean));
    });

    const lines = [];
    const raceItems = Array.from(groupedByRace.values())
      .sort((a, b) => a.raceName.localeCompare(b.raceName));

    raceItems.forEach((raceEntry) => {
      lines.push(`- ${raceEntry.raceName}`);
      Array.from(raceEntry.stages.entries())
        .sort((a, b) => parseStageOrder(a[0]) - parseStageOrder(b[0]))
        .forEach(([stageLabel, stageData]) => {
          const uniqueNames = Array.from(new Set(stageData.names.filter(Boolean))).sort((a, b) => a.localeCompare(b));
          const stagePredictions = breakawayCorrectByStagePlayer.get(stageData.stageId) || { A: new Set(), B: new Set() };
          const markedNames = uniqueNames.map((name) => {
            const correctA = Array.from(stagePredictions.A).some((predictedName) => isRiderNameMatch(predictedName, name));
            const correctB = Array.from(stagePredictions.B).some((predictedName) => isRiderNameMatch(predictedName, name));
            return appendPredictionMarker(name, correctA, correctB);
          });
          lines.push(`  - ${stageLabel}: ${markedNames.join(', ') || '—'}`);
        });
    });

    return lines.join('\n');
  }

  function buildGroupedStageStartText(entries) {
    if (!entries.length) return '';

    const groupedByRace = new Map();
    entries.forEach((item) => {
      const raceId = String(item.race_id);
      const raceName = raceNameById.get(raceId) || item.race_name || raceId;
      if (!groupedByRace.has(raceId)) {
        groupedByRace.set(raceId, { raceName, stages: [] });
      }
      groupedByRace.get(raceId).stages.push(item);
    });

    const lines = [];
    const raceItems = Array.from(groupedByRace.values())
      .sort((a, b) => a.raceName.localeCompare(b.raceName));

    raceItems.forEach((raceEntry) => {
      lines.push(`- ${raceEntry.raceName}`);
      raceEntry.stages
        .slice()
        .sort((a, b) => Number(a.stage_number || 0) - Number(b.stage_number || 0))
        .forEach((stage) => {
          const parts = [];
          if (stage.start_date) parts.push(stage.start_date);
          if (stage.start_time) parts.push(stage.start_time);
          const label = parts.length ? parts.join(' ') : 'unknown';
          lines.push(`  - Stage ${stage.stage_number}: ${label}`);
        });
    });

    return lines.join('\n');
  }

  function buildGroupedRidersByRaceText(entries) {
    if (!entries.length) return '';

    const groupedByRace = new Map();
    entries.forEach((rider) => {
      const raceId = String(rider.race_id);
      const raceName = raceNameById.get(raceId) || raceId;
      if (!groupedByRace.has(raceId)) {
        groupedByRace.set(raceId, { raceName, riders: [] });
      }
      groupedByRace.get(raceId).riders.push(rider);
    });

    const lines = [];
    const raceItems = Array.from(groupedByRace.values())
      .sort((a, b) => a.raceName.localeCompare(b.raceName));

    raceItems.forEach((raceEntry) => {
      lines.push(`- ${raceEntry.raceName}`);
      raceEntry.riders
        .slice()
        .sort((a, b) => String(a.name || '').localeCompare(String(b.name || '')))
        .forEach((rider) => {
          const statusPart = rider.status ? ` (${rider.status})` : '';
          lines.push(`  - ${rider.name || 'Unknown rider'}${statusPart}`);
        });
    });

    return lines.join('\n');
  }

  const scrapedResultEntries = hasStageUpdatedAt
    ? scrapedResults
    : allStages
      .filter((stage) => Boolean(getStageWinnerName(stage)) && resultVisibleRaceIds.has(String(stage.race_id)) && completedResultVisibleStageIds.has(String(stage.id)))
      .sort((a, b) => {
        const dateA = String(a.start_date || '');
        const dateB = String(b.start_date || '');
        if (dateA !== dateB) return dateB.localeCompare(dateA);
        return (b.stage_number || 0) - (a.stage_number || 0);
      })
      .slice(0, 30);

  const allBreakawayNamesByStageId = new Map();
  allBreakaways.forEach((item) => {
    const stage = stageById.get(String(item.stage_id));
    if (!stage) return;
    const stageKey = String(stage.id);
    if (!allBreakawayNamesByStageId.has(stageKey)) allBreakawayNamesByStageId.set(stageKey, new Set());
    const riderName = String(item.rider_name || '').trim();
    if (riderName) allBreakawayNamesByStageId.get(stageKey).add(riderName);
  });

  const breakawayNamesByStageId = new Map();
  allBreakaways.forEach((item) => {
    const stage = stageById.get(String(item.stage_id));
    if (!stage) return;
    if (!resultVisibleRaceIds.has(String(stage.race_id))) return;
    if (!completedResultVisibleStageIds.has(String(stage.id))) return;
    const stageKey = String(stage.id);
    if (!breakawayNamesByStageId.has(stageKey)) breakawayNamesByStageId.set(stageKey, new Set());
    const riderName = String(item.rider_name || '').trim();
    if (riderName) breakawayNamesByStageId.get(stageKey).add(riderName);
  });

  const breakawayEntries = allStages
    .filter((stage) => !stage.is_rest_day)
    .filter((stage) => resultVisibleRaceIds.has(String(stage.race_id)))
    .filter((stage) => completedResultVisibleStageIds.has(String(stage.id)))
    .map((stage) => {
      const stageKey = String(stage.id);
      const names = breakawayNamesByStageId.has(stageKey)
        ? Array.from(breakawayNamesByStageId.get(stageKey)).sort((a, b) => a.localeCompare(b))
        : [];
      return {
        stage_id: stageKey,
        rider_names: names
      };
    })
    .sort((a, b) => {
      const stageA = stageById.get(String(a.stage_id));
      const stageB = stageById.get(String(b.stage_id));
      const raceA = raceNameById.get(String(stageA && stageA.race_id)) || '';
      const raceB = raceNameById.get(String(stageB && stageB.race_id)) || '';
      const raceCompare = raceA.localeCompare(raceB);
      if (raceCompare !== 0) return raceCompare;
      return Number((stageA && stageA.stage_number) || 0) - Number((stageB && stageB.stage_number) || 0);
    });

  const breakawayUpdatedTsByStageId = new Map();
  allBreakaways.forEach((item) => {
    const stageId = String(item && item.stage_id || '').trim();
    if (!stageId) return;
    const ts = parseTimestamp(item && (item.scraped_at || item.updated_at || item.created_at));
    if (!Number.isFinite(ts)) return;
    const existing = breakawayUpdatedTsByStageId.get(stageId);
    if (!Number.isFinite(existing) || ts > existing) {
      breakawayUpdatedTsByStageId.set(stageId, ts);
    }
  });

  const classificationWinnerByRaceType = new Map();
  const classificationUpdatedTsByRaceType = new Map();
  allRaceClassificationResults.forEach((row) => {
    if (String(row && row.source || '') !== 'firstcycling_widget') return;
    const raceId = String(row && row.race_id || '').trim();
    const resultType = String(row && row.result_type || '').trim();
    const riderName = String(row && row.rider_name || '').trim();
    if (!raceId || !resultType || !riderName) return;

    const key = `${raceId}::${resultType}`;
    if (!classificationWinnerByRaceType.has(key)) {
      classificationWinnerByRaceType.set(key, riderName);
    }

    const ts = parseTimestamp(row && (row.updated_at || row.scraped_at || row.created_at));
    if (Number.isFinite(ts)) {
      const existing = classificationUpdatedTsByRaceType.get(key);
      if (!Number.isFinite(existing) || ts > existing) {
        classificationUpdatedTsByRaceType.set(key, ts);
      }
    }
  });

  const stageRaceEndedInWindowIds = new Set();
  stagesByRace.forEach((raceStages, raceId) => {
    const activeStages = (raceStages || [])
      .filter((stage) => !stage.is_rest_day)
      .sort((a, b) => Number(a.stage_number || 0) - Number(b.stage_number || 0));

    if (activeStages.length <= 1) return;

    const stageStarts = activeStages
      .map((stage) => ({ stage, start: getStageStartDateTime(stage) }))
      .filter((item) => Boolean(item.start))
      .sort((a, b) => a.start.getTime() - b.start.getTime());

    if (stageStarts.length > 0) {
      const firstStartTs = stageStarts[0].start.getTime();
      const lastStartTs = stageStarts[stageStarts.length - 1].start.getTime();
      const hasStarted = firstStartTs <= nowTs;
      const hasUpcoming = stageStarts.some((item) => item.start.getTime() > nowTs);
      if (!hasStarted || hasUpcoming) return;
      if ((nowTs - lastStartTs) > resultRetentionMs) return;
    } else {
      const finalStage = activeStages[activeStages.length - 1];
      const finalStageTs = parseTimestamp(finalStage && finalStage.updated_at);
      if (!Number.isFinite(finalStageTs)) return;
      if ((nowTs - finalStageTs) > resultRetentionMs) return;
    }

    stageRaceEndedInWindowIds.add(String(raceId));
  });

  const raceInfoById = new Map(
    allRaces.map((race) => {
      const stageCount = allStages.filter((stage) => String(stage.race_id) === String(race.id)).length;
      return [
        String(race.id),
        {
          isOneDay: stageCount === 1,
          isMonument: Boolean(race.Monument)
        }
      ];
    })
  );

  function isAutoGreenPredictionForReport(prediction) {
    const raceResultType = mapRaceCategoryToResultType(prediction && prediction.category);
    if (raceResultType) {
      const key = `${String(prediction && prediction.race_id || '')}::${raceResultType}`;
      const winner = classificationWinnerByRaceType.get(key);
      if (!winner) return false;
      return normalizeRiderName(winner) === normalizeRiderName(prediction && prediction.rider_name);
    }

    const stage = stageById.get(String(prediction && prediction.stage_id || ''));
    if (!stage) return false;

    const category = String(prediction && prediction.category || '').toLowerCase();
    if (category === 't1') {
      if (getTimeTrialStageLabel(stage) === 'TTT') {
        const normalizedStageWinner = normalizeRiderName(getStageWinnerName(stage));
        if (!normalizedStageWinner) return false;
        return normalizeRiderName(prediction && prediction.rider_name) === normalizedStageWinner;
      }

      const breakawayRiders = allBreakawayNamesByStageId.has(String(stage.id))
        ? Array.from(allBreakawayNamesByStageId.get(String(stage.id)))
        : [];
      return breakawayRiders.some((name) => isRiderNameMatch(name, prediction && prediction.rider_name));
    }

    const normalizedStageWinner = normalizeRiderName(getStageWinnerName(stage));
    if (!normalizedStageWinner) return false;
    return normalizeRiderName(prediction && prediction.rider_name) === normalizedStageWinner;
  }

  function getPredictionResultTs(prediction) {
    const raceResultType = mapRaceCategoryToResultType(prediction && prediction.category);
    if (raceResultType) {
      const key = `${String(prediction && prediction.race_id || '')}::${raceResultType}`;
      return classificationUpdatedTsByRaceType.get(key) || null;
    }

    const stage = stageById.get(String(prediction && prediction.stage_id || ''));
    if (!stage) return null;
    const category = String(prediction && prediction.category || '').toLowerCase();
    if (category === 't1' && getTimeTrialStageLabel(stage) !== 'TTT') {
      return breakawayUpdatedTsByStageId.get(String(stage.id)) || null;
    }
    return parseTimestamp(stage.updated_at);
  }

  const autoPointsByPlayer = {
    [PLAYER_IDS.A]: 0,
    [PLAYER_IDS.B]: 0
  };
  const STAGE_POINT_CATS = ['head', 't1', 'grill'];
  const STAGE_POINT_CAT_LABELS = { head: 'Head', t1: 'T1', grill: 'Grill' };
  function makeEmptyStageCatMap() {
    return { head: 0, t1: 0, grill: 0 };
  }

  const stagePointsByStageId = new Map();
  const pointsDeltaByPlayer = {
    [PLAYER_IDS.A]: 0,
    [PLAYER_IDS.B]: 0
  };
  const pointsDeltaByRaceStage = new Map();

  allPredictions.forEach((prediction) => {
    const playerId = normalizePlayerId(prediction && prediction.player);
    if (!autoPointsByPlayer.hasOwnProperty(playerId)) return;
    if (!isAutoGreenPredictionForReport(prediction)) return;
    const raceResultType = mapRaceCategoryToResultType(prediction && prediction.category);

    const raceInfo = raceInfoById.get(String(prediction && prediction.race_id || '')) || null;
    const points = getPredictionPoints(prediction, raceInfo);
    if (!Number.isFinite(points) || points <= 0) return;

    autoPointsByPlayer[playerId] += points;

    const stageKeyForTotals = String(prediction && prediction.stage_id || '');
    if (!raceResultType && stageKeyForTotals && completedResultVisibleStageIds.has(stageKeyForTotals)) {
      if (!stagePointsByStageId.has(stageKeyForTotals)) {
        stagePointsByStageId.set(stageKeyForTotals, {
          [PLAYER_IDS.A]: makeEmptyStageCatMap(),
          [PLAYER_IDS.B]: makeEmptyStageCatMap()
        });
      }
      const catKey = String(prediction && prediction.category || '').toLowerCase().trim();
      if (STAGE_POINT_CATS.includes(catKey)) {
        stagePointsByStageId.get(stageKeyForTotals)[playerId][catKey] += points;
      }
    }

    const eventTs = getPredictionResultTs(prediction);
    if (!isInWindow(eventTs, fromTs, toTs)) return;
    pointsDeltaByPlayer[playerId] += points;

    const raceName = raceNameById.get(String(prediction && prediction.race_id || '')) || String(prediction && prediction.race_id || 'Race');
    let stageSort = Number.MAX_SAFE_INTEGER;
    let stageLabel = 'Race result';
    let detailLabel = getRaceCategoryLabel(prediction && prediction.category);

    if (!raceResultType) {
      const stage = stageById.get(String(prediction && prediction.stage_id || ''));
      const stageNumber = Number(stage && stage.stage_number || 0);
      stageSort = Number.isFinite(stageNumber) && stageNumber > 0 ? stageNumber : Number.MAX_SAFE_INTEGER;
      stageLabel = Number.isFinite(stageNumber) && stageNumber > 0 ? `Stage ${stageNumber}` : 'Stage ?';
      detailLabel = String(prediction && prediction.category || '').toUpperCase();
    }

    const key = `${raceName}::${stageSort}::${detailLabel}`;
    if (!pointsDeltaByRaceStage.has(key)) {
      pointsDeltaByRaceStage.set(key, {
        raceName,
        stageSort,
        stageLabel,
        detailLabel,
        pointsByPlayer: {
          [PLAYER_IDS.A]: 0,
          [PLAYER_IDS.B]: 0
        }
      });
    }
    pointsDeltaByRaceStage.get(key).pointsByPlayer[playerId] += points;
  });

  const pointsDeltaEntries = Array.from(pointsDeltaByRaceStage.values())
    .sort((a, b) => a.raceName.localeCompare(b.raceName) || a.stageSort - b.stageSort || a.detailLabel.localeCompare(b.detailLabel));

  const stagePointsEntries = allStages
    .filter((stage) => !stage.is_rest_day)
    .filter((stage) => resultVisibleRaceIds.has(String(stage.race_id)))
    .filter((stage) => completedResultVisibleStageIds.has(String(stage.id)))
    .map((stage) => {
      const stageKey = String(stage.id);
      const pointsByPlayer = stagePointsByStageId.get(stageKey) || {
        [PLAYER_IDS.A]: makeEmptyStageCatMap(),
        [PLAYER_IDS.B]: makeEmptyStageCatMap()
      };
      return {
        raceName: raceNameById.get(String(stage.race_id)) || String(stage.race_id),
        stageNumber: Number(stage.stage_number || 0),
        stageId: String(stage.id),
        pointsByPlayer
      };
    })
    .sort((a, b) => a.raceName.localeCompare(b.raceName) || a.stageNumber - b.stageNumber);

  const latestRacePredictions = pickLatestPredictionRows(
    allPredictions.filter((prediction) => {
      const raceId = String(prediction && prediction.race_id || '');
      return stageRaceEndedInWindowIds.has(raceId) && Boolean(mapRaceCategoryToResultType(prediction && prediction.category));
    })
  );

  const racePredictionByRacePlayerCategory = new Map();
  latestRacePredictions.forEach((prediction) => {
    const raceId = String(prediction && prediction.race_id || '').trim();
    const playerId = String(prediction && prediction.player || '').trim();
    const categoryKey = normalizeRaceCategory(prediction && prediction.category);
    if (!raceId || !playerId || !categoryKey) return;
    racePredictionByRacePlayerCategory.set(`${raceId}::${playerId}::${categoryKey}`, prediction);
  });

  const racePredictionResultEntries = Array.from(stageRaceEndedInWindowIds)
    .map((raceId) => {
      const raceName = raceNameById.get(String(raceId)) || String(raceId);
      const categoryRows = Object.keys(RACE_CATEGORY_LABELS)
        .map((categoryKey) => {
          const resultType = mapRaceCategoryToResultType(categoryKey);
          const winner = classificationWinnerByRaceType.get(`${String(raceId)}::${resultType}`) || '';
          const predictionA = racePredictionByRacePlayerCategory.get(`${String(raceId)}::${PLAYER_IDS.A}::${categoryKey}`) || null;
          const predictionB = racePredictionByRacePlayerCategory.get(`${String(raceId)}::${PLAYER_IDS.B}::${categoryKey}`) || null;

          const pickA = String(predictionA && predictionA.rider_name || '').trim();
          const pickB = String(predictionB && predictionB.rider_name || '').trim();
          const pointsValue = POINTS.race[RACE_CATEGORY_LABELS[categoryKey]] || 0;

          return {
            categoryKey,
            categoryLabel: getRaceCategoryLabel(categoryKey),
            winner,
            pickA,
            pickB,
            pointsA: winner && pickA && isRiderNameMatch(pickA, winner) ? pointsValue : 0,
            pointsB: winner && pickB && isRiderNameMatch(pickB, winner) ? pointsValue : 0
          };
        })
        .filter((row) => row.winner || row.pickA || row.pickB);

      const totalA = categoryRows.reduce((sum, row) => sum + Number(row.pointsA || 0), 0);
      const totalB = categoryRows.reduce((sum, row) => sum + Number(row.pointsB || 0), 0);

      return {
        raceId: String(raceId),
        raceName,
        categories: categoryRows,
        totalA,
        totalB
      };
    })
    .filter((entry) => entry.categories.length > 0)
    .sort((a, b) => a.raceName.localeCompare(b.raceName));

  const stageWinnerCorrectByStagePlayer = new Map();
  const breakawayCorrectByStagePlayer = new Map();
  allPredictions.forEach((prediction) => {
    const playerId = normalizePlayerId(prediction && prediction.player);
    if (!playerId) return;
    if (!isAutoGreenPredictionForReport(prediction)) return;
    if (mapRaceCategoryToResultType(prediction && prediction.category)) return;

    const stageId = String(prediction && prediction.stage_id || '');
    if (!stageId) return;
    const category = String(prediction && prediction.category || '').toLowerCase().trim();

    if (category === 't1') {
      const riderName = String(prediction && prediction.rider_name || '').trim();
      if (!riderName) return;
      if (!breakawayCorrectByStagePlayer.has(stageId)) {
        breakawayCorrectByStagePlayer.set(stageId, { A: new Set(), B: new Set() });
      }
      breakawayCorrectByStagePlayer.get(stageId)[playerId].add(riderName);
      return;
    }

    if (!stageWinnerCorrectByStagePlayer.has(stageId)) {
      stageWinnerCorrectByStagePlayer.set(stageId, { A: false, B: false });
    }
    stageWinnerCorrectByStagePlayer.get(stageId)[playerId] = true;
  });

  const fallbackStageStartAddedEntries = allStages
    .filter((stage) => !stage.is_rest_day)
    .filter((stage) => futureRaceIds.has(String(stage.race_id)) || currentStageRaceIds.has(String(stage.race_id)))
    .filter((stage) => Boolean(String(stage.start_date || '').trim()) || Boolean(normalizeStageTime(String(stage.start_time || '').trim())))
    .filter((stage) => isInWindow(parseTimestamp(stage.updated_at), fromTs, toTs))
    .map((stage) => ({
      race_id: String(stage.race_id),
      race_name: raceNameById.get(String(stage.race_id)) || String(stage.race_id),
      stage_id: String(stage.id),
      stage_number: Number(stage.stage_number || 0),
      start_date: String(stage.start_date || '').trim(),
      start_time: normalizeStageTime(String(stage.start_time || '').trim())
    }));

  const stageStartReportEntries = hasExactStageStartSnapshotDiff
    ? stageStartAddedEntries
    : fallbackStageStartAddedEntries;

  const dnfByRaceAndRider = new Map();
  allRiders.forEach((rider) => {
    if (!isDnsDnfStatus(rider.status)) return;
    const key = `${String(rider.race_id)}::${String(rider.id)}`;
    dnfByRaceAndRider.set(key, rider.status || 'DNF/DNS');
  });

  const conflicts = [];
  allPredictions.forEach((prediction) => {
    const raceId = String(prediction.race_id);
    const stageId = String(prediction.stage_id);
    if (!currentStageRaceIds.has(raceId)) return;
    if (!upcomingStageIds.has(stageId)) return;

    const status = dnfByRaceAndRider.get(`${raceId}::${String(prediction.rider_id)}`);
    if (!status) return;

    const stage = stageById.get(stageId);
    conflicts.push({
      player: prediction.player,
      raceName: raceNameById.get(raceId) || raceId,
      stageNumber: stage ? stage.stage_number : '?',
      category: prediction.category,
      riderName: prediction.rider_name || '-',
      status
    });
  });

  const lines = [];
  lines.push(`Run status: ${runStatus}`);
  if (runMessage) lines.push(`Run message: ${runMessage}`);
  lines.push(`Window: ${formatDateTime(previous ? previous.finished_at : latest.finished_at)} -> ${formatDateTime(latest.finished_at)}`);
  lines.push('');

  const reportReferenceTs = Number.isFinite(toTs) ? toTs : Date.now();
  const nextDeadlinesA = getUpcomingDeadlineDisplayForPlayer(PLAYER_IDS.A, reportReferenceTs, deadlineEvents, allPredictions, stageById, raceIsOneDayById);
  const nextDeadlinesB = getUpcomingDeadlineDisplayForPlayer(PLAYER_IDS.B, reportReferenceTs, deadlineEvents, allPredictions, stageById, raceIsOneDayById);
  const formatDeadlineLines = (items) => {
    if (!items || items.length === 0) return ['- none'];
    return items.map((item, index) => `${index + 1}. (${item.typeLabel}) ${item.dateLabel} ${item.timeLabel} - ${item.countdown} | ${item.details}`);
  };

  function buildStartlistOverviewText(entries) {
    if (!entries.length) return '';
    return entries
      .map((entry) => {
        const delta = startlistDeltaByRace.get(String(entry.race_id)) || { added: 0, removed: 0 };
        const net = Number(delta.added || 0) - Number(delta.removed || 0);
        const deltaLabel = net === 0 ? '' : ` (${net > 0 ? `+${net}` : `${net}`})`;
        return `- ${entry.race_name} (${entry.start_date} ${entry.start_time}): ${entry.rider_count} riders${deltaLabel}`;
      })
      .join('\n');
  }

  function buildRemovedRidersBySelectedRacesText(racesList, entries) {
    if (!racesList.length) return '';

    const groupedByRace = new Map();
    entries.forEach((rider) => {
      const raceId = String(rider.race_id);
      if (!groupedByRace.has(raceId)) groupedByRace.set(raceId, []);
      groupedByRace.get(raceId).push(rider);
    });

    const lines = [];
    racesList.forEach((raceEntry) => {
      const raceId = String(raceEntry.race_id);
      const ridersInRace = (groupedByRace.get(raceId) || [])
        .slice()
        .sort((a, b) => String(a.name || '').localeCompare(String(b.name || '')));
      lines.push(`- ${raceEntry.race_name}`);
      if (!ridersInRace.length) {
        lines.push('  - none');
        return;
      }
      ridersInRace.forEach((rider) => {
        const statusPart = rider.status ? ` (${rider.status})` : '';
        lines.push(`  - ${rider.name || 'Unknown rider'}${statusPart}`);
      });
    });

    return lines.join('\n');
  }

  const groupedStartlistOverviewText = buildStartlistOverviewText(startlistOverviewEntries);
  const groupedRemovedRidersText = buildRemovedRidersBySelectedRacesText(selectedUpcomingRaces, removedEntries);

  lines.push(`Startlist size (next ${upcomingRaceCount} upcoming races): ${startlistOverviewEntries.length} races`);
  if (!groupedStartlistOverviewText) {
    lines.push('- none');
  } else {
    lines.push(groupedStartlistOverviewText);
  }

  function formatRacePredictionCell(pick, winner, points) {
    const label = pick || '-';
    if (!pick || !winner) return `${label} (${points})`;
    const matched = isRiderNameMatch(pick, winner);
    return `${label}${matched ? ' *' : ''} (${points})`;
  }

  function buildRacePredictionResultsText(entries) {
    if (!entries.length) return '';

    const nameA = getDisplayNameByPlayerId(PLAYER_IDS.A);
    const nameB = getDisplayNameByPlayerId(PLAYER_IDS.B);
    const lines = [];

    entries.forEach((entry) => {
      lines.push(`- ${entry.raceName}`);
      const col1 = '  Category'.padEnd(18, ' ');
      const col2 = 'Winner'.padEnd(24, ' ');
      const col3 = nameA.padEnd(28, ' ');
      const col4 = nameB;
      lines.push(`${col1}${col2}${col3}${col4}`);

      entry.categories.forEach((row) => {
        const cat = (`  ${row.categoryLabel}`).padEnd(18, ' ');
        const winner = String(row.winner || '-').padEnd(24, ' ');
        const aCell = formatRacePredictionCell(row.pickA, row.winner, row.pointsA).padEnd(28, ' ');
        const bCell = formatRacePredictionCell(row.pickB, row.winner, row.pointsB);
        lines.push(`${cat}${winner}${aCell}${bCell}`);
      });
      lines.push(`  Total points: ${nameA} ${entry.totalA}, ${nameB} ${entry.totalB}`);
    });

    return lines.join('\n');
  }

  lines.push('');

  lines.push(hasExactStageStartSnapshotDiff
    ? `Stage start times added (future/current-stage races, exact diff): ${stageStartReportEntries.length}`
    : (hasStageUpdatedAt
      ? `Stage start times added (future/current-stage races, updated window): ${stageStartReportEntries.length}`
      : `Stage start times snapshot (future/current-stage races): ${stageStartReportEntries.length}`));
  if (stageStartReportEntries.length === 0) {
    lines.push('- none');
  } else {
    lines.push(buildGroupedStageStartText(stageStartReportEntries));
  }
  lines.push('');

  lines.push(`Riders currently removed from start lists (next ${upcomingRaceCount} upcoming races): ${removedEntries.length}`);
  if (!groupedRemovedRidersText) {
    lines.push('- none');
  } else {
    lines.push(groupedRemovedRidersText);
  }
  lines.push('');

  lines.push(hasExactDnfDnsSnapshotDiff
    ? `Riders with DNF/DNS (current races, includes exact snapshot transitions, max ${reportResultsRetentionLabel} after race end):`
    : (hasRiderUpdatedAt ? `Riders with DNF/DNS (current races, max ${reportResultsRetentionLabel} after race end):` : `Riders with DNF/DNS (current races snapshot; updated_at missing; max ${reportResultsRetentionLabel} after race end):`));
  if (dnfDnsEntries.length === 0) {
    lines.push('- none');
  } else {
    lines.push(buildGroupedDnfDnsText(dnfDnsEntries));
  }
  lines.push('');

  lines.push(hasStageUpdatedAt ? `Results updates (current races, completed stages, max ${reportResultsRetentionLabel} after race end):` : `Results snapshot (current races, completed stages; stages.updated_at missing; max ${reportResultsRetentionLabel} after race end):`);
  if (scrapedResultEntries.length === 0) {
    lines.push('- none');
  } else {
    lines.push('Legend: [O] Ole correct, [K] Koen correct, [O+K] both correct');
    lines.push(buildGroupedResultsText(scrapedResultEntries));
  }
  lines.push('');

  lines.push(`Breakaway riders (current races, completed stages, max ${reportResultsRetentionLabel} after race end):`);
  if (breakawayEntries.length === 0) {
    lines.push('- none');
  } else {
    lines.push('Legend: [O] Ole correct, [K] Koen correct, [O+K] both correct');
    lines.push(buildGroupedBreakawaysText(breakawayEntries, stageById, raceNameById));
  }
  lines.push('');

  lines.push(`Race prediction results (ended stage races, max ${reportResultsRetentionLabel} after race end):`);
  if (racePredictionResultEntries.length === 0) {
    lines.push('- none');
  } else {
    lines.push(buildRacePredictionResultsText(racePredictionResultEntries));
    lines.push('* = correct pick');
  }
  lines.push('');

  lines.push(`Points scored per stage (current races, max ${reportResultsRetentionLabel} after race end):`);
  if (stagePointsEntries.length === 0) {
    lines.push('- none');
  } else {
    const groupedStagePointsByRace = new Map();
    stagePointsEntries.forEach((entry) => {
      const raceKey = String(entry.raceName || 'Race');
      if (!groupedStagePointsByRace.has(raceKey)) {
        groupedStagePointsByRace.set(raceKey, {
          raceName: raceKey,
          totalByPlayer: {
            [PLAYER_IDS.A]: makeEmptyStageCatMap(),
            [PLAYER_IDS.B]: makeEmptyStageCatMap()
          },
          stages: []
        });
      }

      const raceEntry = groupedStagePointsByRace.get(raceKey);
      STAGE_POINT_CATS.forEach((cat) => {
        raceEntry.totalByPlayer[PLAYER_IDS.A][cat] += Number((entry.pointsByPlayer[PLAYER_IDS.A] || {})[cat] || 0);
        raceEntry.totalByPlayer[PLAYER_IDS.B][cat] += Number((entry.pointsByPlayer[PLAYER_IDS.B] || {})[cat] || 0);
      });
      raceEntry.stages.push(entry);
    });

    const nameA = getDisplayNameByPlayerId(PLAYER_IDS.A);
    const nameB = getDisplayNameByPlayerId(PLAYER_IDS.B);

    Array.from(groupedStagePointsByRace.values())
      .sort((a, b) => a.raceName.localeCompare(b.raceName))
      .forEach((raceEntry) => {
        const totalA = STAGE_POINT_CATS.reduce((sum, cat) => sum + Number((raceEntry.totalByPlayer[PLAYER_IDS.A] || {})[cat] || 0), 0);
        const totalB = STAGE_POINT_CATS.reduce((sum, cat) => sum + Number((raceEntry.totalByPlayer[PLAYER_IDS.B] || {})[cat] || 0), 0);
        lines.push(`- ${raceEntry.raceName} | Total: ${nameA} ${totalA}, ${nameB} ${totalB}`);
        const col1 = '  Stage'.padEnd(12, ' ');
        const col2 = `${nameA} Head`.padStart(8, ' ');
        const col3 = `${nameA} T1`.padStart(8, ' ');
        const col4 = `${nameA} Grill`.padStart(10, ' ');
        const col5 = `${nameA} Tot`.padStart(8, ' ');
        const col6 = `${nameB} Head`.padStart(8, ' ');
        const col7 = `${nameB} T1`.padStart(8, ' ');
        const col8 = `${nameB} Grill`.padStart(10, ' ');
        const col9 = `${nameB} Tot`.padStart(8, ' ');
        lines.push(`${col1}${col2}${col3}${col4}${col5}${col6}${col7}${col8}${col9}`);

        raceEntry.stages
          .slice()
          .sort((a, b) => Number(a.stageNumber || 0) - Number(b.stageNumber || 0))
          .forEach((entry) => {
            const pointsA = entry.pointsByPlayer[PLAYER_IDS.A] || {};
            const pointsB = entry.pointsByPlayer[PLAYER_IDS.B] || {};
            const totalStageA = STAGE_POINT_CATS.reduce((sum, cat) => sum + Number(pointsA[cat] || 0), 0);
            const totalStageB = STAGE_POINT_CATS.reduce((sum, cat) => sum + Number(pointsB[cat] || 0), 0);
            const stageLabel = `  Stage ${entry.stageNumber}`.padEnd(12, ' ');
            const aHead = String(pointsA.head || 0).padStart(8, ' ');
            const aT1 = String(pointsA.t1 || 0).padStart(8, ' ');
            const aGrill = String(pointsA.grill || 0).padStart(10, ' ');
            const aTotal = String(totalStageA).padStart(8, ' ');
            const bHead = String(pointsB.head || 0).padStart(8, ' ');
            const bT1 = String(pointsB.t1 || 0).padStart(8, ' ');
            const bGrill = String(pointsB.grill || 0).padStart(10, ' ');
            const bTotal = String(totalStageB).padStart(8, ' ');
            lines.push(`${stageLabel}${aHead}${aT1}${aGrill}${aTotal}${bHead}${bT1}${bGrill}${bTotal}`);
          });

        const totalLabel = '  Total'.padEnd(12, ' ');
        const totAHead = String(raceEntry.totalByPlayer[PLAYER_IDS.A].head || 0).padStart(8, ' ');
        const totAT1 = String(raceEntry.totalByPlayer[PLAYER_IDS.A].t1 || 0).padStart(8, ' ');
        const totAGrill = String(raceEntry.totalByPlayer[PLAYER_IDS.A].grill || 0).padStart(10, ' ');
        const totAAll = String(totalA).padStart(8, ' ');
        const totBHead = String(raceEntry.totalByPlayer[PLAYER_IDS.B].head || 0).padStart(8, ' ');
        const totBT1 = String(raceEntry.totalByPlayer[PLAYER_IDS.B].t1 || 0).padStart(8, ' ');
        const totBGrill = String(raceEntry.totalByPlayer[PLAYER_IDS.B].grill || 0).padStart(10, ' ');
        const totBAll = String(totalB).padStart(8, ' ');
        lines.push(`${totalLabel}${totAHead}${totAT1}${totAGrill}${totAAll}${totBHead}${totBT1}${totBGrill}${totBAll}`);
      });
  }
  lines.push('');

  lines.push('Current points standing (auto scoring):');
  const standingEntries = Object.keys(autoPointsByPlayer)
    .map((playerId) => ({ playerId, points: autoPointsByPlayer[playerId] || 0 }))
    .sort((a, b) => b.points - a.points || String(a.playerId).localeCompare(String(b.playerId)));
  if (!standingEntries.length) {
    lines.push('- none');
  } else {
    standingEntries.forEach((entry) => {
      lines.push(`- ${getDisplayNameByPlayerId(entry.playerId)}: ${entry.points}`);
    });
  }
  lines.push('');

  lines.push('Prediction conflicts (current stage races, upcoming stages only):');
  if (conflicts.length === 0) {
    lines.push('- none');
  } else {
    conflicts
      .sort((a, b) => String(a.player).localeCompare(String(b.player)) || a.raceName.localeCompare(b.raceName) || Number(a.stageNumber) - Number(b.stageNumber))
      .forEach((item) => lines.push(`- ${item.player} | ${item.raceName} | Stage ${item.stageNumber} | ${item.category}: ${item.riderName} (${item.status})`));
  }

  const baseBody = lines.join('\n');

  // Always include points per stage in plain text report
  function buildStagePointsText(entries) {
    if (!entries || entries.length === 0) return '- none';
    const grouped = new Map();
    entries.forEach((entry) => {
      const race = entry.raceName;
      if (!grouped.has(race)) grouped.set(race, []);
      grouped.get(race).push(entry);
    });
    let out = [];
    for (const [race, stages] of Array.from(grouped.entries()).sort((a, b) => a[0].localeCompare(b[0]))) {
      out.push(`- ${race}`);
      stages.sort((a, b) => a.stageNumber - b.stageNumber).forEach((entry) => {
        const pointsA = entry.pointsByPlayer[PLAYER_IDS.A] || {};
        const pointsB = entry.pointsByPlayer[PLAYER_IDS.B] || {};
        out.push(`  Stage ${entry.stageNumber}: Ole ${pointsA.head||0}/${pointsA.t1||0}/${pointsA.grill||0} | Koen ${pointsB.head||0}/${pointsB.t1||0}/${pointsB.grill||0}`);
      });
    }
    return out.join('\n');
  }

  const baseBodyWithStagePoints = [
    baseBody,
    '',
    `Points scored per stage (current races, max ${reportResultsRetentionLabel} after race end):`,
    stagePointsEntries.length === 0 ? '- none' : buildStagePointsText(stagePointsEntries),
    ''
  ].join('\n');

  const bodyForOle = [
    'Next 2 race prediction deadlines for Ole at scrape time:',
    ...formatDeadlineLines(nextDeadlinesA),
    '',
    baseBodyWithStagePoints
  ].filter(Boolean).join('\n');

  const bodyForKoen = [
    'Next 2 race prediction deadlines for Koen at scrape time:',
    ...formatDeadlineLines(nextDeadlinesB),
    '',
    baseBodyWithStagePoints
  ].filter(Boolean).join('\n');

  const emailStyles = {
    page: 'margin:0;padding:24px;background:#f5f7fb;font-family:Segoe UI,Arial,sans-serif;color:#1f2937;',
    card: 'margin:0 0 16px 0;padding:16px;background:#ffffff;border:1px solid #dbe3ef;border-radius:10px;',
    h2: 'margin:0 0 10px 0;font-size:18px;color:#0f172a;',
    h3: 'margin:0 0 10px 0;font-size:15px;color:#0f172a;',
    text: 'font-size:13px;line-height:1.45;color:#334155;',
    list: 'margin:0;padding-left:18px;font-size:13px;line-height:1.5;color:#334155;',
    table: 'width:100%;border-collapse:collapse;font-size:12px;background:#fff;',
    th: 'border:1px solid #d8dee8;padding:6px 8px;background:#f8fafc;text-align:left;font-weight:700;',
    td: 'border:1px solid #d8dee8;padding:6px 8px;text-align:left;vertical-align:top;',
    legend: 'margin:0 0 10px 0;padding:8px 10px;border:1px dashed #cbd5e1;border-radius:8px;background:#f8fafc;font-size:12px;color:#334155;',
    summary: 'list-style:none;display:flex;justify-content:space-between;align-items:center;gap:8px;padding:12px 14px;cursor:pointer;background:#f8fafc;border-radius:10px;font-size:14px;',
    summaryTitle: 'font-weight:700;color:#0f172a;',
    badge: 'display:inline-block;font-size:11px;line-height:1;padding:3px 7px;border-radius:999px;background:#e8f5e9;color:#2e7d32;border:1px solid #cce8d0;',
    alert: 'margin:0 0 16px 0;padding:10px 12px;border:1px solid #fde68a;border-radius:8px;background:#fffbeb;color:#7c4a03;font-size:13px;line-height:1.45;',
    green: '#4CAF50',
    blue: '#2196F3'
  };

  function sectionHtml(title, bodyHtml, hasNewInfo = false) {
    return `
      <details style="${emailStyles.card}padding:0;overflow:hidden;">
        <summary style="${emailStyles.summary}">
          <span style="${emailStyles.summaryTitle}">${escapeHtml(title)}</span>
          ${hasNewInfo ? `<span style="${emailStyles.badge}">new</span>` : ''}
        </summary>
        <div style="padding:12px 16px 16px 16px">${bodyHtml}</div>
      </details>
    `;
  }

  function legendHtml() {
    return `<div style="${emailStyles.legend}"><strong>Legend:</strong> <span style="color:${emailStyles.green};font-weight:700">Ole</span> correct, <span style="color:${emailStyles.blue};font-weight:700">Koen</span> correct, <span><span style="color:${emailStyles.green};font-weight:700">Sa</span><span style="color:${emailStyles.blue};font-weight:700">mple</span> = both correct</span></div>`;
  }

  function appendPredictionColorHtml(name, correctA, correctB) {
    const base = String(name || '').trim() || '—';
    if (correctA && correctB) {
      const mid = Math.ceil(base.length / 2);
      return `<span style="color:${emailStyles.green};font-weight:700">${escapeHtml(base.slice(0, mid))}</span><span style="color:${emailStyles.blue};font-weight:700">${escapeHtml(base.slice(mid))}</span>`;
    }
    if (correctA) return `<span style="color:${emailStyles.green};font-weight:700">${escapeHtml(base)}</span>`;
    if (correctB) return `<span style="color:${emailStyles.blue};font-weight:700">${escapeHtml(base)}</span>`;
    return escapeHtml(base);
  }

  function emptyHtml(message) {
    return `<div style="${emailStyles.text};color:#64748b">${escapeHtml(message)}</div>`;
  }

  function deadlinesHtml(title, items) {
    const body = (!items || items.length === 0)
      ? '<div style="font-size:13px;color:#64748b">No upcoming deadlines.</div>'
      : `<ol style="margin:0;padding-left:20px;font-size:13px;line-height:1.5;color:#334155;">${items.map((item) => `<li><strong>${escapeHtml(item.typeLabel)}</strong> ${escapeHtml(item.dateLabel)} ${escapeHtml(item.timeLabel)} - ${escapeHtml(item.countdown)} | ${escapeHtml(item.details)}</li>`).join('')}</ol>`;
    return sectionHtml(title, body, Array.isArray(items) && items.length > 0);
  }

  function groupedRidersHtml(entries, options = {}) {
    const { showStatus = false, stageLabelGetter = null, emptyMessage = 'No entries.' } = options;
    if (!entries || entries.length === 0) return emptyHtml(emptyMessage);

    const groupedByRace = new Map();
    entries.forEach((entry) => {
      const raceId = String(entry.race_id || '');
      const raceName = raceNameById.get(raceId) || raceId;
      if (!groupedByRace.has(raceId)) groupedByRace.set(raceId, { raceName, rows: [] });
      groupedByRace.get(raceId).rows.push(entry);
    });

    return `<ul style="${emailStyles.list}">${Array.from(groupedByRace.values())
      .sort((a, b) => a.raceName.localeCompare(b.raceName))
      .map((raceEntry) => {
        const inner = raceEntry.rows
          .slice()
          .sort((a, b) => String(a.name || a.rider_name || '').localeCompare(String(b.name || b.rider_name || '')))
          .map((entry) => {
            const name = escapeHtml(entry.name || entry.rider_name || 'Unknown rider');
            const stageLabel = stageLabelGetter ? stageLabelGetter(entry) : '';
            const stagePrefix = stageLabel ? `<em>${escapeHtml(stageLabel)}</em>: ` : '';
            const statusSuffix = showStatus && entry.status ? ` (${escapeHtml(entry.status)})` : '';
            return `<li>${stagePrefix}${name}${statusSuffix}</li>`;
          })
          .join('');
        return `<li><strong>${escapeHtml(raceEntry.raceName)}</strong><ul style="${emailStyles.list}">${inner}</ul></li>`;
      })
      .join('')}</ul>`;
  }

  function startlistOverviewHtml(entries) {
    if (!entries || entries.length === 0) return emptyHtml('No upcoming races found.');
    return `<ul style="${emailStyles.list}">${entries
      .map((entry) => {
        const delta = startlistDeltaByRace.get(String(entry.race_id)) || { added: 0, removed: 0 };
        const net = Number(delta.added || 0) - Number(delta.removed || 0);
        const netLabel = net > 0 ? `+${net}` : `${net}`;
        const netHtml = net === 0
          ? ''
          : ` <strong style="color:${net > 0 ? '#2e7d32' : '#c62828'}">(${escapeHtml(netLabel)})</strong>`;
        return `<li><strong>${escapeHtml(entry.race_name)}</strong> (${escapeHtml(entry.start_date)} ${escapeHtml(entry.start_time)}): <strong>${escapeHtml(entry.rider_count)}</strong> riders${netHtml}</li>`;
      })
      .join('')}</ul>`;
  }

  function removedRidersBySelectedRacesHtml(racesList, entries) {
    if (!racesList || racesList.length === 0) return emptyHtml('No upcoming races found.');

    const groupedByRace = new Map();
    (entries || []).forEach((rider) => {
      const raceId = String(rider.race_id || '');
      if (!groupedByRace.has(raceId)) groupedByRace.set(raceId, []);
      groupedByRace.get(raceId).push(rider);
    });

    return `<ul style="${emailStyles.list}">${racesList
      .map((raceEntry) => {
        const raceId = String(raceEntry.race_id || '');
        const ridersInRace = (groupedByRace.get(raceId) || [])
          .slice()
          .sort((a, b) => String(a.name || '').localeCompare(String(b.name || '')));

        if (!ridersInRace.length) {
          return `<li><strong>${escapeHtml(raceEntry.race_name)}</strong><ul style="${emailStyles.list}"><li>none</li></ul></li>`;
        }

        return `<li><strong>${escapeHtml(raceEntry.race_name)}</strong><ul style="${emailStyles.list}">${ridersInRace
          .map((rider) => {
            const statusPart = rider.status ? ` (${escapeHtml(rider.status)})` : '';
            return `<li>${escapeHtml(rider.name || 'Unknown rider')}${statusPart}</li>`;
          })
          .join('')}</ul></li>`;
      })
      .join('')}</ul>`;
  }

  function groupedStageStartsHtml(entries) {
    if (!entries || entries.length === 0) return emptyHtml('No stage start-time additions found for this window.');
    const groupedByRace = new Map();
    entries.forEach((entry) => {
      const raceId = String(entry.race_id || '');
      const raceName = raceNameById.get(raceId) || entry.race_name || raceId;
      if (!groupedByRace.has(raceId)) groupedByRace.set(raceId, { raceName, stages: [] });
      groupedByRace.get(raceId).stages.push(entry);
    });

    return `<ul style="${emailStyles.list}">${Array.from(groupedByRace.values())
      .sort((a, b) => a.raceName.localeCompare(b.raceName))
      .map((raceEntry) => `<li><strong>${escapeHtml(raceEntry.raceName)}</strong><ul style="${emailStyles.list}">${raceEntry.stages
        .slice()
        .sort((a, b) => Number(a.stage_number || 0) - Number(b.stage_number || 0))
        .map((stage) => {
          const label = [String(stage.start_date || '').trim(), String(stage.start_time || '').trim()].filter(Boolean).join(' ') || 'unknown';
          return `<li>Stage ${escapeHtml(stage.stage_number)}: <strong>${escapeHtml(label)}</strong></li>`;
        })
        .join('')}</ul></li>`)
      .join('')}</ul>`;
  }

  function groupedDnfDnsHtml(entries) {
    if (!entries || entries.length === 0) return emptyHtml('No DNF/DNS riders found.');
    const groupedByRace = new Map();
    entries.forEach((rider) => {
      const raceId = String(rider.race_id || '');
      const raceName = raceNameById.get(raceId) || raceId;
      const stageLabel = getDnfDnsStageLabel(rider) || 'Unspecified stage';
      if (!groupedByRace.has(raceId)) groupedByRace.set(raceId, { raceName, stages: new Map() });
      const raceEntry = groupedByRace.get(raceId);
      if (!raceEntry.stages.has(stageLabel)) raceEntry.stages.set(stageLabel, []);
      raceEntry.stages.get(stageLabel).push(rider);
    });

    return `<ul style="${emailStyles.list}">${Array.from(groupedByRace.values())
      .sort((a, b) => a.raceName.localeCompare(b.raceName))
      .map((raceEntry) => `<li><strong>${escapeHtml(raceEntry.raceName)}</strong><ul style="${emailStyles.list}">${Array.from(raceEntry.stages.entries())
        .sort((a, b) => parseStageOrder(a[0]) - parseStageOrder(b[0]) || a[0].localeCompare(b[0]))
        .map(([stageLabel, ridersInStage]) => `<li><em>${escapeHtml(stageLabel)}</em><ul style="${emailStyles.list}">${ridersInStage
          .slice()
          .sort((a, b) => String(a.name || '').localeCompare(String(b.name || '')))
          .map((rider) => `<li>${escapeHtml(rider.name || '')} (${escapeHtml(rider.status || '')})</li>`)
          .join('')}</ul></li>`)
        .join('')}</ul></li>`)
      .join('')}</ul>`;
  }

  function groupedResultsHtml(entries) {
    if (!entries || entries.length === 0) return emptyHtml('No stage results available.');
    const groupedByRace = new Map();
    entries.forEach((stage) => {
      const raceId = String(stage.race_id || '');
      const raceName = raceNameById.get(raceId) || raceId;
      if (!groupedByRace.has(raceId)) groupedByRace.set(raceId, { raceName, stages: [] });
      groupedByRace.get(raceId).stages.push(stage);
    });

    return `${legendHtml()}<ul style="${emailStyles.list}">${Array.from(groupedByRace.values())
      .sort((a, b) => a.raceName.localeCompare(b.raceName))
      .map((raceEntry) => `<li><strong>${escapeHtml(raceEntry.raceName)}</strong><ul style="${emailStyles.list}">${raceEntry.stages
        .slice()
        .sort((a, b) => Number(a.stage_number || 0) - Number(b.stage_number || 0))
        .map((stage) => {
          const winner = getStageWinnerName(stage);
          const stageCorrect = stageWinnerCorrectByStagePlayer.get(String(stage.id)) || { A: false, B: false };
          return `<li>Stage ${escapeHtml(stage.stage_number)}: <strong>${appendPredictionColorHtml(winner, Boolean(stageCorrect.A), Boolean(stageCorrect.B))}</strong></li>`;
        })
        .join('')}</ul></li>`)
      .join('')}</ul>`;
  }

  function groupedBreakawaysHtml(entries) {
    if (!entries || entries.length === 0) return emptyHtml('No breakaway riders available.');
    const groupedByRace = new Map();
    entries.forEach((item) => {
      const stage = stageById.get(String(item.stage_id));
      if (!stage) return;
      const raceId = String(stage.race_id);
      const raceName = raceNameById.get(raceId) || raceId;
      const stageLabel = `Stage ${stage.stage_number}`;
      if (!groupedByRace.has(raceId)) groupedByRace.set(raceId, { raceName, stages: new Map() });
      const raceEntry = groupedByRace.get(raceId);
      if (!raceEntry.stages.has(stageLabel)) {
        raceEntry.stages.set(stageLabel, { stageId: String(stage.id), names: [] });
      }
      const names = Array.isArray(item.rider_names) ? item.rider_names : [String(item.rider_name || '').trim()];
      raceEntry.stages.get(stageLabel).names.push(...names.filter(Boolean));
    });

    return `${legendHtml()}<ul style="${emailStyles.list}">${Array.from(groupedByRace.values())
      .sort((a, b) => a.raceName.localeCompare(b.raceName))
      .map((raceEntry) => `<li><strong>${escapeHtml(raceEntry.raceName)}</strong><ul style="${emailStyles.list}">${Array.from(raceEntry.stages.entries())
        .sort((a, b) => parseStageOrder(a[0]) - parseStageOrder(b[0]))
        .map(([stageLabel, stageData]) => {
          const uniqueNames = Array.from(new Set(stageData.names.filter(Boolean))).sort((a, b) => a.localeCompare(b));
          const stagePredictions = breakawayCorrectByStagePlayer.get(stageData.stageId) || { A: new Set(), B: new Set() };
          const namesHtml = uniqueNames.map((name) => {
            const correctA = Array.from(stagePredictions.A).some((predictedName) => isRiderNameMatch(predictedName, name));
            const correctB = Array.from(stagePredictions.B).some((predictedName) => isRiderNameMatch(predictedName, name));
            return appendPredictionColorHtml(name, correctA, correctB);
          }).join(', ');
          return `<li><em>${escapeHtml(stageLabel)}</em>: ${namesHtml || '—'}</li>`;
        })
        .join('')}</ul></li>`)
      .join('')}</ul>`;
  }

  function racePredictionResultsHtml(entries) {
    if (!entries || entries.length === 0) return emptyHtml('No race prediction results available.');
    const nameA = getDisplayNameByPlayerId(PLAYER_IDS.A);
    const nameB = getDisplayNameByPlayerId(PLAYER_IDS.B);
    return entries.map((entry) => `
      <div style="margin:0 0 14px 0">
        <div style="font-weight:700;font-size:13px;margin:0 0 6px 0">${escapeHtml(entry.raceName)}</div>
        <table style="${emailStyles.table}">
          <thead>
            <tr>
              <th style="${emailStyles.th}">Category</th>
              <th style="${emailStyles.th}">Winner</th>
              <th style="${emailStyles.th}">${escapeHtml(nameA)}</th>
              <th style="${emailStyles.th}">${escapeHtml(nameB)}</th>
            </tr>
          </thead>
          <tbody>
            ${entry.categories.map((row) => `
              <tr>
                <td style="${emailStyles.td}">${escapeHtml(row.categoryLabel)}</td>
                <td style="${emailStyles.td}">${escapeHtml(row.winner || '-')}</td>
                <td style="${emailStyles.td}">${escapeHtml(row.pickA || '-')} ${row.pointsA ? `<strong>(${escapeHtml(row.pointsA)})</strong>` : '(0)'}</td>
                <td style="${emailStyles.td}">${escapeHtml(row.pickB || '-')} ${row.pointsB ? `<strong>(${escapeHtml(row.pointsB)})</strong>` : '(0)'}</td>
              </tr>
            `).join('')}
            <tr>
              <td style="${emailStyles.td};font-weight:700;background:#f8fafc" colspan="2">Total points</td>
              <td style="${emailStyles.td};font-weight:700;background:#f8fafc">${escapeHtml(entry.totalA)}</td>
              <td style="${emailStyles.td};font-weight:700;background:#f8fafc">${escapeHtml(entry.totalB)}</td>
            </tr>
          </tbody>
        </table>
      </div>
    `).join('');
  }

  function pointsDeltaHtml(entries) {
    if (!entries || entries.length === 0) return emptyHtml('No points gained in this scraper window.');
    const nameA = getDisplayNameByPlayerId(PLAYER_IDS.A);
    const nameB = getDisplayNameByPlayerId(PLAYER_IDS.B);
    return `<table style="${emailStyles.table}"><thead><tr><th style="${emailStyles.th}">Race</th><th style="${emailStyles.th}">Stage</th><th style="${emailStyles.th}">Category</th><th style="${emailStyles.th}">${escapeHtml(nameA)}</th><th style="${emailStyles.th}">${escapeHtml(nameB)}</th></tr></thead><tbody>${entries.map((entry) => `<tr><td style="${emailStyles.td}">${escapeHtml(entry.raceName)}</td><td style="${emailStyles.td}">${escapeHtml(entry.stageLabel)}</td><td style="${emailStyles.td}">${escapeHtml(entry.detailLabel)}</td><td style="${emailStyles.td}">${escapeHtml(formatSignedPoints(entry.pointsByPlayer[PLAYER_IDS.A]))}</td><td style="${emailStyles.td}">${escapeHtml(formatSignedPoints(entry.pointsByPlayer[PLAYER_IDS.B]))}</td></tr>`).join('')}</tbody></table>`;
  }

  function stagePointsHtml(entries) {
    if (!entries || entries.length === 0) return emptyHtml('No completed stages available for current races.');
    const groupedStagePointsByRace = new Map();
    entries.forEach((entry) => {
      const raceKey = String(entry.raceName || 'Race');
      if (!groupedStagePointsByRace.has(raceKey)) {
        groupedStagePointsByRace.set(raceKey, {
          raceName: raceKey,
          totalByPlayer: {
            [PLAYER_IDS.A]: makeEmptyStageCatMap(),
            [PLAYER_IDS.B]: makeEmptyStageCatMap()
          },
          stages: []
        });
      }
      const raceEntry = groupedStagePointsByRace.get(raceKey);
      STAGE_POINT_CATS.forEach((cat) => {
        raceEntry.totalByPlayer[PLAYER_IDS.A][cat] += Number((entry.pointsByPlayer[PLAYER_IDS.A] || {})[cat] || 0);
        raceEntry.totalByPlayer[PLAYER_IDS.B][cat] += Number((entry.pointsByPlayer[PLAYER_IDS.B] || {})[cat] || 0);
      });
      raceEntry.stages.push(entry);
    });

    const nameA = getDisplayNameByPlayerId(PLAYER_IDS.A);
    const nameB = getDisplayNameByPlayerId(PLAYER_IDS.B);
    return Array.from(groupedStagePointsByRace.values())
      .sort((a, b) => a.raceName.localeCompare(b.raceName))
      .map((raceEntry) => {
        const totalA = STAGE_POINT_CATS.reduce((sum, cat) => sum + Number(raceEntry.totalByPlayer[PLAYER_IDS.A][cat] || 0), 0);
        const totalB = STAGE_POINT_CATS.reduce((sum, cat) => sum + Number(raceEntry.totalByPlayer[PLAYER_IDS.B][cat] || 0), 0);
        return `
          <div style="margin:0 0 14px 0">
            <div style="font-weight:700;font-size:13px;margin:0 0 6px 0">${escapeHtml(raceEntry.raceName)}</div>
            <table style="${emailStyles.table}">
              <thead>
                <tr>
                  <th style="${emailStyles.th}" rowspan="2">Stage</th>
                  <th style="${emailStyles.th};text-align:center;background:#e8f5e9;color:#2e7d32" colspan="4">${escapeHtml(nameA)}</th>
                  <th style="${emailStyles.th};text-align:center;background:#e3f2fd;color:#1565c0" colspan="4">${escapeHtml(nameB)}</th>
                </tr>
                <tr>
                  <th style="${emailStyles.th}">Head</th>
                  <th style="${emailStyles.th}">T1</th>
                  <th style="${emailStyles.th}">Grill</th>
                  <th style="${emailStyles.th}">Total</th>
                  <th style="${emailStyles.th}">Head</th>
                  <th style="${emailStyles.th}">T1</th>
                  <th style="${emailStyles.th}">Grill</th>
                  <th style="${emailStyles.th}">Total</th>
                </tr>
              </thead>
              <tbody>
                ${raceEntry.stages
                  .slice()
                  .sort((a, b) => Number(a.stageNumber || 0) - Number(b.stageNumber || 0))
                  .map((entry) => {
                    const pointsA = entry.pointsByPlayer[PLAYER_IDS.A] || makeEmptyStageCatMap();
                    const pointsB = entry.pointsByPlayer[PLAYER_IDS.B] || makeEmptyStageCatMap();
                    const totalStageA = STAGE_POINT_CATS.reduce((sum, cat) => sum + Number(pointsA[cat] || 0), 0);
                    const totalStageB = STAGE_POINT_CATS.reduce((sum, cat) => sum + Number(pointsB[cat] || 0), 0);
                    return `<tr><td style="${emailStyles.td}">Stage ${escapeHtml(entry.stageNumber)}</td><td style="${emailStyles.td}">${escapeHtml(pointsA.head)}</td><td style="${emailStyles.td}">${escapeHtml(pointsA.t1)}</td><td style="${emailStyles.td}">${escapeHtml(pointsA.grill)}</td><td style="${emailStyles.td};font-weight:700">${escapeHtml(totalStageA)}</td><td style="${emailStyles.td}">${escapeHtml(pointsB.head)}</td><td style="${emailStyles.td}">${escapeHtml(pointsB.t1)}</td><td style="${emailStyles.td}">${escapeHtml(pointsB.grill)}</td><td style="${emailStyles.td};font-weight:700">${escapeHtml(totalStageB)}</td></tr>`;
                  })
                  .join('')}
                <tr>
                  <td style="${emailStyles.td};font-weight:700;background:#f8fafc">Total</td>
                  <td style="${emailStyles.td};font-weight:700;background:#f8fafc">${escapeHtml(raceEntry.totalByPlayer[PLAYER_IDS.A].head)}</td>
                  <td style="${emailStyles.td};font-weight:700;background:#f8fafc">${escapeHtml(raceEntry.totalByPlayer[PLAYER_IDS.A].t1)}</td>
                  <td style="${emailStyles.td};font-weight:700;background:#f8fafc">${escapeHtml(raceEntry.totalByPlayer[PLAYER_IDS.A].grill)}</td>
                  <td style="${emailStyles.td};font-weight:700;background:#f8fafc">${escapeHtml(totalA)}</td>
                  <td style="${emailStyles.td};font-weight:700;background:#f8fafc">${escapeHtml(raceEntry.totalByPlayer[PLAYER_IDS.B].head)}</td>
                  <td style="${emailStyles.td};font-weight:700;background:#f8fafc">${escapeHtml(raceEntry.totalByPlayer[PLAYER_IDS.B].t1)}</td>
                  <td style="${emailStyles.td};font-weight:700;background:#f8fafc">${escapeHtml(raceEntry.totalByPlayer[PLAYER_IDS.B].grill)}</td>
                  <td style="${emailStyles.td};font-weight:700;background:#f8fafc">${escapeHtml(totalB)}</td>
                </tr>
              </tbody>
            </table>
          </div>
        `;
      })
      .join('');
  }

  function standingsHtml(entries) {
    if (!entries || entries.length === 0) return emptyHtml('No scored predictions yet.');
    return `<table style="${emailStyles.table}"><thead><tr><th style="${emailStyles.th}">Player</th><th style="${emailStyles.th}">Points</th></tr></thead><tbody>${entries.map((entry) => `<tr><td style="${emailStyles.td}">${escapeHtml(getDisplayNameByPlayerId(entry.playerId))}</td><td style="${emailStyles.td};font-weight:700">${escapeHtml(entry.points)}</td></tr>`).join('')}</tbody></table>`;
  }

  function conflictsHtml(entries) {
    if (!entries || entries.length === 0) return emptyHtml('No conflicts found.');
    return `<ul style="${emailStyles.list}">${entries
      .slice()
      .sort((a, b) => String(a.player).localeCompare(String(b.player)) || a.raceName.localeCompare(b.raceName) || Number(a.stageNumber) - Number(b.stageNumber))
      .map((item) => `<li><strong>${escapeHtml(item.player)}</strong> | ${escapeHtml(item.raceName)} | Stage ${escapeHtml(item.stageNumber)} | ${escapeHtml(item.category)}: ${escapeHtml(item.riderName)} (${escapeHtml(item.status)})</li>`)
      .join('')}</ul>`;
  }

  const breakawayHasNamesEmail = (breakawayEntries || []).some((entry) => Array.isArray(entry.rider_names) && entry.rider_names.length > 0);

  function buildUpdatesAlertHtml(extraHeaders = []) {
    const headers = [];
    if (startlistDeltaEntries.length > 0) headers.push('Startlist deltas');
    if (stageStartReportEntries.length > 0) headers.push('Stage start times added');
    if (removedEntries.length > 0) headers.push('Riders removed from start lists');
    if (dnfDnsEntries.length > 0) headers.push('Riders with DNF/DNS');
    if (scrapedResultEntries.length > 0) headers.push('Results updates');
    if (breakawayHasNamesEmail) headers.push('Breakaway riders');
    if (racePredictionResultEntries.length > 0) headers.push('Race prediction results');
    if (pointsDeltaEntries.length > 0) headers.push('Points won since previous scraper run');
    if (conflicts.length > 0) headers.push('Prediction conflicts');
    (extraHeaders || []).forEach((item) => {
      if (item && !headers.includes(item)) headers.push(item);
    });

    if (headers.length === 0) {
      return `<div style="${emailStyles.alert}"><strong>No new information detected</strong> in this scraper window.</div>`;
    }

    return `<div style="${emailStyles.alert}"><strong>New information:</strong> ${headers.map((item) => escapeHtml(item)).join(' • ')}</div>`;
  }

  function buildReportHtmlForPlayer(playerId, deadlines) {
    const alertExtra = (Array.isArray(deadlines) && deadlines.length > 0) ? ['Upcoming deadlines'] : [];
    return `
      <div style="${emailStyles.page}">
        <div style="${emailStyles.card}">
          <div style="${emailStyles.h2}">Scraper report (${escapeHtml(runStatus)})</div>
          <div style="${emailStyles.text}">Window: <strong>${escapeHtml(formatDateTime(previous ? previous.finished_at : latest.finished_at))}</strong> -> <strong>${escapeHtml(formatDateTime(latest.finished_at))}</strong></div>
        </div>
        ${buildUpdatesAlertHtml(alertExtra)}
        ${deadlinesHtml(`Upcoming deadlines for ${getDisplayNameByPlayerId(playerId)} at scrape time`, deadlines)}
        ${sectionHtml(`Startlist size (next ${upcomingRaceCount} upcoming races): ${startlistOverviewEntries.length}`, startlistOverviewHtml(startlistOverviewEntries), startlistOverviewEntries.length > 0)}
        ${sectionHtml(hasExactStageStartSnapshotDiff ? `Stage start times added (future/current-stage races, exact diff): ${stageStartReportEntries.length}` : (hasStageUpdatedAt ? `Stage start times added (future/current-stage races, updated window): ${stageStartReportEntries.length}` : `Stage start times snapshot (future/current-stage races): ${stageStartReportEntries.length}`), groupedStageStartsHtml(stageStartReportEntries), stageStartReportEntries.length > 0)}
        ${sectionHtml(`Riders currently removed from start lists (next ${upcomingRaceCount} upcoming races): ${removedEntries.length}`, removedRidersBySelectedRacesHtml(selectedUpcomingRaces, removedEntries), selectedUpcomingRaces.length > 0)}
        ${sectionHtml(hasExactDnfDnsSnapshotDiff ? `Riders with DNF/DNS (current races, includes exact snapshot transitions, max ${reportResultsRetentionLabel} after race end)` : (hasRiderUpdatedAt ? `Riders with DNF/DNS (current races, max ${reportResultsRetentionLabel} after race end)` : `Riders with DNF/DNS (current races snapshot, max ${reportResultsRetentionLabel} after race end)`), groupedDnfDnsHtml(dnfDnsEntries), dnfDnsEntries.length > 0)}
        ${sectionHtml(hasStageUpdatedAt ? `Results updates (current races, completed stages, max ${reportResultsRetentionLabel} after race end)` : `Results snapshot (current races, completed stages, max ${reportResultsRetentionLabel} after race end)`, groupedResultsHtml(scrapedResultEntries), scrapedResultEntries.length > 0)}
        ${sectionHtml(`Breakaway riders (current races, completed stages, max ${reportResultsRetentionLabel} after race end)`, groupedBreakawaysHtml(breakawayEntries), breakawayHasNamesEmail)}
        ${sectionHtml(`Race prediction results (ended stage races, max ${reportResultsRetentionLabel} after race end)`, racePredictionResultsHtml(racePredictionResultEntries), racePredictionResultEntries.length > 0)}
        ${sectionHtml(`Points won since previous scraper run: ${getDisplayNameByPlayerId(PLAYER_IDS.A)} ${formatSignedPoints(pointsDeltaByPlayer[PLAYER_IDS.A])}, ${getDisplayNameByPlayerId(PLAYER_IDS.B)} ${formatSignedPoints(pointsDeltaByPlayer[PLAYER_IDS.B])}`, pointsDeltaHtml(pointsDeltaEntries), pointsDeltaEntries.length > 0)}
        ${sectionHtml(`Points scored per stage (current races, max ${reportResultsRetentionLabel} after race end)`, stagePointsHtml(stagePointsEntries), stagePointsEntries.length > 0)}
        ${sectionHtml('Current points standing (auto scoring)', standingsHtml(standingEntries), standingEntries.length > 0)}
        ${sectionHtml('Prediction conflicts (current stage races, upcoming stages only)', conflictsHtml(conflicts), conflicts.length > 0)}
      </div>
    `;
  }

  const html = buildReportHtmlForPlayer(PLAYER_IDS.A, nextDeadlinesA);
  const htmlByPlayer = {
    [PLAYER_IDS.A]: buildReportHtmlForPlayer(PLAYER_IDS.A, nextDeadlinesA),
    [PLAYER_IDS.B]: buildReportHtmlForPlayer(PLAYER_IDS.B, nextDeadlinesB)
  };

  return {
    subject: `Scraper report (${runStatus}) ${formatDateTime(latest.finished_at)}`,
    body: baseBody,
    html,
    bodyByPlayer: {
      [PLAYER_IDS.A]: bodyForOle,
      [PLAYER_IDS.B]: bodyForKoen
    },
    htmlByPlayer,
    debug: {
      allStagesCount: allStages.length,
      stagePointsEntriesCount: stagePointsEntries.length,
      resultVisibleRaceIds: Array.from(resultVisibleRaceIds),
      completedResultVisibleStageIds: Array.from(completedResultVisibleStageIds),
      stagePreview: allStages.slice(0, 10).map((stage) => ({
        id: stage.id,
        raceId: stage.race_id,
        raceName: raceNameById.get(String(stage.race_id)) || '',
        stageNumber: stage.stage_number,
        startDate: stage.start_date,
        isRestDay: stage.is_rest_day
      }))
    }
  };
}

async function main() {
  console.log('[EMAIL DEBUG] Script version:', EMAIL_SCRIPT_VERSION);
  // Build report first so snapshot history stays updated even if email send is skipped.
  // DEBUG: Inspect stage and race visibility for Tirreno-Adriatico and Paris-Nice
  const report = await buildReportText();
  try {
    const debug = report.debug || {};
    console.log('[DEBUG] allStages count:', debug.allStagesCount || 0);
    console.log('[DEBUG] stagePointsEntries count:', debug.stagePointsEntriesCount || 0);
    console.log('[DEBUG] stage preview:', debug.stagePreview || []);

    const tirreno = /tirreno/i;
    const paris = /paris[- ]nice/i;
    const relevantStages = (debug.stagePreview || []).filter((s) => tirreno.test(s.raceName) || paris.test(s.raceName));
    console.log('[DEBUG] Relevant stage preview:', relevantStages);
    console.log('[DEBUG] resultVisibleRaceIds:', debug.resultVisibleRaceIds || []);
    console.log('[DEBUG] completedResultVisibleStageIds:', debug.completedResultVisibleStageIds || []);
  } catch (e) { console.warn('[DEBUG] Error inspecting stage/race data:', e); }

  const recipients = await getRecipientMapping();
  console.log('[EMAIL DEBUG] Recipients:', recipients);
  if (!recipients.koenEmail && !recipients.oleEmail) {
    console.log('[EMAIL DEBUG] No scraper report recipients configured; skipping email send.');
    return;
  }

  const smtp = await getSmtpConfigFromSettings();
  console.log('[EMAIL DEBUG] SMTP config:', {
    ...smtp,
    pass: smtp && smtp.pass ? '***' : ''
  });

  const hasSmtpConfig = Boolean(smtp.host && smtp.user && smtp.pass && smtp.from);
  const fallbackFrom = String(process.env.RESEND_FROM || process.env.BREVO_FROM || smtp.from || smtp.user || '').trim();

  const mailPayloads = [];
  if (recipients.koenEmail) {
    mailPayloads.push({
      from: fallbackFrom,
      to: recipients.koenEmail,
      subject: `${report.subject} - Koen`,
      text: (report.bodyByPlayer && report.bodyByPlayer[PLAYER_IDS.B]) || report.body,
      html: (report.htmlByPlayer && report.htmlByPlayer[PLAYER_IDS.B]) || report.html || undefined
    });
  }
  if (recipients.oleEmail) {
    mailPayloads.push({
      from: fallbackFrom,
      to: recipients.oleEmail,
      subject: `${report.subject} - Ole`,
      text: (report.bodyByPlayer && report.bodyByPlayer[PLAYER_IDS.A]) || report.body,
      html: (report.htmlByPlayer && report.htmlByPlayer[PLAYER_IDS.A]) || report.html || undefined
    });
  }

  // Prefer Brevo API first (HTTPS) to avoid SMTP egress issues on some hosts.
  const brevoSentPreferred = await sendViaBrevoIfConfigured(mailPayloads);
  if (brevoSentPreferred) {
    console.log('[EMAIL DEBUG] Delivery completed via Brevo preferred path (before SMTP).');
    return;
  }

  if (!hasSmtpConfig) {
    console.log('[EMAIL DEBUG] SMTP not configured; trying HTTPS providers (Brevo/Resend).');
    const resendSent = await sendViaResendIfConfigured(mailPayloads);
    if (!resendSent) {
        console.log('[EMAIL DEBUG] No SMTP or HTTPS provider configured; skipping email send.');
    }
    return;
  }

  const connectHost = await resolveIpv4Host(smtp.host);
  const smtpResolved = { ...smtp, connectHost };

  let smtpFailedError = null;
  try {
    const transporter = nodemailer.createTransport(buildTransportConfig(smtpResolved, smtp.secure));
    for (const payload of mailPayloads) {
      await transporter.sendMail(payload);
    }
  } catch (err) {
    const isTlsMismatch = isTlsModeMismatchError(err);
    const isConnErr = isConnectionError(err);
    if (!isTlsMismatch && !isConnErr) throw err;

    let fallbackPort = smtp.port;
    let fallbackSecure = !smtp.secure;

    // If connection failed (timeout/refused), switch to the complementary port
    // rather than just flipping secure on the same blocked port.
    if (isConnErr) {
      if (smtp.port === 587) { fallbackPort = 465; fallbackSecure = true; }
      else if (smtp.port === 465) { fallbackPort = 587; fallbackSecure = false; }
    }

    console.warn(
      `SMTP failed (${err.code || err.message.slice(0, 80)}) with secure=${smtp.secure} port=${smtp.port}; retrying with secure=${fallbackSecure} port=${fallbackPort}.`
    );

    try {
      const smtpForFallback = { ...smtpResolved, port: fallbackPort };
      const fallbackTransporter = nodemailer.createTransport(buildTransportConfig(smtpForFallback, fallbackSecure));
      for (const payload of mailPayloads) {
        await fallbackTransporter.sendMail(payload);
      }
    } catch (fallbackErr) {
      smtpFailedError = fallbackErr;
    }
  }

  if (smtpFailedError) {
    const brevoSent = await sendViaBrevoIfConfigured(mailPayloads);
    if (!brevoSent) {
      const resendSent = await sendViaResendIfConfigured(mailPayloads);
      if (!resendSent) throw smtpFailedError;
    }
  }

  console.log(`Scraper report email sent to: ${mailPayloads.map(payload => payload.to).join(', ')}`);
}

main().catch((err) => {
  console.error('Failed to send scraper report email:', err && err.message ? err.message : err);
  process.exitCode = 1;
});

// EMAIL_SCRIPT_VERSION_END: 2026-03-16-email-v1
