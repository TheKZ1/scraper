

const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
const http = require('http');
const axios = require('axios');

const PORT = process.env.PORT || 10000;
const projectRoot = path.resolve(__dirname, '..');
const logDir = path.join(projectRoot, 'logs');
if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir);
}
const timestamp = new Date().toISOString().replace(/[:T]/g, '-').split('.')[0];
const logPath = path.join(logDir, `scraper-run-${timestamp}.log`);
const statePath = path.join(logDir, 'daily-run-state.json');
const todayKey = new Date().toISOString().slice(0, 10);

function writeLog(message) {
  const line = `[${new Date().toISOString().replace('T', ' ').slice(0, 19)}] ${message}`;
  console.log(line);
  fs.appendFileSync(logPath, line + '\n');
}

// Minimal web server for Render port scan
http.createServer((req, res) => {
  res.end('OK');
}).listen(PORT, () => {
  writeLog(`Web server running on port ${PORT}`);
});

function getRunState() {
  if (!fs.existsSync(statePath)) return null;
  try {
    return JSON.parse(fs.readFileSync(statePath, 'utf8'));
  } catch {
    return null;
  }
}

function saveRunState(date, status, lastRunAt) {
  const payload = { date, status, lastRunAt };
  fs.writeFileSync(statePath, JSON.stringify(payload, null, 2), 'utf8');
}

async function finishSupabaseRun(runId, status, message = '') {
  const supabaseUrl = 'https://zbvibhtopcsqrnecxgim.supabase.co';
  const serviceKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InpidmliaHRvcGNzcXJuZWN4Z2ltIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MTkyMjI5OCwiZXhwIjoyMDg3NDk4Mjk4fQ.jBvKDo_j0TvZ3FuQ2DpNAQmHXGABuyFcVhfjSc-G42w';
  if (!supabaseUrl || !serviceKey) return;
  const headers = {
    apikey: serviceKey,
    Authorization: `Bearer ${serviceKey}`,
    'Content-Type': 'application/json',
  };
  const body = {
    finished_at: new Date().toISOString(),
    status,
    message,
  };
  try {
    if (runId) {
      await axios.patch(`${supabaseUrl}/rest/v1/scraper_runs?id=eq.${runId}`, body, { headers });
      return;
    }
  } catch (err) {
    writeLog(`Warning: Could not update Supabase run entry id=${runId}: ${err.message}`);
  }
  // Fallback
  try {
    await axios.post(`${supabaseUrl}/rest/v1/scraper_runs`, body, { headers });
    writeLog('Fallback Supabase run entry created.');
  } catch (err) {
    writeLog(`Warning: Could not create fallback Supabase run entry: ${err.message}`);
  }
}

function runChildProcess(command, args, env, { logToConsole = false } = {}) {
  return new Promise((resolve, reject) => {
    const proc = spawn(command, args, {
      cwd: projectRoot,
      env,
      shell: true,
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    const handleData = (data) => {
      fs.appendFileSync(logPath, data);
      if (logToConsole) {
        const text = data.toString();
        text.split('\n').forEach(line => { if (line.trim()) writeLog('[email] ' + line.trimEnd()); });
      }
    };

    proc.stdout.on('data', handleData);
    proc.stderr.on('data', handleData);

    proc.on('error', reject);
    proc.on('exit', code => resolve(code));
  });
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function summarizeIssuesForMessage(issueLines, maxLines = 8, maxChars = 1800) {
  const lines = Array.isArray(issueLines) ? issueLines.filter(Boolean) : [];
  if (!lines.length) return '';

  const kept = lines.slice(0, maxLines);
  const summary = kept.map((line, idx) => `${idx + 1}. ${line}`).join('\n');
  if (summary.length <= maxChars) {
    return summary;
  }

  return summary.slice(0, maxChars - 3) + '...';
}

function verifyDailyScraperSources() {
  const rootScraperPath = path.join(projectRoot, 'scraper-cycling-archives.js');
  if (!fs.existsSync(rootScraperPath)) {
    throw new Error(`Missing required file: ${rootScraperPath}`);
  }

  const rootScraperContent = fs.readFileSync(rootScraperPath, 'utf8');
  const hasNetworkImport = /from\s+['"]https?:\/\//i.test(rootScraperContent)
    || /import\s*\(\s*['"]https?:\/\//i.test(rootScraperContent);

  if (hasNetworkImport) {
    throw new Error(
      'Invalid root scraper-cycling-archives.js detected: network imports found. ' +
      'This usually means a Worker file was deployed over the Node scraper file.'
    );
  }
}

async function sendScraperReportEmail(status, message = '') {
  const env = { ...process.env, SCRAPER_REPORT_STATUS: status, SCRAPER_REPORT_MESSAGE: message };
  const emailScriptVersion = '2026-03-16-email-v1';

  const nodeExecutable = process.execPath || 'node';

  writeLog(`Expecting email script version ${emailScriptVersion}`);
  writeLog(`Sending scraper report email via direct node (${nodeExecutable})...`);
  const firstCode = await runChildProcess(nodeExecutable, ['scripts/send-scraper-report-email.js'], env, { logToConsole: true });
  if (firstCode === 0) {
    writeLog('Scraper report email command completed successfully.');
    return true;
  }

  writeLog(`Warning: report email command exited with code ${firstCode}; retrying once in 3s.`);
  await sleep(3000);
  const retryCode = await runChildProcess(nodeExecutable, ['scripts/send-scraper-report-email.js'], env, { logToConsole: true });
  if (retryCode === 0) {
    writeLog('Scraper report email retry completed successfully.');
    return true;
  }

  writeLog(`Warning: report email retry exited with code ${retryCode}`);
  return false;
}

// Main logic
(async () => {
  let runId = null;
  const runState = getRunState();
  const forceDailyRun = process.env.FORCE_DAILY_RUN;
  const isForced = forceDailyRun === '1' || forceDailyRun === 'true' || forceDailyRun === 'TRUE';

  if (!isForced && runState && runState.date === todayKey) {
    if (runState.status === 'success') {
      writeLog('Daily scraper already ran successfully today; skipping.');
      process.exit(0);
    }
    writeLog(`Previous run today ended with status '${runState.status}'; retrying.`);
  }

  writeLog('Daily scraper run started');
  writeLog('Running scrapers...');

  try {
    verifyDailyScraperSources();
    writeLog(`Source preflight OK (projectRoot=${projectRoot})`);
  } catch (err) {
    const message = err && err.message ? err.message : String(err);
    writeLog(`ERROR: Daily scraper source preflight failed: ${message}`);
    saveRunState(todayKey, 'failed', new Date().toISOString());
    await finishSupabaseRun(runId, 'failed', `Source preflight failed: ${message}`);
    process.exit(1);
  }

  writeLog('Starting scraper process...');
  const scraperIssueLines = [];
  const scraperIssueSet = new Set();
  const issueRegex = /\b(error|fatal|exception|unhandled|axioserror)\b/i;

  const pushScraperIssue = (label, rawLine) => {
    const line = String(rawLine || '').replace(/\s+/g, ' ').trim();
    if (!line) return;

    const entry = `${label}: ${line}`;
    const key = entry.toLowerCase();
    if (scraperIssueSet.has(key)) return;

    scraperIssueSet.add(key);
    scraperIssueLines.push(entry);
  };

  const collectIssuesFromChunk = (label, chunk, collectAllLines = false) => {
    const text = String(chunk || '');
    text
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean)
      .forEach((line) => {
        if (collectAllLines || issueRegex.test(line)) {
          pushScraperIssue(label, line);
        }
      });
  };

  const scraper = spawn('npm', ['run', 'scrape:daily:all'], {
    cwd: projectRoot,
    env: process.env,
    shell: true,
  });

  scraper.stdout.on('data', data => {
    writeLog('[scraper stdout] ' + data.toString());
    fs.appendFileSync(logPath, data);
    collectIssuesFromChunk('stdout', data, false);
  });
  scraper.stderr.on('data', data => {
    writeLog('[scraper stderr] ' + data.toString());
    fs.appendFileSync(logPath, data);
    collectIssuesFromChunk('stderr', data, true);
  });

  scraper.on('error', err => {
    writeLog(`[scraper error] ${err.message}`);
    pushScraperIssue('process', err && err.message ? err.message : String(err));
  });

  scraper.on('exit', async code => {
    writeLog(`Scraper process exited with code ${code}`);
    const issuesSummary = summarizeIssuesForMessage(scraperIssueLines);

    if (code === 0) {
      const hasIssues = scraperIssueLines.length > 0;
      const successMessage = hasIssues
        ? `Completed with issues detected during scrape:\n${issuesSummary}`
        : 'Completed successfully';

      if (hasIssues) {
        writeLog(`Warning: scraper finished with ${scraperIssueLines.length} issue line(s) detected; including summary in report.`);
      }

      writeLog('Daily scraper run completed successfully');
      saveRunState(todayKey, 'success', new Date().toISOString());
      await finishSupabaseRun(runId, 'success', successMessage);
      try {
        await sendScraperReportEmail('success', successMessage);
      } catch (err) {
        writeLog(`Warning: failed to send scraper report email: ${err.message}`);
      }
    } else {
      writeLog(`ERROR: Scrapers failed with exit code ${code}`);
      const failureMessage = issuesSummary
        ? `Scrapers failed with exit code ${code}. Error summary:\n${issuesSummary}`
        : `Scrapers failed with exit code ${code}`;
      saveRunState(todayKey, 'failed', new Date().toISOString());
      await finishSupabaseRun(runId, 'failed', failureMessage);
      try {
        await sendScraperReportEmail('failed', failureMessage);
      } catch (err) {
        writeLog(`Warning: failed to send scraper report email: ${err.message}`);
      }
      process.exit(1);
    }
  });
})();
