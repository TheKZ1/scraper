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

function runChildProcess(command, args, env) {
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

async function sendScraperReportEmail(status, message = '') {
  const env = { ...process.env, SCRAPER_REPORT_STATUS: status, SCRAPER_REPORT_MESSAGE: message };

  const nodeExecutable = process.execPath || 'node';

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

  writeLog('Starting scraper process...');
  const scraper = spawn('npm', ['run', 'scrape:daily:all'], { shell: true });

  scraper.stdout.on('data', data => {
    writeLog('[scraper stdout] ' + data.toString());
    fs.appendFileSync(logPath, data);
  });
  scraper.stderr.on('data', data => {
    writeLog('[scraper stderr] ' + data.toString());
    fs.appendFileSync(logPath, data);
  });

  scraper.on('error', err => {
    writeLog(`[scraper error] ${err.message}`);
  });

  scraper.on('exit', async code => {
    writeLog(`Scraper process exited with code ${code}`);
    if (code === 0) {
      writeLog('Daily scraper run completed successfully');
      saveRunState(todayKey, 'success', new Date().toISOString());
      await finishSupabaseRun(runId, 'success', 'Completed successfully');
      try {
        await sendScraperReportEmail('success', 'Completed successfully');
      } catch (err) {
        writeLog(`Warning: failed to send scraper report email: ${err.message}`);
      }
    } else {
      writeLog(`ERROR: Scrapers failed with exit code ${code}`);
      saveRunState(todayKey, 'failed', new Date().toISOString());
      await finishSupabaseRun(runId, 'failed', `Scrapers failed with exit code ${code}`);
      try {
        await sendScraperReportEmail('failed', `Scrapers failed with exit code ${code}`);
      } catch (err) {
        writeLog(`Warning: failed to send scraper report email: ${err.message}`);
      }
      process.exit(1);
    }
  });
})();
