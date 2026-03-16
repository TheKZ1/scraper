const express = require('express');
const { spawn } = require('child_process');

const app = express();
const PORT = Number(process.env.PORT) || 5000;
const HOST = process.env.HOST || '0.0.0.0';
const TOKEN = process.env.SCRAPER_TOKEN || '';
const JOB_TTL_MS = 5 * 60 * 1000;

const COMMANDS = {
  cycling: 'npm run scrape:cycling',
  riders: 'npm run scrape:riders',
  profiles: 'npm run scrape:profiles',
  daily: 'npm run scrape:daily:all',
  reportEmail: 'npm run scrape:report:email',
};

const jobs = new Map();

app.get('/health', (req, res) => {
  res.status(200).json({ ok: true, service: 'scraper-server' });
});

app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, x-scraper-token');
  if (req.method === 'OPTIONS') {
    return res.sendStatus(204);
  }
  next();
});

const getToken = (req) => req.get('x-scraper-token') || req.query.token || '';

const getRunningJob = (type) => {
  for (const job of jobs.values()) {
    if (job.type === type && job.status === 'running') {
      return job;
    }
  }
  return null;
};

const sendEvent = (res, payload) => {
  res.write(`data: ${JSON.stringify(payload)}\n\n`);
};

const appendJobLine = (job, line) => {
  const trimmed = line.trimEnd();
  if (!trimmed) {
    return;
  }

  job.lines.push(trimmed);
  if (job.lines.length > 500) {
    job.lines.shift();
  }

  job.clients.forEach((client) => {
    sendEvent(client, { type: 'log', message: trimmed });
  });
};

const finalizeJob = (job, status, code) => {
  job.status = status;
  job.exitCode = code;

  job.clients.forEach((client) => {
    sendEvent(client, { type: 'status', status: job.status, exitCode: code });
    client.end();
  });

  job.clients.clear();
  setTimeout(() => jobs.delete(job.id), JOB_TTL_MS);
};

const startJob = (type) => {
  const command = COMMANDS[type];
  if (!command) {
    return null;
  }

  const jobId = `${type}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const child = spawn(command, { cwd: __dirname, shell: true });
  const job = {
    id: jobId,
    type,
    status: 'running',
    startedAt: new Date().toISOString(),
    lines: [],
    clients: new Set(),
    child,
  };

  jobs.set(jobId, job);

  child.stdout.on('data', (data) => appendJobLine(job, data.toString()));
  child.stderr.on('data', (data) => appendJobLine(job, data.toString()));

  child.on('close', (code) => {
    finalizeJob(job, code === 0 ? 'success' : 'error', code);
  });

  child.on('error', (err) => {
    appendJobLine(job, err.message || 'Failed to start scraper.');
    finalizeJob(job, 'error', 1);
  });

  return job;
};

const handleRunScraper = (type) => (req, res) => {
  const token = getToken(req);
  if (TOKEN && token !== TOKEN) {
    return res.status(401).send('Unauthorized');
  }

  const running = getRunningJob(type);
  if (running) {
    return res.status(409).json({ jobId: running.id, status: 'running' });
  }

  const job = startJob(type);
  if (!job) {
    return res.status(400).send('Unknown scraper type');
  }

  return res.json({ jobId: job.id, status: 'started', type: job.type });
};

app.post('/run-scraper', handleRunScraper('cycling'));
app.post('/run-scraper-riders', handleRunScraper('riders'));
app.post('/run-scraper-profiles', handleRunScraper('profiles'));
app.post('/run-scraper-daily', handleRunScraper('daily'));
app.post('/run-scraper-report-email', handleRunScraper('reportEmail'));

app.get('/scraper-events', (req, res) => {
  const token = getToken(req);
  if (TOKEN && token !== TOKEN) {
    return res.status(401).send('Unauthorized');
  }

  const jobId = req.query.jobId || '';
  const job = jobs.get(jobId);
  if (!job) {
    return res.status(404).send('Job not found');
  }

  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
  });

  sendEvent(res, { type: 'status', status: job.status, exitCode: job.exitCode || null });
  job.lines.forEach((line) => sendEvent(res, { type: 'log', message: line }));

  if (job.status !== 'running') {
    return res.end();
  }

  job.clients.add(res);
  req.on('close', () => job.clients.delete(res));
});

app.get('/run-scraper', (req, res) => {
  res.status(405).send('Use POST /run-scraper');
});

app.listen(PORT, HOST, () => {
  console.log(`Scraper server running at http://${HOST}:${PORT}`);
});
