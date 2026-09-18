/**
 * Renders the two deployed FraudWatch sites in a real Chromium and writes report screenshots.
 *
 * This is the browser-render check that the HTTP-level checks in docs/status-checklist.md could not
 * make: it initialises WebGL, runs the SPA router, and records whatever the browser console says.
 * Console output is written next to the images so a failing page is visible rather than silently
 * producing a blank screenshot.
 *
 * The Live pass drives the app the way a visitor does. All Live routes share ONE browser context,
 * because the demo session is held in that context's localStorage: a fresh context per route would
 * throw the session away and the console would be photographed empty. The pass starts a real demo
 * session, starts a real bounded scenario run, waits for events to actually be scored, and only then
 * takes the console/alerts shots. If the run cannot be started the shots are still taken and the
 * reason is recorded in capture-log.json, rather than a pretty but empty screenshot being passed off
 * as a working console.
 *
 *   node scripts/capture_screenshots.mjs                      # deployed Cloud Run origins
 *   LIVE_URL=http://127.0.0.1:4173 LAB_URL=http://127.0.0.1:4174 node scripts/capture_screenshots.mjs
 */
import { chromium, devices } from '@playwright/test';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';

const LIVE = process.env.LIVE_URL ?? 'https://fraudwatch-live-2djxmgtj5a-el.a.run.app';
const LAB = process.env.LAB_URL ?? 'https://fraudwatch-lab-2djxmgtj5a-el.a.run.app';
const OUT = process.env.SHOT_DIR ?? 'reports/media/screenshots';

/** Lab routes need no session, so each gets its own clean context. */
const LAB_SHOTS = [
  { site: LAB, route: '/', name: 'lab-landing', canvas: true },
  { site: LAB, route: '/ca1', name: 'lab-ca1' },
  { site: LAB, route: '/ca2', name: 'lab-ca2' },
  { site: LAB, route: '/experiments', name: 'lab-experiments' },
  { site: LAB, route: '/architecture', name: 'lab-architecture' },
  { site: LAB, route: '/system', name: 'lab-system' },
  { site: LAB, route: '/evidence', name: 'lab-evidence' },
];

/** Live routes, captured in this order inside one session-bearing context. */
const LIVE_ROUTES = [
  { route: '/', name: 'live-landing', canvas: true },
  { route: '/scenario', name: 'live-scenario' },
  { route: '/console', name: 'live-console' },
  { route: '/alerts', name: 'live-alerts' },
  { route: '/pipeline', name: 'live-pipeline', canvas: true },
];

const log = [];
const notes = [];

function watch(page, sink) {
  page.on('console', (m) => { if (m.type() === 'error' || m.type() === 'warning') sink.push(`${m.type()}: ${m.text()}`); });
  page.on('pageerror', (e) => sink.push(`pageerror: ${e.message}`));
}

async function settle(page, canvas) {
  if (canvas) {
    // The 3D scenes are lazy-loaded; give the canvas a chance to mount and paint a frame.
    await page.waitForSelector('canvas, [data-testid="scene-2d"]', { timeout: 20_000 }).catch(() => {});
    await page.waitForTimeout(2500);
  } else {
    await page.waitForTimeout(1200);
  }
}

/** Lab (and the mobile pass): one context per shot, no session needed. */
async function shoot(browser, { site, route, name, canvas }, viewport, suffix) {
  const context = await browser.newContext(viewport);
  const page = await context.newPage();
  const console_ = [];
  watch(page, console_);
  const url = site + route;
  let status = 'ok';
  try {
    const res = await page.goto(url, { waitUntil: 'networkidle', timeout: 60_000 });
    status = `http ${res?.status()}`;
    await settle(page, canvas);
    await page.screenshot({ path: path.join(OUT, `${name}${suffix}.png`), fullPage: !canvas });
  } catch (err) {
    status = `FAILED: ${err.message.split('\n')[0]}`;
  }
  log.push({ url, shot: `${name}${suffix}.png`, viewport: suffix || 'desktop', status, console: console_ });
  console.log(`${status.padEnd(12)} ${name}${suffix}  ${url}`);
  await context.close();
}

/**
 * Starts a demo session and a bounded scenario run, then waits until events have actually been
 * scored. Returns a note describing what happened — including the failure, if it failed.
 */
async function startRun(page) {
  await page.goto(`${LIVE}/console`, { waitUntil: 'networkidle', timeout: 90_000 });
  const startDemo = page.getByTestId('start-demo');
  if (await startDemo.count()) {
    await startDemo.first().click();
    // The session gate disappears once createSession resolves.
    await startDemo.first().waitFor({ state: 'detached', timeout: 60_000 }).catch(() => {});
  }

  await page.goto(`${LIVE}/scenario`, { waitUntil: 'networkidle', timeout: 90_000 });
  const startRunBtn = page.getByTestId('start-run');
  if (!(await startRunBtn.count())) return 'no start-run control on /scenario; no run was started';
  if (await startRunBtn.first().isDisabled()) return 'start-run is disabled (session cap reached or no scenario selected); no run was started';
  await startRunBtn.first().click();

  // A run row appearing only means it was accepted. Wait for the events-used counter to actually
  // move, which is the point at which scored events exist to photograph.
  await page.getByTestId('current-run').waitFor({ timeout: 60_000 }).catch(() => {});
  // Report the counter's raw text rather than a parsed number: the element renders a formatted
  // figure, and stripping its separators silently concatenates digits into a fictional count.
  const used = page.getByTestId('events-used');
  const deadline = Date.now() + 120_000;
  let raw = '';
  let first = null;
  while (Date.now() < deadline) {
    raw = ((await used.first().textContent().catch(() => '')) ?? '').trim();
    if (first === null) first = raw;
    if (raw && raw !== first) break; // the counter moved, so events are being consumed
    await page.waitForTimeout(3000);
  }
  return raw && raw !== first
    ? `run started; session events-used counter moved from "${first}" to "${raw}" before the console was captured`
    : `run accepted but the events-used counter did not move (stayed "${raw}"); console may have been captured without live data`;
}

async function captureLive(browser) {
  const context = await browser.newContext({ viewport: { width: 1440, height: 960 } });
  const page = await context.newPage();
  const console_ = [];
  watch(page, console_);

  try {
    notes.push({ step: 'live-session', detail: await startRun(page) });
  } catch (err) {
    notes.push({ step: 'live-session', detail: `FAILED: ${err.message.split('\n')[0]}` });
  }

  for (const { route, name, canvas } of LIVE_ROUTES) {
    const url = LIVE + route;
    const before = console_.length;
    let status = 'ok';
    try {
      const res = await page.goto(url, { waitUntil: 'networkidle', timeout: 60_000 });
      status = `http ${res?.status()}`;
      await settle(page, canvas);
      await page.screenshot({ path: path.join(OUT, `${name}.png`), fullPage: !canvas });
    } catch (err) {
      status = `FAILED: ${err.message.split('\n')[0]}`;
    }
    log.push({ url, shot: `${name}.png`, viewport: 'desktop', status, console: console_.slice(before) });
    console.log(`${status.padEnd(12)} ${name}  ${url}`);
  }
  await context.close();
}

const browser = await chromium.launch();
await mkdir(OUT, { recursive: true });

await captureLive(browser);
for (const shot of LAB_SHOTS) await shoot(browser, shot, { viewport: { width: 1440, height: 960 } }, '');
// One phone-width pass over the two landing pages, to check the mobile layout and the 2D fallback.
await shoot(browser, { site: LIVE, route: '/', name: 'live-landing', canvas: true }, devices['Pixel 7'], '-mobile');
await shoot(browser, { site: LAB, route: '/', name: 'lab-landing', canvas: true }, devices['Pixel 7'], '-mobile');
await browser.close();

await writeFile(path.join(OUT, 'capture-log.json'),
  JSON.stringify({ captured_at: new Date().toISOString(), live: LIVE, lab: LAB, notes, shots: log }, null, 2));
for (const n of notes) console.log(`note: ${n.step}: ${n.detail}`);
const bad = log.filter((l) => !l.status.startsWith('http 2'));
console.log(`\n${log.length - bad.length}/${log.length} pages rendered; log in ${OUT}/capture-log.json`);
if (bad.length) process.exitCode = 1;
