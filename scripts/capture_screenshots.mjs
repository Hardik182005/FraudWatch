/**
 * Renders the two deployed FraudWatch sites in a real Chromium and writes report screenshots.
 *
 * This is the browser-render check that the HTTP-level checks in docs/status-checklist.md could not
 * make: it initialises WebGL, runs the SPA router, and records whatever the browser console says.
 * Console output is written next to the images so a failing page is visible rather than silently
 * producing a blank screenshot.
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

/** Each shot: the site origin, the route, a file name, and whether to wait for the 3D canvas. */
const SHOTS = [
  { site: LIVE, route: '/', name: 'live-landing', canvas: true },
  { site: LIVE, route: '/console', name: 'live-console' },
  { site: LIVE, route: '/scenario', name: 'live-scenario' },
  { site: LIVE, route: '/alerts', name: 'live-alerts' },
  { site: LIVE, route: '/pipeline', name: 'live-pipeline', canvas: true },
  { site: LAB, route: '/', name: 'lab-landing', canvas: true },
  { site: LAB, route: '/ca1', name: 'lab-ca1' },
  { site: LAB, route: '/ca2', name: 'lab-ca2' },
  { site: LAB, route: '/experiments', name: 'lab-experiments' },
  { site: LAB, route: '/architecture', name: 'lab-architecture' },
  { site: LAB, route: '/system', name: 'lab-system' },
  { site: LAB, route: '/evidence', name: 'lab-evidence' },
];

const log = [];

async function shoot(browser, { site, route, name, canvas }, viewport, suffix) {
  const context = await browser.newContext(viewport);
  const page = await context.newPage();
  const console_ = [];
  page.on('console', (m) => { if (m.type() === 'error' || m.type() === 'warning') console_.push(`${m.type()}: ${m.text()}`); });
  page.on('pageerror', (e) => console_.push(`pageerror: ${e.message}`));
  const url = site + route;
  let status = 'ok';
  try {
    const res = await page.goto(url, { waitUntil: 'networkidle', timeout: 60_000 });
    status = `http ${res?.status()}`;
    // The 3D scenes are lazy-loaded; give the canvas a chance to mount and paint a frame.
    if (canvas) {
      await page.waitForSelector('canvas, [data-testid="scene-2d"]', { timeout: 20_000 }).catch(() => {});
      await page.waitForTimeout(2500);
    } else {
      await page.waitForTimeout(1200);
    }
    await page.screenshot({ path: path.join(OUT, `${name}${suffix}.png`), fullPage: !canvas });
  } catch (err) {
    status = `FAILED: ${err.message.split('\n')[0]}`;
  }
  log.push({ url, shot: `${name}${suffix}.png`, viewport: suffix || 'desktop', status, console: console_ });
  console.log(`${status.padEnd(12)} ${name}${suffix}  ${url}`);
  await context.close();
}

const browser = await chromium.launch();
await mkdir(OUT, { recursive: true });
for (const shot of SHOTS) await shoot(browser, shot, { viewport: { width: 1440, height: 960 } }, '');
// One phone-width pass over the two landing pages, to check the mobile layout and the 2D fallback.
for (const shot of SHOTS.filter((s) => s.route === '/')) await shoot(browser, shot, devices['Pixel 7'], '-mobile');
await browser.close();

await writeFile(path.join(OUT, 'capture-log.json'),
  JSON.stringify({ captured_at: new Date().toISOString(), live: LIVE, lab: LAB, shots: log }, null, 2));
const bad = log.filter((l) => !l.status.startsWith('http 2'));
console.log(`\n${log.length - bad.length}/${log.length} pages rendered; log in ${OUT}/capture-log.json`);
if (bad.length) process.exitCode = 1;
