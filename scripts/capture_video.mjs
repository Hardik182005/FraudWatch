// Records a guided walkthrough video of each deployed site in real Chromium.
//
// One video per site. The tour visits every route, pauses on each so the 3D
// heroes and any live panels have time to render, and scrolls the long pages so
// the recording shows their full content rather than just the fold.
//
// Output: <OUT>/live-walkthrough.webm, <OUT>/lab-walkthrough.webm plus
// tour-log.json recording what was visited, the HTTP status of each route and
// any console/page errors seen. Nothing here is a measurement of the pipeline;
// it is a recording of what the sites rendered at capture time.
import { chromium } from 'playwright';
import { mkdir, rename, readdir, writeFile } from 'node:fs/promises';
import path from 'node:path';

const LIVE = process.env.LIVE_URL ?? 'https://fraudwatch-live-2djxmgtj5a-el.a.run.app';
const LAB  = process.env.LAB_URL  ?? 'https://fraudwatch-lab-2djxmgtj5a-el.a.run.app';
const OUT  = process.env.VIDEO_DIR ?? 'reports/media/video';

const TOURS = [
  {
    name: 'live-walkthrough',
    site: LIVE,
    startRun: true,
    routes: [
      { route: '/',         hold: 9000, canvas: true },
      { route: '/console',  hold: 8000 },
      { route: '/scenario', hold: 7000 },
      { route: '/alerts',   hold: 7000 },
      { route: '/pipeline', hold: 9000, canvas: true },
    ],
  },
  {
    name: 'lab-walkthrough',
    site: LAB,
    routes: [
      { route: '/',             hold: 9000, canvas: true },
      { route: '/ca1',          hold: 9000 },
      { route: '/ca2',          hold: 9000 },
      { route: '/experiments',  hold: 7000 },
      { route: '/architecture', hold: 9000 },
      { route: '/system',       hold: 7000 },
      { route: '/evidence',     hold: 7000 },
    ],
  },
];

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Scroll in steps rather than jumping to the bottom: a jump reads as a glitch in
// the recording and skips past everything in between.
async function scrollThrough(page, holdMs) {
  const height = await page.evaluate(() => document.body.scrollHeight);
  const viewport = page.viewportSize().height;
  const steps = Math.max(1, Math.min(6, Math.ceil((height - viewport) / (viewport * 0.8))));
  const per = Math.max(600, Math.floor(holdMs / (steps + 2)));
  await sleep(per);
  for (let i = 1; i <= steps; i += 1) {
    await page.evaluate((y) => window.scrollTo({ top: y, behavior: 'smooth' }), i * viewport * 0.8);
    await sleep(per);
  }
  await page.evaluate(() => window.scrollTo({ top: 0, behavior: 'smooth' }));
  await sleep(per);
}

async function runTour(browser, tour, log) {
  const context = await browser.newContext({
    viewport: { width: 1440, height: 900 },
    recordVideo: { dir: OUT, size: { width: 1440, height: 900 } },
  });
  const page = await context.newPage();
  const problems = [];
  page.on('console', (m) => {
    if (m.type() === 'error') problems.push({ kind: 'console', text: m.text() });
  });
  page.on('pageerror', (e) => problems.push({ kind: 'pageerror', text: String(e) }));

  // On the Live tour, start a real demo session and a real bounded run first, so the recording shows
  // the console carrying data instead of an empty shell. The Lab tour needs no session.
  if (tour.startRun) {
    await page.goto(`${tour.site}/console`, { waitUntil: 'load', timeout: 90_000 });
    const startDemo = page.getByTestId('start-demo');
    if (await startDemo.count()) {
      await startDemo.first().click();
      await startDemo.first().waitFor({ state: 'detached', timeout: 60_000 }).catch(() => {});
    }
    await page.goto(`${tour.site}/scenario`, { waitUntil: 'networkidle', timeout: 90_000 });
    const startRunBtn = page.getByTestId('start-run');
    // The button stays disabled until the scenario list has loaded and one is selected, so wait for
    // it to become enabled rather than sampling it once and concluding it is unavailable.
    await startRunBtn.first().waitFor({ state: 'visible', timeout: 60_000 }).catch(() => {});
    const enabledBy = Date.now() + 60_000;
    while (Date.now() < enabledBy && (await startRunBtn.count()) && (await startRunBtn.first().isDisabled())) {
      await sleep(2000);
    }
    if ((await startRunBtn.count()) && !(await startRunBtn.first().isDisabled())) {
      await startRunBtn.first().click();
      await page.getByTestId('current-run').waitFor({ timeout: 60_000 }).catch(() => {});
      await sleep(20_000); // let events accumulate before the tour starts filming the console
      log.startedRun = true;
    } else {
      log.startedRun = false;
      log.startRunNote = 'start-run unavailable or disabled; the Live tour was recorded without a run';
    }
  }

  for (const step of tour.routes) {
    const url = `${tour.site}${step.route}`;
    const res = await page.goto(url, { waitUntil: 'load', timeout: 90_000 });
    const status = res ? res.status() : null;
    log.routes.push({ tour: tour.name, url, status });
    if (step.canvas) {
      // The 3D hero falls back to a 2D scene when WebGL is unavailable, so accept either.
      await page
        .waitForSelector('canvas, [data-testid="scene-2d"]', { timeout: 20_000 })
        .catch(() => log.routes.at(-1).note = 'no canvas or 2d fallback appeared within 20s');
    }
    await scrollThrough(page, step.hold);
  }

  await page.close();
  await context.close(); // the video file is only finalised once the context closes
  log.problems.push(...problems.map((p) => ({ tour: tour.name, ...p })));
}

async function main() {
  await mkdir(OUT, { recursive: true });
  const before = new Set(await readdir(OUT).catch(() => []));
  const browser = await chromium.launch();
  const log = { capturedAt: new Date().toISOString(), routes: [], problems: [], videos: [] };

  for (const tour of TOURS) {
    const seen = new Set(await readdir(OUT));
    await runTour(browser, tour, log);
    // Playwright names videos with a random id; rename the new one after its tour.
    const produced = (await readdir(OUT)).find((f) => f.endsWith('.webm') && !seen.has(f) && !before.has(f));
    if (produced) {
      const target = `${tour.name}.webm`;
      await rename(path.join(OUT, produced), path.join(OUT, target));
      log.videos.push(target);
    }
  }

  await browser.close();
  await writeFile(path.join(OUT, 'tour-log.json'), `${JSON.stringify(log, null, 2)}\n`);

  const bad = log.routes.filter((r) => r.status === null || r.status < 200 || r.status >= 300);
  for (const r of log.routes) console.log(`${r.status ?? 'ERR'}  ${r.url}`);
  console.log(`videos: ${log.videos.join(', ') || 'none'}`);
  console.log(`console/page errors captured: ${log.problems.length}`);
  if (bad.length) {
    console.error(`non-2xx routes: ${bad.map((r) => r.url).join(', ')}`);
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
