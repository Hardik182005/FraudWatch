/**
 * Browser check for cross-site synchronisation (status-checklist item 9) against the deployed origins.
 *
 * Starts a real demo session and bounded run on Live, uses "Open in Lab" to mint a read-only share
 * link, follows it to Lab, and records whether Lab shows the same run id that Live started. Writes
 * cross-site-log.json and two screenshots next to the other report media.
 *
 *   node scripts/verify_cross_site.mjs
 */
import { chromium } from '@playwright/test';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';

const LIVE = process.env.LIVE_URL ?? 'https://fraudwatch-live-2djxmgtj5a-el.a.run.app';
const OUT = process.env.SHOT_DIR ?? 'reports/media/screenshots';

const log = { checked_at: new Date().toISOString(), live: LIVE, steps: [], errors: [] };
const step = (name, detail) => { log.steps.push({ name, detail }); console.log(`${name}: ${detail}`); };

const browser = await chromium.launch();
await mkdir(OUT, { recursive: true });
const context = await browser.newContext({ viewport: { width: 1440, height: 960 } });
const page = await context.newPage();
page.on('console', (m) => { if (m.type() === 'error') log.errors.push(`live: ${m.text()}`); });

try {
  await page.goto(`${LIVE}/console`, { waitUntil: 'networkidle', timeout: 90_000 });
  const startDemo = page.getByTestId('start-demo');
  if (await startDemo.count()) {
    await startDemo.first().click();
    await startDemo.first().waitFor({ state: 'detached', timeout: 60_000 }).catch(() => {});
  }
  await page.goto(`${LIVE}/scenario`, { waitUntil: 'networkidle', timeout: 90_000 });
  const startRun = page.getByTestId('start-run').first();
  const until = Date.now() + 300_000; // covers a cold wake of the VM
  while (Date.now() < until && (await startRun.isDisabled().catch(() => true))) await page.waitForTimeout(2000);
  await startRun.click();
  await page.getByTestId('current-run').waitFor({ timeout: 60_000 });
  await page.waitForTimeout(5000);
  await page.getByRole('button', { name: 'Open in Lab' }).first().click();
  const link = page.getByTestId('open-lab-link');
  await link.waitFor({ timeout: 60_000 });
  const href = await link.getAttribute('href');
  const url = new URL(href);
  // The run id comes from Live's own session state: it is what Live says it is running.
  const liveRun = url.searchParams.get('run');
  step('share-link', `Live's share link carries run=${liveRun} engine=${url.searchParams.get('engine')} (view token present: ${Boolean(url.searchParams.get('vt'))})`);

  const lab = await context.newPage();
  lab.on('console', (m) => { if (m.type() === 'error') log.errors.push(`lab: ${m.text()}`); });
  // Landing page: the hero chip reads "Live · run <id>" once Lab has taken the shared context.
  await lab.goto(href, { waitUntil: 'networkidle', timeout: 90_000 });
  await lab.waitForTimeout(4000);
  const landingText = (await lab.locator('body').innerText()).replace(/\s+/g, ' ');
  const landingRun = landingText.match(/Live · run (r_[0-9a-f]+)/)?.[1] ?? null;
  step('lab-landing', landingRun ? `Lab landing chip reads "Live · run ${landingRun}"` : 'Lab landing shows no "Live · run" chip');
  await lab.screenshot({ path: path.join(OUT, 'cross-site-lab-received.png') });
  // Study home: the context persists across in-app navigation and is named in a banner.
  await lab.goto(new URL('/study', href).toString(), { waitUntil: 'networkidle', timeout: 90_000 });
  await lab.waitForTimeout(3000);
  const studyText = (await lab.locator('body').innerText()).replace(/\s+/g, ' ');
  const studyRun = studyText.match(/Run selected from Live\..{0,160}?(r_[0-9a-f]+)/)?.[1] ?? null;
  step('lab-study', studyRun ? `Lab /study banner: "Run selected from Live" with run ${studyRun}` : 'Lab /study shows no "Run selected from Live" banner');
  await lab.screenshot({ path: path.join(OUT, 'cross-site-lab-study.png') });

  log.live_run = liveRun;
  log.synchronised = Boolean(liveRun && landingRun === liveRun && studyRun === liveRun);
  step('result', log.synchronised ? `SYNCHRONISED: Lab shows run ${liveRun}, the run Live started, on both the landing page and /study` : `NOT confirmed (live=${liveRun}, lab landing=${landingRun}, lab study=${studyRun})`);
  await page.screenshot({ path: path.join(OUT, 'cross-site-live-share.png') });
} catch (err) {
  log.synchronised = false;
  step('FAILED', err.message.split('\n')[0]);
  process.exitCode = 1;
}

await browser.close();
await writeFile(path.join(OUT, 'cross-site-log.json'), `${JSON.stringify(log, null, 2)}\n`);
console.log(`console errors: ${log.errors.length}`);
