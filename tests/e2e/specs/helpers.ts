import { expect, type Page, type ConsoleMessage } from '@playwright/test';

/**
 * Console/page errors that are expected when the backend is asleep or absent. The deployed sites
 * must render a truthful "unavailable" state in that case, and the network failures that produce it
 * are not front-end defects. Everything else is treated as a real error.
 */
const EXPECTED = [
  /Failed to load resource/i,
  /net::ERR_/i,
  /ERR_CONNECTION_REFUSED/i,
  /the server responded with a status of (0|4\d\d|5\d\d)/i,
  /EventSource/i,
  /pipeline_(sleeping|starting_vm|starting_services|restoring|failed|stopping)/i,
  /WebGL|THREE\.WebGLRenderer|Could not create a WebGL context/i,
  // A backend that is running but does not allow this origin is the same class of condition as one
  // that is absent: the site must still render a truthful unavailable state, not a blank page.
  /blocked by CORS policy|Access to fetch at /i,
];

export function collectErrors(page: Page): string[] {
  const errors: string[] = [];
  const keep = (text: string) => {
    if (!EXPECTED.some((re) => re.test(text))) errors.push(text);
  };
  page.on('console', (m: ConsoleMessage) => m.type() === 'error' && keep(m.text()));
  page.on('pageerror', (e) => keep(`pageerror: ${e.message}`));
  return errors;
}

/** Every page must have exactly one h1, a main landmark, and a document title naming the product. */
export async function expectPageShell(page: Page) {
  await expect(page.locator('h1')).toHaveCount(1);
  await expect(page.locator('main, [role="main"]').first()).toBeVisible();
  await expect(page).toHaveTitle(/FraudWatch/);
}

/** The body must never scroll horizontally, at any viewport width. */
export async function expectNoHorizontalOverflow(page: Page) {
  const overflow = await page.evaluate(() => {
    const d = document.documentElement;
    return { scroll: d.scrollWidth, client: d.clientWidth };
  });
  expect(overflow.scroll, `document scrollWidth ${overflow.scroll} exceeds clientWidth ${overflow.client}`).toBeLessThanOrEqual(overflow.client + 1);
}

/** The landing scene renders either the 3D canvas or the 2D fallback — never nothing. */
export async function expectSceneRendered(page: Page) {
  const scene = page.locator('[data-testid="scene-3d"], [data-testid="scene-2d"]');
  await expect(scene.first()).toBeVisible({ timeout: 20_000 });
  await expect(page.getByTestId('scene-detail')).toBeVisible();
}
