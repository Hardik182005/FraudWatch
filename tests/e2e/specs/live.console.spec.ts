import { expect, test } from '@playwright/test';
import { collectErrors, expectNoHorizontalOverflow, expectPageShell } from './helpers';

const ROUTES: Array<{ path: string; title: RegExp }> = [
  { path: '/console', title: /Overview/i },
  { path: '/alerts', title: /Alerts/i },
  { path: '/scenario', title: /Scenario/i },
  { path: '/pipeline', title: /Pipeline/i },
];

test.describe('FraudWatch Live console', () => {
  for (const route of ROUTES) {
    test(`${route.path} renders with the app shell and sets its title`, async ({ page }) => {
      const errors = collectErrors(page);
      await page.goto(route.path);
      await expectPageShell(page, 'Live');
      await expect(page).toHaveTitle(route.title);
      await expectNoHorizontalOverflow(page);
      expect(errors, `unexpected console errors on ${route.path}:\n${errors.join('\n')}`).toEqual([]);
    });
  }

  test('a backend that is unavailable produces a visible, truthful state — never a blank page', async ({ page }) => {
    await page.goto('/console');
    // Whatever the backend does, the page must show either real data or an explicit error/empty state.
    const anyState = page.locator(
      '[data-testid="overview-stats"], [data-testid="error-strip"], [data-testid="wake-banner"], .fw-error, .fw-empty, .fw-skeleton',
    );
    await expect(anyState.first()).toBeVisible({ timeout: 20_000 });
    const body = await page.locator('main, [role="main"]').first().innerText();
    expect(body.trim().length, 'console main region rendered empty').toBeGreaterThan(20);
  });

  test('the primary navigation moves between console pages', async ({ page }) => {
    await page.goto('/console');
    await page.getByRole('link', { name: /^Alerts$/ }).first().click();
    await expect(page).toHaveURL(/\/alerts/);
    await page.getByRole('link', { name: /^Pipeline$/ }).first().click();
    await expect(page).toHaveURL(/\/pipeline/);
  });

  test('an unknown route renders the not-found page rather than crashing', async ({ page }) => {
    await page.goto('/this-route-does-not-exist');
    await expect(page.getByText(/not found/i).first()).toBeVisible();
  });

  test('theme toggle switches the document theme and persists across a reload', async ({ page }) => {
    await page.goto('/console');
    const before = await page.evaluate(() => document.documentElement.getAttribute('data-theme'));
    await page.getByRole('button', { name: /theme|dark|light/i }).first().click();
    await expect
      .poll(() => page.evaluate(() => document.documentElement.getAttribute('data-theme')))
      .not.toBe(before);
    const after = await page.evaluate(() => document.documentElement.getAttribute('data-theme'));
    await page.reload();
    await expect
      .poll(() => page.evaluate(() => document.documentElement.getAttribute('data-theme')))
      .toBe(after);
  });
});
