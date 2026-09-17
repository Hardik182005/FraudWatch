import { expect, test } from '@playwright/test';
import { collectErrors, expectNoHorizontalOverflow, expectPageShell } from './helpers';

const ROUTES = ['/study', '/ca1', '/ca2', '/experiments', '/architecture', '/evidence'];

test.describe('FraudWatch Lab pages', () => {
  for (const path of ROUTES) {
    test(`${path} renders content`, async ({ page }) => {
      const errors = collectErrors(page);
      await page.goto(path);
      await expectPageShell(page, 'Lab');
      const main = await page.locator('main, [role="main"]').first().innerText();
      expect(main.trim().length, `${path} rendered an empty main region`).toBeGreaterThan(40);
      await expectNoHorizontalOverflow(page);
      expect(errors, `unexpected console errors on ${path}:\n${errors.join('\n')}`).toEqual([]);
    });
  }

  test('the evidence page carries the method questions and the pending-report banner', async ({ page }) => {
    await page.goto('/evidence');
    await expect(page.getByText(/Method questions/i)).toBeVisible();
    await expect(page.getByText(/Report template pending/i)).toBeVisible();
    const q = page.locator('details, .fw-disclosure').first();
    await expect(q).toBeVisible();
  });

  test('no invented currency, merchant or city copy survives the dataset override', async ({ page }) => {
    for (const path of ['/', '/study', '/ca1', '/evidence']) {
      await page.goto(path);
      const body = (await page.locator('body').innerText()).toLowerCase();
      for (const banned of ['₹', 'rupee', 'sample merchant', 'merchants are sample']) {
        expect(body, `${path} still claims "${banned}"`).not.toContain(banned);
      }
    }
  });
});
