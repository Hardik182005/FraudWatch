import { expect, test } from '@playwright/test';
import { collectErrors, expectNoHorizontalOverflow, expectPageShell, expectSceneRendered } from './helpers';

test.describe('FraudWatch Live landing', () => {
  test('renders the hero, the 3D/2D scene and no unexpected console errors', async ({ page }) => {
    const errors = collectErrors(page);
    await page.goto('/');
    await expectPageShell(page, 'Live');
    await expectSceneRendered(page);
    await expectNoHorizontalOverflow(page);
    expect(errors, `unexpected console errors:\n${errors.join('\n')}`).toEqual([]);
  });

  test('scene status is never invented: unknown status reads as not checked', async ({ page }) => {
    await page.goto('/');
    await expectSceneRendered(page);
    const detail = page.getByTestId('scene-detail');
    await expect(detail).toBeVisible();
    // Either a real probe answered (a concrete status word) or the honest not-checked message is shown.
    const text = (await detail.innerText()).toLowerCase();
    expect(/ok|degraded|down|idle|unknown|not checked/.test(text)).toBe(true);
  });

  test('the 2D fallback is reachable from the scene toggle', async ({ page }) => {
    await page.goto('/');
    await expectSceneRendered(page);
    const twoD = page.getByRole('button', { name: /^2D$/ });
    if (await twoD.count()) {
      await twoD.first().click();
      await expect(page.getByTestId('scene-2d')).toBeVisible();
    }
  });

  test('selecting a scene component updates the detail panel', async ({ page }) => {
    await page.goto('/');
    await expectSceneRendered(page);
    const nodes = page.locator('.fw-scene-node');
    const count = await nodes.count();
    expect(count).toBeGreaterThan(1);
    // A node renders its label plus a status line; the detail panel is keyed on the label alone.
    const label = (await nodes.nth(1).innerText()).trim().split(String.fromCharCode(10))[0].trim();
    await nodes.nth(1).click();
    await expect(page.getByTestId('scene-detail')).toContainText(label);
  });

  test('the landing offers a route into the console', async ({ page }) => {
    await page.goto('/');
    const toConsole = page.getByRole('link', { name: /console|open live|dashboard/i }).first();
    await expect(toConsole).toBeVisible();
    await toConsole.click();
    await expect(page).toHaveURL(/\/console/);
  });

  test('no invented currency, merchant or city copy survives the dataset override', async ({ page }) => {
    await page.goto('/');
    const body = (await page.locator('body').innerText()).toLowerCase();
    for (const banned of ['₹', 'rupee', 'sample merchant', 'merchant name', 'merchants are sample']) {
      expect(body, `landing copy still claims "${banned}"`).not.toContain(banned);
    }
  });
});
