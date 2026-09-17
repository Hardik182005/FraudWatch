import { expect, test } from '@playwright/test';
import { expectNoHorizontalOverflow, expectSceneRendered } from './helpers';

test.describe('FraudWatch Live on a phone viewport', () => {
  test('the landing fits the viewport and uses the 2D scene by default', async ({ page }) => {
    await page.goto('/');
    await expectSceneRendered(page);
    await expectNoHorizontalOverflow(page);
    // The stage picks 2D under 640px wide.
    await expect(page.getByTestId('scene-2d')).toBeVisible();
  });

  test('the console fits the viewport', async ({ page }) => {
    await page.goto('/console');
    await expect(page.locator('main, [role="main"]').first()).toBeVisible();
    await expectNoHorizontalOverflow(page);
  });
});
