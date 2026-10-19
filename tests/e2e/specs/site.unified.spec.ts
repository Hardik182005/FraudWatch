import { expect, test } from '@playwright/test';
import { collectErrors, expectNoHorizontalOverflow, expectPageShell, expectSceneRendered } from './helpers';

test.describe('FraudWatch Lab landing', () => {
  test('renders its own hero scene and no unexpected console errors', async ({ page }) => {
    const errors = collectErrors(page);
    await page.goto('/');
    await expectPageShell(page, 'Lab');
    await expectSceneRendered(page);
    await expectNoHorizontalOverflow(page);
    expect(errors, `unexpected console errors:\n${errors.join('\n')}`).toEqual([]);
  });

  test('the lab scene is the Flink/Spark comparison, distinct from the Live scene', async ({ page }) => {
    await page.goto('/');
    await expectSceneRendered(page);
    const names = (await page.locator('.fw-scene-node').allInnerTexts()).join(' ').toLowerCase();
    expect(names).toContain('flink');
    expect(names).toContain('spark');
  });

  test('/viva is removed and redirects to the landing', async ({ page }) => {
    await page.goto('/viva');
    await expect(page).toHaveURL(/\/$/);
    const body = (await page.locator('body').innerText()).toLowerCase();
    expect(body).not.toContain('viva mode');
  });
});
