import { expect, test } from '@playwright/test';
import { collectErrors, expectNoHorizontalOverflow, expectSceneRendered } from './helpers';

test.describe('One website for CA1 and CA2', () => {
  test('the landing links both tracks and they open in the same site', async ({ page }) => {
    const errors = collectErrors(page);
    await page.goto('/');
    await expect(page.getByTestId('track-ca1')).toBeVisible();
    await expect(page.getByTestId('track-ca2')).toBeVisible();
    await page.getByTestId('track-ca2').click();
    await expect(page).toHaveURL(/\/ca2$/);
    await expect(page.getByRole('heading', { level: 1 })).toContainText(/stream processing/i);
    expect(errors, `unexpected console errors:\n${errors.join('\n')}`).toEqual([]);
  });

  test('the CA2 scene is the Flink/Spark comparison, with prerecorded measurements', async ({ page }) => {
    await page.goto('/ca2');
    await expectSceneRendered(page);
    const names = (await page.locator('.fw-scene-node').allInnerTexts()).join(' ').toLowerCase();
    expect(names).toContain('flink');
    expect(names).toContain('spark');
    await expect(page.getByTestId('measured-comparison')).toBeVisible();
    await page.getByTestId('measured-run').selectOption({ index: 1 });
    await expect(page.getByTestId('measured-comparison')).toContainText(/Prerecorded/);
    await expectNoHorizontalOverflow(page);
  });

  test('CA1 shows model evidence even without a reachable API', async ({ page }) => {
    await page.goto('/ca1');
    // Either the live API answered, or the labelled bundled artifact is shown. Never an empty page.
    await expect(page.getByText(/Test PR-AUC/i).first()).toBeVisible({ timeout: 20_000 });
  });

  test('one grouped navigation covers operating and studying', async ({ page }) => {
    await page.goto('/console');
    const nav = page.getByRole('navigation', { name: 'Main' });
    for (const name of ['Overview', 'Alerts', 'System', 'CA1', 'CA2', 'Evidence']) {
      await expect(nav.getByRole('link', { name: new RegExp(`^${name}$`) })).toBeVisible();
    }
    await nav.getByRole('link', { name: /^CA1$/ }).click();
    await expect(page).toHaveURL(/\/ca1$/);
  });

  test('the command palette opens with Ctrl+K and navigates', async ({ page }) => {
    await page.goto('/console');
    await page.keyboard.press('Control+k');
    const palette = page.getByTestId('command-palette');
    await expect(palette).toBeVisible();
    await page.keyboard.type('architecture');
    await page.keyboard.press('Enter');
    await expect(page).toHaveURL(/\/architecture$/);
    await expect(palette).toBeHidden();
  });

  test('the system page renders its tabs and follows ?tab=', async ({ page }) => {
    const errors = collectErrors(page);
    await page.goto('/system?tab=quality');
    await expect(page.getByRole('heading', { level: 1 })).toHaveText(/System intelligence/);
    await expect(page.getByRole('tab', { name: /Data quality/ })).toHaveAttribute('aria-selected', 'true');
    expect(errors, `unexpected console errors:\n${errors.join('\n')}`).toEqual([]);
  });

  test('old addresses redirect inside the site', async ({ page }) => {
    await page.goto('/viva');
    await expect(page).toHaveURL(/\/evidence$/);
    await page.goto('/lab');
    await expect(page).toHaveURL(/\/study$/);
  });
  test('the Results page explains outcomes in plain language', async ({ page }) => {
    const errors = collectErrors(page);
    await page.goto('/results');
    await expect(page.getByRole('heading', { level: 1 })).toHaveText(/What FraudWatch found/);
    await expect(page.getByTestId('results-start').or(page.getByTestId('results-summary'))).toBeVisible();
    await expectNoHorizontalOverflow(page);
    expect(errors, `unexpected console errors:\n${errors.join('\n')}`).toEqual([]);
  });

  test('CA2 stream labs are interactive', async ({ page }) => {
    const errors = collectErrors(page);
    await page.goto('/ca2#beyond');
    const labs = page.getByTestId('beyond-syllabus');
    await expect(labs).toBeVisible();
    for (const name of ['Watermarks', 'Windows', 'Micro-batch model', 'Exactly-once', 'Hot keys']) {
      await labs.getByRole('tab', { name }).click();
      await expect(labs.getByRole('tab', { name })).toHaveAttribute('aria-selected', 'true');
    }
    await labs.getByRole('tab', { name: 'Exactly-once' }).click();
    await labs.getByRole('radio', { name: 'At-least-once' }).or(labs.getByRole('button', { name: 'At-least-once' })).first().click();
    await expect(labs.getByTestId('delivery-sink')).toBeVisible();
    await expectNoHorizontalOverflow(page);
    expect(errors, `unexpected console errors:\n${errors.join('\n')}`).toEqual([]);
  });
});
