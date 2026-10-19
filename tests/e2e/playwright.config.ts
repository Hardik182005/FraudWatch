import { defineConfig, devices } from '@playwright/test';

/**
 * Browser checks for the website (CA1 + CA2 on one origin). The suite runs against the production Vite build served by
 * `vite preview`, so it exercises the same bundle that is deployed to Cloud Run.
 *
 * The backend is optional: every spec here must pass whether or not the pipeline API is
 * reachable, because the deployed sites are expected to render a truthful unavailable state
 * while the VM is asleep. Specs that need real data are tagged @backend and are skipped
 * automatically when FRAUDWATCH_E2E_API is not set.
 */
const PORT = Number(process.env.FRAUDWATCH_E2E_PORT ?? 4173);

export default defineConfig({
  testDir: './specs',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: [['list'], ['html', { outputFolder: 'report', open: 'never' }]],
  timeout: 45_000,
  expect: { timeout: 10_000 },
  use: {
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    video: 'off',
  },
  projects: [
    {
      name: 'web',
      testMatch: /(live|lab|site)\..*\.spec\.ts/,
      // The responsive spec asserts phone-only behaviour (the 2D scene under 640px), so it belongs
      // to the mobile project alone; running it at 1280px asserted something that is not true there.
      testIgnore: /live\.responsive\.spec\.ts/,
      use: { ...devices['Desktop Chrome'], baseURL: `http://127.0.0.1:${PORT}` },
    },
    {
      name: 'mobile-live',
      testMatch: /live\.responsive\.spec\.ts/,
      use: { ...devices['Pixel 7'], baseURL: `http://127.0.0.1:${PORT}` },
    },
  ],
  webServer: {
    command: `npm run preview -- --port ${PORT} --host 127.0.0.1`,
    url: `http://127.0.0.1:${PORT}/`,
    cwd: '../..',
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
  },
});
