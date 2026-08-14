import { defineConfig } from '@playwright/test';

// The admin server (test/e2e/adminserver) is a Go process backed by a fresh
// Postgres database from the pgtest harness. It listens on 127.0.0.1:2444
// and prints "admin server ready" on stdout once the listener is bound.
export default defineConfig({
  testDir: './test/e2e',
  testMatch: /.*\.spec\.ts$/,
  timeout: 30_000,
  retries: 0,
  workers: 1,
  reporter: [['list']],
  use: {
    baseURL: 'http://127.0.0.1:2444',
    trace: 'retain-on-failure',
  },
  webServer: {
    command: 'go run -tags e2e_admin_server ./test/e2e/adminserver',
    env: { TG_ADMIN_E2E_TOKEN: 'e2e-secret-token' },
    url: 'http://127.0.0.1:2444/admin/login',
    stdout: 'pipe',
    stderr: 'pipe',
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
  },
});
