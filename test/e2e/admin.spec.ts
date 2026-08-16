import { expect, test, type Page, type Response } from '@playwright/test';

// The admin token the Go e2e server (test/e2e/adminserver) was started with.
const ADMIN_TOKEN = 'e2e-secret-token';

const SESSION_COOKIE = '__Host-admin-session';
const CSRF_COOKIE = '__Host-csrf-token';

// extractCsrfToken pulls the token out of a rendered form field. The tests
// deliberately never compute tokens: a template change that drops the field
// must fail the test, not silently pass.
async function extractCsrfToken(page: Page, url: string): Promise<string> {
  const response = await page.goto(url);
  expect(response, `GET ${url}`).not.toBeNull();
  expect(response!.status(), `GET ${url} status`).toBe(200);
  const token = await page.locator('input[name="csrf_token"]').getAttribute('value');
  expect(token, `csrf_token field on ${url}`).toMatch(/^[0-9a-f]{64}$/);
  return token;
}

// login drives the real login form and returns the session cookie value.
async function login(page: Page): Promise<string> {
  await extractCsrfToken(page, '/admin/login');
  await page.locator('input[name="token"]').fill(ADMIN_TOKEN);
  const [response] = await Promise.all([
    page.waitForResponse(
      (r) => r.url().includes('/admin/login') && r.request().method() === 'POST',
      { timeout: 15_000 },
    ),
    page.locator('form[action="/admin/login"] button[type="submit"]').click(),
  ]);
  expect(response.status(), 'login POST status').toBe(302);

  const session = (await page.context().cookies()).find((c) => c.name === SESSION_COOKIE);
  expect(session, 'session cookie after login').toBeTruthy();
  return session!.value;
}

// logoutFromDashboard submits the dashboard's logout form and returns the
// final response after the redirect chain.
async function logoutFromDashboard(page: Page): Promise<Response> {
  await extractCsrfToken(page, '/admin/dashboard');
  const [response] = await Promise.all([
    page.waitForResponse(
      (r) => r.url().includes('/admin/logout') && r.request().method() === 'POST',
      { timeout: 15_000 },
    ),
    page.locator('form.logout-form button[type="submit"]').click(),
  ]);
  expect(response.status(), 'logout POST status').toBe(302);
  // The 302 lands on /admin/login, which the browser follows.
  await page.waitForURL('**/admin/login');
  return response;
}

// The admin session cookie is a __Host- prefix cookie: Chromium rejects
// addCookies for it unless the full attribute set (httpOnly, secure,
// sameSite) is present, so every synthetic cookie carries them.
const cookieAttrs = { domain: '127.0.0.1', path: '/', httpOnly: true, secure: true, sameSite: 'Strict' as const };

// postLogout issues a raw POST /admin/logout with an explicit Cookie header,
// so scenarios that need a stale or missing cookie can be driven precisely.
// The header overrides the context cookie jar; addCookies would also send
// both cookie names on every subsequent request.
async function postLogout(
  page: Page,
  cookies: Array<{ name: string; value: string }>,
  csrfToken: string,
): Promise<Response> {
  const cookieHeader = cookies.map((c) => `${c.name}=${c.value}`).join('; ');
  return page.request.post('/admin/logout', {
    form: { csrf_token: csrfToken },
    headers: cookieHeader ? { Cookie: cookieHeader } : {},
    // No Origin header: the handler only rejects a present-but-wrong Origin.
  });
}

test.describe('admin login/logout CSRF flow', () => {
  test('full flow: login, dashboard, logout', async ({ page }) => {
    // Pre-auth: the login page issues a CSRF cookie and a form-bound token.
    const loginPage = await page.goto('/admin/login');
    expect(loginPage).not.toBeNull();
    expect(loginPage!.status()).toBe(200);
    const preAuthCookie = (await page.context().cookies()).find((c) => c.name === CSRF_COOKIE);
    expect(preAuthCookie, 'pre-auth CSRF cookie').toBeTruthy();
    expect(preAuthCookie!.value).toMatch(/^[0-9a-f]{64}$/);

    const sessionValue = await login(page);
    expect(sessionValue).toMatch(/^[0-9a-f]{64}$/);

    // The CSRF cookie is cleared on successful login.
    const csrfAfterLogin = (await page.context().cookies()).find((c) => c.name === CSRF_COOKIE);
    expect(csrfAfterLogin?.value ?? '', 'CSRF cookie must be cleared after login').toBe('');

    // Dashboard renders with a session-bound CSRF token.
    const dashboardToken = await extractCsrfToken(page, '/admin/dashboard');
    expect(dashboardToken).toMatch(/^[0-9a-f]{64}$/);

    // Logout with the session cookie and the session-bound token redirects
    // to the login page: no 401, no 403.
    await logoutFromDashboard(page);
    expect(page.url()).toContain('/admin/login');

    // The session is gone: the dashboard now rejects the old cookie.
    const dashboard = await page.goto('/admin/dashboard');
    expect(dashboard!.status()).toBe(401);
  });

  test('multi-tab: identical token across contexts, logout from either succeeds', async ({
    browser,
    page,
  }) => {
    const sessionValue = await login(page);

    // Two contexts sharing the session cookie simulate two tabs.
    const contextA = await browser.newContext();
    const contextB = await browser.newContext();
    for (const context of [contextA, contextB]) {
      await context.addCookies([{ name: SESSION_COOKIE, value: sessionValue, ...cookieAttrs }]);
    }
    const pageA = await contextA.newPage();
    const pageB = await contextB.newPage();

    const tokenA = await extractCsrfToken(pageA, '/admin/dashboard');
    const tokenB = await extractCsrfToken(pageB, '/admin/dashboard');
    expect(tokenA, 'CSRF token must be identical across tabs').toBe(tokenB);

    // Logout from either context succeeds.
    await logoutFromDashboard(pageA);
    expect(pageA.url()).toContain('/admin/login');

    // The session is deleted server-side, so the second tab is logged out too.
    const dashboardB = await pageB.goto('/admin/dashboard');
    expect(dashboardB!.status()).toBe(401);

    await contextA.close();
    await contextB.close();
  });

  test('stale pre-auth CSRF cookie does not break post-auth logout', async ({ page }) => {
    // Grab a pre-auth CSRF cookie, then log in (which clears it).
    const preAuth = await page.goto('/admin/login');
    expect(preAuth!.status()).toBe(200);
    const preAuthCookie = (await page.context().cookies()).find((c) => c.name === CSRF_COOKIE);
    expect(preAuthCookie?.value, 'pre-auth CSRF cookie').toMatch(/^[0-9a-f]{64}$/);

    const sessionValue = await login(page);

    // Logout carrying the stale pre-auth CSRF cookie alongside the session
    // cookie still succeeds: the logout check derives the expected token from
    // the session, not the CSRF cookie.
    const dashboardToken = await extractCsrfToken(page, '/admin/dashboard');
    const logoutResponse = await postLogout(
      page,
      [
        { name: SESSION_COOKIE, value: sessionValue },
        { name: CSRF_COOKIE, value: preAuthCookie!.value },
      ],
      dashboardToken,
    );
    // The 302 redirect to /admin/login is followed by the API client, so the
    // final status is 200; what matters is that it is not a 401.
    expect(logoutResponse.status(), 'logout with stale CSRF cookie').toBe(200);
    expect(logoutResponse.url()).toContain('/admin/login');
  });

  test('rejection: wrong or missing CSRF token, missing session', async ({ page }) => {
    const sessionValue = await login(page);
    const dashboardToken = await extractCsrfToken(page, '/admin/dashboard');

    // Wrong token: 401, not 200.
    const wrongToken = await postLogout(page, [{ name: SESSION_COOKIE, value: sessionValue }], '0'.repeat(64));
    expect(wrongToken.status(), 'logout with wrong CSRF token').toBe(401);

    // Missing token: 401.
    const missingToken = await postLogout(
      page,
      [{ name: SESSION_COOKIE, value: sessionValue }],
      '',
    );
    expect(missingToken.status(), 'logout with missing CSRF token').toBe(401);

    // No session cookie: 401, even with a well-formed token.
    const noSession = await postLogout(page, [], dashboardToken);
    expect(noSession.status(), 'logout with no session cookie').toBe(401);
  });
});

test.describe('admin SSE stream', () => {
  test('live tick delivers DashboardFragmentRenderer markup', async ({ page }) => {
    // Log in so the page context holds the session cookie.
    await login(page);
    await page.goto('/admin/dashboard');

    // Open a native EventSource from the browser context. The session cookie is
    // sent automatically; this exercises the full broadcaster path including the
    // wired DashboardFragmentRenderer.
    const eventData = await page.evaluate((): Promise<string> => {
      return new Promise((resolve, reject) => {
        const es = new EventSource('/admin/events');
        const timer = setTimeout(() => {
          es.close();
          reject(new Error('SSE timeout: no datastar-merge-fragments event within 5 s'));
        }, 5000);
        es.addEventListener('datastar-merge-fragments', (e: Event) => {
          clearTimeout(timer);
          es.close();
          resolve((e as MessageEvent).data);
        });
        es.onerror = () => {
          clearTimeout(timer);
          es.close();
          reject(new Error('SSE connection error'));
        };
      });
    });

    // The Datastar data lines carry the patch target and merge mode; the
    // bundle drops the event silently if either drifts from the page.
    expect(eventData).toContain('selector #metrics-stream');
    expect(eventData).toContain('mergeMode morph');

    // DashboardFragmentRenderer produces shadcn-templ card markup.
    // DefaultFragmentRenderer produces minimal <span data-metric="..."> elements
    // with no card structure. Either assertion distinguishes the two renderers.
    expect(eventData).toContain('data-slot="card-content"');
    expect(eventData).toContain('id="v-connections"');
  });

  // The raw-EventSource test above proves the server emits the right bytes. It
  // cannot tell whether the bundle acts on them: a wrong event name, wire key,
  // merge mode or selector is answered with a 200 and no patch, so the page
  // just stops updating. This drives the bundle itself and asserts what an
  // operator sees.
  test('Datastar bundle patches the page and the chip reports live', async ({ page }) => {
    await login(page);
    await page.goto('/admin/dashboard');

    // First paint is server-rendered and must not wait on the stream.
    await expect(page.locator('#v-connections')).toBeVisible();

    // The chip reaches its live state only if the bundle dispatched
    // datastar-sse with elId sse-root, and only reports a fresh age if the
    // MutationObserver saw a patch land on #metrics-stream.
    await expect(page.locator('#chip-text')).toHaveText(/Live · updated/, {
      timeout: 15000,
    });

    // The patch must leave the dashboard whole. A fragment with more than one
    // top-level node is merged node by node into the same selector, so every
    // card but the last disappears while the chip still reads live.
    await expect(page.locator('#v-connections')).toBeVisible();
    await expect(page.locator('#v-total_users')).toBeVisible();
    await expect(page.locator('#storage-tbody tr').first()).toBeVisible();
    // The fragment carries the target id: merged into itself, never nested.
    await expect(page.locator('#metrics-stream #metrics-stream')).toHaveCount(0);
  });

  // Criterion: the chip flips to its critical state when the stream ends and
  // recovers when it comes back. The server only closes on the 25-minute cap
  // or shutdown, so the transition is driven through the lifecycle events the
  // bundle dispatches — "started" / "finished" / "error" on datastar-sse, the
  // names read off the bundle itself.
  test('chip reports disconnect and recovers on reconnect', async ({ page }) => {
    await login(page);
    await page.goto('/admin/dashboard');
    await expect(page.locator('#chip-text')).toHaveText(/Live · updated/, { timeout: 15000 });

    const lifecycle = (type: string) =>
      page.evaluate((t) => {
        document.dispatchEvent(
          new CustomEvent('datastar-sse', { detail: { type: t, elId: 'sse-root' } }),
        );
      }, type);

    await lifecycle('finished');
    await expect(page.locator('#chip-text')).toHaveText(/Disconnected/);

    await lifecycle('started');
    await expect(page.locator('#chip-text')).toHaveText(/Live/);

    // An error on the stream is the same observable state as a clean close.
    await lifecycle('error');
    await expect(page.locator('#chip-text')).toHaveText(/Disconnected/);
  });
});

test.describe('admin dashboard component script', () => {
  // shadcn-templ.js registers every progressbar it finds and re-registers
  // markup swapped in later, via a MutationObserver on document.body. The
  // script is loaded from <head>, so whether that observer is ever attached
  // depends on the script tag: without defer, document.body is still null when
  // that line runs and the registration path dies with it. Load-time init
  // survives — it waits for DOMContentLoaded — so this only shows up once the
  // first SSE patch replaces the markup that init had already registered.
  test('components re-init after an SSE swap', async ({ page }) => {
    const errors: string[] = [];
    page.on('pageerror', (e) => errors.push(e.message));

    await login(page);

    // Count swaps from inside the page: the first patch can land before the
    // test could tag anything, so the swap has to be observed, not inferred.
    await page.addInitScript(() => {
      (window as unknown as { __swaps: number }).__swaps = 0;
      document.addEventListener('DOMContentLoaded', () => {
        const target = document.getElementById('metrics-stream');
        if (!target) return;
        new MutationObserver(() => {
          (window as unknown as { __swaps: number }).__swaps++;
        }).observe(target, { childList: true });
      });
    });
    await page.goto('/admin/dashboard');

    await expect
      .poll(() => page.evaluate(() => (window as unknown as { __swaps: number }).__swaps), {
        timeout: 20000,
      })
      .toBeGreaterThan(0);

    // Every bar on the page now came from the stream. Each carries the marker
    // only if the observer re-registered it after the swap.
    await expect(
      page.locator('[role="progressbar"]:not([data-tui-progress-observed])'),
    ).toHaveCount(0);

    expect(errors, 'JS errors on dashboard load').toEqual([]);
  });
});
