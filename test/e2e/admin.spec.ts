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

// The disconnected state the lifecycle tests below assert is not stable while
// the real stream is open: every arriving data patch re-runs the handler's
// connected branch, which re-hides the banner and rewrites the chip to "Live"
// (internal/admin/dashboard.templ). A patch landing between the dispatched
// event and the assertion erases the state before Playwright samples it, and
// nothing puts it back — no further disconnect fires — so the assertion fails
// outright rather than merely arriving late. The broadcaster ticks every 10s
// (sseInterval in internal/admin/sse.go), with one off-cadence wake on first
// subscribe, so a patch lands well inside the 5s default expect window.
//
// So the disconnected state is recorded as the handler writes it instead of
// sampled afterwards. Nothing is widened, slept on or retried: the recorder is
// installed before the event is dispatched, and what it captures is exactly the
// transition the handler owns. A handler that never enters its disconnect
// branch records no "Disconnected" entry and still fails the test.
interface DisconnectRecord {
  chip: string[];
  bannerShown: boolean;
}

declare global {
  interface Window {
    __disconnectRecord?: DisconnectRecord;
  }
}

async function recordDisconnectState(page: Page): Promise<void> {
  await page.evaluate(() => {
    const record: DisconnectRecord = { chip: [], bannerShown: false };
    window.__disconnectRecord = record;

    const snapshot = () => {
      const chip = document.getElementById('chip-text');
      if (chip?.textContent) record.chip.push(chip.textContent);
      const banner = document.getElementById('banner-disconnected');
      if (banner && banner.getClientRects().length > 0 && getComputedStyle(banner).display !== 'none') {
        record.bannerShown = true;
      }
    };
    snapshot();

    new MutationObserver(snapshot).observe(document.body, {
      subtree: true,
      childList: true,
      characterData: true,
      attributes: true,
      attributeFilter: ['class'],
    });
  });
}

// bannerWasShown / chipTexts read back what the recorder captured since
// recordDisconnectState was last called.
const bannerWasShown = (page: Page): Promise<boolean> =>
  page.evaluate(() => {
    if (!window.__disconnectRecord) throw new Error('recordDisconnectState was not called');
    return window.__disconnectRecord.bannerShown;
  });

const chipTexts = (page: Page): Promise<string[]> =>
  page.evaluate(() => {
    if (!window.__disconnectRecord) throw new Error('recordDisconnectState was not called');
    return window.__disconnectRecord.chip;
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

    await recordDisconnectState(page);
    await lifecycle('finished');
    await expect
      .poll(() => chipTexts(page), { message: 'chip text after "finished"' })
      .toEqual(expect.arrayContaining([expect.stringMatching(/Disconnected/)]));

    // "started" sets the chip to exactly "● Live · connecting…" — distinct from
    // the data-patch branch, which calls updateChip() and writes "● Live · updated
    // Xs ago". Reading synchronously in the same evaluate call blocks any async
    // patch from landing between the dispatch and the assertion.
    const chipTextAfterStarted = await page.evaluate(() => {
      document.dispatchEvent(
        new CustomEvent('datastar-sse', { detail: { type: 'started', elId: 'sse-root' } }),
      );
      return document.getElementById('chip-text')?.textContent ?? '';
    });
    expect(chipTextAfterStarted, '"started" sets chip to connecting state').toMatch(/Live · connecting/);

    // An error on the stream is the same observable state as a clean close.
    await recordDisconnectState(page);
    await lifecycle('error');
    await expect
      .poll(() => chipTexts(page), { message: 'chip text after "error"' })
      .toEqual(expect.arrayContaining([expect.stringMatching(/Disconnected/)]));
  });

  // The chip test above proves the bundle reacts to the lifecycle event, but
  // it asserts chip text only. Both assertions would survive the banner toggle
  // being dropped from the disconnect branch, so the banner is pinned here:
  // same event, recorded by getClientRects/getComputedStyle rather than a
  // class check, so an inline display:none or a hidden attribute also fails it.
  test('disconnect reveals the banner', async ({ page }) => {
    await login(page);
    await page.goto('/admin/dashboard');
    await expect(page.locator('#chip-text')).toHaveText(/Live · updated/, { timeout: 15000 });

    const banner = page.locator('#banner-disconnected');
    await expect(banner).toBeHidden();

    // "finished" is the lifecycle event the bundle dispatches when the SSE
    // stream closes; the chip and the banner share its handler branch.
    await recordDisconnectState(page);
    await page.evaluate(() => {
      document.dispatchEvent(
        new CustomEvent('datastar-sse', { detail: { type: 'finished', elId: 'sse-root' } }),
      );
    });

    await expect
      .poll(() => bannerWasShown(page), { message: 'banner revealed on disconnect' })
      .toBe(true);
  });

  // The test above pins the reveal half; this one pins the reverse. The
  // reconnect branch of the same handler re-hides the banner, and a
  // regression there leaves it on screen after the stream is back — until an
  // unrelated data patch happens to re-hide it. Both transitions are driven
  // by the lifecycle events the page listens for, never by class edits.
  test('reconnect re-hides the banner', async ({ page }) => {
    await login(page);
    await page.goto('/admin/dashboard');
    await expect(page.locator('#chip-text')).toHaveText(/Live · updated/, { timeout: 15000 });

    const banner = page.locator('#banner-disconnected');
    await expect(banner).toBeHidden();

    // Drive both transitions inside a single evaluate so no SSE data patch can
    // land between them. finished removes "hidden" (banner visible); started adds
    // it back (banner hidden). Asserting { before: false, after: true } proves the
    // reconnect branch ran — there is no other transition that satisfies it. A data
    // patch cannot intervene because JavaScript is single-threaded and no external
    // event can interleave with a synchronous evaluate.
    const { before, after } = await page.evaluate(() => {
      const b = document.getElementById('banner-disconnected');
      document.dispatchEvent(
        new CustomEvent('datastar-sse', { detail: { type: 'finished', elId: 'sse-root' } }),
      );
      const before = b?.classList.contains('hidden') ?? true;
      document.dispatchEvent(
        new CustomEvent('datastar-sse', { detail: { type: 'started', elId: 'sse-root' } }),
      );
      const after = b?.classList.contains('hidden') ?? false;
      return { before, after };
    });
    expect(before, 'finished reveals the banner').toBe(false);
    expect(after, 'reconnect re-hides the banner').toBe(true);
  });
});

test.describe('admin dashboard stylesheet', () => {
  // dashboard.css is generated and committed, and nothing regenerates it
  // automatically. When it goes stale against the templates the page still
  // renders — it just loses the rules it was built without. `hidden` is the
  // one that changes behaviour rather than looks: the templates hide the
  // banners by toggling it, so a missing rule leaves them permanently on
  // screen. Asserted here rather than in Go because only a browser resolves
  // the stylesheet.
  test('hidden class actually hides', async ({ page }) => {
    await login(page);
    await page.goto('/admin/dashboard');

    const banner = page.locator('#banner-disconnected');
    await expect(banner).toHaveClass(/\bhidden\b/);
    await expect(banner).toBeHidden();

    // Nothing else is keeping it off the page: dropping the class alone must
    // reveal it, which is what the chip does on disconnect.
    //
    // The class is dropped on a copy, never on the live banner. Every arriving
    // data patch re-runs the chip handler, which re-adds `hidden` to the real
    // banner — so removing it there is undone by the next tick and the reveal
    // assertion fails whenever a patch lands inside its window. The copy
    // carries the same classes and the same computed style, which is what this
    // test reads, and the chip script holds a direct reference to the original
    // so it never touches the copy.
    await page.evaluate(() => {
      const source = document.getElementById('banner-disconnected');
      if (!source) throw new Error('no #banner-disconnected to copy');

      const copy = source.cloneNode(true) as HTMLElement;
      // Ids would otherwise be duplicated across the document.
      copy.querySelectorAll('[id]').forEach((el) => el.removeAttribute('id'));
      copy.id = 'banner-style-probe';

      const host = document.getElementById('main');
      if (!host) throw new Error('no #main to host the banner copy');
      host.appendChild(copy);
    });

    const probe = page.locator('#banner-style-probe');
    await expect(probe).toHaveClass(/\bhidden\b/);
    await expect(probe).toBeHidden();

    await probe.evaluate((el) => el.classList.remove('hidden'));
    await expect(probe).toBeVisible();
  });
});

test.describe('admin dashboard component script', () => {
  // shadcn-templ.js ends by attaching a MutationObserver to document.body, so
  // that markup arriving after load registers itself. Loaded from <head>
  // without defer, that line runs while document.body is still null: the
  // script dies there with a TypeError, taking the observer with it. Load-time
  // registration survives — it waits for DOMContentLoaded — so the page looks
  // correct until something inserts markup.
  test('no JS errors on dashboard load', async ({ page }) => {
    const errors: string[] = [];
    page.on('pageerror', (e) => errors.push(e.message));

    await login(page);
    await page.goto('/admin/dashboard');
    await expect(page.locator('#v-connections')).toBeVisible();

    expect(errors, 'JS errors on dashboard load').toEqual([]);
  });

  // Datastar morphs the fragment into #metrics-stream, so a steady-state patch
  // updates attributes on the bars already there and inserts nothing. Nodes do
  // appear once the patch changes structure — a storage row coming or going —
  // and the server's numbers decide when that happens, which is not something
  // a test can provoke. So the insertion is done here instead, with a clone of
  // a real server-rendered bar: what the observer sees is the same either way.
  //
  // The clone goes into #main, never into #metrics-stream. The observer this
  // test pins watches document.body with subtree: true, so the insertion point
  // makes no difference to what it sees — but a morph deletes any node inside
  // the swap target that the server did not render, and the first patch lands
  // within ~100 ms of load. Injecting into the target raced that patch and
  // reddened unrelated PRs. #main is outside the target and is never patched.
  test('markup inserted after load registers itself', async ({ page }) => {
    await login(page);
    await page.goto('/admin/dashboard');
    await expect(page.locator('#v-connections')).toBeVisible();

    await page.evaluate(() => {
      const source = document.querySelector('[role="progressbar"]');
      if (!source) throw new Error('no server-rendered progressbar to clone');

      const clone = source.cloneNode(true) as HTMLElement;
      clone.id = 'probe-bar';
      // Arrive as unregistered markup would, at a value no bar on the page
      // holds, so a stale indicator cannot pass for a fresh one.
      clone.removeAttribute('data-tui-progress-observed');
      clone.removeAttribute('aria-valuetext');
      clone.setAttribute('aria-valuenow', '37');
      clone.setAttribute('aria-valuemax', '100');
      clone
        .querySelectorAll<HTMLElement>('[data-tui-progress-indicator]')
        .forEach((el) => (el.style.width = ''));

      const target = document.getElementById('metrics-stream');
      if (!target) throw new Error('no #metrics-stream swap target on the page');
      const host = document.getElementById('main');
      if (!host) throw new Error('no #main to host the probe bar');
      if (target.contains(host)) {
        throw new Error('#main sits inside the swap target: the probe would race the morph');
      }
      host.appendChild(clone);
    });

    // The observer registers the bar and runs it through updateProgress: the
    // marker and the value text come from that one path, and neither appears
    // if the observer never attached. The indicator's width is not asserted
    // here: /.+/ matches every computed width value, so such a check could
    // never fail on any build.
    const probe = page.locator('#probe-bar');
    await expect(probe).toHaveAttribute('data-tui-progress-observed', 'true');
    await expect(probe).toHaveAttribute('aria-valuetext', '37%');

    // Registration is what wires later attribute changes to the indicator, so
    // the bar must now track its own value the way a patched one does.
    await page.evaluate(() => {
      const bar = document.getElementById('probe-bar');
      if (!bar) throw new Error('probe bar vanished before the second value change');
      bar.setAttribute('aria-valuenow', '81');
    });
    await expect(probe).toHaveAttribute('aria-valuetext', '81%');
  });
});
