# Isolated link edge

This Compose project runs the link selector and static landing page beside the
separately managed Telegram Web container. It does not use the server stack's
Compose file. The selector reaches Web through `telegram-web-edge-net` as
`web:8080` and reaches landing over the private `private-edge` bridge. Only the
selector and landing containers publish ports, both on host loopback. The
project has no environment, secret, volume, or server-service dependency.

## Validate and build

From the repository root, inspect the resolved service settings and build only
these images:

```sh
docker compose --env-file /dev/null --project-directory deploy/link-edge --file deploy/link-edge/compose.yaml config
docker compose --env-file /dev/null --project-directory deploy/link-edge --file deploy/link-edge/compose.yaml build
```

`/dev/null` prevents the server stack's `.env` from entering this project.
The selector's Docker health check requests a synthetic username path and Web
`/`; landing's check requests the same synthetic route directly. Neither probe
uses a new HTTP endpoint or prints a URL or response body.

## Activation contract

Activate only after the image changes are reviewed and merged, latest-base CI
is green, the deployed Web and username-reservation prerequisites are current,
and a fresh count-only claim audit still shows no conflicting claimant. Confirm
the existing `web` alias on `telegram-web-edge-net`, loopback listeners 8081
and 8082 are unused, and Serve still has HTTPS `/` to Web on 8080, `/apiws` to
2444, `/admin` to 2445, and `/.well-known/telegramd/client` to 39481. Keep Funnel
off and do not change the MTProto listeners.

The rollout starts and stops this project only:

```sh
docker compose --env-file /dev/null --project-directory deploy/link-edge --file deploy/link-edge/compose.yaml up -d --build
docker compose --env-file /dev/null --project-directory deploy/link-edge --file deploy/link-edge/compose.yaml ps -a
```

Before changing Serve, verify the health checks and synthetic landing over
`http://127.0.0.1:8081/` and `http://127.0.0.1:8082/syntheticname`; verify Web
`/` through the selector. Then change only the HTTPS `/` upstream to
`http://127.0.0.1:8081` with the installed Tailscale Serve CLI. Save a
0600 pre-change status snapshot, then apply the root-only mount:

```sh
snapshot="/root/tailscale-serve-pre-link-edge-$(date +%Y%m%d%H%M%S).json"
umask 077
tailscale serve status --json > "$snapshot"
chmod 0600 "$snapshot"
tailscale serve --bg --https=443 --set-path=/ http://127.0.0.1:8081
tailscale serve status --json
```

The `--set-path=/` invocation updates the root handler; verify `/apiws`,
`/admin`, and `/.well-known/telegramd/client` remain configured and reachable.
See the [Tailscale Serve CLI reference](https://tailscale.com/docs/reference/tailscale-cli/serve).
Do not replace the full Serve handler table or enable Funnel.

Run the synthetic route, malformed-path, no-log-leak, stopped-upstream,
Web/CSP, method/header, `/admin`, WebSocket, discovery, and network-isolation
checks from MAIN-1160 before acceptance. Use only synthetic link values. Verify
the published ports are still loopback-only and the selector cannot reach
Postgres or host admin listeners.

## Rollback

Restore only HTTPS `/` to Web at `http://127.0.0.1:8080`, confirm `/apiws`,
`/admin`, and discovery are unchanged, then stop this isolated project:

```sh
tailscale serve --bg --https=443 --set-path=/ http://127.0.0.1:8080
tailscale serve status --json
docker compose --env-file /dev/null --project-directory deploy/link-edge --file deploy/link-edge/compose.yaml down
```

This removes only the selector, landing container, and this project's private
bridge. It does not touch the Web container, server stack, migrations, volumes,
or credentials. The pre-change Serve status and the exact restore commands must
be recorded on the deployment issue before activation.
