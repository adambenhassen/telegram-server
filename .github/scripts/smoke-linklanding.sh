#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
temp_dir="$(mktemp -d)"
service_pid=""

cleanup() {
	if [[ -n "$service_pid" ]] && kill -0 "$service_pid" 2>/dev/null; then
		kill -TERM "$service_pid" 2>/dev/null || true
		wait "$service_pid" 2>/dev/null || true
	fi
	rm -rf -- "$temp_dir"
}
trap cleanup EXIT

port="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')"
(cd "$repo_root" && go build -o "$temp_dir/linklanding" ./cmd/linklanding)
env -i "$temp_dir/linklanding" -port "$port" >"$temp_dir/stdout" 2>"$temp_dir/log" &
service_pid=$!

origin="http://127.0.0.1:$port"
invite_url="$origin/+synthetic-live-capability?secret=query-secret"
ready=false
for ((attempt = 0; attempt < 100; attempt++)); do
	if curl --silent --fail --output "$temp_dir/body" --dump-header "$temp_dir/headers" \
		--write-out '%{http_code}' --header 'Referer: https://referer-secret.example/path' \
		"$invite_url" >"$temp_dir/status" 2>/dev/null; then
		ready=true
		break
	fi
	sleep 0.05
done
if [[ "$ready" != true ]]; then
	cat "$temp_dir/log" >&2
	printf 'local link landing service did not become ready\n' >&2
	exit 1
fi

if [[ "$(<"$temp_dir/status")" != 200 ]]; then
	printf 'landing GET returned %s, want 200\n' "$(<"$temp_dir/status")" >&2
	exit 1
fi
if [[ "$(<"$temp_dir/body")" != '<!doctype html><html lang="en"><body><main>Open this in Telegramd</main></body></html>' ]]; then
	printf 'landing GET returned unexpected content\n' >&2
	exit 1
fi

assert_header() {
	local name="$1"
	local want="$2"
	local file="$3"
	if ! awk -v want_name="$name" -v want_value="$want" '
		{
			line = $0
			sub(/\r$/, "", line)
			separator = index(line, ":")
			if (separator > 0 && tolower(substr(line, 1, separator - 1)) == tolower(want_name) && substr(line, separator + 2) == want_value) {
				found = 1
			}
		}
		END { exit !found }
	' "$file"; then
		printf 'missing or incorrect %s header in %s\n' "$name" "$file" >&2
		exit 1
	fi
}

assert_header 'Cache-Control' 'no-store' "$temp_dir/headers"
assert_header 'Referrer-Policy' 'no-referrer' "$temp_dir/headers"
assert_header 'X-Content-Type-Options' 'nosniff' "$temp_dir/headers"
assert_header 'Content-Type' 'text/html; charset=utf-8' "$temp_dir/headers"
assert_header 'Content-Security-Policy' "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'" "$temp_dir/headers"

head_status="$(curl --silent --output /dev/null --dump-header "$temp_dir/head-headers" \
	--write-out '%{http_code}' --head "$invite_url")"
if [[ "$head_status" != 200 ]]; then
	printf 'landing HEAD returned %s, want 200\n' "$head_status" >&2
	exit 1
fi
for header in 'Content-Type' 'Content-Length' 'Cache-Control' 'Referrer-Policy' 'X-Content-Type-Options' 'Content-Security-Policy'; do
	get_value="$(awk -v want_name="$header" '{ line = $0; sub(/\r$/, "", line); separator = index(line, ":"); if (separator > 0 && tolower(substr(line, 1, separator - 1)) == tolower(want_name)) value = substr(line, separator + 2) } END { print value }' "$temp_dir/headers")"
	head_value="$(awk -v want_name="$header" '{ line = $0; sub(/\r$/, "", line); separator = index(line, ":"); if (separator > 0 && tolower(substr(line, 1, separator - 1)) == tolower(want_name)) value = substr(line, separator + 2) } END { print value }' "$temp_dir/head-headers")"
	if [[ "$head_value" != "$get_value" ]]; then
		printf 'HEAD %s = %q, GET = %q\n' "$header" "$head_value" "$get_value" >&2
		exit 1
	fi
done

method_status="$(curl --silent --output "$temp_dir/method-body" --dump-header "$temp_dir/method-headers" \
	--write-out '%{http_code}' --request POST --header 'Referer: https://referer-secret.example/path' \
	"$origin/username-secret?token=query-secret")"
if [[ "$method_status" != 405 ]]; then
	printf 'POST returned %s, want 405\n' "$method_status" >&2
	exit 1
fi
options_status="$(curl --silent --output "$temp_dir/options-body" --dump-header "$temp_dir/options-headers" \
	--write-out '%{http_code}' --request-target '*' --request OPTIONS \
	--header 'Referer: https://referer-secret.example/path' --header 'X-Synthetic-Secret: options-secret' \
	"$origin/")"
if [[ "$options_status" != 405 ]]; then
	printf 'OPTIONS * returned %s, want 405\n' "$options_status" >&2
	exit 1
fi
if [[ "$(<"$temp_dir/options-body")" != "$(<"$temp_dir/method-body")" ]]; then
	printf 'OPTIONS * returned a different generic method response\n' >&2
	exit 1
fi
for header in 'Allow' 'Content-Type' 'Content-Length' 'Cache-Control' 'Referrer-Policy' 'X-Content-Type-Options' 'Content-Security-Policy'; do
	options_value="$(awk -v want_name="$header" '{ line = $0; sub(/\r$/, "", line); separator = index(line, ":"); if (separator > 0 && tolower(substr(line, 1, separator - 1)) == tolower(want_name)) value = substr(line, separator + 2) } END { print value }' "$temp_dir/options-headers")"
	method_value="$(awk -v want_name="$header" '{ line = $0; sub(/\r$/, "", line); separator = index(line, ":"); if (separator > 0 && tolower(substr(line, 1, separator - 1)) == tolower(want_name)) value = substr(line, separator + 2) } END { print value }' "$temp_dir/method-headers")"
	if [[ "$options_value" != "$method_value" ]]; then
		printf 'OPTIONS * %s = %q, POST = %q\n' "$header" "$options_value" "$method_value" >&2
		exit 1
	fi
done
admin_status="$(curl --silent --output "$temp_dir/admin-body" --dump-header "$temp_dir/admin-headers" \
	--write-out '%{http_code}' --header 'Referer: https://referer-secret.example/path' \
	"$origin/admin/private-route?secret=query-secret")"
if [[ "$admin_status" != 404 ]]; then
	printf '/admin request returned %s, want 404\n' "$admin_status" >&2
	exit 1
fi

logs="$(<"$temp_dir/log")"
for expected in 'route_class=invite status=200' 'route_class=username status=405' 'route_class=other status=405' 'route_class=admin status=404'; do
	if [[ "$logs" != *"$expected"* ]]; then
		printf 'missing privacy-safe log record: %s\n' "$expected" >&2
		exit 1
	fi
done
for marker in 'synthetic-live-capability' 'username-secret' 'private-route' 'query-secret' 'referer-secret' 'options-secret'; do
	if [[ "$logs" == *"$marker"* ]]; then
		printf 'request data leaked to logs\n' >&2
		exit 1
	fi
done

printf 'isolated link landing process smoke passed\n'
