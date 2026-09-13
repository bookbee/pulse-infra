#!/usr/bin/env bash
# Re-resolve the manifest-list digest for every pinned image and compare against
# images.lock. Reports drift; writes nothing.
#
# Manifest-LIST (OCI image index) digests are used deliberately: a
# platform-specific digest pinned on an arm64 laptop fails to pull on amd64.
#
# Bump procedure (deliberate, never automatic):
#   1. ./bootstrap/resolve-digests.sh
#   2. edit images.lock AND compose/compose.yaml together
#   3. make reset && make up && make verify
set -euo pipefail

cd "$(dirname "$0")/.."

ACCEPT='application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json'

resolve() {
  local repo="$1" tag="$2"
  local token
  token=$(curl -fsS "https://auth.docker.io/token?service=registry.docker.io&scope=repository:${repo}:pull" \
    | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
  curl -fsSI \
    -H "Authorization: Bearer ${token}" \
    -H "Accept: ${ACCEPT}" \
    "https://registry-1.docker.io/v2/${repo}/manifests/${tag}" \
    | tr -d '\r' | sed -n 's/^[Dd]ocker-[Cc]ontent-[Dd]igest: //p'
}

status=0
while read -r repo tag; do
  [ -n "${repo:-}" ] || continue
  case "$repo" in \#*) continue ;; esac

  live=$(resolve "$repo" "$tag" || true)
  locked=$(grep -F "${repo}:${tag}@" images.lock 2>/dev/null | sed -n 's/.*@\(sha256:[0-9a-f]*\).*/\1/p' | head -1)

  if [ -z "$live" ]; then
    printf '  ?  %-28s %-16s could not reach registry\n' "$repo" "$tag"
    status=1
  elif [ "$live" = "$locked" ]; then
    printf '  ok %-28s %-16s %s\n' "$repo" "$tag" "$live"
  else
    printf '  DRIFT %-25s %-16s\n      locked: %s\n      live:   %s\n' \
      "$repo" "$tag" "${locked:-<absent from images.lock>}" "$live"
    status=1
  fi
done <<'IMAGES'
apache/kafka 4.3.1
library/redis 7.4.11-alpine
fsouza/fake-gcs-server 1.56.1
IMAGES

echo
if [ "$status" -eq 0 ]; then
  echo "images.lock matches the registry."
else
  echo "Drift or lookup failure above. A tag whose digest moved means upstream"
  echo "re-pushed it — decide deliberately whether to bump."
fi
exit "$status"
