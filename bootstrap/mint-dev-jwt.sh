#!/usr/bin/env bash
# Mint a local HS256 JWT the gateway will accept.
#
# The gateway validates with jwt.WithValidMethods(HS256/HS384/HS512) against the
# jwtSecrets list in bootstrap/fixtures/api_keys.local.json, trying each in order.
# Tokens are GENERATED, never checked in — that is why this script exists.
#
# Usage:
#   ./bootstrap/mint-dev-jwt.sh                        # 1h token, default claims
#   ./bootstrap/mint-dev-jwt.sh 86400 my-client        # ttl seconds, subject
#
#   TOKEN=$(./bootstrap/mint-dev-jwt.sh)
#   curl -H "Authorization: Bearer $TOKEN" ...
#
# Requires: openssl (present on macOS and every Linux dev box).
set -euo pipefail

TTL="${1:-3600}"
SUB="${2:-pulse-client-local}"
SECRET="${JWT_SECRET:-local-not-a-secret-jwt-primary}"

# base64url without padding, as JWT requires.
b64url() { openssl base64 -A | tr '+/' '-_' | tr -d '='; }

now=$(date +%s)
header='{"alg":"HS256","typ":"JWT"}'
payload="{\"sub\":\"${SUB}\",\"iat\":${now},\"exp\":$((now + TTL)),\"client_id\":\"${SUB}\",\"scope\":\"telemetry:write\"}"

h=$(printf '%s' "$header" | b64url)
p=$(printf '%s' "$payload" | b64url)
sig=$(printf '%s' "${h}.${p}" \
  | openssl dgst -sha256 -hmac "$SECRET" -binary \
  | b64url)

printf '%s.%s.%s\n' "$h" "$p" "$sig"
