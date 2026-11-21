#!/bin/sh
set -eu

# Entrypoint that wraps the outpace-engine image startup.
#
# Three modes, driven by two environment variables (injected by acexy):
#
#   1. No VPN         ACESTREAM_VPN_CONTAINER empty
#   2. VPN permanent  ACESTREAM_VPN_CONTAINER set + ACESTREAM_VPN_SWITCH_AFTER=0
#   3. VPN switch     ACESTREAM_VPN_CONTAINER set + ACESTREAM_VPN_SWITCH_AFTER>0
#
#   ACESTREAM_VPN_CONTAINER       name of the VPN/gateway container (e.g. mullvad)
#   ACESTREAM_VPN_SWITCH_AFTER    seconds before switching back to normal.
#                                 0 = stay on VPN permanently. Default 45.
#
# Unlike the AceStream variant, the default route is changed BEFORE the engine
# starts, so no traffic ever leaves through the normal gateway. If a VPN is
# requested and cannot be set up, the container exits instead of silently
# running without it (fail closed).

VPN_CONTAINER="${ACESTREAM_VPN_CONTAINER:-}"
SWITCH_AFTER="${ACESTREAM_VPN_SWITCH_AFTER:-45}"
RESOLVE_ATTEMPTS=10

log() {
  echo "[vpn-switch] $*"
}

fail() {
  echo "[vpn-switch] ERROR: $*" >&2
  exit 1
}

if [ -n "$VPN_CONTAINER" ]; then
  case "$SWITCH_AFTER" in
    '' | *[!0-9]*) fail "ACESTREAM_VPN_SWITCH_AFTER must be a non-negative integer, got '$SWITCH_AFTER'" ;;
  esac

  # Capture the normal gateway BEFORE overwriting the route.
  NORMAL_GW="$(ip route show default 2>/dev/null | awk '{print $3; exit}')"

  # Resolve the VPN container name to its IP (Docker internal DNS). Retry a few
  # times in case the VPN container has just been (re)created.
  VPN_IP=""
  attempt=1
  while [ "$attempt" -le "$RESOLVE_ATTEMPTS" ]; do
    VPN_IP="$(getent hosts "$VPN_CONTAINER" 2>/dev/null | awk '{print $1; exit}')"
    [ -n "$VPN_IP" ] && break
    sleep 1
    attempt=$((attempt + 1))
  done
  [ -n "$VPN_IP" ] || fail "could not resolve VPN container '$VPN_CONTAINER'"

  # "replace" swaps the route atomically: there is no moment without a default route.
  ip route replace default via "$VPN_IP" ||
    fail "could not set default route via $VPN_IP (is NET_ADMIN granted?)"
  log "traffic routed via VPN $VPN_CONTAINER ($VPN_IP)"

  if [ "$SWITCH_AFTER" -gt 0 ]; then
    if [ -n "$NORMAL_GW" ]; then
      # Runs in the background as root; does not block the engine startup.
      (
        sleep "$SWITCH_AFTER"
        ip route replace default via "$NORMAL_GW" &&
          log "traffic routed back to normal gateway ($NORMAL_GW)"
      ) &
    else
      log "WARNING: normal gateway unknown, staying on VPN"
    fi
  else
    log "VPN permanent mode: staying routed through $VPN_CONTAINER"
  fi
fi

# Route changes need root (CAP_NET_ADMIN); the engine itself runs as 'outpace'.
# HOME is set explicitly because setpriv keeps the inherited HOME (/root).
exec setpriv --reuid=outpace --regid=outpace --init-groups --no-new-privs \
  env HOME=/var/lib/outpace /usr/local/bin/outpace "$@"
