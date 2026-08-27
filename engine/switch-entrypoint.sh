#!/bin/sh
set -e

# Entrypoint that wraps the original AceStream image startup.
#
# Three modes, driven by two environment variables (injected by acexy):
#
#   1. No VPN        ACESTREAM_VPN_CONTAINER empty
#   2. VPN permanent  ACESTREAM_VPN_CONTAINER set + ACESTREAM_VPN_SWITCH_AFTER=0
#   3. VPN switch     ACESTREAM_VPN_CONTAINER set + ACESTREAM_VPN_SWITCH_AFTER>0
#
#   ACESTREAM_VPN_CONTAINER       name of the VPN container (e.g. warp)
#   ACESTREAM_VPN_SWITCH_AFTER    seconds before switching back to normal.
#                                 0 = stay on VPN permanently. Default 45.
#
# The normal gateway is detected automatically (it is the default route the
# container already has at startup, before anything is changed).

VPN_CONTAINER="${ACESTREAM_VPN_CONTAINER}"
SWITCH_AFTER="${ACESTREAM_VPN_SWITCH_AFTER:-45}"

# Capture the normal gateway BEFORE overwriting the route.
NORMAL_GW="$(ip route show default 2>/dev/null | awk '{print $3}')"

# The switch runs in the background; it does not block the engine startup.
(
  # Wait for the container network to be ready.
  sleep 5

  if [ -n "$VPN_CONTAINER" ]; then
    # Resolve the VPN container name to its IP (Docker internal DNS).
    VPN_IP="$(getent hosts "$VPN_CONTAINER" 2>/dev/null | awk '{print $1}' | head -1)"

    if [ -n "$VPN_IP" ]; then
      # 1. Default route -> VPN (bootstrap through VPN).
      ip route del default 2>/dev/null || true
      ip route add default via "$VPN_IP"
      echo "[vpn-switch] traffic routed via VPN $VPN_CONTAINER ($VPN_IP)"

      # 2. SWITCH_AFTER=0 means stay on VPN permanently; otherwise wait and
      #    route back to the normal network.
      if [ "$SWITCH_AFTER" -gt 0 ] 2>/dev/null; then
        sleep "$SWITCH_AFTER"

        if [ -n "$NORMAL_GW" ]; then
          ip route del default 2>/dev/null || true
          ip route add default via "$NORMAL_GW"
          echo "[vpn-switch] traffic routed back to normal gateway ($NORMAL_GW)"
        fi
      else
        echo "[vpn-switch] VPN permanent mode: staying routed through $VPN_CONTAINER"
      fi
    else
      echo "[vpn-switch] WARNING: could not resolve VPN container '$VPN_CONTAINER'"
    fi
  fi
) &

# Run the ORIGINAL entrypoint of the image (wrapped, not replaced).
# Drop to the 'ace' user (the one from the base image) before launching the
# engine, preserving the original behavior. Route switching needs root, so it
# is done in the background above, while this final process runs as 'ace'.
# The HOME is set explicitly: setpriv changes the UID but keeps the inherited
# HOME, which would still point to /root and break the engine's cache writes.
export HOME=/srv/ace
exec setpriv --reuid=ace --regid=ace --init-groups /srv/ace/start-engine "$@"
