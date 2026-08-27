# acestream-vpn-switch

Docker image of AceStream that routes its traffic through a VPN container
either permanently or temporarily (switching back to the normal network after a
configurable delay).

## Motivation

In Spain, ISPs apply dynamic IP blocks during football matches (ordered by
LaLiga), which affect AceStream's bootstrap servers. Routing all traffic
through a VPN works, but leaves the client as a "pure leecher" (no inbound
port) and causes stream stutters.

This image lets you choose the trade-off:

1. **Bootstrap through VPN**: the initial traffic (peer discovery) goes through
   the VPN, bypassing the ISP block.
2. **Data through the normal network**: after `ACESTREAM_VPN_SWITCH_AFTER`
   seconds, traffic returns to the normal network, at full speed and with the
   host's own NAT (peers can connect to you).

## Modes

Three modes are driven by two environment variables:

| Mode | `ACESTREAM_VPN_CONTAINER` | `ACESTREAM_VPN_SWITCH_AFTER` |
|---|---|---|
| **No VPN** | empty | ignored |
| **VPN permanent** | set (e.g. `warp`) | `0` |
| **VPN switch** (temporal) | set (e.g. `warp`) | `> 0` (e.g. `45`) |

- **No VPN**: the container uses the normal network only.
- **VPN permanent**: traffic stays routed through the VPN forever.
- **VPN switch**: traffic goes through the VPN at startup, then switches back
  to the normal network after `ACESTREAM_VPN_SWITCH_AFTER` seconds.

## Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `ACESTREAM_VPN_CONTAINER` | no | — | name of the VPN container on the network (e.g. `warp`). Empty = no VPN |
| `ACESTREAM_VPN_SWITCH_AFTER` | no | `45` | seconds before switching back to the normal network. `0` = stay on VPN permanently |

> The normal network gateway is detected automatically at startup (it is the
> default route the container already has). No configuration needed.

> These are injected automatically by the [acexy](https://github.com/franlerma/acexy)
> orchestrator when it creates each instance.

## How it works

`switch-entrypoint.sh` wraps (with `exec`) the original entrypoint of the
`wafy80/acestream` image. In the background:

1. Waits 5s for the network to be ready.
2. Resolves the VPN container name to its IP (Docker internal DNS).
3. Switches the default route to the VPN.
4. If `ACESTREAM_VPN_SWITCH_AFTER` is `> 0`, waits that many seconds and then
   switches the default route back to the normal network. If it is `0`, it
   stays on the VPN permanently.

The AceStream engine starts and runs normally throughout the whole process.

## Release

On every published release, the `.github/workflows/release.yaml` workflow
builds a multi-architecture image (amd64, arm64, arm/v7) and publishes it to
Docker Hub with the release tag and `latest`.
