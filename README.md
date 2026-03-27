# IPFS CID Docker

This image reuses the official `ipfs/kubo` image for the `ipfs` binary and adds a lightweight Python scanner that:

- scans `/mnt` recursively on a schedule
- tracks file state in SQLite
- adds new and changed files to IPFS and records their CIDs
- removes deleted files from the active index
- writes a JSON manifest for quick lookup
- keeps persistent state under `/config`

The default behavior is intentionally close to zero configuration: mount one or more host directories anywhere under `/mnt/<name>`, start the container, and it will index everything below `/mnt`.

## How It Works

- `ipfs/kubo` provides the daemon and content storage.
- `/config/ipfs` stores the IPFS repo and pins.
- `/config/index/index.db` stores scan state and the latest known CID per file.
- `/config/index/current-index.json` exports the active index as JSON after every scan.

This version is intentionally file-CID focused. It does not maintain an extra mirrored directory tree in IPFS MFS, which keeps scans lighter and avoids extra copy/remove operations.

The first scan runs immediately on startup once the IPFS daemon is ready. `RESCAN_INTERVAL` controls the delay between later scans, not the initial import.

## Environment Variables

| Variable | Default | Notes |
| --- | --- | --- |
| `SCAN_PATHS` | `/mnt` | Comma or newline separated list of directories to scan. Every path must stay under `/mnt`. Leave it as `/mnt` for zero-config behavior. |
| `RESCAN_INTERVAL` | `5m` | Plain text interval such as `30s`, `10m`, `1 hour`, or `1h 30m`. |
| `SCAN_PRIORITY` | `normal` | `high`, `normal`, or `low`. Lower priorities add more pauses and run IPFS indexing subprocesses with a lower CPU nice level. |
| `CONFIG_PATH` | `/config` | Root path for persistent state. Mount this to the host for restart-safe storage. |
| `INDEX_DB_PATH` | `/config/index/index.db` | SQLite database path. |
| `INDEX_EXPORT_PATH` | `/config/index/current-index.json` | JSON manifest path. |
| `IPFS_PATH` | `/config/ipfs` | IPFS repo path. Persist this volume if you want your node and pins to survive restarts. |
| `IPFS_PROFILE` | `server` | Comma-separated Kubo config profiles applied only when the repo is first initialized. |
| `UPLOAD_BANDWIDTH_LIMIT` | disabled | Optional container-wide outbound bandwidth cap such as `10mbit`, `100Mbps`, or `5MiB/s`. This throttles uploads and any other egress traffic from the container when the host honors `tc` and `NET_ADMIN`. |
| `UPLOAD_BANDWIDTH_METHOD` | `auto` | `auto`, `tbf`, `htb`, or `netem`. `auto` tries multiple Linux traffic-control methods for better host compatibility. |
| `UPLOAD_BANDWIDTH_REQUIRED` | `false` | If `true`, startup fails unless a bandwidth limit can actually be applied. |
| `BANDWIDTH_INTERFACE` | auto-detect | Optional override for the Linux network interface that `tc` should shape if auto-detection does not pick the right one. |

## Quick Start

```bash
docker run -d \
  --name ipfs-autoscan \
  --restart unless-stopped \
  --cap-add NET_ADMIN \
  -p 4001:4001 \
  -p 4001:4001/udp \
  -p 5001:5001 \
  -p 8080:8080 \
  -e CONFIG_PATH="/config" \
  -e RESCAN_INTERVAL="15m" \
  -e SCAN_PRIORITY="low" \
  -e UPLOAD_BANDWIDTH_LIMIT="10mbit" \
  -v /srv/ipfs-autoscan:/config \
  -v /stuff/stupidity/whatever:/mnt/whatever:ro \
  ghcr.io/bstone108/ipfs-cid-docker:latest
```

If you do not want an upload cap, leave `UPLOAD_BANDWIDTH_LIMIT` unset or set it to `off`, and you can also drop `NET_ADMIN`.

To scan more than one host directory, mount each one under a unique path beneath `/mnt`:

```bash
docker run -d \
  --name ipfs-autoscan \
  --cap-add NET_ADMIN \
  -p 4001:4001 \
  -p 4001:4001/udp \
  -p 5001:5001 \
  -p 8080:8080 \
  -e UPLOAD_BANDWIDTH_LIMIT="10mbit" \
  -v /srv/ipfs-autoscan:/config \
  -v /stuff/stupidity/whatever:/mnt/whatever:ro \
  -v /other/media:/mnt/media:ro \
  ghcr.io/bstone108/ipfs-cid-docker:latest
```

If you do not want everything under `/mnt`, narrow it with `SCAN_PATHS`:

```bash
-e SCAN_PATHS="/mnt/whatever,/mnt/media"
```

## Looking Up CIDs

The easiest lookup is the exported manifest:

- database: `/config/index/index.db`
- manifest: `/config/index/current-index.json`
- IPFS repo: `/config/ipfs`

Example `current-index.json` shape:

```json
{
  "scan_paths": ["/mnt"],
  "file_count": 2,
  "files": [
    {
      "path": "/mnt/whatever/file.txt",
      "relative_path": "whatever/file.txt",
      "cid": "bafy..."
    }
  ]
}
```

If the gateway port is published, individual files are available at:

```text
http://<host>:8080/ipfs/<file_cid>
```

## Compose

A sample [`compose.yaml`](compose.yaml) is included. This is the same basic setup in copy-paste form:

```yaml
services:
  ipfs-autoscan:
    image: ghcr.io/bstone108/ipfs-cid-docker:latest
    restart: unless-stopped
    cap_add:
      - NET_ADMIN
    environment:
      CONFIG_PATH: /config
      SCAN_PATHS: /mnt
      RESCAN_INTERVAL: 15m
      SCAN_PRIORITY: low
      UPLOAD_BANDWIDTH_LIMIT: 10mbit
      # UPLOAD_BANDWIDTH_METHOD: auto
      # UPLOAD_BANDWIDTH_REQUIRED: "false"
      # IPFS_PATH: /config/ipfs
      # INDEX_DB_PATH: /config/index/index.db
      # INDEX_EXPORT_PATH: /config/index/current-index.json
      # IPFS_PROFILE: server
      # BANDWIDTH_INTERFACE: eth0
    ports:
      - "4001:4001"
      - "4001:4001/udp"
      - "5001:5001"
      - "8080:8080"
    volumes:
      - ./config:/config
      - /stuff/stupidity/whatever:/mnt/whatever:ro
      - /other/media:/mnt/media:ro
```

Start it with:

```bash
docker compose up -d
```

The commented entries are optional overrides. If you do not want an upload cap, remove `UPLOAD_BANDWIDTH_LIMIT` and `NET_ADMIN`.

## Unraid

A dedicated Unraid template is included at [`unraid/ipfs-cid-docker.xml`](unraid/ipfs-cid-docker.xml).

Raw template URL:

```text
https://raw.githubusercontent.com/bstone108/IPFS-CID-Docker/main/unraid/ipfs-cid-docker.xml
```

If you manage user templates manually on Unraid, place the XML in:

```text
/boot/config/plugins/dockerMan/templates-user/
```

The template is set up so Unraid uses `Extra Parameters` for:

```text
--cap-add=NET_ADMIN
```

That gives Unraid a better chance of honoring the capability than some compose wrappers. The template leaves `UPLOAD_BANDWIDTH_LIMIT` set to `off` by default, so first install is safe. If you want upload throttling, set `UPLOAD_BANDWIDTH_LIMIT` to something like `10mbit` and leave the `Extra Parameters` value in place.

Template notes:

- `Config Storage` maps to `/config` and should point at your appdata location.
- `Scan Path 1` maps to `/mnt/scan1` and is the first host folder to index.
- `Scan Path 2` and `Scan Path 3` are optional extra mounts under `/mnt`.
- Leave `SCAN_PATHS` as `/mnt` unless you want to restrict scanning to specific mounted subpaths.
- If you need more than three scan roots, add more path entries in Unraid under `/mnt/<name>`.
- `UPLOAD_BANDWIDTH_METHOD` defaults to `auto`, which tries `tbf`, then `htb`, then `netem`.
- Set `UPLOAD_BANDWIDTH_REQUIRED=true` only if you want the container to fail startup when no shaping method can be applied.

## Kubernetes

A sample Kubernetes manifest is included at [`k8s/ipfs-cid-docker.yaml`](k8s/ipfs-cid-docker.yaml).

Apply it with:

```bash
kubectl apply -f k8s/ipfs-cid-docker.yaml
```

Before applying it, update the `hostPath` values to match the directories on your node that you want mounted under `/mnt`.

## Notes and Tradeoffs

- Symlinks are skipped so the scanner cannot accidentally walk out of `/mnt` or loop forever.
- Change detection uses `size`, `mtime_ns`, `inode`, and `device`. That is fast and practical for scheduled scans, but it is not a cryptographic diff.
- When a file disappears from disk, the container marks it inactive in SQLite and unpins the CID if no other active path still references it.
- When a file changes, it is re-added to IPFS and gets a new CID if the content changed.
- `UPLOAD_BANDWIDTH_LIMIT` uses Linux traffic control on the container's egress interface, so it caps all outbound traffic from this container and usually requires `NET_ADMIN`.
- `UPLOAD_BANDWIDTH_METHOD=auto` tries `tbf` first, then `htb`, then `netem` so the container has a better chance of finding a qdisc the host kernel actually provides.
- `UPLOAD_BANDWIDTH_REQUIRED=false` keeps the service running with a warning if no shaping method can be applied. Set it to `true` if you want startup to fail instead.

## Build

```bash
docker build -t ghcr.io/bstone108/ipfs-cid-docker:latest .
```

If you want to pin the upstream Kubo tag:

```bash
docker build \
  --build-arg KUBO_IMAGE_TAG=latest \
  -t ghcr.io/bstone108/ipfs-cid-docker:latest .
```
