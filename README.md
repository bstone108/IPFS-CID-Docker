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

## Quick Start

```bash
docker run -d \
  --name ipfs-autoscan \
  --restart unless-stopped \
  -p 4001:4001 \
  -p 4001:4001/udp \
  -p 5001:5001 \
  -p 8080:8080 \
  -e CONFIG_PATH="/config" \
  -e RESCAN_INTERVAL="15m" \
  -e SCAN_PRIORITY="low" \
  -v /srv/ipfs-autoscan:/config \
  -v /stuff/stupidity/whatever:/mnt/whatever:ro \
  ghcr.io/bstone108/ipfs-cid-docker:latest
```

To scan more than one host directory, mount each one under a unique path beneath `/mnt`:

```bash
docker run -d \
  --name ipfs-autoscan \
  -p 4001:4001 \
  -p 4001:4001/udp \
  -p 5001:5001 \
  -p 8080:8080 \
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

A sample [`compose.yaml`](/Users/brandonstone/Documents/Source%20Code/IPFS%20Docker/compose.yaml) is included. Update the bind mounts and image name before using it.

## Kubernetes

A sample Kubernetes manifest is included at [`k8s/ipfs-cid-docker.yaml`](/Users/brandonstone/Documents/Source%20Code/IPFS%20Docker/k8s/ipfs-cid-docker.yaml).

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

## Publish From GitHub

The repo includes a GitHub Actions workflow at [.github/workflows/publish-ghcr.yml](/Users/brandonstone/Documents/Source%20Code/IPFS%20Docker/.github/workflows/publish-ghcr.yml).

It will build multi-arch images for `linux/amd64` and `linux/arm64` and publish them to:

```text
ghcr.io/bstone108/ipfs-cid-docker
```

The workflow runs on:

- pushes to `main`
- version tags like `v0.1.0`
- manual runs from the Actions tab

On the first publish, GitHub Container Registry creates the package as private by default. After the first successful workflow run, open the package page in GitHub and change its visibility to public if you want anonymous pulls.

If you also want Docker Hub, the simplest path is to either:

- point Docker Hub automated builds at this GitHub repo
- or add a second login and push step to the workflow using Docker Hub secrets
