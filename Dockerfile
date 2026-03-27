ARG KUBO_IMAGE_TAG=v0.40.1
FROM ipfs/kubo:${KUBO_IMAGE_TAG} AS kubo

FROM python:3.12-slim-bookworm
ARG KUBO_IMAGE_TAG=v0.40.1

LABEL org.opencontainers.image.source="https://github.com/bstone108/IPFS-CID-Docker" \
      org.opencontainers.image.description="Kubo-based IPFS container that scans /mnt, tracks files in SQLite, and publishes per-file CIDs." \
      org.opencontainers.image.title="IPFS CID Docker"

ENV PYTHONUNBUFFERED=1 \
    CONFIG_PATH=/config \
    KUBO_VERSION=${KUBO_IMAGE_TAG} \
    IPFS_PATH=/config/ipfs \
    INDEX_DB_PATH=/config/index/index.db \
    INDEX_EXPORT_PATH=/config/index/current-index.json \
    SCAN_PATHS=/mnt \
    RESCAN_INTERVAL=5m \
    SCAN_PRIORITY=normal \
    IPFS_PROFILE=server \
    IPFS_ADD_PROFILE=matrix-share-client \
    IPFS_ADD_CID_VERSION= \
    IPFS_ADD_RAW_LEAVES= \
    IPFS_ADD_HASH= \
    IPFS_ADD_CHUNKER= \
    IPFS_ADD_TRICKLE= \
    UPLOAD_BANDWIDTH_LIMIT= \
    UPLOAD_BANDWIDTH_METHOD=auto \
    UPLOAD_BANDWIDTH_REQUIRED=false \
    BANDWIDTH_INTERFACE=

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends tini ca-certificates iproute2; \
    rm -rf /var/lib/apt/lists/*

COPY --from=kubo /usr/local/bin/ipfs /usr/local/bin/ipfs

WORKDIR /app

COPY app /app

RUN mkdir -p /config/ipfs /config/index /mnt

VOLUME ["/config", "/mnt"]

EXPOSE 4001 4001/udp 5001 8080

ENTRYPOINT ["tini", "--", "python3", "/app/service.py"]
