ARG KUBO_IMAGE_TAG=latest
FROM ipfs/kubo:${KUBO_IMAGE_TAG}

LABEL org.opencontainers.image.source="https://github.com/bstone108/IPFS-CID-Docker" \
      org.opencontainers.image.description="Kubo-based IPFS container that scans /mnt, tracks files in SQLite, and publishes per-file CIDs." \
      org.opencontainers.image.title="IPFS CID Docker"

ENV PYTHONUNBUFFERED=1 \
    CONFIG_PATH=/config \
    IPFS_PATH=/config/ipfs \
    INDEX_DB_PATH=/config/index/index.db \
    INDEX_EXPORT_PATH=/config/index/current-index.json \
    SCAN_PATHS=/mnt \
    RESCAN_INTERVAL=5m \
    SCAN_PRIORITY=normal \
    IPFS_PROFILE=server

RUN set -eux; \
    if command -v apk >/dev/null 2>&1; then \
        apk add --no-cache python3 sqlite tini; \
    elif command -v apt-get >/dev/null 2>&1; then \
        apt-get update; \
        apt-get install -y --no-install-recommends python3 sqlite3 tini; \
        rm -rf /var/lib/apt/lists/*; \
    elif command -v microdnf >/dev/null 2>&1; then \
        microdnf install -y python3 sqlite tini; \
        microdnf clean all; \
    else \
        echo "Unsupported ipfs/kubo base image package manager" >&2; \
        exit 1; \
    fi

WORKDIR /app

COPY app /app

RUN mkdir -p /config/ipfs /config/index /mnt

VOLUME ["/config", "/mnt"]

EXPOSE 4001 4001/udp 5001 8080

ENTRYPOINT ["tini", "--", "python3", "/app/service.py"]
