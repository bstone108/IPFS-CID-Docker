#!/usr/bin/env python3

from __future__ import annotations

import json
import logging
import math
import os
import re
import signal
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


LOGGER = logging.getLogger("ipfs-autoscan")


INTERVAL_UNITS = {
    "s": 1,
    "sec": 1,
    "secs": 1,
    "second": 1,
    "seconds": 1,
    "m": 60,
    "min": 60,
    "mins": 60,
    "minute": 60,
    "minutes": 60,
    "h": 3600,
    "hr": 3600,
    "hrs": 3600,
    "hour": 3600,
    "hours": 3600,
    "d": 86400,
    "day": 86400,
    "days": 86400,
}

BANDWIDTH_DISABLED_VALUES = {
    "0",
    "0bit",
    "0bps",
    "disable",
    "disabled",
    "false",
    "none",
    "off",
    "unlimited",
}

BOOLEAN_TRUE_VALUES = {"1", "true", "yes", "on"}
BOOLEAN_FALSE_VALUES = {"0", "false", "no", "off", ""}
BANDWIDTH_METHOD_CHOICES = ("auto", "tbf", "htb", "netem")
IPFS_ADD_PROFILE_CHOICES = (
    "matrix-share-client",
    "cidv1-raw",
    "kubo-default",
)

BANDWIDTH_UNIT_MULTIPLIERS = {
    "b/s": 1,
    "bit": 1,
    "bit/s": 1,
    "bits": 1,
    "bits/s": 1,
    "bps": 1,
    "kbit": 1_000,
    "kbit/s": 1_000,
    "kbits": 1_000,
    "kb/s": 1_000,
    "kbps": 1_000,
    "mbit": 1_000_000,
    "mbit/s": 1_000_000,
    "mbits": 1_000_000,
    "mb/s": 1_000_000,
    "mbps": 1_000_000,
    "gbit": 1_000_000_000,
    "gbit/s": 1_000_000_000,
    "gbits": 1_000_000_000,
    "gb/s": 1_000_000_000,
    "gbps": 1_000_000_000,
    "tbit": 1_000_000_000_000,
    "tbit/s": 1_000_000_000_000,
    "tb/s": 1_000_000_000_000,
    "tbps": 1_000_000_000_000,
    "B/s": 8,
    "KB/s": 8_000,
    "MB/s": 8_000_000,
    "GB/s": 8_000_000_000,
    "TB/s": 8_000_000_000_000,
    "KiB/s": 8 * 1024,
    "MiB/s": 8 * 1024 * 1024,
    "GiB/s": 8 * 1024 * 1024 * 1024,
    "TiB/s": 8 * 1024 * 1024 * 1024 * 1024,
}


@dataclass(frozen=True)
class PriorityProfile:
    niceness: int
    per_file_pause: float
    changed_file_pause: float
    batch_size: int
    batch_pause: float


PRIORITY_PROFILES = {
    "high": PriorityProfile(
        niceness=0,
        per_file_pause=0.0,
        changed_file_pause=0.0,
        batch_size=0,
        batch_pause=0.0,
    ),
    "normal": PriorityProfile(
        niceness=4,
        per_file_pause=0.0,
        changed_file_pause=0.01,
        batch_size=250,
        batch_pause=0.01,
    ),
    "low": PriorityProfile(
        niceness=10,
        per_file_pause=0.002,
        changed_file_pause=0.05,
        batch_size=50,
        batch_pause=0.05,
    ),
}


@dataclass(frozen=True)
class BandwidthLimit:
    raw: str
    bits_per_second: int
    tc_rate: str
    tc_burst_bytes: int


@dataclass(frozen=True)
class IpfsAddProfile:
    profile_name: str
    cid_version: int | None
    raw_leaves: bool | None
    hash_function: str | None
    chunker: str | None
    trickle: bool | None
    kubo_version: str

    @property
    def cli_args(self) -> tuple[str, ...]:
        args = ["-Q", "--pin=true", "--wrap-with-directory=false"]
        if self.cid_version is not None:
            args.append(f"--cid-version={self.cid_version}")
        if self.raw_leaves is not None:
            args.append(f"--raw-leaves={'true' if self.raw_leaves else 'false'}")
        if self.hash_function:
            args.append(f"--hash={self.hash_function}")
        if self.chunker:
            args.append(f"--chunker={self.chunker}")
        if self.trickle is not None:
            args.append(f"--trickle={'true' if self.trickle else 'false'}")
        return tuple(args)

    @property
    def signature(self) -> str:
        return json.dumps(
            {
                "chunker": self.chunker,
                "cid_version": self.cid_version,
                "hash_function": self.hash_function,
                "kubo_version": self.kubo_version,
                "profile_name": self.profile_name,
                "raw_leaves": self.raw_leaves,
                "trickle": self.trickle,
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    def as_manifest_object(self) -> dict[str, object | None]:
        return {
            "profile": self.profile_name,
            "cid_version": self.cid_version,
            "raw_leaves": self.raw_leaves,
            "hash_function": self.hash_function,
            "chunker": self.chunker,
            "trickle": self.trickle,
            "kubo_version": self.kubo_version,
            "signature": self.signature,
        }


@dataclass(frozen=True)
class Config:
    config_path: Path
    mount_root: Path
    scan_paths_raw: str
    rescan_interval_seconds: float
    rescan_interval_text: str
    scan_priority: str
    profile: PriorityProfile
    db_path: Path
    export_path: Path
    ipfs_path: Path
    ipfs_profiles: tuple[str, ...]
    kubo_version: str
    ipfs_add_profile: IpfsAddProfile
    upload_bandwidth_limit: BandwidthLimit | None
    upload_bandwidth_method: str
    upload_bandwidth_required: bool
    bandwidth_interface: str | None


@dataclass
class ScanSummary:
    files_seen: int = 0
    files_added: int = 0
    files_updated: int = 0
    files_removed: int = 0
    files_unchanged: int = 0
    errors: int = 0


class ServiceStop(SystemExit):
    pass


class Scanner:
    def __init__(self, config: Config):
        self.config = config
        self._stop = False

    def stop(self) -> None:
        self._stop = True

    def should_stop(self) -> bool:
        return self._stop

    def scan_once(self) -> ScanSummary:
        roots = resolve_scan_roots(self.config.scan_paths_raw, self.config.mount_root)
        mount_root = self.config.mount_root.resolve()
        ensure_parent_dir(self.config.db_path)
        ensure_parent_dir(self.config.export_path)

        with sqlite3.connect(self.config.db_path) as conn:
            conn.row_factory = sqlite3.Row
            initialize_schema(conn)
            scan_id = start_scan_record(conn)
            existing = load_existing_entries(conn, roots)
            summary = ScanSummary()

            try:
                seen_paths: set[str] = set()
                file_counter = 0

                for root in roots:
                    if self.should_stop():
                        raise ServiceStop()

                    for path in iter_regular_files(root):
                        if self.should_stop():
                            raise ServiceStop()

                        file_counter += 1
                        self.maybe_pause(file_counter, changed=False)

                        path_str = str(path)
                        relative_path = str(path.relative_to(mount_root))
                        stat_result = path.stat()

                        existing_row = existing.get(path_str)
                        seen_paths.add(path_str)
                        summary.files_seen += 1

                        if existing_row and row_matches_file_state(
                            existing_row,
                            stat_result,
                            self.config.ipfs_add_profile.signature,
                        ):
                            summary.files_unchanged += 1
                            touch_seen_row(conn, path_str)
                            continue

                        try:
                            cid = self.add_file_to_ipfs(path)
                            upsert_successful_file(
                                conn=conn,
                                path=path_str,
                                relative_path=relative_path,
                                root_path=str(root),
                                cid=cid,
                                import_profile=self.config.ipfs_add_profile.signature,
                                stat_result=stat_result,
                            )
                            if existing_row and existing_row["cid"]:
                                summary.files_updated += 1
                            else:
                                summary.files_added += 1
                            if (
                                existing_row
                                and existing_row["cid"]
                                and existing_row["cid"] != cid
                            ):
                                self.maybe_unpin(
                                    conn=conn,
                                    cid=existing_row["cid"],
                                    excluded_paths={path_str},
                                )
                            self.maybe_pause(file_counter, changed=True)
                        except Exception as exc:  # noqa: BLE001
                            summary.errors += 1
                            LOGGER.exception("Failed to index %s", path_str)
                            if existing_row and existing_row["cid"]:
                                mark_failed_file(
                                    conn=conn,
                                    path=path_str,
                                    relative_path=relative_path,
                                    root_path=str(root),
                                    stat_result=stat_result,
                                    error_message=str(exc),
                                    keep_cid=existing_row["cid"],
                                )
                                self.maybe_unpin(
                                    conn=conn,
                                    cid=existing_row["cid"],
                                    excluded_paths={path_str},
                                )
                            else:
                                mark_failed_file(
                                    conn=conn,
                                    path=path_str,
                                    relative_path=relative_path,
                                    root_path=str(root),
                                    stat_result=stat_result,
                                    error_message=str(exc),
                                    keep_cid=None,
                                )

                deleted_paths = sorted(set(existing) - seen_paths)
                for deleted_path in deleted_paths:
                    if self.should_stop():
                        raise ServiceStop()

                    row = existing[deleted_path]
                    mark_deleted_file(conn, deleted_path)
                    summary.files_removed += 1
                    if row["cid"]:
                        self.maybe_unpin(
                            conn=conn,
                            cid=row["cid"],
                            excluded_paths={deleted_path},
                        )

                export_active_manifest(
                    conn=conn,
                    export_path=self.config.export_path,
                    add_profile=self.config.ipfs_add_profile,
                    scan_paths=[str(root) for root in roots],
                )
                finish_scan_record(conn, scan_id, "ok", summary)
                conn.commit()
                return summary
            except ServiceStop:
                finish_scan_record(conn, scan_id, "stopped", summary)
                conn.commit()
                raise
            except Exception:
                finish_scan_record(conn, scan_id, "error", summary)
                conn.commit()
                raise

    def maybe_pause(self, file_counter: int, changed: bool) -> None:
        profile = self.config.profile
        if profile.per_file_pause:
            time.sleep(profile.per_file_pause)
        if changed and profile.changed_file_pause:
            time.sleep(profile.changed_file_pause)
        if profile.batch_size and file_counter % profile.batch_size == 0:
            time.sleep(profile.batch_pause)

    def add_file_to_ipfs(self, path: Path) -> str:
        result = run_ipfs(
            [
                "add",
                *self.config.ipfs_add_profile.cli_args,
                str(path),
            ],
            niceness=self.config.profile.niceness,
        )
        cid = result.stdout.strip()
        if not cid:
            raise RuntimeError(f"ipfs add returned no CID for {path}")
        return cid

    def maybe_unpin(
        self,
        conn: sqlite3.Connection,
        cid: str | None,
        excluded_paths: set[str],
    ) -> None:
        if not cid:
            return

        active_refs = conn.execute(
            """
            SELECT COUNT(*)
            FROM files
            WHERE cid = ?
              AND active = 1
              AND path NOT IN ({})
            """.format(",".join("?" for _ in excluded_paths) or "''"),
            [cid, *sorted(excluded_paths)],
        ).fetchone()[0]

        if active_refs == 0:
            run_ipfs(
                ["pin", "rm", cid],
                niceness=self.config.profile.niceness,
                check=False,
            )


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def parse_interval(value: str) -> float:
    text = value.strip().lower()
    if not text:
        raise ValueError("interval cannot be empty")

    token_re = re.compile(r"(\d+(?:\.\d+)?)\s*([a-z]+)?")
    position = 0
    total = 0.0

    while position < len(text):
        while position < len(text) and text[position] in " ,":
            position += 1
        if position >= len(text):
            break

        match = token_re.match(text, position)
        if not match:
            raise ValueError(f"could not parse interval near {text[position:]!r}")

        number = float(match.group(1))
        unit = match.group(2) or "s"
        if unit not in INTERVAL_UNITS:
            raise ValueError(f"unsupported interval unit {unit!r}")
        total += number * INTERVAL_UNITS[unit]
        position = match.end()

    if total <= 0:
        raise ValueError("interval must be greater than zero")
    return total


def parse_profiles(value: str) -> tuple[str, ...]:
    if not value.strip():
        return ()
    return tuple(part.strip() for part in value.split(",") if part.strip())


def parse_optional_bool(value: str, *, name: str) -> bool | None:
    if not value.strip():
        return None
    return parse_bool(value, name=name)


def parse_optional_cid_version(value: str) -> int | None:
    text = value.strip()
    if not text:
        return None
    if text not in {"0", "1"}:
        raise ValueError("IPFS_ADD_CID_VERSION must be 0 or 1")
    return int(text)


def parse_ipfs_add_profile_name(value: str) -> str:
    text = value.strip().lower() or "matrix-share-client"
    if text not in IPFS_ADD_PROFILE_CHOICES:
        raise ValueError(
            f"IPFS_ADD_PROFILE must be one of {', '.join(IPFS_ADD_PROFILE_CHOICES)}"
        )
    return text


def build_ipfs_add_profile(
    *,
    profile_name: str,
    kubo_version: str,
    cid_version_override: int | None,
    raw_leaves_override: bool | None,
    hash_function_override: str | None,
    chunker_override: str | None,
    trickle_override: bool | None,
) -> IpfsAddProfile:
    presets: dict[str, dict[str, object | None]] = {
        "matrix-share-client": {
            "cid_version": 1,
            "raw_leaves": None,
            "hash_function": None,
            "chunker": None,
            "trickle": None,
        },
        "cidv1-raw": {
            "cid_version": 1,
            "raw_leaves": True,
            "hash_function": None,
            "chunker": None,
            "trickle": None,
        },
        "kubo-default": {
            "cid_version": 0,
            "raw_leaves": False,
            "hash_function": None,
            "chunker": None,
            "trickle": None,
        },
    }
    settings = dict(presets[profile_name])

    if cid_version_override is not None:
        settings["cid_version"] = cid_version_override
    if raw_leaves_override is not None:
        settings["raw_leaves"] = raw_leaves_override
    if hash_function_override:
        settings["hash_function"] = hash_function_override
    if chunker_override:
        settings["chunker"] = chunker_override
    if trickle_override is not None:
        settings["trickle"] = trickle_override

    return IpfsAddProfile(
        profile_name=profile_name,
        cid_version=settings["cid_version"],
        raw_leaves=settings["raw_leaves"],
        hash_function=settings["hash_function"],
        chunker=settings["chunker"],
        trickle=settings["trickle"],
        kubo_version=kubo_version,
    )


def parse_bool(value: str, *, name: str) -> bool:
    text = value.strip().lower()
    if text in BOOLEAN_TRUE_VALUES:
        return True
    if text in BOOLEAN_FALSE_VALUES:
        return False
    raise ValueError(f"{name} must be one of true/false, yes/no, on/off, or 1/0")


def parse_bandwidth_method(value: str) -> str:
    text = value.strip().lower() or "auto"
    if text not in BANDWIDTH_METHOD_CHOICES:
        raise ValueError(
            f"UPLOAD_BANDWIDTH_METHOD must be one of {', '.join(BANDWIDTH_METHOD_CHOICES)}"
        )
    return text


def parse_bandwidth_limit(value: str) -> BandwidthLimit | None:
    text = value.strip()
    if not text or text.lower() in BANDWIDTH_DISABLED_VALUES:
        return None

    match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([A-Za-z]+(?:/[A-Za-z]+)?)", text)
    if not match:
        raise ValueError(
            "UPLOAD_BANDWIDTH_LIMIT must look like 10mbit, 100Mbps, or 5MiB/s"
        )

    number = float(match.group(1))
    unit = match.group(2)
    multiplier = BANDWIDTH_UNIT_MULTIPLIERS.get(unit)
    if multiplier is None:
        multiplier = BANDWIDTH_UNIT_MULTIPLIERS.get(unit.lower())
    if multiplier is None:
        raise ValueError(f"unsupported upload bandwidth unit {unit!r}")

    bits_per_second = int(number * multiplier)
    if bits_per_second <= 0:
        raise ValueError("UPLOAD_BANDWIDTH_LIMIT must be greater than zero")

    burst_bytes = max(1_600, math.ceil((bits_per_second / 8) / 10))
    return BandwidthLimit(
        raw=text,
        bits_per_second=bits_per_second,
        tc_rate=f"{bits_per_second}bit",
        tc_burst_bytes=burst_bytes,
    )


def load_config() -> Config:
    config_path = Path(os.getenv("CONFIG_PATH", "/config"))
    mount_root = Path("/mnt")
    scan_paths_raw = os.getenv("SCAN_PATHS", "/mnt").strip() or "/mnt"
    interval_text = os.getenv("RESCAN_INTERVAL", "5m").strip()
    priority = os.getenv("SCAN_PRIORITY", "normal").strip().lower()
    db_path = Path(os.getenv("INDEX_DB_PATH", str(config_path / "index" / "index.db")))
    export_path = Path(
        os.getenv("INDEX_EXPORT_PATH", str(config_path / "index" / "current-index.json"))
    )
    ipfs_path = Path(os.getenv("IPFS_PATH", str(config_path / "ipfs")))
    ipfs_profiles = parse_profiles(os.getenv("IPFS_PROFILE", "server"))
    kubo_version = os.getenv("KUBO_VERSION", "").strip() or "unknown"
    ipfs_add_profile_name = parse_ipfs_add_profile_name(
        os.getenv("IPFS_ADD_PROFILE", "matrix-share-client")
    )
    ipfs_add_profile = build_ipfs_add_profile(
        profile_name=ipfs_add_profile_name,
        kubo_version=kubo_version,
        cid_version_override=parse_optional_cid_version(
            os.getenv("IPFS_ADD_CID_VERSION", "")
        ),
        raw_leaves_override=parse_optional_bool(
            os.getenv("IPFS_ADD_RAW_LEAVES", ""),
            name="IPFS_ADD_RAW_LEAVES",
        ),
        hash_function_override=os.getenv("IPFS_ADD_HASH", "").strip() or None,
        chunker_override=os.getenv("IPFS_ADD_CHUNKER", "").strip() or None,
        trickle_override=parse_optional_bool(
            os.getenv("IPFS_ADD_TRICKLE", ""),
            name="IPFS_ADD_TRICKLE",
        ),
    )
    upload_bandwidth_limit = parse_bandwidth_limit(
        os.getenv("UPLOAD_BANDWIDTH_LIMIT", "")
    )
    upload_bandwidth_method = parse_bandwidth_method(
        os.getenv("UPLOAD_BANDWIDTH_METHOD", "auto")
    )
    upload_bandwidth_required = parse_bool(
        os.getenv("UPLOAD_BANDWIDTH_REQUIRED", "false"),
        name="UPLOAD_BANDWIDTH_REQUIRED",
    )
    bandwidth_interface = os.getenv("BANDWIDTH_INTERFACE", "").strip() or None

    if priority not in PRIORITY_PROFILES:
        raise ValueError(
            f"SCAN_PRIORITY must be one of {', '.join(sorted(PRIORITY_PROFILES))}"
        )

    return Config(
        config_path=config_path,
        mount_root=mount_root,
        scan_paths_raw=scan_paths_raw,
        rescan_interval_seconds=parse_interval(interval_text),
        rescan_interval_text=interval_text,
        scan_priority=priority,
        profile=PRIORITY_PROFILES[priority],
        db_path=db_path,
        export_path=export_path,
        ipfs_path=ipfs_path,
        ipfs_profiles=ipfs_profiles,
        kubo_version=kubo_version,
        ipfs_add_profile=ipfs_add_profile,
        upload_bandwidth_limit=upload_bandwidth_limit,
        upload_bandwidth_method=upload_bandwidth_method,
        upload_bandwidth_required=upload_bandwidth_required,
        bandwidth_interface=bandwidth_interface,
    )


def normalize_scan_path(raw_path: str, mount_root: Path) -> Path:
    path = Path(raw_path.strip())
    if not path.is_absolute():
        path = mount_root / path
    return path


def resolve_scan_roots(scan_paths_raw: str, mount_root: Path) -> list[Path]:
    mount_root = mount_root.resolve()
    requested = [part.strip() for part in re.split(r"[,\n]+", scan_paths_raw) if part.strip()]
    if not requested:
        requested = [str(mount_root)]

    resolved_roots: list[Path] = []
    for raw_path in requested:
        path = normalize_scan_path(raw_path, mount_root)
        resolved = path.resolve()
        if resolved != mount_root and mount_root not in resolved.parents:
            raise ValueError(f"scan path {path} must stay under {mount_root}")
        if not resolved.exists():
            LOGGER.warning("Skipping missing scan path %s", path)
            continue
        if not resolved.is_dir():
            LOGGER.warning("Skipping non-directory scan path %s", path)
            continue
        resolved_roots.append(resolved)

    if not resolved_roots:
        resolved_roots = [mount_root]

    resolved_roots.sort(key=lambda item: (len(item.parts), str(item)))
    collapsed: list[Path] = []
    for root in resolved_roots:
        if any(existing == root or existing in root.parents for existing in collapsed):
            continue
        collapsed.append(root)
    return collapsed


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def initialize_schema(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS files (
            path TEXT PRIMARY KEY,
            relative_path TEXT NOT NULL,
            root_path TEXT NOT NULL,
            cid TEXT,
            import_profile TEXT,
            size INTEGER NOT NULL,
            mtime_ns INTEGER NOT NULL,
            inode INTEGER NOT NULL,
            device INTEGER NOT NULL,
            active INTEGER NOT NULL DEFAULT 0,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            last_indexed_at TEXT,
            removed_at TEXT,
            last_error TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL,
            files_seen INTEGER NOT NULL DEFAULT 0,
            files_added INTEGER NOT NULL DEFAULT 0,
            files_updated INTEGER NOT NULL DEFAULT 0,
            files_removed INTEGER NOT NULL DEFAULT 0,
            files_unchanged INTEGER NOT NULL DEFAULT 0,
            errors INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_files_active ON files(active, path)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_files_cid_active ON files(cid, active)"
    )
    ensure_column(
        conn,
        table_name="files",
        column_name="import_profile",
        column_sql="ALTER TABLE files ADD COLUMN import_profile TEXT",
    )
    conn.commit()


def start_scan_record(conn: sqlite3.Connection) -> int:
    cursor = conn.execute(
        "INSERT INTO scans (started_at, status) VALUES (?, ?)",
        (utcnow(), "running"),
    )
    return int(cursor.lastrowid)


def finish_scan_record(
    conn: sqlite3.Connection,
    scan_id: int,
    status: str,
    summary: ScanSummary,
) -> None:
    conn.execute(
        """
        UPDATE scans
        SET finished_at = ?,
            status = ?,
            files_seen = ?,
            files_added = ?,
            files_updated = ?,
            files_removed = ?,
            files_unchanged = ?,
            errors = ?
        WHERE id = ?
        """,
        (
            utcnow(),
            status,
            summary.files_seen,
            summary.files_added,
            summary.files_updated,
            summary.files_removed,
            summary.files_unchanged,
            summary.errors,
            scan_id,
        ),
    )


def load_existing_entries(
    conn: sqlite3.Connection,
    roots: Iterable[Path],
) -> dict[str, sqlite3.Row]:
    rows = conn.execute("SELECT * FROM files WHERE active = 1").fetchall()
    root_strings = tuple(str(root) for root in roots)
    result: dict[str, sqlite3.Row] = {}

    for row in rows:
        path = row["path"]
        if any(path == root or path.startswith(f"{root}{os.sep}") for root in root_strings):
            result[path] = row
    return result


def touch_seen_row(conn: sqlite3.Connection, path: str) -> None:
    conn.execute(
        "UPDATE files SET last_seen_at = ?, last_error = NULL WHERE path = ?",
        (utcnow(), path),
    )


def ensure_column(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    column_name: str,
    column_sql: str,
) -> None:
    columns = {
        row[1]
        for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    if column_name not in columns:
        conn.execute(column_sql)


def row_matches_file_state(
    row: sqlite3.Row,
    stat_result: os.stat_result,
    import_profile_signature: str,
) -> bool:
    return (
        row["size"] == stat_result.st_size
        and row["mtime_ns"] == stat_result.st_mtime_ns
        and row["inode"] == stat_result.st_ino
        and row["device"] == stat_result.st_dev
        and row["import_profile"] == import_profile_signature
    )


def upsert_successful_file(
    conn: sqlite3.Connection,
    path: str,
    relative_path: str,
    root_path: str,
    cid: str,
    import_profile: str,
    stat_result: os.stat_result,
) -> None:
    now = utcnow()
    conn.execute(
        """
        INSERT INTO files (
            path,
            relative_path,
            root_path,
            cid,
            import_profile,
            size,
            mtime_ns,
            inode,
            device,
            active,
            first_seen_at,
            last_seen_at,
            last_indexed_at,
            removed_at,
            last_error
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, NULL, NULL)
        ON CONFLICT(path) DO UPDATE SET
            relative_path = excluded.relative_path,
            root_path = excluded.root_path,
            cid = excluded.cid,
            import_profile = excluded.import_profile,
            size = excluded.size,
            mtime_ns = excluded.mtime_ns,
            inode = excluded.inode,
            device = excluded.device,
            active = 1,
            last_seen_at = excluded.last_seen_at,
            last_indexed_at = excluded.last_indexed_at,
            removed_at = NULL,
            last_error = NULL
        """,
        (
            path,
            relative_path,
            root_path,
            cid,
            import_profile,
            stat_result.st_size,
            stat_result.st_mtime_ns,
            stat_result.st_ino,
            stat_result.st_dev,
            now,
            now,
            now,
        ),
    )


def mark_failed_file(
    conn: sqlite3.Connection,
    path: str,
    relative_path: str,
    root_path: str,
    stat_result: os.stat_result,
    error_message: str,
    keep_cid: str | None,
) -> None:
    now = utcnow()
    conn.execute(
        """
        INSERT INTO files (
            path,
            relative_path,
            root_path,
            cid,
            size,
            mtime_ns,
            inode,
            device,
            active,
            first_seen_at,
            last_seen_at,
            last_indexed_at,
            removed_at,
            last_error
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, NULL, ?, ?)
        ON CONFLICT(path) DO UPDATE SET
            relative_path = excluded.relative_path,
            root_path = excluded.root_path,
            cid = excluded.cid,
            size = excluded.size,
            mtime_ns = excluded.mtime_ns,
            inode = excluded.inode,
            device = excluded.device,
            active = 0,
            last_seen_at = excluded.last_seen_at,
            removed_at = excluded.removed_at,
            last_error = excluded.last_error
        """,
        (
            path,
            relative_path,
            root_path,
            keep_cid,
            stat_result.st_size,
            stat_result.st_mtime_ns,
            stat_result.st_ino,
            stat_result.st_dev,
            now,
            now,
            now,
            error_message,
        ),
    )


def mark_deleted_file(conn: sqlite3.Connection, path: str) -> None:
    conn.execute(
        """
        UPDATE files
        SET active = 0,
            removed_at = ?,
            last_error = NULL
        WHERE path = ?
        """,
        (utcnow(), path),
    )


def export_active_manifest(
    conn: sqlite3.Connection,
    export_path: Path,
    add_profile: IpfsAddProfile,
    scan_paths: list[str],
) -> None:
    rows = conn.execute(
        """
        SELECT path, relative_path, root_path, cid, import_profile, size, mtime_ns, last_seen_at, last_indexed_at
        FROM files
        WHERE active = 1
        ORDER BY relative_path
        """
    ).fetchall()

    payload = {
        "generated_at": utcnow(),
        "ipfs_add": add_profile.as_manifest_object(),
        "scan_paths": scan_paths,
        "file_count": len(rows),
        "files": [
            {
                "path": row["path"],
                "relative_path": row["relative_path"],
                "root_path": row["root_path"],
                "cid": row["cid"],
                "import_profile": row["import_profile"],
                "size": row["size"],
                "mtime_ns": row["mtime_ns"],
                "last_seen_at": row["last_seen_at"],
                "last_indexed_at": row["last_indexed_at"],
            }
            for row in rows
        ],
    }

    temp_path = export_path.with_suffix(export_path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    temp_path.replace(export_path)


def iter_regular_files(root: Path) -> Iterable[Path]:
    for current_root, dirnames, filenames in os.walk(root):
        dirnames.sort()
        filenames.sort()

        for dirname in list(dirnames):
            dirpath = Path(current_root, dirname)
            if dirpath.is_symlink():
                LOGGER.warning("Skipping symlinked directory %s", dirpath)
                dirnames.remove(dirname)

        for filename in filenames:
            path = Path(current_root, filename)
            if path.is_symlink():
                LOGGER.warning("Skipping symlinked file %s", path)
                continue
            if not path.is_file():
                LOGGER.warning("Skipping non-regular file %s", path)
                continue
            yield path


def set_subprocess_niceness(niceness: int) -> None:
    if niceness > 0:
        try:
            os.nice(niceness)
        except OSError:
            pass


def run_ipfs(
    args: list[str],
    *,
    niceness: int,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    command = ["ipfs", *args]
    LOGGER.debug("Running: %s", " ".join(command))
    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        env=os.environ.copy(),
        preexec_fn=(lambda: set_subprocess_niceness(niceness)),
    )
    if check and result.returncode != 0:
        stderr = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"{' '.join(command)} failed: {stderr}")
    return result


def ensure_ipfs_repo(config: Config) -> None:
    config.ipfs_path.mkdir(parents=True, exist_ok=True)
    repo_config = config.ipfs_path / "config"
    if repo_config.exists():
        LOGGER.info("Using existing IPFS repo at %s", config.ipfs_path)
        return

    LOGGER.info("Initializing IPFS repo at %s", config.ipfs_path)
    subprocess.run(["ipfs", "init"], check=True, env=os.environ.copy())
    for profile in config.ipfs_profiles:
        subprocess.run(
            ["ipfs", "config", "profile", "apply", profile],
            check=True,
            env=os.environ.copy(),
        )
    subprocess.run(
        ["ipfs", "config", "Addresses.API", "/ip4/0.0.0.0/tcp/5001"],
        check=True,
        env=os.environ.copy(),
    )
    subprocess.run(
        ["ipfs", "config", "Addresses.Gateway", "/ip4/0.0.0.0/tcp/8080"],
        check=True,
        env=os.environ.copy(),
    )


def start_ipfs_daemon() -> subprocess.Popen[str]:
    LOGGER.info("Starting IPFS daemon")
    return subprocess.Popen(
        ["ipfs", "daemon", "--migrate=true"],
        env=os.environ.copy(),
        stdout=sys.stdout,
        stderr=sys.stderr,
        text=True,
        start_new_session=True,
    )


def get_ipfs_api_http_url(ipfs_path: Path) -> str:
    config_file = ipfs_path / "config"
    default_host = "127.0.0.1"
    default_port = "5001"

    try:
        payload = json.loads(config_file.read_text())
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return f"http://{default_host}:{default_port}/api/v0/version"

    api_addr = payload.get("Addresses", {}).get("API", "")
    match = re.fullmatch(r"/ip4/([^/]+)/tcp/(\d+)", api_addr)
    if match:
        host, port = match.groups()
        if host == "0.0.0.0":
            host = "127.0.0.1"
        return f"http://{host}:{port}/api/v0/version"

    match = re.fullmatch(r"/ip6/([^/]+)/tcp/(\d+)", api_addr)
    if match:
        host, port = match.groups()
        if host == "::":
            host = "::1"
        return f"http://[{host}]:{port}/api/v0/version"

    return f"http://{default_host}:{default_port}/api/v0/version"


def wait_for_ipfs(
    config: Config,
    *,
    daemon: subprocess.Popen[str] | None = None,
    timeout_seconds: float = 60.0,
) -> None:
    api_url = get_ipfs_api_http_url(config.ipfs_path)
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if daemon is not None and daemon.poll() is not None:
            raise RuntimeError(
                f"IPFS daemon exited unexpectedly with code {daemon.returncode} "
                "before the API became ready"
            )
        request = urllib.request.Request(api_url, data=b"", method="POST")
        try:
            with urllib.request.urlopen(request, timeout=3) as response:
                if 200 <= getattr(response, "status", 200) < 300:
                    LOGGER.info("IPFS API is ready at %s", api_url)
                    return
        except (urllib.error.URLError, OSError):
            pass
        time.sleep(1)
    raise TimeoutError(f"Timed out waiting for IPFS API at {api_url}")


def detect_egress_interface() -> str:
    route_commands = [
        ["ip", "route", "show", "default"],
        ["ip", "-6", "route", "show", "default"],
    ]
    for command in route_commands:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
        )
        match = re.search(r"\bdev\s+(\S+)", result.stdout)
        if match and match.group(1) != "lo":
            return match.group(1)

    result = subprocess.run(
        ["ip", "-o", "link", "show", "up"],
        check=False,
        capture_output=True,
        text=True,
    )
    for line in result.stdout.splitlines():
        match = re.match(r"\d+:\s+([^:@]+)", line)
        if match and match.group(1) != "lo":
            return match.group(1)

    raise RuntimeError("Could not determine the container egress network interface")


def run_tc(args: list[str]) -> None:
    result = subprocess.run(
        ["tc", *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip() or result.stdout.strip() or "unknown error"
        raise RuntimeError(stderr)


def clear_root_qdisc(interface: str) -> None:
    subprocess.run(
        ["tc", "qdisc", "del", "dev", interface, "root"],
        check=False,
        capture_output=True,
        text=True,
    )


def apply_tbf_bandwidth_limit(interface: str, limit: BandwidthLimit) -> None:
    run_tc(
        [
            "qdisc",
            "replace",
            "dev",
            interface,
            "root",
            "tbf",
            "rate",
            limit.tc_rate,
            "burst",
            f"{limit.tc_burst_bytes}b",
            "latency",
            "100ms",
        ]
    )


def apply_htb_bandwidth_limit(interface: str, limit: BandwidthLimit) -> None:
    run_tc(
        [
            "qdisc",
            "replace",
            "dev",
            interface,
            "root",
            "handle",
            "1:",
            "htb",
            "default",
            "1",
        ]
    )
    try:
        run_tc(
            [
                "class",
                "replace",
                "dev",
                interface,
                "parent",
                "1:",
                "classid",
                "1:1",
                "htb",
                "rate",
                limit.tc_rate,
                "ceil",
                limit.tc_rate,
                "burst",
                f"{limit.tc_burst_bytes}b",
                "cburst",
                f"{limit.tc_burst_bytes}b",
            ]
        )
    except Exception:
        clear_root_qdisc(interface)
        raise


def apply_netem_bandwidth_limit(interface: str, limit: BandwidthLimit) -> None:
    run_tc(
        [
            "qdisc",
            "replace",
            "dev",
            interface,
            "root",
            "netem",
            "rate",
            limit.tc_rate,
        ]
    )


def bandwidth_method_sequence(method: str) -> tuple[str, ...]:
    if method == "auto":
        return ("tbf", "htb", "netem")
    return (method,)


def apply_upload_bandwidth_limit(config: Config) -> bool:
    limit = config.upload_bandwidth_limit
    if limit is None:
        return False

    methods = bandwidth_method_sequence(config.upload_bandwidth_method)
    appliers = {
        "tbf": apply_tbf_bandwidth_limit,
        "htb": apply_htb_bandwidth_limit,
        "netem": apply_netem_bandwidth_limit,
    }

    try:
        interface = config.bandwidth_interface or detect_egress_interface()
    except FileNotFoundError as exc:
        message = (
            "tc is not installed in the container image, so upload bandwidth limiting "
            f"cannot be enabled ({exc})"
        )
        if config.upload_bandwidth_required:
            raise RuntimeError(message) from exc
        LOGGER.warning("%s; continuing without it", message)
        return False
    except Exception as exc:  # noqa: BLE001
        message = f"Could not prepare upload bandwidth limiting ({exc})"
        if config.upload_bandwidth_required:
            raise RuntimeError(message) from exc
        LOGGER.warning("%s; continuing without it", message)
        return False

    LOGGER.info(
        "Applying upload bandwidth limit %s on interface %s using methods=%s",
        limit.raw,
        interface,
        ",".join(methods),
    )

    errors: list[str] = []
    for method in methods:
        try:
            appliers[method](interface, limit)
            LOGGER.info(
                "Applied upload bandwidth limit %s on interface %s using method=%s",
                limit.raw,
                interface,
                method,
            )
            return True
        except FileNotFoundError as exc:
            errors.append(f"{method}: tc missing ({exc})")
            break
        except Exception as exc:  # noqa: BLE001
            clear_root_qdisc(interface)
            errors.append(f"{method}: {exc}")

    message = (
        f"Failed to apply upload bandwidth limit using methods={','.join(methods)}. "
        + "; ".join(errors)
    )
    if config.upload_bandwidth_required:
        raise RuntimeError(message)
    LOGGER.warning("%s. Continuing without bandwidth limiting.", message)
    return False


def stop_process_group(process: subprocess.Popen[str] | None) -> None:
    if process is None or process.poll() is not None:
        return

    try:
        os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=20)
    except ProcessLookupError:
        return
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)


def main() -> int:
    setup_logging()
    config = load_config()
    os.environ["IPFS_PATH"] = str(config.ipfs_path)

    LOGGER.info(
        "Starting autoscan service with roots=%s interval=%s priority=%s upload_limit=%s upload_method=%s required=%s ipfs_add_profile=%s kubo_version=%s add_args=%s",
        config.scan_paths_raw,
        config.rescan_interval_text,
        config.scan_priority,
        config.upload_bandwidth_limit.raw if config.upload_bandwidth_limit else "off",
        config.upload_bandwidth_method,
        config.upload_bandwidth_required,
        config.ipfs_add_profile.profile_name,
        config.kubo_version,
        " ".join(config.ipfs_add_profile.cli_args),
    )

    ensure_parent_dir(config.db_path)
    ensure_parent_dir(config.export_path)
    ensure_ipfs_repo(config)
    apply_upload_bandwidth_limit(config)

    scanner = Scanner(config)
    daemon: subprocess.Popen[str] | None = None

    def handle_signal(signum: int, _frame: object) -> None:
        LOGGER.info("Received signal %s, shutting down", signum)
        scanner.stop()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    try:
        daemon = start_ipfs_daemon()
        wait_for_ipfs(config, daemon=daemon)

        while not scanner.should_stop():
            if daemon.poll() is not None:
                raise RuntimeError(f"IPFS daemon exited unexpectedly with code {daemon.returncode}")

            started = time.monotonic()
            summary = scanner.scan_once()
            LOGGER.info(
                "Scan complete seen=%s added=%s updated=%s removed=%s unchanged=%s errors=%s",
                summary.files_seen,
                summary.files_added,
                summary.files_updated,
                summary.files_removed,
                summary.files_unchanged,
                summary.errors,
            )

            while not scanner.should_stop():
                elapsed = time.monotonic() - started
                remaining = config.rescan_interval_seconds - elapsed
                if remaining <= 0:
                    break
                time.sleep(min(1.0, remaining))

        return 0
    finally:
        stop_process_group(daemon)


if __name__ == "__main__":
    raise SystemExit(main())
