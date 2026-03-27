import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import app.service as service
from app.service import (
    Config,
    IpfsAddProfile,
    PriorityProfile,
    ScanSummary,
    build_ipfs_add_profile,
    initialize_schema,
    load_config,
    parse_bandwidth_limit,
    parse_interval,
    resolve_scan_roots,
    upsert_successful_file,
)


def make_config(
    *,
    config_path: str = "/config",
    mount_root: str = "/mnt",
    scan_paths_raw: str = "/mnt",
    rescan_interval_seconds: float = 300,
    rescan_interval_text: str = "5m",
    scan_priority: str = "normal",
    db_path: str | None = None,
    export_path: str | None = None,
    ipfs_path: str | None = None,
    kubo_version: str = "v0.40.1",
    ipfs_add_profile: IpfsAddProfile | None = None,
    upload_bandwidth_limit=None,
    upload_bandwidth_method: str = "auto",
    upload_bandwidth_required: bool = False,
    bandwidth_interface: str | None = None,
) -> Config:
    config_root = Path(config_path)
    ipfs_add_profile = ipfs_add_profile or build_ipfs_add_profile(
        profile_name="matrix-share-client",
        kubo_version=kubo_version,
        cid_version_override=None,
        raw_leaves_override=None,
        hash_function_override=None,
        chunker_override=None,
        trickle_override=None,
    )
    return Config(
        config_path=config_root,
        mount_root=Path(mount_root),
        scan_paths_raw=scan_paths_raw,
        rescan_interval_seconds=rescan_interval_seconds,
        rescan_interval_text=rescan_interval_text,
        scan_priority=scan_priority,
        profile=PriorityProfile(
            niceness=0,
            per_file_pause=0.0,
            changed_file_pause=0.0,
            batch_size=0,
            batch_pause=0.0,
        ),
        db_path=Path(db_path) if db_path else config_root / "index" / "index.db",
        export_path=Path(export_path)
        if export_path
        else config_root / "index" / "current-index.json",
        ipfs_path=Path(ipfs_path) if ipfs_path else config_root / "ipfs",
        ipfs_profiles=("server",),
        kubo_version=kubo_version,
        ipfs_add_profile=ipfs_add_profile,
        upload_bandwidth_limit=upload_bandwidth_limit,
        upload_bandwidth_method=upload_bandwidth_method,
        upload_bandwidth_required=upload_bandwidth_required,
        bandwidth_interface=bandwidth_interface,
    )


class ParseIntervalTests(unittest.TestCase):
    def test_supports_short_units(self) -> None:
        self.assertEqual(parse_interval("30s"), 30)
        self.assertEqual(parse_interval("5m"), 300)
        self.assertEqual(parse_interval("2h"), 7200)

    def test_supports_human_readable_units(self) -> None:
        self.assertEqual(parse_interval("1 hour 30 minutes"), 5400)
        self.assertEqual(parse_interval("1h 15m"), 4500)

    def test_rejects_bad_units(self) -> None:
        with self.assertRaises(ValueError):
            parse_interval("5fortnights")


class ParseBandwidthLimitTests(unittest.TestCase):
    def test_supports_bit_rates(self) -> None:
        limit = parse_bandwidth_limit("10mbit")
        self.assertIsNotNone(limit)
        self.assertEqual(limit.bits_per_second, 10_000_000)
        self.assertEqual(limit.tc_rate, "10000000bit")

        limit = parse_bandwidth_limit("10mbit/s")
        self.assertIsNotNone(limit)
        self.assertEqual(limit.bits_per_second, 10_000_000)

    def test_supports_byte_rates(self) -> None:
        limit = parse_bandwidth_limit("5 MiB/s")
        self.assertIsNotNone(limit)
        self.assertEqual(limit.bits_per_second, 41_943_040)

    def test_supports_disabled_values(self) -> None:
        self.assertIsNone(parse_bandwidth_limit("off"))
        self.assertIsNone(parse_bandwidth_limit("0"))

    def test_rejects_bad_units(self) -> None:
        with self.assertRaises(ValueError):
            parse_bandwidth_limit("10 llamas")


class ResolveScanRootsTests(unittest.TestCase):
    def test_collapses_nested_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            mount_root = Path(tmpdir, "mnt")
            nested = mount_root / "nested"
            mount_root.mkdir()
            nested.mkdir()

            roots = resolve_scan_roots(f"{mount_root},{nested}", mount_root)
            self.assertEqual(roots, [mount_root.resolve()])

    def test_rejects_paths_outside_mount_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            mount_root = Path(tmpdir, "mnt")
            outside = Path(tmpdir, "outside")
            mount_root.mkdir()
            outside.mkdir()

            with self.assertRaises(ValueError):
                resolve_scan_roots(str(outside), mount_root)


class LoadConfigTests(unittest.TestCase):
    def test_config_path_changes_default_storage_locations(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "CONFIG_PATH": "/state",
                "KUBO_VERSION": "v0.40.1",
                "RESCAN_INTERVAL": "5m",
                "SCAN_PRIORITY": "normal",
            },
            clear=True,
        ):
            config = load_config()

        self.assertEqual(config.config_path, Path("/state"))
        self.assertEqual(config.ipfs_path, Path("/state/ipfs"))
        self.assertEqual(config.db_path, Path("/state/index/index.db"))
        self.assertEqual(config.export_path, Path("/state/index/current-index.json"))
        self.assertEqual(config.ipfs_add_profile.profile_name, "matrix-share-client")
        self.assertEqual(
            config.ipfs_add_profile.cli_args,
            ("-Q", "--pin=true", "--wrap-with-directory=false", "--cid-version=1"),
        )

    def test_loads_upload_bandwidth_limit(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "KUBO_VERSION": "v0.40.1",
                "RESCAN_INTERVAL": "5m",
                "SCAN_PRIORITY": "normal",
                "UPLOAD_BANDWIDTH_LIMIT": "10mbit",
                "UPLOAD_BANDWIDTH_METHOD": "netem",
                "UPLOAD_BANDWIDTH_REQUIRED": "true",
                "BANDWIDTH_INTERFACE": "eth9",
            },
            clear=True,
        ):
            config = load_config()

        self.assertIsNotNone(config.upload_bandwidth_limit)
        self.assertEqual(config.upload_bandwidth_limit.bits_per_second, 10_000_000)
        self.assertEqual(config.upload_bandwidth_method, "netem")
        self.assertTrue(config.upload_bandwidth_required)
        self.assertEqual(config.bandwidth_interface, "eth9")

    def test_loads_custom_ipfs_add_profile_overrides(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "KUBO_VERSION": "v0.40.1",
                "RESCAN_INTERVAL": "5m",
                "SCAN_PRIORITY": "normal",
                "IPFS_ADD_PROFILE": "kubo-default",
                "IPFS_ADD_RAW_LEAVES": "true",
                "IPFS_ADD_CHUNKER": "size-1048576",
                "IPFS_ADD_HASH": "sha2-256",
                "IPFS_ADD_TRICKLE": "false",
            },
            clear=True,
        ):
            config = load_config()

        self.assertEqual(config.ipfs_add_profile.profile_name, "kubo-default")
        self.assertEqual(config.ipfs_add_profile.cid_version, 0)
        self.assertTrue(config.ipfs_add_profile.raw_leaves)
        self.assertEqual(config.ipfs_add_profile.chunker, "size-1048576")
        self.assertEqual(config.ipfs_add_profile.hash_function, "sha2-256")
        self.assertFalse(config.ipfs_add_profile.trickle)


class MainLoopTests(unittest.TestCase):
    def test_runs_first_scan_immediately_on_startup(self) -> None:
        config = make_config()
        scanner_instances = []

        class FakeScanner:
            def __init__(self, _config: Config):
                self.stopped = False
                self.scan_once = Mock(side_effect=self._scan_once)
                scanner_instances.append(self)

            def _scan_once(self) -> ScanSummary:
                self.stopped = True
                return ScanSummary(files_seen=1, files_added=1)

            def should_stop(self) -> bool:
                return self.stopped

            def stop(self) -> None:
                self.stopped = True

        fake_daemon = Mock()
        fake_daemon.poll.return_value = None

        with (
            patch.object(service, "load_config", return_value=config),
            patch.object(service, "setup_logging"),
            patch.object(service, "ensure_parent_dir"),
            patch.object(service, "ensure_ipfs_repo"),
            patch.object(service, "apply_upload_bandwidth_limit"),
            patch.object(service, "start_ipfs_daemon", return_value=fake_daemon),
            patch.object(service, "wait_for_ipfs"),
            patch.object(service, "stop_process_group"),
            patch.object(service.signal, "signal"),
            patch.object(service.time, "sleep") as sleep_mock,
            patch.object(service, "Scanner", FakeScanner),
        ):
            result = service.main()

        self.assertEqual(result, 0)
        self.assertEqual(len(scanner_instances), 1)
        scanner_instances[0].scan_once.assert_called_once()
        sleep_mock.assert_not_called()


class WaitForIpfsTests(unittest.TestCase):
    def test_raises_if_daemon_exits_before_api_is_ready(self) -> None:
        config = make_config()
        daemon = Mock()
        daemon.poll.return_value = 1

        with (
            patch.object(service, "get_ipfs_api_http_url", return_value="http://127.0.0.1:5001/api/v0/version"),
            patch.object(service.time, "monotonic", side_effect=[0, 0]),
        ):
            with self.assertRaisesRegex(RuntimeError, "before the API became ready"):
                service.wait_for_ipfs(config, daemon=daemon, timeout_seconds=1)


class UploadBandwidthLimitTests(unittest.TestCase):
    def test_falls_back_to_htb_when_tbf_is_unavailable(self) -> None:
        config = make_config(
            upload_bandwidth_limit=parse_bandwidth_limit("10mbit"),
            upload_bandwidth_method="auto",
            upload_bandwidth_required=False,
            bandwidth_interface="eth0",
        )

        def fake_run(command, check, capture_output, text):
            if command[:6] == ["tc", "qdisc", "replace", "dev", "eth0", "root"] and "tbf" in command:
                return Mock(returncode=1, stderr="Error: Specified qdisc kind is unknown.")
            if command[:5] == ["tc", "qdisc", "del", "dev", "eth0"]:
                return Mock(returncode=0, stderr="")
            if command[:9] == ["tc", "qdisc", "replace", "dev", "eth0", "root", "handle", "1:", "htb"]:
                return Mock(returncode=0, stderr="")
            if command[:9] == ["tc", "class", "replace", "dev", "eth0", "parent", "1:", "classid", "1:1"]:
                return Mock(returncode=0, stderr="")
            raise AssertionError(f"Unexpected tc command: {command}")

        with patch.object(service.subprocess, "run", side_effect=fake_run):
            applied = service.apply_upload_bandwidth_limit(config)

        self.assertTrue(applied)

    def test_continues_without_limit_if_tc_fails(self) -> None:
        config = make_config(
            upload_bandwidth_limit=parse_bandwidth_limit("10mbit"),
            upload_bandwidth_method="tbf",
            upload_bandwidth_required=False,
            bandwidth_interface="eth0",
        )
        failed_result = Mock(returncode=1, stderr="RTNETLINK answers: Operation not permitted")

        with (
            patch.object(service.subprocess, "run", return_value=failed_result) as run_mock,
            patch.object(service.LOGGER, "warning") as warning_mock,
        ):
            applied = service.apply_upload_bandwidth_limit(config)

        self.assertFalse(applied)
        self.assertEqual(run_mock.call_count, 2)
        warning_mock.assert_called_once()


class ScannerImportProfileTests(unittest.TestCase):
    def test_reindexes_when_import_profile_changes_and_unpins_old_cid(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            mount_root = Path(tmpdir, "mnt")
            config_root = Path(tmpdir, "config")
            file_path = mount_root / "sample.bin"
            mount_root.mkdir()
            file_path.write_bytes(b"hello world")
            mount_root = mount_root.resolve()
            file_path = file_path.resolve()

            current_profile = build_ipfs_add_profile(
                profile_name="matrix-share-client",
                kubo_version="v0.40.1",
                cid_version_override=None,
                raw_leaves_override=None,
                hash_function_override=None,
                chunker_override=None,
                trickle_override=None,
            )
            config = make_config(
                config_path=str(config_root),
                mount_root=str(mount_root),
                scan_paths_raw=str(mount_root),
                db_path=str(config_root / "index" / "index.db"),
                export_path=str(config_root / "index" / "current-index.json"),
                ipfs_path=str(config_root / "ipfs"),
                ipfs_add_profile=current_profile,
            )
            config.db_path.parent.mkdir(parents=True, exist_ok=True)
            config.export_path.parent.mkdir(parents=True, exist_ok=True)

            with service.sqlite3.connect(config.db_path) as conn:
                conn.row_factory = service.sqlite3.Row
                initialize_schema(conn)
                upsert_successful_file(
                    conn=conn,
                    path=str(file_path),
                    relative_path=str(file_path.relative_to(mount_root)),
                    root_path=str(mount_root),
                    cid="cid-old",
                    import_profile="legacy-profile",
                    stat_result=file_path.stat(),
                )
                conn.commit()

            commands: list[list[str]] = []

            def fake_run_ipfs(args, *, niceness, check=True):
                commands.append(list(args))
                if args[0] == "add":
                    return Mock(stdout="cid-new\n", returncode=0, stderr="")
                if args[:2] == ["pin", "rm"]:
                    return Mock(stdout="", returncode=0, stderr="")
                raise AssertionError(f"Unexpected ipfs command: {args}")

            with patch.object(service, "run_ipfs", side_effect=fake_run_ipfs):
                summary = service.Scanner(config).scan_once()

            self.assertEqual(summary.files_updated, 1)
            self.assertEqual(summary.files_unchanged, 0)
            self.assertIn(
                [
                    "add",
                    "-Q",
                    "--pin=true",
                    "--wrap-with-directory=false",
                    "--cid-version=1",
                    str(file_path),
                ],
                commands,
            )
            self.assertIn(["pin", "rm", "cid-old"], commands)

            with service.sqlite3.connect(config.db_path) as conn:
                conn.row_factory = service.sqlite3.Row
                row = conn.execute(
                    "SELECT cid, import_profile FROM files WHERE path = ?",
                    (str(file_path),),
                ).fetchone()

            self.assertEqual(row["cid"], "cid-new")
            self.assertEqual(row["import_profile"], current_profile.signature)

if __name__ == "__main__":
    unittest.main()
