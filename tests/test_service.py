import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import app.service as service
from app.service import (
    Config,
    PriorityProfile,
    ScanSummary,
    load_config,
    parse_bandwidth_limit,
    parse_interval,
    resolve_scan_roots,
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

    def test_loads_upload_bandwidth_limit(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "RESCAN_INTERVAL": "5m",
                "SCAN_PRIORITY": "normal",
                "UPLOAD_BANDWIDTH_LIMIT": "10mbit",
                "BANDWIDTH_INTERFACE": "eth9",
            },
            clear=True,
        ):
            config = load_config()

        self.assertIsNotNone(config.upload_bandwidth_limit)
        self.assertEqual(config.upload_bandwidth_limit.bits_per_second, 10_000_000)
        self.assertEqual(config.bandwidth_interface, "eth9")


class MainLoopTests(unittest.TestCase):
    def test_runs_first_scan_immediately_on_startup(self) -> None:
        config = Config(
            config_path=Path("/config"),
            mount_root=Path("/mnt"),
            scan_paths_raw="/mnt",
            rescan_interval_seconds=300,
            rescan_interval_text="5m",
            scan_priority="normal",
            profile=PriorityProfile(
                niceness=0,
                per_file_pause=0.0,
                changed_file_pause=0.0,
                batch_size=0,
                batch_pause=0.0,
            ),
            db_path=Path("/config/index/index.db"),
            export_path=Path("/config/index/current-index.json"),
            ipfs_path=Path("/config/ipfs"),
            ipfs_profiles=("server",),
            upload_bandwidth_limit=None,
            bandwidth_interface=None,
        )
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

if __name__ == "__main__":
    unittest.main()
