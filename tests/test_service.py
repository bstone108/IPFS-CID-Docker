import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.service import load_config, parse_bandwidth_limit, parse_interval, resolve_scan_roots


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


if __name__ == "__main__":
    unittest.main()
