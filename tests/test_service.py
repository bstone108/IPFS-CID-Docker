import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.service import load_config, parse_interval, resolve_scan_roots


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


if __name__ == "__main__":
    unittest.main()
