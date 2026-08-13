import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.local_env import load_local_env


class LocalEnvironmentTests(unittest.TestCase):
    def test_loads_values_and_preserves_exported_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env.local"
            path.write_text(
                "# local secrets\nHARDCOVER_API_TOKEN='file-token'\nEXTRA_SETTING=value=with=equals\n",
                encoding="utf-8",
            )
            with patch.dict(os.environ, {"HARDCOVER_API_TOKEN": "shell-token"}, clear=True):
                load_local_env(path)
                self.assertEqual(os.environ["HARDCOVER_API_TOKEN"], "shell-token")
                self.assertEqual(os.environ["EXTRA_SETTING"], "value=with=equals")


if __name__ == "__main__":
    unittest.main()
