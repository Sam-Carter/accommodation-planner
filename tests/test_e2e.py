import unittest
import subprocess
import shutil
from pathlib import Path
import os
import tempfile

REPO_ROOT = Path(__file__).resolve().parents[1]
UTILS = REPO_ROOT / "utils.py"
SEND = REPO_ROOT / "send.csv"
RECEIVE = REPO_ROOT / "receive.csv"

EXPECTED_UPDATED = [
    "name,people to move,x,y",
    "a,10,1,1",
    "b,11,3,3",
    "c,28,7,7",
    "d,20,9,9",
]

EXPECTED_FLOWS = [
    "from name,from index,to name,to index,flow",
    "b,1,e,0,4",
    "b,1,f,1,5",
    "b,1,g,2,30",
    "c,2,h,3,2",
]

class TestEndToEnd(unittest.TestCase):
    def run_tool(self, tmp_dir: Path):
        shutil.copy(SEND, tmp_dir / "send.csv")
        shutil.copy(RECEIVE, tmp_dir / "receive.csv")
        env = os.environ.copy()
        env["PYTHONPATH"] = str(REPO_ROOT)
        result = subprocess.run(
            ["python", str(UTILS), "-send", "send.csv", "-receive", "receive.csv", "-percentage", "20"],
            cwd=tmp_dir,
            capture_output=True,
            text=True,
            env=env,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)

    def test_outputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            self.run_tool(tmp_path)
            updated = (tmp_path / "updated_send.csv").read_text().splitlines()
            final = (tmp_path / "Final Flows.csv").read_text().splitlines()
            self.assertEqual(updated, EXPECTED_UPDATED)
            self.assertEqual(final, EXPECTED_FLOWS)

if __name__ == "__main__":
    unittest.main()
