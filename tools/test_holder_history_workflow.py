import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "holder_history_publish.yml"


class HolderHistoryWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = WORKFLOW.read_text(encoding="utf-8")

    def test_runs_friday_evening_with_saturday_fallback_in_taipei(self):
        self.assertIn('- cron: "30 13 * * 5"', self.workflow)
        self.assertIn('- cron: "30 1 * * 6"', self.workflow)
        self.assertIn("workflow_dispatch:", self.workflow)

    def test_recomputes_and_publishes_complete_tdcc_top_50(self):
        required_commands = (
            "python tdcc_holder_snapshot.py",
            "python tdcc_holder_history.py --limit 50",
            "python weekly_holder_risers.py --limit 50",
            "python generate_site.py --holder-only",
        )
        for command in required_commands:
            with self.subTest(command=command):
                self.assertIn(command, self.workflow)

        self.assertIn("group: daily-stock-site-update", self.workflow)


if __name__ == "__main__":
    unittest.main()
