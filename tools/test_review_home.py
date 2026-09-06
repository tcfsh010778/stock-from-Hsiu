import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import generate_site
from review_home import write_assets


class HomeTests(unittest.TestCase):
    def test_one_queue_and_collapsed_background_survives_flow_refresh(self):
        with patch.object(generate_site,'build_daily_market_flow_panel',return_value='FLOW'), \
             patch.object(generate_site,'build_market_sentiment_panel',return_value='SENTIMENT'), \
             patch.object(generate_site,'build_sector_heat_widget',return_value='SECTORS'):
            html = generate_site.build_index_page([{'date':'2026-09-03','stocks':[]}])
        self.assertEqual(html.count('id="review-list"'),1)
        self.assertNotIn('daily-decision-card',html)
        start = html.index('<summary>市場背景與其他分析</summary>')
        self.assertLess(start,html.index('FLOW'))
        new = generate_site.replace_home_market_flow_panel(html,'NEW FLOW')
        self.assertIn('NEW FLOW',new)
        self.assertEqual(new.count('id="review-list"'),1)

    def test_browser_assets_syntax(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); write_assets(root)
            subprocess.run(['node','--check',str(root/'js/review-home.js')],check=True,capture_output=True)


if __name__ == '__main__':
    unittest.main()
