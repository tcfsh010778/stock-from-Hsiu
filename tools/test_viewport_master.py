from __future__ import annotations

import unittest

from stock_v2_public.site import STOCK_PAGE_HTML, V2_CSS, V2_JS


class ViewportMasterTests(unittest.TestCase):
    def test_price_chart_is_the_only_master_viewport(self):
        self.assertNotIn("subscribeVisibleTimeRangeChange", V2_JS)
        self.assertNotIn("setCrosshairPosition", V2_JS)
        self.assertIn("if(index>0)entry.chart.timeScale().fitContent()", V2_JS)
        self.assertIn('panel("日 K 與成交量","主視窗', V2_JS)

    def test_latest_edge_stays_pinned_during_wheel_zoom(self):
        self.assertIn("scrollPosition()<=1", V2_JS)
        self.assertIn("scrollToPosition(0,false)", V2_JS)
        self.assertIn("scrollToRealTime()", V2_JS)
        self.assertIn("latest-btn", V2_CSS)

    def test_holdings_are_not_described_as_a_viewport_controller(self):
        self.assertIn("日 K 是主視窗；籌碼副圖各自縮放", STOCK_PAGE_HTML)
        self.assertNotIn("其他圖表會同步", STOCK_PAGE_HTML)


if __name__ == "__main__":
    unittest.main()
