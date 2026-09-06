import unittest
from unittest.mock import Mock, patch
from official_price_refresh import _get_json, OfficialSourceBlocked

class SourceGateTests(unittest.TestCase):
    def test_gate_stops_without_retry(self):
        for status in (401,403,428,429):
            with self.subTest(status=status), patch('official_price_refresh.requests.get', return_value=Mock(status_code=status)) as get, patch('official_price_refresh.time.sleep') as sleep:
                with self.assertRaises(OfficialSourceBlocked):
                    _get_json('https://example.invalid/official', attempts=3)
                self.assertEqual(get.call_count,1)
                sleep.assert_not_called()
