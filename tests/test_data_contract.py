import tempfile
import unittest

from data_contract import SCHEMA_VERSION, build_homepage_index


class HomepageIndexTests(unittest.TestCase):
    def test_keeps_search_and_daily_change_data(self):
        source = {"1234": {"name": "測試股", "etf_holdings": {"00981A": {
            "current_count": 12.5,
            "history": [{"date": "2026/08/21", "count": 12.5, "weight": 1.2,
                         "count_change": 2.5, "weight_change": 0.2, "status": "加碼"}]
        }}}}
        index = build_homepage_index(source)
        self.assertEqual(index["schema_version"], SCHEMA_VERSION)
        self.assertEqual(index["latest_data_date"], "20260821")
        self.assertEqual(index["stock_search"][0]["holdings"]["00981A"]["current_count"], 12.5)
        self.assertEqual(index["changes_by_date"]["20260821"]["00981A"][0]["code"], "1234")


if __name__ == "__main__":
    unittest.main()
