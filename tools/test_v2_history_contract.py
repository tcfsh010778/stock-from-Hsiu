from __future__ import annotations

import json
import math
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from generate_v2 import (
    _official_sidecar,
    analyze_stock_task,
    build_v2,
    load_market_evidence,
    load_price_basis,
    safe_decision,
)
from tools.refresh_v2_chip_panels import refresh as refresh_chip_panels


def fixture(root: Path, stock_id: str, count: int) -> pd.DataFrame:
    dates = pd.bdate_range(end="2026-09-04", periods=count)
    rows = []
    for index, day in enumerate(dates):
        close = 80 + index / 10
        factor = 0.5 if index < count - 20 else 1.0
        raw = dict(open=close, high=close+1, low=close-1, close=close, volume=10000+index)
        rows.append({"date": day.date().isoformat(), **{key: value*factor for key, value in raw.items() if key != "volume"}, "volume": raw["volume"], **{f"raw_{key}": value for key,value in raw.items()}, "adjustment_factor":factor})
    frame = pd.DataFrame(rows)
    (root/"prices").mkdir(exist_ok=True)
    (root/"price_basis").mkdir(exist_ok=True)
    frame.to_csv(root/"prices"/f"{stock_id}.csv", index=False)
    metadata={"stock_id":stock_id,"mode":"official_reference_ratio_back_adjusted_v1","verified":True,"source":"synthetic test fixture","adjustment_as_of":"2026-09-04","event_count":1,"volume_basis":"official_raw_shares"}
    (root/"price_basis"/f"{stock_id}.json").write_text(json.dumps(metadata),encoding="utf-8")
    return frame


class HistoryContractTests(unittest.TestCase):
    def test_239_240_and_full_history_before_trim(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            for count in (1,29,239,240,520):
                sid=str(9000+count)
                frame=fixture(root,sid,count)
                _,_,packets,error=analyze_stock_task((sid,"synthetic",str(root/"prices"/f"{sid}.csv"),str(root),safe_decision(sid,None),"fresh","2026-09-04",[],True))
                self.assertIsNone(error)
                daily=packets[0]
                self.assertEqual(len(daily["series"]),min(count,240))
                if count<240:
                    self.assertTrue(all(row["sma240"] is None for row in daily["series"]))
                    self.assertIn("資料不足",daily["series_coverage"]["message"])
                else:
                    expected=math.fsum(frame["close"].tail(240))/240
                    self.assertAlmostEqual(daily["series"][-1]["sma240"],expected,places=5)
                if count==520:
                    expected=math.fsum(frame["close"].iloc[41:281])/240
                    self.assertAlmostEqual(daily["series"][0]["sma240"],expected,places=5)
                if count<30:
                    self.assertEqual(daily["patterns"],[])
                    self.assertIsNone(daily["candlestick_annotations"])

    def test_mixed_basis_is_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);frame=fixture(root,"9001",240)
            frame.loc[0,"close"]+=1
            with self.assertRaisesRegex(ValueError,"adjusted/raw"):
                load_price_basis(root,"9001",frame,"2026-09-04")

    def test_generator_writes_assets_and_short_history_page(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);data=root/"data";data.mkdir();docs=root/"docs";docs.mkdir()
            fixture(data,"9001",29)
            (data/"price_refresh_summary.json").write_text(json.dumps({"status":"fresh","latest_data_date":"2026-09-04"}),encoding="utf-8")
            result=build_v2(docs_dir=docs,data_dir=data,only={"9001"},validate=True,workers=1)
            self.assertEqual(result["stock_count"],1)
            self.assertEqual(result["failure_count"],0)
            self.assertTrue((docs/"v2/assets/volume_profile.js").exists())
            self.assertTrue((docs/"v2/volume-profile.html").exists())

    def test_real_producer_sidecar_shape_and_as_of_filter(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            chip={
                "dataset_id":"official_chip_history","schema_version":"1.0.0","date":"2026-09-05",
                "unit":"shares","rows":[
                    {"date":"2026-09-04","security_id":"2330","foreign_net_shares":1200,"investment_trust_net_shares":-300,"dealer_net_shares":50},
                    {"date":"2026-09-05","security_id":"2330","foreign_net_shares":900,"investment_trust_net_shares":200,"dealer_net_shares":-10},
                ],
            }
            margin={
                "dataset_id":"official_margin_snapshot","schema_version":"1.0.0","date":"2026-09-05",
                "unit":"lots","rows":[
                    {"date":"2026-09-05","security_id":"2330","margin_balance_previous_lots":100,"margin_balance_lots":120,"short_balance_previous_lots":5,"short_balance_lots":4},
                ],
            }
            (root/"official_chip_history.json").write_text(json.dumps(chip),encoding="utf-8")
            (root/"official_margin_snapshot.json").write_text(json.dumps(margin),encoding="utf-8")
            _official_sidecar.cache_clear()
            result=load_market_evidence(root,"2330",as_of="2026-09-04")
            self.assertEqual(result["institutional"],[{"date":"2026-09-04","foreign":1.2,"trust":-0.3,"dealer":0.05,"total":0.95}])
            self.assertEqual(result["margin"],[])
            self.assertIn("margin",result["gaps"])
            current=load_market_evidence(root,"2330",as_of="2026-09-05")
            self.assertEqual(current["margin"],[{"date":"2026-09-05","margin_balance":120.0,"short_balance":4.0}])

    def test_sidecar_rejects_row_newer_than_declared_dataset_date(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            payload={"dataset_id":"official_chip_history","schema_version":"1.0.0","date":"2026-09-04","unit":"shares","rows":[{"date":"2026-09-05","security_id":"2330","foreign_net_shares":1}]}
            (root/"official_chip_history.json").write_text(json.dumps(payload),encoding="utf-8")
            _official_sidecar.cache_clear()
            with self.assertRaisesRegex(ValueError,"newer than dataset date"):
                load_market_evidence(root,"2330",as_of="2026-09-04")

    def test_chip_refresh_is_per_packet_and_does_not_share_future_evidence(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);data=root/"data";packet_dir=root/"docs/v2/data"
            data.mkdir();packet_dir.mkdir(parents=True)
            chip={"dataset_id":"official_chip_history","schema_version":"1.0.0","date":"2026-09-05","unit":"shares","rows":[
                {"date":"2026-09-04","security_id":"2330","foreign_net_shares":1000,"investment_trust_net_shares":0,"dealer_net_shares":0},
                {"date":"2026-09-05","security_id":"2330","foreign_net_shares":2000,"investment_trust_net_shares":0,"dealer_net_shares":0},
            ]}
            (data/"official_chip_history.json").write_text(json.dumps(chip),encoding="utf-8")
            packets=[
                {"timeframe":"daily","data_date":"2026-09-04","market_evidence":{"institutional":[],"margin":[]}},
                {"timeframe":"daily","data_date":"2026-09-05","market_evidence":{"institutional":[],"margin":[]}},
                {"timeframe":"weekly","data_date":"2026-09-05","market_evidence":{"institutional":[{"date":"2099-01-01"}]}},
            ]
            (packet_dir/"2330.json").write_text(json.dumps(packets),encoding="utf-8")
            _official_sidecar.cache_clear()
            self.assertEqual(refresh_chip_panels(root),1)
            updated=json.loads((packet_dir/"2330.json").read_text(encoding="utf-8"))
            self.assertEqual([row["date"] for row in updated[0]["market_evidence"]["institutional"]],["2026-09-04"])
            self.assertEqual([row["date"] for row in updated[1]["market_evidence"]["institutional"]],["2026-09-04","2026-09-05"])
            self.assertNotIn("market_evidence",updated[2])


if __name__=="__main__":
    unittest.main()
