"""Refresh V2 chip evidence from verified sidecars without relabeling price dates."""
from __future__ import annotations

import json
import sys
from copy import deepcopy
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from generate_v2 import load_market_evidence


def refresh(root: Path=ROOT) -> int:
    updated=0
    for path in sorted((root/"docs/v2/data").glob("*.json")):
        if path.name=="index.json":
            continue
        packets=json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(packets,list):
            continue
        for packet in packets:
            if packet.get("timeframe") != "daily":
                packet.pop("market_evidence", None)
                continue
            as_of=str(packet.get("data_date") or "")
            evidence=deepcopy(load_market_evidence(root/"data",path.stem,as_of=as_of))
            old=packet.get("market_evidence") or {}
            # Preserve already-published dates in the margin/flow history, then
            # overlay newly verified official rows. Never rewrite OHLCV as-of.
            for field in ("institutional","margin"):
                merged={row["date"]:row for row in old.get(field,[]) if str(row.get("date") or "") <= as_of}
                merged.update({row["date"]:row for row in evidence.get(field,[])})
                evidence[field]=[merged[day] for day in sorted(merged)][-260:]
            evidence["source_dates"] = {
                key: rows[-1]["date"]
                for key, rows in (
                    ("institutional", evidence.get("institutional", [])),
                    ("foreign_ownership", evidence.get("foreign_ownership", [])),
                    ("margin", evidence.get("margin", [])),
                    ("holdings", evidence.get("holdings", [])),
                )
                if rows
            }
            evidence["gaps"] = [
                key for key in ("institutional", "foreign_ownership", "margin", "holdings")
                if key not in evidence["source_dates"]
            ]
            packet["market_evidence"]=evidence
        temporary=path.with_suffix('.tmp')
        temporary.write_text(json.dumps(packets,ensure_ascii=False,separators=(',',':'))+'\n',encoding='utf-8')
        temporary.replace(path)
        updated+=1
    return updated


if __name__=="__main__":
    print(f"V2 chip panels updated: {refresh()}; price dates preserved")
