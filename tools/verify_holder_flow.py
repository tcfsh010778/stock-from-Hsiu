"""Explain every holder/flow publication gate before a workflow can commit."""
from __future__ import annotations
import argparse, json
from pathlib import Path

def verify(flow: dict, holder: dict) -> dict:
    supplemental=flow.get("supplemental_data") or {}
    retail=supplemental.get("retail_200") or {}
    facts={"flow_schema":flow.get("schema_version"),"flow_date":flow.get("date"),
           "flow_quality":(flow.get("data_quality") or {}).get("state"),
           "institutional_sessions":(flow.get("institutional_history") or {}).get("session_count"),
           "retail_coverage":retail.get("coverage_count"),"retail_date":retail.get("date"),
           "holder_date":holder.get("date"),"holder_metric_count":supplemental.get("holder_metric_count")}
    checks={"flow_schema":facts["flow_schema"]=="1.3.0",
            "flow_quality":facts["flow_quality"]=="ok",
            "institutional_sessions":facts["institutional_sessions"]==20,
            "retail_coverage":int(facts["retail_coverage"] or 0)>0,
            "retail_date":bool(facts["holder_date"]) and facts["retail_date"]==facts["holder_date"],
            "holder_metric_count":int(facts["holder_metric_count"] or 0)>0}
    failed=[name for name,passed in checks.items() if not passed]
    if failed:raise ValueError("holder/flow publication blocked: "+json.dumps({"failed":failed,"actual":facts},ensure_ascii=False))
    return facts

def main():
    parser=argparse.ArgumentParser();parser.add_argument("--data-dir",type=Path,default=Path("data"));args=parser.parse_args()
    load=lambda name:json.loads((args.data_dir/name).read_text(encoding="utf-8"))
    print(json.dumps(verify(load("daily_market_flow.json"),load("weekly_holder_risers.json")),ensure_ascii=False))
if __name__=="__main__":main()
