"""Refresh anonymous official-only data; never accepts a provider key."""
import argparse
from datetime import datetime
from pathlib import Path

import market_flow
from build_review_data import verified_frame
from workspace_public.build import read, write
from workspace_public.official_data import collect_institutional, collect_revenue

ROOT=Path(__file__).resolve().parents[1]


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--cache',required=True,type=Path)
    args=parser.parse_args();data=ROOT/'data'
    as_of=read(data/'official_adjusted_update_manifest.json')['data_as_of']
    sessions=list(verified_frame(data,'2330',as_of)['date'])[-10:]
    revenue=collect_revenue(datetime.now().strftime('%Y-%m-%d'),args.cache,months=13)
    institutional=collect_institutional(sessions,args.cache,market_flow)
    if len(revenue['sources'])<20 or institutional['failures']:
        # Keep usable partitions and explicit gaps; a stale source never passes a current filter.
        print('Some official partitions remain unavailable; status will be visible.')
    write(data/'workspace/revenue.json',revenue)
    write(data/'workspace/institutional.json',institutional)


if __name__=='__main__':main()
