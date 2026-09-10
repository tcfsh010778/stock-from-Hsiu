"""Workspace publication entry point; generate_site.py remains the durable front door."""
import json
import re
from pathlib import Path

from build_review_data import expected_session, verified_frame
from stock_v2_public.analysis.sfz_universe import analyze_sfz_universe
from workspace_public.build import build

ROOT=Path(__file__).resolve().parent
PRIVATE_SOURCE_SHA='40f048a4d21ccde73071cecae76e1ab88743484c'


def retire_routes(output):
    paths=list(output.rglob('*.html'))
    retired_json=[p for p in (output/'data').glob('*.json') if p.name!='index.json']
    retired_json+=list((output/'v2/data').rglob('*.json'))
    root=output.resolve()
    # All targets are generated site artifacts; preserve source data and new packets.
    for path in paths+retired_json:
        if not path.resolve().is_relative_to(root):
            raise ValueError('Retired artifact resolves outside the output directory')
    for path in paths:
        if path==output/'index.html':continue
        relative=path.relative_to(output)
        target='../'*(len(relative.parts)-1) or './'
        if re.fullmatch(r'\d{4}',path.stem):target+='?stock='+path.stem
        redirect=f'<!doctype html><html lang="zh-Hant"><meta charset="utf-8"><meta http-equiv="refresh" content="0;url={target}"><title>選股工作台</title><a href="{target}">開啟新版選股工作台</a></html>'
        path.write_text(redirect,encoding='utf-8',newline='\n')
    for path in retired_json:
        path.unlink()


def generate(output=None):
    output=Path(output) if output else ROOT/'docs'
    result=build(ROOT,output,verified_frame,analyze_sfz_universe,expected_session)
    # Old bookmarks must not bring retired prescriptions back into the active UI.
    retire_routes(output)
    print(json.dumps({'workspace':result['coverage'],'data_date':result['data_date'],'fresh':result['fresh']},ensure_ascii=False))
    return result


if __name__=='__main__':generate()
