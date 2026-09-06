"""Standalone entry to the same Volume Profile UI used by the V2 workbench."""
from .site import STOCK_PAGE_HTML


def profile_page_html() -> str:
    start = STOCK_PAGE_HTML.index('    <section id="volume-profile"')
    end = STOCK_PAGE_HTML.index('    <section id="chips"', start)
    pane = STOCK_PAGE_HTML[start:end].replace('class="pane card"', 'class="pane card active"', 1)
    return '''<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Volume Profile｜Stock from Hsiu</title>
<link rel="stylesheet" href="assets/v2.css"></head><body><main class="shell">
<nav class="top"><a href="../index.html">網站首頁</a><a id="v2-link" href="stock.html?id=2330">個股工作台</a></nav>
<h1>Volume Profile · 成交量分布</h1>
<p class="notice">近期分鐘資料工具。原始成交價可直接使用；還原權息圖層需取得已驗證的日 K 資料。</p>
<label>股票 <select id="vp-stock"><option value="2330">2330 台積電</option><option value="6488">6488 環球晶</option><option value="2317">2317 鴻海</option><option value="2353">2353 宏碁</option></select></label>
''' + pane + '''</main><script src="assets/volume_profile.js"></script><script src="assets/volume_profile_ui.js"></script>
<script>
const selected=new URLSearchParams(location.search).get('id')||'2330';
const id=['2330','6488','2317','2353'].includes(selected)?selected:'2330';
document.querySelector('#vp-stock').value=id;
document.querySelector('#v2-link').href='stock.html?id='+id;
document.querySelector('#vp-stock').onchange=e=>location.search='?id='+e.target.value;
// Raw profile remains usable independently of unavailable long-history factors.
fetch('data/'+id+'.json').then(r=>r.ok?r.json():[]).then(packets=>{
 const daily=packets.find(p=>p.timeframe==='daily');
 const packet=daily||{stock_id:id,series:[],timeframe:'daily',price_adjustment:{mode:'none',verified:false}};
 window.dispatchEvent(new CustomEvent('stock-packets-ready',{detail:[packet]}));
}).catch(()=>window.dispatchEvent(new CustomEvent('stock-packets-ready',{detail:[{stock_id:id,series:[],timeframe:'daily',price_adjustment:{mode:'none',verified:false}}]})));
</script></body></html>'''
