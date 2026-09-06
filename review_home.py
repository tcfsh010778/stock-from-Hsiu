"""Single human-review queue presentation; strategy evidence is not rescored."""
from pathlib import Path

BODY = '''
<div class="container review-home" data-review-home>
  <div class="page-title">今天有哪些變化，值得打開圖？</div>
  <p class="page-sub">Stock from Zero 技術／動能突破 · M 大籌碼／布局觀察</p>
  <div id="review-health" class="review-notice" role="status">正在讀取判讀資料…</div>
  <div class="review-routes">
    <section><span class="review-kicker">STOCK FROM ZERO</span><h2>技術／動能突破</h2><p>獨立價格股票池：趨勢、整理區間、突破與回測。工程化觀察規則，不代表教材完整公式或 AI 結論。</p><small id="sfz-coverage"></small></section>
    <section><span class="review-kicker">M 大 ABC</span><h2>籌碼／布局觀察</h2><p>保留 A 長期結構、B1 長期籌碼與 B2 賣壓；區分未發動與已發動後的等待階段。</p><small id="mda-coverage"></small></section>
  </div>
  <div class="review-toolbar">
    <div class="review-buttons" aria-label="閱讀範圍"><button data-review-mode="alerts" aria-pressed="true">今日變化</button><button data-review-mode="pool" aria-pressed="false">觀察池</button></div>
    <label>路徑 <select id="review-route"><option value="all">全部路徑</option><option value="sfz">Stock from Zero</option><option value="mda">M 大籌碼</option></select></label>
    <label>股票 <input id="review-search" type="search" placeholder="代號或名稱"></label>
  </div>
  <p id="review-count" class="review-muted" aria-live="polite"></p>
  <div id="review-empty" class="review-notice" hidden></div>
  <section id="review-list" aria-label="人工複判清單"></section>
  <div class="review-pagination"><button id="review-prev">上一頁</button><span id="review-page"></span><button id="review-next">下一頁</button></div>
  <details class="review-method"><summary>這份清單如何產生？</summary><p>程式比較兩次有效來源，提示首次符合、突破、回測、明確失效或已驗證的長期籌碼條件變化。首次建立基準不當成今日新增；資料缺漏不當成失效。</p><p>同股票只出現一張卡，各路徑理由分開；不計算混合總分，也不改寫既有策略決策。這是結構化規則判讀，尚未加入 AI 選區、AI 認可 POC 或機率評分。</p><p>每日日期採台北 16 時後最近平日估計；交易所假日若尚未確認，會保守停止提醒。日價格與週籌碼分別驗證。</p><a href="review-rules.html">查看規則、來源與限制 →</a></details>
</div>
'''

CSS = '''
.review-home h2{font-size:19px;margin:8px 0}.review-notice{border-left:3px solid #d8b263;background:#242627;padding:15px 18px;color:#e4ce9b;margin:18px 0;line-height:1.65}.review-routes{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin:24px 0}.review-routes section,.review-stock{border:1px solid #303d50;background:#18212d;border-radius:10px;padding:20px}.review-routes p,.review-muted{color:#a8b6c8;line-height:1.65}.review-kicker{font-size:12px;letter-spacing:2px;color:#84b7ec}.review-routes section:nth-child(2) .review-kicker{color:#82c7b5}.review-routes small{color:#b4c0cd}.review-toolbar{display:flex;gap:16px;align-items:center;flex-wrap:wrap}.review-toolbar label{display:flex;gap:8px;align-items:center}.review-toolbar input,.review-toolbar select{background:#111923;color:#e1e9f2;padding:9px;border:1px solid #3b4c61;border-radius:6px;max-width:220px}.review-buttons{display:flex;gap:8px}.review-home button{background:#172433;color:#ccdced;border:1px solid #4b6076;padding:9px 14px;border-radius:7px;cursor:pointer}.review-home button[aria-pressed=true]{background:#c5d8eb;color:#152333}.review-home button:disabled{opacity:.4;cursor:default}.review-stock{margin:14px 0}.review-stock h3{font-size:19px;margin:0}.review-stock-head{display:flex;justify-content:space-between;align-items:center;gap:12px}.review-badges{display:flex;gap:6px;flex-wrap:wrap}.review-badge{font-size:12px;border:1px solid #476078;border-radius:14px;padding:4px 9px}.review-route-evidence{border-top:1px solid #304054;margin-top:14px;padding-top:12px}.review-route-evidence h4{margin:0 0 8px;color:#a4c9ed}.review-route-evidence p{line-height:1.65;margin:8px 0}.review-stock details{color:#adbacb;margin:12px 0}.review-stock summary,.review-method summary{cursor:pointer}.review-conflict{color:#e1bb7f}.review-pagination{display:flex;justify-content:center;gap:18px;align-items:center;margin:24px}.review-method{margin:28px 0;border-top:1px solid #334254;padding-top:18px;color:#b5c3d2;line-height:1.7}.review-home [hidden]{display:none!important}.review-route-date{font-size:12px;color:#97a8bc}.review-rule-table{width:100%;border-collapse:collapse}.review-rule-table td,.review-rule-table th{padding:12px;border-bottom:1px solid #394553;text-align:left;vertical-align:top}@media(max-width:680px){.review-routes{grid-template-columns:1fr}.review-stock-head{align-items:flex-start}.review-toolbar{align-items:flex-start}.review-toolbar input{max-width:160px}.review-stock{padding:15px}}
'''

JS = r'''
(() => {
 'use strict';
 const $=id=>document.getElementById(id), labels={sfz:'Stock from Zero',mda:'M 大籌碼'};
 const stageLabels={no_setup:'尚未形成結構',box_forming:'整理觀察',breakout_wait_retest:'突破後等待回測',retest_confirmed:'回測結構確認',invalidated:'原結構失效'};
 const eventLabels={stage_invalidated:'原結構失效',sfz_breakout:'突破觀察',sfz_retest:'突破後回測',first_qualified:'首次符合',mda_chip_changed:'長期籌碼條件變化'};
 let payload,mode=location.pathname.endsWith('/review-pool.html')||new URLSearchParams(location.search).get('view')==='pool'?'pool':'alerts',page=0;
 const size=12;
 function node(tag,text,cls){const n=document.createElement(tag);if(text!==undefined)n.textContent=text;if(cls)n.className=cls;return n;}
 function card(s,events){
  const el=node('article',undefined,'review-stock');el.dataset.stockId=s.stock_id;
  const head=node('div',undefined,'review-stock-head');head.append(node('h3',s.stock_id+' '+(s.name||'')));
  const badges=node('div',undefined,'review-badges');for(const id of s.route_ids||[])badges.append(node('span',labels[id]||id,'review-badge'));head.append(badges);el.append(head);
  if(events.length)el.append(node('p',events.map(e=>(labels[e.route_id]||e.route_id)+'：'+(eventLabels[e.event_type]||e.event_type)).join('；')));
  for(const r of s.routes||[]){
   const section=node('div',undefined,'review-route-evidence');section.append(node('h4',(labels[r.route_id]||r.route_id)+' · '+(stageLabels[r.stage]||r.stage||'觀察中')));
   section.append(node('div','來源日期 '+(r.data_date||'未提供')+' · '+(r.quality?.alert_eligible?'資料條件通過':'資料待補／歷史觀察'),'review-route-date'));
   const reasons=(r.reasons||[]).length?r.reasons:(r.evidence||[]).map(e=>e.summary).filter(Boolean);
   section.append(node('p',reasons.slice(0,2).join('；')||'尚無足夠判讀證據'));
   if(r.next_observation)section.append(node('p','下一個觀察條件：'+r.next_observation));
   const detail=node('details');detail.append(node('summary','查看完整證據與缺漏'));
   for(const evidence of r.evidence||[])detail.append(node('p',(evidence.summary||evidence.id)+' '+JSON.stringify(evidence.metrics||{})));
   for(const text of [...(r.conflicts||[]),...(r.missing||[])])detail.append(node('p',text==='current_source_unavailable'?'本期資料不可用，保留前次觀察。':String(text),'review-conflict'));
   section.append(detail);el.append(section);
  }
  for(const conflict of s.conflicts||[])el.append(node('p','矛盾證據：'+conflict,'review-conflict'));
  if(s.detail_href){const a=node('a','打開個股圖與完整指標 →');a.href=s.detail_href;el.append(a);}else el.append(node('p','個股圖尚未生成；先核對上方來源與證據。','review-muted'));
  return el;
 }
 function render(){
  if(!payload)return;const eventMap=new Map();for(const e of payload.alerts||[]){if(!eventMap.has(e.stock_id))eventMap.set(e.stock_id,[]);eventMap.get(e.stock_id).push(e);}
  const route=$('review-route').value,q=$('review-search').value.trim().toLowerCase();
  const observable=r=>r.candidate||['box_forming','breakout_wait_retest','retest_confirmed'].includes(r.stage);
  let stocks=(payload.stocks||[]).filter(s=>mode==='alerts'?(eventMap.get(s.stock_id)||[]).some(e=>route==='all'||e.route_id===route):(s.routes||[]).some(r=>observable(r)&&(route==='all'||r.route_id===route)));
  stocks=stocks.filter(s=>!q||(s.stock_id+' '+(s.name||'')).toLowerCase().includes(q));
  if(mode==='alerts')stocks.sort((a,b)=>Math.min(...eventMap.get(a.stock_id).map(e=>e.priority))-Math.min(...eventMap.get(b.stock_id).map(e=>e.priority))||a.stock_id.localeCompare(b.stock_id));
  page=Math.max(0,Math.min(page,Math.max(0,Math.ceil(stocks.length/size)-1)));
  $('review-list').replaceChildren(...stocks.slice(page*size,(page+1)*size).map(s=>card(s,(eventMap.get(s.stock_id)||[]).filter(e=>route==='all'||e.route_id===route))));
  $('review-count').textContent=(mode==='alerts'?'今日有變化':'觀察池')+'：'+stocks.length+' 檔。同股票合併顯示，兩路徑各自保留理由。';
  $('review-empty').hidden=stocks.length>0;
  $('review-empty').textContent=q?'沒有符合搜尋的股票。':mode==='alerts'?(payload.status==='blocked'?'資料尚未通過驗證，暫不產生今日提醒。可切換觀察池查看歷史名單。':'本期沒有新的有效變化；首次建立基準也不會當成新訊號。'):'目前沒有符合此路徑的可觀察名單；不以其他路徑的名單補足。';
  $('review-page').textContent=stocks.length?`${page+1} / ${Math.ceil(stocks.length/size)}`:'0 / 0';$('review-prev').disabled=page===0;$('review-next').disabled=(page+1)*size>=stocks.length;
  document.querySelectorAll('[data-review-mode]').forEach(b=>b.setAttribute('aria-pressed',String(b.dataset.reviewMode===mode)));
 }
 document.querySelectorAll('[data-review-mode]').forEach(b=>b.addEventListener('click',()=>{mode=b.dataset.reviewMode;page=0;render();}));
 $('review-route').addEventListener('change',()=>{page=0;render();});$('review-search').addEventListener('input',()=>{page=0;render();});
 $('review-prev').addEventListener('click',()=>{page--;render();});$('review-next').addEventListener('click',()=>{page++;render();});
 fetch('data/review_queue.json').then(r=>{if(!r.ok)throw Error('queue unavailable');return r.json();}).then(p=>{
  if(p.dataset_id!=='review_queue'||!Array.isArray(p.stocks)||!Array.isArray(p.alerts))throw Error('invalid queue');payload=p;
  const sources=p.source_summary||{},a=sources.sfz||{},b=sources.mda||{};
  $('review-health').textContent='檢查基準日 '+p.as_of+'｜SFZ 資料 '+(a.data_date||'待補')+'｜M 大名單 '+(b.data_date||'待補')+'。'+(p.status==='blocked'?'目前資料驗證未通過，今日提醒暫停；下方觀察池保留歷史證據。':'提醒只根據可驗證的新變化；未通過的個股保留缺漏說明。');
  $('sfz-coverage').textContent=`${a.status==='fresh'?'本期可判讀':'資料待補，提醒暫停'} · 獨立股票池 ${a.universe_count||0} 檔 · 已驗證可計算 ${a.evaluated_count||0} 檔 · 待補 ${a.excluded_count||0} 檔`;
  $('mda-coverage').textContent=`${b.status==='fresh'?'本期可判讀':'資料待補，提醒暫停'} · 既有候選 ${b.candidate_count||0} 檔 · 本期證據通過 ${b.eligible_count||0} 檔`;
  render();
 }).catch(()=>{$('review-health').textContent='判讀資料載入失敗，沒有產生任何今日提醒。請稍後重試。';});
})();
'''


def write_assets(docs: Path):
    (docs / 'css').mkdir(parents=True, exist_ok=True)
    (docs / 'js').mkdir(parents=True, exist_ok=True)
    (docs / 'css/review-home.css').write_text(CSS, encoding='utf-8')
    (docs / 'js/review-home.js').write_text(JS, encoding='utf-8')


def body():
    return '<link rel="stylesheet" href="css/review-home.css">' + BODY + '<script src="js/review-home.js" defer></script>'


RULES = '''<div class="container"><div class="page-title">兩條路徑的規則與限制</div><p>規則版本 sfz-technical-review-candidate-v1.0.0。Stock from Zero 技術分析教材中的趨勢、箱型、突破、回測概念被整理為可重跑的工程候選規則；數值門檻不是教材原文公式，也不是 AI 勝率或買進指令。</p>
<table class="review-rule-table"><tr><th>SFZ 技術觀察</th><th>目前實作</th></tr><tr><td>趨勢背景</td><td>收盤 > SMA20 > SMA60，SMA20 較五根前上升；至少完整 60 根。</td></tr><tr><td>箱型</td><td>突破前 20 根、寬度不超過 12%；上下緣各至少兩次接觸，接觸容差 2%。</td></tr><tr><td>突破</td><td>收盤高於原箱頂 1%，成交量至少為箱型均量 1.2 倍；只使用當時已知資料。</td></tr><tr><td>回測</td><td>靠近原箱頂 +2%／-1% 範圍後，重新收在箱頂以上；最新收盤不低於前一根。</td></tr><tr><td>失效</td><td>突破後曾收盤低於原箱頂 2%。不是資料缺漏或搜尋視窗結束就當作失效。</td></tr></table>
<p>M 大使用既有候選池及原本長期條件，不要求先通過 SFZ。四／八週股權結構與 A／B1／B2 各自保留，熱族群不是新加的必要條件。只在日期、還原價及長期籌碼驗證通過後產生新提醒。</p>
<p>目前不涵蓋 SFZ 所有教材章節、週／日層級切換、所有旗形與三角形、AI 選區／POC。公開歷史資料仍在修復，缺少已驗證還原價的股票會排除計算，而非假裝沒有訊號。</p>
<p>首頁保留每日差異與完整觀察池；首次建立資料基準、規則版本改變、來源曾失效後恢復時先建立基準。來源日期與判讀日期分開，日價格與週籌碼不混成同一天。</p><a href="index.html">回人工複判首頁 →</a></div>'''
