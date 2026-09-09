
(() => {
 'use strict';
 const $=id=>document.getElementById(id), labels={sfz:'Stock from Zero',mda:'M 大籌碼'};
 const stageLabels={universe_candidate:'通過教材初篩，待圖形複判',filtered:'未通過本期初篩',insufficient_data:'完整歷史不足',no_setup:'尚未形成結構',box_forming:'整理觀察',breakout_wait_retest:'突破後等待回測',retest_confirmed:'回測結構確認',invalidated:'原結構失效',weekly_review:'本週逐項檢核',mda_waiting:'已納觀察，等待賣壓減少',mda_ready_review:'型態與籌碼待人工複判'};
 const checkLabels={pass:'符合',fail:'未符合',candidate:'符合工程觀察',unknown:'資料／判讀不足',manual:'人工確認',conflict:'走勢分歧'};
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
   if(r.route_id==='mda'){
    const compact=node('div',undefined,'review-badges');
    for(const [key,label] of [['long_bull_A','長多 A'],['reversal_X','轉折 X'],['long_term_B1','長期籌碼'],['long_term_comovement','價／籌碼'],['higher_lows_daily','底底高'],['selling_pressure_B2','賣壓']]){
     const state=r.checklist?.checks?.[key]?.status||'unknown';compact.append(node('span',label+'：'+(checkLabels[state]||state),'review-badge'));
    }
    section.append(compact);const a=node('a','打開此股完整檢核表 →');a.href='mda-checklist.html?id='+encodeURIComponent(s.stock_id);section.append(a);
   }else if(r.next_observation)section.append(node('p','下一個觀察條件：'+r.next_observation));
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
  const observable=r=>r.weekly_pool_member||r.watch_pool_member||r.candidate||['box_forming','breakout_wait_retest','retest_confirmed'].includes(r.stage);
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
  $('mda-coverage').textContent=`${b.status==='fresh'?'本期可判讀':'資料待補，提醒暫停'} · 本週檢核 ${b.candidate_count||0} 檔 · 留池追蹤 ${b.retained_count||0} 檔 · 本期證據通過 ${b.eligible_count||0} 檔`;
  const coverage=b.coverage||{},exceptions=[...(coverage.excluded_official_suspensions||[]),...(coverage.previous_coverage?.excluded_official_suspensions||[])];
  if(exceptions.length)$('mda-coverage').textContent+=` · 股權涵蓋 ${coverage.observed_count}／${coverage.expected_count} 檔；${[...new Set(exceptions.map(e=>e.security_id))].join('、')} 因官方停牌缺少比較資料，未列入排行`;
  render();
 }).catch(()=>{$('review-health').textContent='判讀資料載入失敗，沒有產生任何今日提醒。請稍後重試。';});
})();
