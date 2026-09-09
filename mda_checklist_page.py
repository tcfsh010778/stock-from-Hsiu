"""Individual-stock checklist presentation; no rules are evaluated in JS."""
BODY = '''<style>#mda-table{overflow-x:auto}#mda-table pre{white-space:pre-wrap;overflow-wrap:anywhere;max-width:44rem}#mda-table td{vertical-align:top}</style><div class="container review-home"><div class="page-title" id="mda-title">M 大個股檢核表</div>
<p>每週大戶增加排行是研究入口；A／X、長期籌碼與賣壓各自核對。工程觀察不等於買進指令。</p>
<div id="mda-status" role="status">正在讀取…</div><div id="mda-table"></div>
<p><a id="mda-chart" href="stocks.html">查看 K 線與完整指標 →</a> · <a href="review-pool.html">回觀察池</a></p></div>
<script>
(()=>{'use strict';
const sid=new URLSearchParams(location.search).get('id'), status=document.getElementById('mda-status');
const labels={pass:'符合',candidate:'符合工程觀察',fail:'未符合',unknown:'資料／判讀不足',manual:'人工確認',conflict:'走勢分歧'};
const n=(tag,text)=>{const el=document.createElement(tag);if(text!==undefined)el.textContent=text;return el};
if(!/^\\d{4}$/.test(sid||'')){status.textContent='請從觀察池選擇股票。';return;}
fetch('data/mda_checklist_candidates.json').then(r=>{if(!r.ok)throw Error();return r.json();}).then(p=>{
 if(p.selection_source!=='mda_weekly_top50')throw Error();
 const row=(p.stocks||[]).find(r=>r.stock_id===sid);
 if(!row){status.textContent='此股不在本週已驗證的大戶增加檢核名單。';return;}
 document.getElementById('mda-title').textContent=sid+' '+row.name+' · M 大檢核表';
 document.getElementById('mda-chart').href='v2/stock.html?id='+encodeURIComponent(sid);
 status.textContent='股權週別 '+p.pool_date+' · 價格基準 '+p.data_date+(row.weekly_pool_member?' · 本週排名 '+row.pool_rank:' · 長期觀察池（納入週別 '+(row.admitted_week||'既有紀錄')+'）');
 const host=document.getElementById('mda-table'), analysis=row.checklist;
 const coverage=p.pool_coverage||{}, excluded=[...(coverage.excluded_official_suspensions||[]),...(coverage.previous_coverage?.excluded_official_suspensions||[])];
 if(excluded.length){const codes=[...new Set(excluded.map(e=>e.security_id))];host.append(n('p','本週股權涵蓋 '+coverage.observed_count+'／'+coverage.expected_count+' 檔；'+codes.join('、')+' 因官方減資／面額變更停牌期間缺少比較資料，未納入兩期可比較排行。'));}
 if(!analysis){host.append(n('p',(row.missing||[]).join('；')||'資料不足，尚無檢核結果。'));return;}
 for(const [section,ids] of Object.entries(analysis.table_sections||{})){
  host.append(n('h2',section));const table=n('table');table.className='review-rule-table';
  const header=n('tr');for(const text of ['檢核項目','結果','可核對證據'])header.append(n('th',text));table.append(header);
  for(const id of ids){const criterion=analysis.criteria_registry.find(r=>r.id===id);if(!criterion)continue;
   const tr=n('tr');tr.append(n('td',criterion.label),n('td',labels[criterion.status]||criterion.status));const td=n('td');
   for(const ref of criterion.metric_refs||[]){const c=analysis.checks[ref];if(!c)continue;const detail=n('details');detail.append(n('summary',c.summary));detail.append(n('pre',JSON.stringify(c.metrics,null,2)));td.append(detail);}
   if((criterion.missing_components||[]).length)td.append(n('span','此項仍需補足資料或人工圖形確認。'));
   tr.append(td);table.append(tr);
  }host.append(table);
 }
 if(analysis.short_term_review){host.append(n('h2','發動後的短線觀察'));host.append(n('p',analysis.short_term_review.note));
  for(const e of analysis.short_term_review.evidence||[]){const d=n('details');d.append(n('summary',e.summary||e.id),n('pre',JSON.stringify(e.metrics||{},null,2)));host.append(d);}}
 const notes=n('details');notes.append(n('summary','資料缺漏與不同證據'));
 for(const text of [...(analysis.missing||[]),...(analysis.conflicts||[])])notes.append(n('p',text));host.append(notes);
}).catch(()=>{status.textContent='檢核資料載入失敗，沒有產生判讀。';});
})();</script>'''
