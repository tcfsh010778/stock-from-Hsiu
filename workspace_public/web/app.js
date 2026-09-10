'use strict';
const $ = id => document.getElementById(id);
const esc = value => String(value ?? '').replace(/[&<>"']/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const num = (value,d=2) => value == null || !Number.isFinite(Number(value)) ? '—' : Number(value).toLocaleString('zh-TW',{maximumFractionDigits:d,minimumFractionDigits:d});
const color = value => value == null ? '' : value>0?'up':value<0?'down':'';
const pct = value => value == null ? '—' : (value>0?'+':'')+num(value)+'%';
let index, selected, detail, route='selected', frame='day', generation=0, busy=false;
const cache = new Map();
let aiPatterns={};
async function refreshAIPatterns(){try{const result=await getJSON('api/patterns?frequency=day');aiPatterns=result.stocks;$('ai-filter-status').textContent=`${Object.keys(aiPatterns).length} 檔已有目前圖表的 AI 日 K 判讀；不會因篩選自動呼叫 API。`;renderList();}catch{aiPatterns={};$('ai-filter-status').textContent='請先連接並登入私有 AI 服務，再分析個股日 K。';renderList();}}
async function getJSON(url,expectedHash){const response=await fetch(url,{cache:'no-cache'});if(!response.ok)throw Error(`資料讀取失敗 (${response.status})`);const raw=await response.arrayBuffer();if(expectedHash){const actual=[...new Uint8Array(await crypto.subtle.digest('SHA-256',raw))].map(b=>b.toString(16).padStart(2,'0')).join('');if(actual!==expectedHash)throw Error('資料版本更新中，請稍後重新整理。');}return JSON.parse(new TextDecoder().decode(raw));}
function message(text){$('message').textContent=text;$('message').hidden=!text;}
function ruleChecks(checks){return checks.map(c=>{const m=c.metrics||{};const descriptions={weekly_ma_5_21_89:`週均線 MA5 > MA21 > MA89 · ${num(m.ma5)} / ${num(m.ma21)} / ${num(m.ma89)}`,five_day_volume:`近 5 日成交量 > ${num(m.threshold_lots,0)} 張 · 目前 ${num(m.value_lots,0)} 張`,eight_week_average_gain:`近 8 週平均漲幅 > ${num(m.threshold_pct,1)}% · 目前 ${num(m.average_gain_pct,1)}%`,minimum_price:`收盤 > ${num(m.threshold)} 元 · 目前 ${num(m.close)} 元`,daily_ma34_position:`收盤在 MA34 之上 · MA34 ${num(m.ma34)}`,daily_ma34_rising:`日 MA34 上升 · 較前日 ${num(m.ma34_change_one_bar,3)}`};return `<div class="check"><span class="${c.status==='pass'?'good':['unknown','candidate'].includes(c.status)?'warn':''}">${c.status==='pass'?'✓':c.status==='fail'?'−':'?'}</span><div>${esc(descriptions[c.id]||c.summary||c.label||c.id)}<small>${esc(c.status==='pass'?'符合':c.status==='fail'?'未符合':c.status==='candidate'?'工程條件候選':'待補資料／待判讀')}${c.threshold_basis?.startsWith('engineering')?' · 工程定義':''}${c.id==='eight_week_average_gain'?' · 算術平均是舊程式工程詮釋':''}</small></div></div>`;}).join('');}
function renderList(){
 if(!index)return;
 const query=$('search').value.trim().toLowerCase(), market=$('market').value, pattern=$('pattern').value, chips=$('chip-filter').value, revenue=$('revenue-filter').value;
 const stocks=index.stocks.filter(s=>{
  if(query&&!`${s.stock_id} ${s.name}`.toLowerCase().includes(query))return false;
  // Searching always includes the full verified universe, irrespective of the current route tab.
  if(!query&&route==='selected'&&!s.sfz.member&&!s.mda.member)return false;
  if(!query&&route==='sfz'&&!s.sfz.member)return false;
  if(!query&&route==='mda'&&!s.mda.member)return false;
  if(market&&s.market!==market)return false;
  if(industryFilter&&!(s.industry||[]).some(i=>i.id===industryFilter))return false;
  const source=$('mda-source').value;if(source&&!(s.mda.source_labels||[]).includes(source))return false;
  const behavior=$('margin-behavior').value;if(behavior){const m=s.mda.sources.margin_observation.windows[behavior==='inverse'?'5':behavior];if(!m?.complete||!(m.change_lots>0)||(behavior==='inverse'&&!(m.price_change_pct<0)))return false;}
  const useAI=$('pattern-source').value==='ai';
  if(useAI&&!(s.stock_id in aiPatterns))return false;
  if(pattern&&!(useAI?aiPatterns[s.stock_id]:s.patterns).includes(pattern))return false;
  if(chips&&!(s.chips[chips].complete&&s.chips[chips].values.institutional_total_net>0))return false;
  if(revenue){const r=s.revenue;if(!r||r.period!==index.expected_revenue_period)return false;if(['mom','both'].includes(revenue)&&!(r.mom_pct>0))return false;if(['yoy','both'].includes(revenue)&&!(r.yoy_pct>0))return false;}
  return true;
 });
 const source=$('mda-source').value;stocks.sort((a,b)=>source==='每日漲幅'?b.change_pct-a.change_pct:source==='外資連買'?b.mda.sources.foreign_consecutive.sessions-a.mda.sources.foreign_consecutive.sessions:source==='融資增加待辨識'?b.mda.sources.margin_observation.latest_change_lots-a.mda.sources.margin_observation.latest_change_lots:source==='六週股權趨勢'?b.mda.sources.holder_six_weeks.net_change-a.mda.sources.holder_six_weeks.net_change:0);
 $('result-count').textContent=`${stocks.length} 檔`;
 $('empty').hidden=stocks.length>0;
 $('stock-list').innerHTML=stocks.map(s=>`<tr data-stock="${esc(s.stock_id)}" tabindex="0" aria-label="開啟 ${esc(s.stock_id+' '+s.name)}" class="${selected===s.stock_id?'selected':''}"><td><strong>${esc(s.name)}</strong><small>${esc(s.stock_id)} · ${esc(s.market||'—')}</small></td><td>${num(s.close)}</td><td class="${color(s.change_pct)}">${pct(s.change_pct)}</td><td>${s.sfz.member?'<span class="badge good">SFZ 初篩</span>':''}${s.mda.member?'<span class="badge warn">M 大初篩</span>':''}${s.mda.member?'<small class="source-reasons">'+esc((s.mda.source_labels||[]).join(' · '))+'</small>':''}${!s.sfz.member&&!s.mda.member?'<span class="muted">未入選</span>':''}</td></tr>`).join('');
 if(!selected&&stocks.length)openStock(stocks[0].stock_id);
}
async function openStock(sid){
 const request=++generation;
 selected=sid;chartOffset=0;renderList();
 $('stock-title').textContent='讀取中…';
 try{
  const stock=index.stocks.find(s=>s.stock_id===sid);if(!stock)throw Error('這檔股票目前沒有可驗證的資料');
  const data=cache.get(sid)||await getJSON(stock.detail,stock.detail_sha256);if(request!==generation)return;
  cache.set(sid,data);detail=data;
  const url=new URL(location.href);url.searchParams.set('stock',sid);history.replaceState(null,'',url);
  $('stock-title').textContent=`${data.name} ${sid}`;$('stock-market').textContent=`${data.market||'台股'} · ${data.data_date}`;
  $('stock-price').textContent=num(data.close);$('stock-change').textContent=pct(data.change_pct);$('stock-change').className=color(data.change_pct);
  $('overview').innerHTML=`<div class="route-card"><div class="section-top"><h3>SFZ 教材初篩</h3><span class="badge ${data.sfz.member?'good':''}">${esc(data.sfz.stage)}</span></div>${ruleChecks(data.sfz.checks)}<p class="muted">${esc(data.sfz.note)}</p></div><div class="route-card" id="mda-sources"></div><div id="mda-checklist"></div>`;
  renderChips();renderPatterns();renderMda();renderChart();renderProfile();
  $('ai-result').textContent='尚未執行這張圖表的 AI 判讀。';
 }catch(error){if(request===generation){$('stock-title').textContent='無法開啟';message(error.message);}}
}
function renderChips(){
 const c=detail.chips.windows,r=detail.revenue,h=detail.holder;
 const fields=[['foreign_net','外資'],['investment_trust_net','投信'],['dealer_net','自營商'],['institutional_total_net','三大法人合計']];
 $('chips').innerHTML=`<div class="section-top"><h3>法人買賣超</h3><span class="muted">單位：張（原始股數 ÷ 1,000）</span></div><table><thead><tr><th>類別</th><th>1 日</th><th>5 日</th><th>10 日</th></tr></thead><tbody>${fields.map(([key,name])=>`<tr><td>${name}</td>${['1','5','10'].map(w=>`<td class="${color(c[w].values[key])}" title="${esc(c[w].start)} 至 ${esc(c[w].end)}">${c[w].values[key]==null?'—':num(c[w].values[key]/1000,1)}</td>`).join('')}</tr>`).join('')}</tbody></table><p class="muted">${esc(c['10'].end||'—')} · ${c['10'].observed_sessions}/10 個交易日。缺漏日期不補零。</p><div class="route-card"><div class="section-top"><h3>月營收</h3><span class="badge ${r?.period===detail.expected_revenue_period?'good':'warn'}">${esc(r?.period||'尚未取得')}${r&&r.period!==detail.expected_revenue_period?' · 非最新應公告月份':''}</span></div><div class="metric-grid"><div class="metric"><span>營收（億元）</span><strong>${r?.revenue==null?'—':num(r.revenue/100000)}</strong></div><div class="metric"><span>月增率</span><strong class="${color(r?.mom_pct)}">${pct(r?.mom_pct)}</strong></div><div class="metric"><span>年增率</span><strong class="${color(r?.yoy_pct)}">${pct(r?.yoy_pct)}</strong></div></div><table><thead><tr><th>月份</th><th>營收（億元）</th><th>月增</th><th>年增</th></tr></thead><tbody>${detail.revenue_history.slice(-6).reverse().map(v=>`<tr><td>${esc(v.period)}</td><td>${v.revenue==null?'—':num(v.revenue/100000)}</td><td class="${color(v.mom_pct)}">${pct(v.mom_pct)}</td><td class="${color(v.yoy_pct)}">${pct(v.yoy_pct)}</td></tr>`).join('')}</tbody></table></div><div class="route-card"><div class="section-top"><h3>大戶股權比例</h3><span class="muted">${esc(detail.weekly_date||'—')}</span></div><div class="metric-grid"><div class="metric"><span>400 張以上持股</span><strong>${h?num(h.major_400_percent)+'%':'—'}</strong></div><div class="metric"><span>較前週變化（百分點）</span><strong>${h?num(h.delta_percentage_points):'—'}</strong></div><div class="metric"><span>每週增加排名</span><strong>${h?.rank||'—'}</strong></div></div><p class="muted">${h?'週股權變化不是當週買賣超；長期 B1 另需連續歷史。':'目前股權資料未包含這檔；不代表持股為零。'}</p></div>`;
}
function renderPatterns(){const p=detail.patterns.observations;$('patterns').innerHTML=`<div class="section-top"><h3>常見型態篩選</h3><span class="badge">幾何條件 v2</span></div><p class="muted">以下是明示門檻的工程篩選，不是 SFZ／M 大教材新增規則，也不是 AI 結果。</p>${p.length?p.map(o=>`<div class="pattern-card"><div class="section-top"><strong>${esc(o.name)}</strong><span class="muted">${esc(o.start)} → ${esc(o.end)}</span></div><p>${esc(o.evidence)}</p></div>`).join(''):'<div class="empty">目前未符合已定義的型態條件。</div>'}`;}
function renderChart(){if(typeof renderResearchCharts==='function')renderResearchCharts();}

function renderHealth(){const c=index.coverage;$('health-content').innerHTML=`<div class="metric-grid"><div class="metric"><span>可驗證歷史價格</span><strong>${c.verified} / ${c.universe}</strong></div><div class="metric"><span>10 日法人資料完整</span><strong>${c.complete_chips_10} / ${c.visible||c.universe}</strong></div><div class="metric"><span>已有月營收</span><strong>${c.revenue} / ${c.visible||c.universe}</strong></div></div><p>價格截至 ${esc(index.data_date)}，目前應完成交易日 ${esc(index.expected_session)}。${index.fresh?'日期對齊。':'資料未對齊，通知停用，畫面保留最近一次可驗證結果。'}</p><p>來源：<a href="https://www.twse.com.tw/" target="_blank" rel="noreferrer">臺灣證券交易所</a>、<a href="https://www.tpex.org.tw/" target="_blank" rel="noreferrer">證券櫃檯買賣中心</a>、<a href="https://mops.twse.com.tw/" target="_blank" rel="noreferrer">公開資訊觀測站</a>、<a href="https://www.tdcc.com.tw/" target="_blank" rel="noreferrer">集保結算所</a>。</p><p>大戶／散戶及股東人數採 TDCC 週快照。六週增減需要七個連續端點；資料不足時不判定。投信持股總張數待接來源。Volume Profile 支援逐筆成交 CSV，POC 由成交量計算。</p><p id="service-health">Telegram 與 AI 服務尚待私密金鑰設定；網站不保存 API 金鑰。GitHub 排程更新可能延遲，通知速度受官方公布與收集完成時間影響。</p>`;}
document.querySelectorAll('[data-route]').forEach(button=>button.onclick=()=>{route=button.dataset.route;document.querySelectorAll('[data-route]').forEach(b=>b.classList.toggle('active',b===button));renderList();});
document.querySelectorAll('[data-frame]').forEach(button=>button.onclick=()=>{frame=button.dataset.frame;chartOffset=0;document.querySelectorAll('[data-frame]').forEach(b=>b.classList.toggle('active',b===button));renderChart();$('ai-result').textContent='週期已切換；請重新分析這張圖表。';});
document.querySelectorAll('[data-panel]').forEach(button=>button.onclick=()=>{document.querySelectorAll('[data-panel]').forEach(b=>b.classList.toggle('active',b===button));document.querySelectorAll('.panel').forEach(p=>p.hidden=p.id!==button.dataset.panel);});
for(const id of ['search','market','pattern','chip-filter','revenue-filter'])$(id).addEventListener(id==='search'?'input':'change',renderList);
$('pattern-source').onchange=()=>{const useAI=$('pattern-source').value==='ai';$('ai-filter-status').hidden=!useAI;if(useAI)refreshAIPatterns();else renderList();};
$('chart-range').onchange=()=>{chartOffset=0;renderChart();};
$('stock-list').onclick=event=>{const row=event.target.closest('[data-stock]');if(row)openStock(row.dataset.stock);};
$('stock-list').onkeydown=event=>{if(['Enter',' '].includes(event.key)){const row=event.target.closest('[data-stock]');if(row){event.preventDefault();openStock(row.dataset.stock);}}};
$('more-filters').onclick=()=>{const show=$('extra-filters').hidden;$('extra-filters').hidden=!show;$('more-filters').setAttribute('aria-expanded',String(show));};
$('health-nav').onclick=()=>{$('health').open=true;$('health').scrollIntoView({behavior:'smooth'});};
$('theme').onclick=()=>{document.body.classList.toggle('light');try{localStorage.setItem('theme',document.body.classList.contains('light')?'light':'dark');}catch{}};
try{if(localStorage.getItem('theme')==='light')document.body.classList.add('light');}catch{}
new ResizeObserver(()=>renderChart()).observe($('chart'));
$('analyze').onclick=async()=>{
 if(busy||!detail)return;busy=true;$('analyze').disabled=true;
 const sid=selected,period=frame;$('ai-result').textContent='正在判讀圖表…';
 try{const response=await fetch('api/analyze',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({stock_id:sid,frequency:period,packet_sha256:index.stocks.find(s=>s.stock_id===sid).detail_sha256})});if(!response.headers.get('content-type')?.includes('application/json'))throw Error('此網址未連接私有 AI 服務');const data=await response.json();if(response.status===401)$('login-form').hidden=false;if(!response.ok)throw Error(data.message||'AI 服務未啟用');if(selected===sid&&frame===period){aiOverlay=data;renderChart();}if(period==='day'){aiPatterns[sid]=data.observations.map(o=>o.pattern_id);renderList();}if(selected===sid&&frame===period)$('ai-result').innerHTML=`<p class="muted">${esc(data.model)} · ${esc(data.data_date)} · ${esc(data.frequency)}</p><p>${esc(data.summary)}</p>${data.observations.map(o=>`<div class="pattern-card"><strong>${esc(index.patterns[o.pattern_id]||o.pattern_id)}</strong><p>${esc(o.evidence)}</p></div>`).join('')}<p class="muted">${esc(data.uncertainty)}</p>`;}catch(error){$('ai-result').textContent=error.message==='Unexpected token \'<\', "<!DOCTYPE "... is not valid JSON'?'AI 服務尚未連接。':error.message;}finally{busy=false;$('analyze').disabled=false;}
};
window.addEventListener('DOMContentLoaded',()=>{getJSON('data/index.json').then(data=>{if(data.schema_version!=='sfz-mda-workspace-1')throw Error('資料版本不相容');index=data;$('data-date').textContent=data.data_date;$('coverage').textContent=`${data.coverage.verified.toLocaleString()} 檔有驗證歷史`;$('sfz-count').textContent=data.stocks.filter(s=>s.sfz.member).length;$('mda-count').textContent=data.stocks.filter(s=>s.mda.member).length;$('weekly-date').textContent=`股權資料 ${data.weekly_date||'待更新'}`;$('notification-status').textContent=data.services.telegram;$('freshness').textContent=data.fresh?'● 盤後資料已對齊':'◷ 保留最近可驗證資料';$('freshness').classList.add(data.fresh?'good':'warn');$('pattern').innerHTML+='<optgroup label="幾何條件篩選">'+Object.entries(data.patterns).map(([id,label])=>`<option value="${esc(id)}">${esc(label)}</option>`).join('')+'</optgroup>';$('heat-chain').innerHTML+=[...new Set(data.stocks.flatMap(s=>(s.industry||[]).map(i=>i.chain)))].sort().map(c=>`<option>${esc(c)}</option>`).join('');renderHealth();renderHeatmap();const sid=new URL(location.href).searchParams.get('stock');if(sid&&data.stocks.some(s=>s.stock_id===sid))openStock(sid);else renderList();}).catch(error=>message(error.message));},{once:true});

$('login-form').onsubmit=async event=>{event.preventDefault();try{const response=await fetch('api/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password:$('service-password').value})});$('service-password').value='';const result=await response.json();if(!response.ok)throw Error(result.message);$('login-form').hidden=true;$('ai-result').textContent='已登入，可以分析圖表。';refreshAIPatterns();}catch(error){$('ai-result').textContent=error.message;}};
getJSON('api/health').then(health=>{$('notification-status').textContent=health.telegram;const info=$('service-health');if(info)info.textContent=`Telegram：${health.telegram}；AI：${health.ai}。${health.outbox?.uncertain?'有送達結果不確定的訊息，請檢查私有服務紀錄。':''}`;}).catch(()=>{});
