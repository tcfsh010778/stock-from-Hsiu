(()=>{"use strict";
const q=new URLSearchParams(location.search),id=(q.get("id")||"").replace(/[^0-9A-Za-z]/g,"");
const $=s=>document.querySelector(s),all=s=>[...document.querySelectorAll(s)];
const esc=s=>String(s??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const fmt=(v,d=2)=>Number.isFinite(Number(v))?Number(v).toLocaleString("zh-TW",{minimumFractionDigits:d,maximumFractionDigits:d}):"—";
let packets=[],active=null,chartsReady=false,chartEntries=[],syncing=false;
const layers={candles:true,patterns:true,zones:true,lines:true};
const gapNames={institutional:"三大法人",foreign_ownership:"外資持股",margin:"融資融券",holdings:"股東結構"};
function err(msg){$("#loading").classList.add("error");$("#loading").textContent=msg}
if(!id){err("網址缺少股票代號。");return}
Promise.all([
  fetch(`data/${id}.json`).then(r=>{if(!r.ok)throw Error("找不到此股票的 V2 分析資料");return r.json()}),
  fetch("data/index.json").then(r=>r.json())
]).then(([p,index])=>{
  packets=p;active=p.find(x=>x.timeframe==="daily")||p[0];if(!active)throw Error("分析資料為空");
  const meta=index.stocks?.[id]||{};renderHeader(meta);renderTabs();renderTimeframes();renderOverview();renderPlan();renderPatterns();drawTrend();renderChipStatus();
  $("#legacy-link").href=`../stocks/${id}.html`;$("#loading").hidden=true;$("#app").hidden=false;
}).catch(e=>err(`V2 載入失敗：${e.message}`));

function daily(){return packets.find(x=>x.timeframe==="daily")||packets[0]}
function latest(){const rows=daily().series||[];return rows[rows.length-1]||{}}
function nearest(kind){
  const close=Number(latest().close),zones=(daily().support_resistance||[]).filter(z=>z.kind===kind);
  if(kind==="support") return zones.filter(z=>Number(z.price_high)<close).sort((a,b)=>Number(b.price_high)-Number(a.price_high))[0]||null;
  return zones.filter(z=>Number(z.price_low)>close).sort((a,b)=>Number(a.price_low)-Number(b.price_low))[0]||null;
}
function zoneText(z){return z?`${fmt(z.price_low)} – ${fmt(z.price_high)}`:"資料不足"}
function warningText(value){
  const map={
    "mda_candidate_pool freshness is missing":"MDA 候選資料缺少更新日期，暫不作語意判讀。",
    "carybot_signals freshness is fallback_stale":"CaryBot 資料已過期，現階段只保留警示。",
    "adjusted-price metadata is missing; long-horizon geometry confidence was reduced":"缺少還原權息資料，長週期幾何分數已降低。"
  };return map[value]||value;
}
function renderHeader(meta){
  const d=daily(),row=latest(),rows=d.series||[],prev=rows.length>1?rows[rows.length-2]:null,change=prev&&Number(prev.close)?(Number(row.close)/Number(prev.close)-1)*100:null;
  document.title=`${id} ${meta.name||""}｜Stock from Hsiu V2`;$("#stock-title").textContent=`${id} ${meta.name||""}`;
  $("#meta").innerHTML=`<span>資料 ${esc(d.data_date)}</span><span>引擎 ${esc(d.engine_version)}</span><span>品質 ${esc(d.freshness?.status||"unknown")}</span>`;
  $("#latest-price").textContent=fmt(row.close);$("#latest-date").textContent=row.date||"";$("#support-price").textContent=zoneText(nearest("support"));$("#resistance-price").textContent=zoneText(nearest("resistance"));
  $("#stop-price").textContent=fmt(d.risk_control?.stop_price);const ce=$("#change");ce.textContent=change==null?"":`${change>=0?"+":""}${change.toFixed(2)}%`;ce.className=`change ${change>=0?"up":"down"}`;
  const warnings=[...new Set([...(d.warnings||[]),...((d.freshness||{}).warnings||[])])].map(warningText);$("#warnings").innerHTML=(warnings.length?warnings:["目前沒有額外資料警示。"] ).map(x=>`<li>${esc(x)}</li>`).join("");
}
function renderTabs(){
  all(".tab").forEach(b=>b.onclick=()=>{all(".tab,.pane").forEach(x=>x.classList.remove("active"));b.classList.add("active");$("#"+b.dataset.pane).classList.add("active");if(b.dataset.pane==="trend")drawTrend();if(b.dataset.pane==="chips")requestAnimationFrame(initChipWorkbench)});
  all(".layer").forEach(b=>b.onclick=()=>{layers[b.dataset.layer]=!layers[b.dataset.layer];b.classList.toggle("active",layers[b.dataset.layer]);drawTrend()});
}
function renderTimeframes(){
  $("#timeframes").innerHTML=packets.map(p=>`<button class="tf ${p===active?"active":""}" data-tf="${p.timeframe}">${({daily:"日線",weekly:"週線",monthly:"月線"})[p.timeframe]||p.timeframe}</button>`).join("");
  all(".tf").forEach(b=>b.onclick=()=>{active=packets.find(p=>p.timeframe===b.dataset.tf)||active;all(".tf").forEach(x=>x.classList.remove("active"));b.classList.add("active");renderPatterns();drawTrend()});
}
function renderOverview(){
  const d=daily(),m=d.market_evidence||{},available=Object.keys(gapNames).filter(k=>!(m.gaps||[]).includes(k));
  const metrics=[
    ["價格資料",`${(d.series||[]).length} 根日 K`],["型態證據",`${(d.patterns||[]).length} 項`],["趨勢線",`${(d.trendlines||[]).length} 條`],["籌碼資料",available.length?available.map(k=>gapNames[k]).join("、"):"尚無"]
  ];$("#overview-grid").innerHTML=metrics.map(([a,b])=>`<div class="metric"><span>${esc(a)}</span><strong>${esc(b)}</strong></div>`).join("");
}
function renderPlan(){
  const support=nearest("support"),resistance=nearest("resistance"),stop=daily().risk_control?.stop_price;
  const cards=[
    ["突破情境",resistance?`收盤站上 ${fmt(resistance.price_high)}`:"等待壓力區形成",resistance?"突破後觀察成交量與回測是否守住壓力區上緣。":"目前沒有足夠壓力區資料，不建立突破觸發價。",""] ,
    ["整理情境",support&&resistance?`${fmt(support.price_low)} – ${fmt(resistance.price_high)}`:"區間資料不足",support&&resistance?"價格留在支撐與壓力之間，只更新證據，不預設方向。":"等待支撐與壓力同時形成。",""] ,
    ["跌破情境",support?`收盤跌破 ${fmt(support.price_low)}`:`固定停損 ${fmt(stop)}`,`結構跌破時重新檢查；固定停損價為 ${fmt(stop)}。`,"breakdown"]
  ];$("#scenario-grid").innerHTML=cards.map(([title,trigger,text,cls])=>`<article class="scenario ${cls}"><h3>${title}</h3><div class="trigger">${trigger}</div><p>${text}</p></article>`).join("");
}
function renderPatterns(){
  const items=active.patterns||[];$("#pattern-list").innerHTML=items.length?items.slice(0,18).map(v=>`<article class="mini"><span class="tag">${esc(v.status)} · ${esc(v.direction)}</span><span class="score">${esc(v.quality_score)}</span><h3>${esc(v.name)}</h3><p>${esc((v.evidence||[]).join("；")||"尚無證據說明")}</p><p class="muted">缺少：${esc((v.missing_conditions||[]).join("；")||"無")}</p><p class="muted">反向：${esc((v.counterevidence||[]).join("；")||"無")}</p></article>`).join(""):"<p class=muted>目前沒有通過品質門檻的型態。</p>";
}
function svg(name,attrs={}){const n=document.createElementNS("http://www.w3.org/2000/svg",name);for(const[k,v]of Object.entries(attrs))n.setAttribute(k,v);return n}
function drawTrend(){
  const root=$("#chart");if(!root)return;root.replaceChildren();const rows=(active.series||[]).slice(-120);if(!rows.length)return;
  const W=1120,H=520,p={l:62,r:65,t:18,b:35},min=Math.min(...rows.map(r=>r.low)),max=Math.max(...rows.map(r=>r.high)),span=Math.max(.001,max-min),xi=i=>p.l+i/Math.max(1,rows.length-1)*(W-p.l-p.r),y=v=>p.t+(max-v)/span*(H-p.t-p.b),index=new Map(rows.map((r,i)=>[r.date,i]));
  for(let g=0;g<=5;g++){const price=min+span*g/5,yy=y(price);root.append(svg("line",{x1:p.l,x2:W-p.r,y1:yy,y2:yy,stroke:"#202a3a"}));const t=svg("text",{x:W-p.r+8,y:yy+4,class:"axis"});t.textContent=price.toFixed(2);root.append(t)}
  if(layers.zones)(active.support_resistance||[]).slice(0,9).forEach(z=>root.append(svg("rect",{x:p.l,y:y(z.price_high),width:W-p.l-p.r,height:Math.max(2,y(z.price_low)-y(z.price_high)),fill:z.kind==="support"?"#26a69a":"#ef5350",class:"zone"})));
  if(layers.candles)rows.forEach((r,i)=>{const x=xi(i),c=r.close>=r.open?"#ef5350":"#26a69a";root.append(svg("line",{x1:x,x2:x,y1:y(r.high),y2:y(r.low),stroke:c}));root.append(svg("rect",{x:x-2.8,y:y(Math.max(r.open,r.close)),width:5.6,height:Math.max(1,Math.abs(y(r.open)-y(r.close))),fill:c}))});
  if(layers.patterns)(active.patterns||[]).slice(0,14).forEach(v=>{const i=index.get(v.end_date);if(i===undefined)return;const r=rows[i],t=svg("text",{x:xi(i)-5,y:y(v.direction==="bullish"?r.low:r.high)+(v.direction==="bullish"?17:-7),fill:v.category==="candlestick"?"#2dd4bf":"#f6c453",class:"mark"});t.textContent=v.category==="candlestick"?"◆":"●";const tip=svg("title");tip.textContent=`${v.name}｜${v.status}｜${v.quality_score}`;t.append(tip);root.append(t)});
  if(layers.lines)(active.trendlines||[]).filter(l=>l.kind!=="channel").slice(0,6).forEach(l=>{const a=index.get(l.start.date),b=index.get(l.end.date);if(a===undefined&&b===undefined)return;const startI=a??0,endI=b??rows.length-1,startPrice=a===undefined?l.start.price+l.slope_per_bar*(startI-(b??0)):l.start.price,endPrice=b===undefined?l.end.price+l.slope_per_bar*(endI-(a??0)):l.end.price,line=svg("line",{x1:xi(startI),x2:xi(endI),y1:y(startPrice),y2:y(endPrice),stroke:l.kind==="support"?"#2dd4bf":"#f6c453",class:"trend"});line.onclick=()=>$("#chart-detail").textContent=`${l.kind}｜${l.status}｜端點 ${l.start.date} ${l.start.price} → ${l.end.date} ${l.end.price}｜接觸 ${l.touch_count}｜穿越 ${l.violation_count}｜分數 ${l.quality_score}`;root.append(line)});renderLines();
}
function renderLines(){const lines=(active.trendlines||[]).slice(0,8);$("#line-list").innerHTML=lines.length?lines.map(l=>`<article class="mini"><span class="tag">${esc(l.kind)}</span><span class="score">${esc(l.quality_score)}</span><h3>${esc(l.status)}</h3><p>接觸 ${esc(l.touch_count)} 次 · 穿越 ${esc(l.violation_count)} 次</p><p class="muted">${esc(l.start.date)} ${esc(l.start.price)} → ${esc(l.end.date)} ${esc(l.end.price)}</p></article>`).join(""):"<p class=muted>目前沒有已確認的高品質趨勢線。</p>"}
function renderChipStatus(){
  const m=daily().market_evidence||{},gaps=m.gaps||[],available=Object.keys(gapNames).filter(k=>!gaps.includes(k));
  $("#chip-status").innerHTML=`<div class="gap-list">${available.map(k=>`<span class="available">${gapNames[k]} · ${esc(m.source_dates?.[k]||"")}</span>`).join("")}${gaps.map(k=>`<span class="gap">缺少 ${gapNames[k]}</span>`).join("")}</div>`;
}
function panel(title,note,height=210){const wrap=document.createElement("section");wrap.className="tv-panel";wrap.innerHTML=`<div class="tv-panel-head"><b>${title}</b><small>${note}</small></div><div class="tv-chart" style="height:${height}px"></div>`;$("#chip-workbench").append(wrap);return wrap.querySelector(".tv-chart")}
function baseOptions(){const L=window.LightweightCharts;return{autoSize:true,layout:{background:{type:L.ColorType.Solid,color:"#0b1018"},textColor:"#8b9bb1"},grid:{vertLines:{color:"#202a3a"},horzLines:{color:"#202a3a"}},rightPriceScale:{borderColor:"#273245"},timeScale:{borderColor:"#273245",timeVisible:false,secondsVisible:false,rightOffset:4,barSpacing:7,minBarSpacing:2},crosshair:{mode:L.CrosshairMode.Normal},localization:{locale:"zh-TW"},handleScroll:{mouseWheel:true,pressedMouseMove:true},handleScale:{mouseWheel:true,pinch:true}}}
function addEntry(el,rows,primary,key){const chart=primary.chart,entry={chart,primary:primary.series,rows,lookup:new Map(rows.map(r=>[r.date,r])),value:r=>Number(r[key])};chartEntries.push(entry);return entry}
function wireCharts(){
  chartEntries.forEach(entry=>{
    entry.chart.timeScale().subscribeVisibleTimeRangeChange(range=>{if(syncing||!range)return;syncing=true;chartEntries.forEach(other=>{if(other!==entry){try{other.chart.timeScale().setVisibleRange(range)}catch(_){}}});syncing=false});
    entry.chart.subscribeCrosshairMove(param=>{if(syncing)return;const time=typeof param?.time==="string"?param.time:param?.time;if(!time){chartEntries.forEach(x=>{try{x.chart.clearCrosshairPosition()}catch(_){}});return}$("#crosshair-date").textContent=String(time);syncing=true;chartEntries.forEach(other=>{if(other===entry)return;const row=other.lookup.get(String(time)),value=row?other.value(row):null;if(Number.isFinite(value)){try{other.chart.setCrosshairPosition(value,time,other.primary)}catch(_){}}});syncing=false});
  });
  const rows=daily().series||[];if(rows.length){const from=rows[Math.max(0,rows.length-120)].date,to=rows[rows.length-1].date;try{chartEntries[0].chart.timeScale().setVisibleRange({from,to})}catch(_){chartEntries[0].chart.timeScale().fitContent()}}
}
function initChipWorkbench(){
  if(chartsReady)return;chartsReady=true;const root=$("#chip-workbench"),L=window.LightweightCharts;if(!L){root.innerHTML='<div class="method-note">圖表元件載入失敗；請確認網路後重新整理。資料本身仍保留在頁面封包中。</div>';return}
  const d=daily(),m=d.market_evidence||{},price=d.series||[];
  const priceEl=panel("日 K 與成交量","紅漲綠跌 · 可縮放、拖曳",430),priceChart=L.createChart(priceEl,baseOptions());
  const candles=priceChart.addSeries(L.CandlestickSeries,{upColor:"#ef5350",downColor:"#26a69a",wickUpColor:"#ef5350",wickDownColor:"#26a69a",borderVisible:false});candles.setData(price.map(r=>({time:r.date,open:r.open,high:r.high,low:r.low,close:r.close})));
  const volume=priceChart.addSeries(L.HistogramSeries,{priceFormat:{type:"volume"},priceScaleId:"volume",lastValueVisible:false,priceLineVisible:false});priceChart.priceScale("volume").applyOptions({scaleMargins:{top:.78,bottom:0}});volume.setData(price.map(r=>({time:r.date,value:r.volume,color:r.close>=r.open?"rgba(239,83,80,.45)":"rgba(38,166,154,.45)"})));addEntry(priceEl,price,{chart:priceChart,series:candles},"close");
  if((m.institutional||[]).length){const rows=m.institutional,el=panel("三大法人買賣超","外資／投信／自營商，單位：張"),chart=L.createChart(el,baseOptions());let primary=null;[["foreign","#4da3ff"],["trust","#f6c453"],["dealer","#b98cff"]].forEach(([key,color],i)=>{const s=chart.addSeries(L.LineSeries,{color,lineWidth:2,priceLineVisible:false,lastValueVisible:i===0});s.setData(rows.map(r=>({time:r.date,value:r[key]})));if(!primary)primary=s});addEntry(el,rows,{chart,series:primary},"foreign")}
  if((m.margin||[]).length){const rows=m.margin,el=panel("信用交易","融資餘額／融券餘額"),chart=L.createChart(el,baseOptions()),a=chart.addSeries(L.LineSeries,{color:"#f6c453",lineWidth:2}),b=chart.addSeries(L.LineSeries,{color:"#b98cff",lineWidth:2});a.setData(rows.filter(r=>r.margin_balance!=null).map(r=>({time:r.date,value:r.margin_balance})));b.setData(rows.filter(r=>r.short_balance!=null).map(r=>({time:r.date,value:r.short_balance})));addEntry(el,rows,{chart,series:a},"margin_balance")}
  if((m.foreign_ownership||[]).length){const rows=m.foreign_ownership,el=panel("外資持股","持股比率（%）"),chart=L.createChart(el,baseOptions()),s=chart.addSeries(L.LineSeries,{color:"#4da3ff",lineWidth:2});s.setData(rows.filter(r=>r.foreign_ratio!=null).map(r=>({time:r.date,value:r.foreign_ratio})));addEntry(el,rows,{chart,series:s},"foreign_ratio")}
  if((m.holdings||[]).length){const rows=m.holdings,el=panel("股東結構","大戶／中實戶／散戶持股比（每週）",230),chart=L.createChart(el,baseOptions());let primary=null;[["major","#ef5350"],["middle","#f6c453"],["retail","#26a69a"]].forEach(([key,color])=>{const s=chart.addSeries(L.LineSeries,{color,lineWidth:2,priceLineVisible:false});s.setData(rows.map(r=>({time:r.date,value:r[key]})));if(!primary)primary=s});addEntry(el,rows,{chart,series:primary},"major")}
  if(chartEntries.length)wireCharts();else root.innerHTML='<div class="method-note">目前沒有可繪製的籌碼序列；資料補齊後會自動出現副圖。</div>';
}
})();
