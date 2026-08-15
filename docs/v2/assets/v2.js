(()=>{"use strict";
const q=new URLSearchParams(location.search),id=(q.get("id")||"").replace(/[^0-9A-Za-z]/g,"");
const $=s=>document.querySelector(s),all=s=>[...document.querySelectorAll(s)];
const esc=s=>String(s??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const fmt=(v,d=2)=>Number.isFinite(Number(v))?Number(v).toLocaleString("zh-TW",{minimumFractionDigits:d,maximumFractionDigits:d}):"—";
let packets=[],active=null,chartsReady=false,chartEntries=[];
const layers={candles:true,patterns:true,zones:true,lines:true};
const gapNames={institutional:"三大法人",foreign_ownership:"外資持股",margin:"融資融券",holdings:"股東結構"};
function err(msg){$("#loading").classList.add("error");$("#loading").textContent=msg}
if(!id){err("網址缺少股票代號。");return}
Promise.all([
  fetch(`data/${id}.json`).then(r=>{if(!r.ok)throw Error("找不到此股票的 V2 分析資料");return r.json()}),
  fetch("data/index.json").then(r=>r.json())
]).then(([p,index])=>{
  packets=p;active=p.find(x=>x.timeframe==="daily")||p[0];if(!active)throw Error("分析資料為空");
  const meta=index.stocks?.[id]||{};renderHeader(meta);renderTabs();renderTimeframes();renderOverview();renderTechnicalEvidence();renderTechnicalPatterns();renderPlan();renderPatterns();drawTrend();renderChipStatus();
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
function renderTimeframes(){
  $("#timeframes").innerHTML=packets.map(p=>`<button class="tf ${p===active?"active":""}" data-tf="${p.timeframe}">${({daily:"日線",weekly:"週線",monthly:"月線"})[p.timeframe]||p.timeframe}</button>`).join("");
  all(".tf").forEach(b=>b.onclick=()=>{active=packets.find(p=>p.timeframe===b.dataset.tf)||active;all(".tf").forEach(x=>x.classList.remove("active"));b.classList.add("active");renderTechnicalEvidence();renderTechnicalPatterns();renderPatterns();drawTrend()});
}
const indicatorMeanings={rsi_14:"動能位置，提供 30／70 參考區間",macd_12_26_9:"快慢指數差與訊號線的相對位置",bollinger_20_2:"收盤位於上下軌之間的位置",volume_vs_avg_3:"最新量與前 3 根已閉合 K 線均量比較",volume_vs_avg_5:"最新量與前 5 根已閉合 K 線均量比較",volume_vs_avg_10:"最新量與前 10 根已閉合 K 線均量比較"};
const statusLabels={available:"可用",insufficient_history:"歷史不足",missing:"缺少資料",non_finite:"數值無效"};
const comparisonLabels={current_volume:"最新量",average_volume:"均量",delta_volume:"差額",delta_percent:"差異 %",oversold_reference:"超賣參考",overbought_reference:"超買參考",macd:"MACD",signal:"訊號線",histogram:"柱狀體",close:"收盤",upper:"上軌",middle:"中軌",lower:"下軌",bandwidth:"帶寬",required_rows:"所需根數",actual_rows:"目前根數",zone:"區間",bias:"相對位置",direction:"量能方向"};
const patternMeanings={"十字線":"開收接近，需搭配位置與量能觀察","錘頭線":"下影線較長，需搭配後續 K 線確認","吞噬":"前後兩根 K 線的實體關係，需搭配趨勢確認"};
function evidenceValue(item){return item.value_status==="available"?fmt(item.value,item.unit==="index_0_100"?2:4):statusLabels[item.value_status]||"—"}
function evidenceComparison(item){return Object.entries(item.comparison_values||{}).filter(([,v])=>v!=="unavailable").map(([k,v])=>`${esc(comparisonLabels[k]||k)}：${typeof v==="number"?esc(fmt(v,4)):esc(v)}`).join(" · ")}
function renderTechnicalEvidence(){
  const items=daily().technical_evidence||[];
  $("#technical-evidence-cards").innerHTML=items.map(item=>`<article class="evidence-card ${esc(item.value_status)}"><span class="evidence-state ${esc(item.value_status)}">${esc(statusLabels[item.value_status]||item.value_status)}</span><h3>${esc(item.name)}</h3><p class="evidence-meta">${esc(indicatorMeanings[item.indicator_id]||"此數值只作為輔助證據")}</p><div class="evidence-value">${evidenceValue(item)}</div><p class="evidence-meta">${esc(item.comparison_basis)}</p><p>${evidenceComparison(item)||"目前沒有可比較欄位"}</p><p class="muted">${esc(item.calculation_basis)} · ${esc(item.data_date)} · ${esc(item.freshness?.status||"unknown")}</p>${item.known_gaps?.length?`<p class="muted">限制：${item.known_gaps.map(esc).join("；")}</p>`:""}</article>`).join("")||"<p class=muted>目前沒有技術證據卡；請等待足夠的已閉合 K 線。</p>";
}
function renderPlan(){
  const support=nearest("support"),resistance=nearest("resistance"),stop=daily().risk_control?.stop_price;
  const cards=[
    ["突破情境",resistance?`收盤站上 ${fmt(resistance.price_high)}`:"等待壓力區形成",resistance?"突破後觀察成交量與回測是否守住壓力區上緣。":"目前沒有足夠壓力區資料，不建立突破觸發價。",""] ,
    ["整理情境",support&&resistance?`${fmt(support.price_low)} – ${fmt(resistance.price_high)}`:"區間資料不足",support&&resistance?"價格留在支撐與壓力之間，只更新證據，不預設方向。":"等待支撐與壓力同時形成。",""] ,
    ["跌破情境",support?`收盤跌破 ${fmt(support.price_low)}`:`固定停損 ${fmt(stop)}`,`結構跌破時重新檢查；固定停損價為 ${fmt(stop)}。`,"breakdown"]
  ];$("#scenario-grid").innerHTML=cards.map(([title,trigger,text,cls])=>`<article class="scenario ${cls}"><h3>${title}</h3><div class="trigger">${trigger}</div><p>${text}</p></article>`).join("");
}
function svg(name,attrs={}){const n=document.createElementNS("http://www.w3.org/2000/svg",name);for(const[k,v]of Object.entries(attrs))n.setAttribute(k,v);return n}
function renderLines(){const lines=(active.trendlines||[]).slice(0,8);$("#line-list").innerHTML=lines.length?lines.map(l=>`<article class="mini"><span class="tag">${esc(l.kind)}</span><span class="score">${esc(l.quality_score)}</span><h3>${esc(l.status)}</h3><p>接觸 ${esc(l.touch_count)} 次 · 穿越 ${esc(l.violation_count)} 次</p><p class="muted">${esc(l.start.date)} ${esc(l.start.price)} → ${esc(l.end.date)} ${esc(l.end.price)}</p></article>`).join(""):"<p class=muted>目前沒有已確認的高品質趨勢線。</p>"}
function renderChipStatus(){
  const m=daily().market_evidence||{},gaps=m.gaps||[],available=Object.keys(gapNames).filter(k=>!gaps.includes(k));
  $("#chip-status").innerHTML=`<div class="gap-list">${available.map(k=>`<span class="available">${gapNames[k]} · ${esc(m.source_dates?.[k]||"")}</span>`).join("")}${gaps.map(k=>`<span class="gap">缺少 ${gapNames[k]}</span>`).join("")}</div>`;
}
function baseOptions(){const L=window.LightweightCharts;return{autoSize:true,layout:{background:{type:L.ColorType.Solid,color:"#0b1018"},textColor:"#8b9bb1"},grid:{vertLines:{color:"#202a3a"},horzLines:{color:"#202a3a"}},rightPriceScale:{borderColor:"#273245"},timeScale:{borderColor:"#273245",timeVisible:false,secondsVisible:false,rightOffset:4,barSpacing:7,minBarSpacing:2},crosshair:{mode:L.CrosshairMode.Normal},localization:{locale:"zh-TW"},handleScroll:{mouseWheel:true,pressedMouseMove:true},handleScale:{mouseWheel:true,pinch:true}}}
function addEntry(el,rows,primary,key){const chart=primary.chart,entry={chart,primary:primary.series,rows,lookup:new Map(rows.map(r=>[r.date,r])),value:r=>Number(r[key])};chartEntries.push(entry);return entry}
var annotationMarkerApi=null,currentMarkers=[];
function annotationEvents(packet=daily()){return packet?.timeframe==="daily"?(packet.candlestick_annotations?.events||[]):[]}
function groupedAnnotations(packet=daily()){
  const grouped=new Map();annotationEvents(packet).forEach(event=>{const items=grouped.get(event.bar_date)||[];items.push(event);grouped.set(event.bar_date,items)});
  return [...grouped.entries()].map(([date,events])=>{events.sort((a,b)=>Number(b.display?.display_priority||0)-Number(a.display?.display_priority||0)||String(a.pattern_id).localeCompare(String(b.pattern_id)));return{date,events,primary:events[0]}});
}
function visibleAnnotationGroups(packet=daily()){return groupedAnnotations(packet).filter(group=>Number(group.primary.display?.display_priority||0)>=55)}
function objectiveNumber(value,digits=2){return value==null?"—":fmt(value,digits)}
function annotationSummary(event){
  const geometry=event.geometry||{},context=event.context||{},position={high_zone:"前 60 日區間高位",mid_zone:"前 60 日區間中段",low_zone:"前 60 日區間低位",unknown:"位置歷史不足"}[context.position]||context.position;
  return `${event.label_zh}｜${event.bar_date}｜日 K 已收盤｜實體/振幅 ${objectiveNumber(geometry.body_ratio==null?null:geometry.body_ratio*100,1)}%｜實體/前日 ATR14 ${objectiveNumber(geometry.body_atr_prev,2)}｜${position}｜feature ${daily().candlestick_annotations?.feature_version||"unknown"}｜僅供圖形閱讀與研究，不代表交易訊號`;
}
function markerColor(event){if(event.orientation==="up_body")return"#ef5350";if(event.orientation==="down_body")return"#26a69a";if(event.family==="three_bar"||event.family==="two_bar")return"#f6c453";return"#9aa7b8"}
function setAnnotationVisibility(){if(annotationMarkerApi)annotationMarkerApi.setMarkers(layers.patterns?currentMarkers:[])}
function renderTabs(){
  const patternToggle=$('.layer[data-layer="patterns"]');if(patternToggle)patternToggle.textContent="日 K 型態";
  const activate=pane=>{const target=$("#"+pane);if(!target)return;all(".tab,.pane").forEach(x=>x.classList.remove("active"));const tab=$(`.tab[data-pane="${pane}"]`);if(tab)tab.classList.add("active");target.classList.add("active");if(pane==="trend")drawTrend();if(pane==="chips")requestAnimationFrame(initChipWorkbench)};
  all(".tab").forEach(button=>button.onclick=()=>activate(button.dataset.pane));
  all(".analysis-entry-link").forEach(button=>button.onclick=()=>activate(button.dataset.pane));
  all(".layer").forEach(button=>button.onclick=()=>{layers[button.dataset.layer]=!layers[button.dataset.layer];button.classList.toggle("active",layers[button.dataset.layer]);drawTrend();setAnnotationVisibility()});
}
function renderOverview(){
  const packet=daily(),market=packet.market_evidence||{},available=Object.keys(gapNames).filter(key=>!(market.gaps||[]).includes(key));
  const metrics=[["價格資料",`${(packet.series||[]).length} 根日 K`],["日 K 型態註記",`${annotationEvents(packet).length} 筆`],["趨勢線",`${(packet.trendlines||[]).length} 條`],["籌碼資料",available.length?available.map(key=>gapNames[key]).join("、"):"尚無"]];
  $("#overview-grid").innerHTML=metrics.map(([name,value])=>`<div class="metric"><span>${esc(name)}</span><strong>${esc(value)}</strong></div>`).join("");
}
function renderTechnicalPatterns(){
  const annotations=annotationEvents(active).slice(-5).reverse(),structures=(active.patterns||[]).slice(0,3),lines=(active.trendlines||[]).filter(line=>line.kind!=="channel").slice(0,3);
  const cards=annotations.map(event=>`<article class="mini annotation-note"><span class="tag">${esc(event.bar_date)} · 日 K 已收盤</span><h3>${esc(event.label_zh)}</h3><p>${esc(annotationSummary(event))}</p></article>`)
    .concat(structures.map(item=>`<article class="mini"><span class="tag">價格結構 · ${esc(item.status)}</span><h3>${esc(item.name)}</h3><p>${esc((item.evidence||[]).join("；")||"目前沒有補充證據")}</p></article>`))
    .concat(lines.map(line=>`<article class="mini"><span class="tag">趨勢線 · ${esc(line.kind)}</span><h3>${esc(line.status)}</h3><p>接觸 ${esc(line.touch_count)} 次 · 穿越 ${esc(line.violation_count)} 次 · 分數 ${esc(line.quality_score)}</p></article>`));
  $("#technical-pattern-summary").innerHTML=cards.join("")||"<p class=muted>目前沒有可顯示的日 K 型態或線型資料。</p>";
}
function renderPatterns(){
  const items=annotationEvents(active).slice(-18).reverse();
  $("#pattern-list").innerHTML=items.length?items.map(event=>`<article class="mini annotation-note"><span class="tag">${esc(event.bar_date)} · 日 K 已收盤</span><h3>${esc(event.label_zh)}</h3><p>${esc(annotationSummary(event))}</p><p class="muted">資料 ${esc(daily().candlestick_annotations?.as_of||"")} · config ${esc(daily().candlestick_annotations?.pattern_config_version||"")}</p></article>`).join(""):"<p class=muted>目前沒有近期日 K 型態註記。</p>";
}
function drawTrend(){
  const root=$("#chart");if(!root)return;root.replaceChildren();const rows=(active.series||[]).slice(-120);if(!rows.length)return;
  const width=1120,height=520,padding={l:62,r:65,t:18,b:35},minimum=Math.min(...rows.map(row=>row.low)),maximum=Math.max(...rows.map(row=>row.high)),span=Math.max(.001,maximum-minimum),xIndex=index=>padding.l+index/Math.max(1,rows.length-1)*(width-padding.l-padding.r),y=value=>padding.t+(maximum-value)/span*(height-padding.t-padding.b),indexByDate=new Map(rows.map((row,index)=>[row.date,index]));
  for(let grid=0;grid<=5;grid++){const price=minimum+span*grid/5,yy=y(price);root.append(svg("line",{x1:padding.l,x2:width-padding.r,y1:yy,y2:yy,stroke:"#202a3a"}));const label=svg("text",{x:width-padding.r+8,y:yy+4,class:"axis"});label.textContent=price.toFixed(2);root.append(label)}
  if(layers.zones)(active.support_resistance||[]).slice(0,9).forEach(zone=>root.append(svg("rect",{x:padding.l,y:y(zone.price_high),width:width-padding.l-padding.r,height:Math.max(2,y(zone.price_low)-y(zone.price_high)),fill:zone.kind==="support"?"#26a69a":"#ef5350",class:"zone"})));
  if(layers.candles)rows.forEach((row,index)=>{const xx=xIndex(index),color=row.close>=row.open?"#ef5350":"#26a69a";root.append(svg("line",{x1:xx,x2:xx,y1:y(row.high),y2:y(row.low),stroke:color}));root.append(svg("rect",{x:xx-2.8,y:y(Math.max(row.open,row.close)),width:5.6,height:Math.max(1,Math.abs(y(row.open)-y(row.close))),fill:color}))});
  if(layers.patterns){
    visibleAnnotationGroups(active).forEach(group=>{const index=indexByDate.get(group.date);if(index===undefined)return;const row=rows[index],mark=svg("text",{x:xIndex(index)-5,y:y(row.high)-7,fill:markerColor(group.primary),class:"mark"});mark.textContent=`◆${group.events.length>1?`+${group.events.length-1}`:""}`;const title=svg("title");title.textContent=group.events.map(annotationSummary).join("\n");mark.append(title);root.append(mark)});
    (active.patterns||[]).slice(0,8).forEach(item=>{const index=indexByDate.get(item.end_date);if(index===undefined)return;const row=rows[index],mark=svg("text",{x:xIndex(index)-5,y:y(row.low)+17,fill:"#f6c453",class:"mark"});mark.textContent="●";const title=svg("title");title.textContent=`${item.name}｜${item.status}｜${item.quality_score}`;mark.append(title);root.append(mark)});
  }
  if(layers.lines)(active.trendlines||[]).filter(line=>line.kind!=="channel").slice(0,6).forEach(line=>{const start=indexByDate.get(line.start.date),end=indexByDate.get(line.end.date);if(start===undefined&&end===undefined)return;const startIndex=start??0,endIndex=end??rows.length-1,startPrice=start===undefined?line.start.price+line.slope_per_bar*(startIndex-(end??0)):line.start.price,endPrice=end===undefined?line.end.price+line.slope_per_bar*(endIndex-(start??0)):line.end.price,element=svg("line",{x1:xIndex(startIndex),x2:xIndex(endIndex),y1:y(startPrice),y2:y(endPrice),stroke:line.kind==="support"?"#2dd4bf":"#f6c453",class:"trend"});element.onclick=()=>$("#chart-detail").textContent=`${line.kind}｜${line.status}｜端點 ${line.start.date} ${line.start.price} → ${line.end.date} ${line.end.price}｜接觸 ${line.touch_count}｜穿越 ${line.violation_count}｜分數 ${line.quality_score}`;root.append(element)});
  renderLines();
}
function panel(title,note,height=210,master=false){const wrap=document.createElement("section");wrap.className="tv-panel";wrap.innerHTML=`<div class="tv-panel-head"><b>${title}</b><small>${note}</small>${master?'<button class="latest-btn" type="button">最新</button>':""}</div><div class="tv-chart" style="height:${height}px"></div>`;$("#chip-workbench").append(wrap);return wrap.querySelector(".tv-chart")}
function wireCharts(){
  chartEntries.forEach((entry,index)=>{entry.chart.subscribeCrosshairMove(param=>{const time=typeof param?.time==="string"?param.time:param?.time;if(time)$("#crosshair-date").textContent=String(time)});if(index>0)entry.chart.timeScale().fitContent()});
  const rows=daily().series||[];if(rows.length){const from=rows[Math.max(0,rows.length-120)].date,to=rows[rows.length-1].date;try{chartEntries[0].chart.timeScale().setVisibleRange({from,to})}catch(_){chartEntries[0].chart.timeScale().fitContent()}}
}
function initChipWorkbench(){
  if(chartsReady)return;chartsReady=true;const root=$("#chip-workbench"),L=window.LightweightCharts;if(!L){root.innerHTML='<div class="method-note">圖表元件載入失敗；請確認網路後重新整理。資料本身仍保留在頁面封包中。</div>';return}
  const packet=daily(),market=packet.market_evidence||{},price=packet.series||[];
  const priceElement=panel("日 K 與成交量","主視窗 · 紅漲綠跌 · 縮放保持最新端",430,true),priceChart=L.createChart(priceElement,baseOptions());
  const candles=priceChart.addSeries(L.CandlestickSeries,{upColor:"#ef5350",downColor:"#26a69a",wickUpColor:"#ef5350",wickDownColor:"#26a69a",borderVisible:false});candles.setData(price.map(row=>({time:row.date,open:row.open,high:row.high,low:row.low,close:row.close})));
  const volume=priceChart.addSeries(L.HistogramSeries,{priceFormat:{type:"volume"},priceScaleId:"volume",lastValueVisible:false,priceLineVisible:false});priceChart.priceScale("volume").applyOptions({scaleMargins:{top:.78,bottom:0}});volume.setData(price.map(row=>({time:row.date,value:row.volume,color:row.close>=row.open?"rgba(239,83,80,.45)":"rgba(38,166,154,.45)"})));addEntry(priceElement,price,{chart:priceChart,series:candles},"close");
  const groups=groupedAnnotations(packet),visibleGroups=visibleAnnotationGroups(packet),byDate=new Map(groups.map(group=>[group.date,group]));currentMarkers=visibleGroups.map(group=>({time:group.date,position:"aboveBar",shape:"circle",color:markerColor(group.primary),text:`${group.primary.display?.short_label||group.primary.label_zh}${group.events.length>1?` +${group.events.length-1}`:""}`}));annotationMarkerApi=L.createSeriesMarkers(candles,currentMarkers,{autoScale:true});setAnnotationVisibility();
  priceChart.subscribeCrosshairMove(param=>{const group=byDate.get(String(param?.time||""));if(group)$("#annotation-detail").textContent=group.events.map(annotationSummary).join(" ｜ ")});
  priceElement.closest(".tv-panel").querySelector(".latest-btn").onclick=()=>priceChart.timeScale().scrollToRealTime();
  priceElement.addEventListener("wheel",()=>{const pinLatest=priceChart.timeScale().scrollPosition()<=1;if(pinLatest)setTimeout(()=>priceChart.timeScale().scrollToPosition(0,false),0)},{capture:true,passive:true});
  if((market.institutional||[]).length){const rows=market.institutional,element=panel("三大法人買賣超","獨立副圖 · 外資／投信／自營商，單位：張"),chart=L.createChart(element,baseOptions());let primary=null;[["foreign","#4da3ff"],["trust","#f6c453"],["dealer","#b98cff"]].forEach(([key,color],index)=>{const series=chart.addSeries(L.LineSeries,{color,lineWidth:2,priceLineVisible:false,lastValueVisible:index===0});series.setData(rows.map(row=>({time:row.date,value:row[key]})));if(!primary)primary=series});addEntry(element,rows,{chart,series:primary},"foreign")}
  if((market.margin||[]).length){const rows=market.margin,element=panel("信用交易","獨立副圖 · 融資餘額／融券餘額"),chart=L.createChart(element,baseOptions()),margin=chart.addSeries(L.LineSeries,{color:"#f6c453",lineWidth:2}),short=chart.addSeries(L.LineSeries,{color:"#b98cff",lineWidth:2});margin.setData(rows.filter(row=>row.margin_balance!=null).map(row=>({time:row.date,value:row.margin_balance})));short.setData(rows.filter(row=>row.short_balance!=null).map(row=>({time:row.date,value:row.short_balance})));addEntry(element,rows,{chart,series:margin},"margin_balance")}
  if((market.foreign_ownership||[]).length){const rows=market.foreign_ownership,element=panel("外資持股","獨立副圖 · 持股比率（%）"),chart=L.createChart(element,baseOptions()),series=chart.addSeries(L.LineSeries,{color:"#4da3ff",lineWidth:2});series.setData(rows.filter(row=>row.foreign_ratio!=null).map(row=>({time:row.date,value:row.foreign_ratio})));addEntry(element,rows,{chart,series},"foreign_ratio")}
  if((market.holdings||[]).length){const rows=market.holdings,element=panel("股東結構","獨立副圖 · 大戶／中實戶／散戶持股比（每週）",230),chart=L.createChart(element,baseOptions());let primary=null;[["major","#ef5350"],["middle","#f6c453"],["retail","#26a69a"]].forEach(([key,color])=>{const series=chart.addSeries(L.LineSeries,{color,lineWidth:2,priceLineVisible:false});series.setData(rows.map(row=>({time:row.date,value:row[key]})));if(!primary)primary=series});addEntry(element,rows,{chart,series:primary},"major")}
  if(chartEntries.length)wireCharts();else root.innerHTML='<div class="method-note">目前沒有可繪製的序列。</div>';
}
})();
