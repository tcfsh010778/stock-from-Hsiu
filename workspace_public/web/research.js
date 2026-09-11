/* Chart observations and manual MDA review stay independent from entry/exit rules. */
let industryFilter='',chartOffset=0,chartCount=120,chartGeometry=null,chartFrameRequest=0,chartDrag=null,aiOverlay=null,profileData=null;
const chartYZoom={};
function renderMacro(){
 const macro=index?.macro;if(!macro)return;
 let saved={};try{saved=JSON.parse(localStorage.getItem('market-context:1')||'{}');}catch{}
 const configured=Array.isArray(saved.items)?saved.items:macro.items.map(r=>({id:r.id,label:r.label,enabled:true}));
 const items=configured.filter(r=>r&&typeof r.id==='string'&&typeof r.label==='string').slice(0,30);
 $('macro-date').textContent=`全站共用 · ${macro.data_date} · AI 尚未判讀`;
 $('macro-content').innerHTML=items.filter(r=>r.enabled!==false).map(r=>{const fact=macro.items.find(x=>x.id===r.id&&x.label===r.label);return `<div class="macro-item"><strong>${esc(r.label)}</strong><small>${esc(fact?.evidence||'自訂觀察項目；待接資料與 AI 判讀')}</small></div>`;}).join('');
 $('macro-items').value=items.filter(r=>r.enabled!==false).map(r=>r.label).join('\n');
 $('macro-save').onclick=()=>{const labels=[...new Set($('macro-items').value.split('\n').map(s=>s.trim()).filter(Boolean))].slice(0,30);if(!labels.length){$('macro-status').textContent='請至少保留一項觀察';return;}const next=labels.map((label,i)=>({id:macro.items.find(r=>r.label===label)?.id||`custom:${i}`,label:label.slice(0,60),enabled:true}));try{localStorage.setItem('market-context:1',JSON.stringify({items:next,updated_at:new Date().toISOString()}));renderMacro();$('macro-status').textContent='觀察項目已更新，套用所有股票';}catch{$('macro-status').textContent='瀏覽器儲存空間不可用';}};
 $('macro-reset').onclick=()=>{try{localStorage.removeItem('market-context:1');}catch{}renderMacro();};
}
function conditionStatus(v){return v===true?'符合':v===false?'不符合':'未知';}
function downloadJSON(value,name){const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([JSON.stringify(value,null,2)],{type:'application/json'}));a.download=name;a.click();setTimeout(()=>URL.revokeObjectURL(a.href),1000);}
function renderMda(){
 const table=detail.mda_table,src=detail.mda.sources||{},gate=detail.mda.familiar_pattern||{};
 const trend=src.holder_six_weeks||{},foreign=src.foreign_consecutive||{},trust=src.trust_consecutive||{};
 $('mda-sources').innerHTML=`<div class="section-top"><h3>M 大觀察條件</h3><span class="badge ${detail.mda.member?'good':'warn'}">${esc(detail.mda.stage)}</span></div><p>${esc(detail.mda.note)}</p><div class="metric-grid"><div class="metric"><span>籌碼交集</span><strong>${detail.mda.initial_pool_member?'符合':'未成立'}</strong><small>六週股權 ${trend.net_change==null?'未知':num(trend.net_change)+' 百分點'} · 外資 ${foreign.sessions??'—'} 日／投信 ${trust.sessions??'—'} 日連買</small></div>${['A','X'].map(k=>`<div class="metric"><span>${k==='A'?'A · 長多均線':'X · 跌勢轉變'}</span><strong class="${gate[k]?.decision===true?'good':'muted'}">${conditionStatus(gate[k]?.decision)}</strong><small>${esc(gate[k]?.method||'等待價格資料')}</small></div>`).join('')}</div><details><summary>查看 A／X 實際數值與定義</summary><pre class="condition-metrics">${esc(JSON.stringify(gate,null,2))}</pre></details>`;
 if(!table){$('mda-checklist').innerHTML='<p class="muted">完整選股表等待資料補齊。</p>';return;}
 const rows=table.sections.flatMap(s=>s.rows),matched=rows.filter(r=>r.status==='pass');
 const legacy={};for(const v of ['mda-stock-table-20260910-v3','mda-full-table-20260910-v2']){try{const value=JSON.parse(localStorage.getItem(`mda-review:${v}:${selected}`)||'null');if(value)legacy[v]=value;}catch{}}
 const noteKey=`mda-notes:1:${selected}`;let saved={};try{saved=JSON.parse(localStorage.getItem(noteKey)||'{}');}catch{}
 $('mda-checklist').innerHTML=`<div class="section-top"><h3>逐項條件結果</h3><span class="muted">${esc(table.data_date)} · 自動計算</span></div><p class="muted">${esc(table.note)}</p><div class="matched-conditions">${matched.map(r=>`<span class="badge good">${esc(r.id)} ${esc(r.label)}</span>`).join(' ')||'<span class="muted">目前沒有已證實符合的條件。</span>'}</div><div class="table-actions"><label>顯示<select id="condition-display"><option value="all">全部條件</option><option value="pass">符合</option><option value="fail">不符合</option><option value="unknown">未知／尚無定義</option></select></label><button id="export-review">匯出條件結果</button>${Object.keys(legacy).length?'<button id="export-legacy">備份舊人工記錄</button>':''}</div>${table.sections.map(section=>`<details class="review-section" open><summary>${esc(section.title)}</summary><div class="review-rows">${section.rows.map(r=>`<div class="review-row automated-condition" data-condition="${esc(r.id)}" data-result="${esc(r.status)}"><div><strong>${esc(r.label)}</strong><small>${esc(r.method)}</small>${r.reason?`<small class="muted">${esc(r.reason)}</small>`:''}<details class="condition-detail"><summary>數值與依據</summary><p class="muted">${esc(r.basis==='engineering'?'工程定義：數值窗為可重現的觀察設定，並非教材唯一公式':r.basis==='not_defined'?'尚未定義可檢證條件':'原條件的客觀量測')}</p><pre class="condition-metrics">${esc(JSON.stringify(r.metrics,null,2))}</pre></details></div><span class="badge ${r.status==='pass'?'good':r.status==='unknown'?'warn':''}">${conditionStatus(r.decision)}</span></div>`).join('')}</div></details>`).join('')}<details class="optional-notes"><summary>補充筆記（選填）${Object.keys(legacy).length?' · 舊人工勾選已保留，可備份':''}</summary><p class="muted">筆記不改變自動條件或觀察名單。</p><textarea id="review-note" rows="3" maxlength="6000" aria-label="選填觀察筆記">${esc(saved.note||'')}</textarea><button id="clear-review">清除筆記</button><span class="muted" id="review-saved"></span></details>`;
 const persist=()=>{saved={note:$('review-note').value,stock_id:selected,data_date:detail.data_date,updated_at:new Date().toISOString()};try{localStorage.setItem(noteKey,JSON.stringify(saved));$('review-saved').textContent='已儲存';}catch{$('review-saved').textContent='儲存空間不可用，請匯出備份';}};
 $('review-note').oninput=persist;
 $('condition-display').onchange=()=>{const v=$('condition-display').value;document.querySelectorAll('[data-condition]').forEach(el=>el.hidden=v!=='all'&&el.dataset.result!==v);document.querySelectorAll('.review-section').forEach(el=>el.hidden=![...el.querySelectorAll('[data-condition]')].some(row=>!row.hidden));};
 $('export-review').onclick=()=>{persist();downloadJSON({stock_id:selected,data_date:detail.data_date,price_sha256:detail.price_basis.csv_sha256,observation:detail.mda,conditions:table,note:saved.note},`M大條件-${selected}-${detail.data_date}.json`);};
 if($('export-legacy'))$('export-legacy').onclick=()=>downloadJSON({stock_id:selected,legacy},`M大舊人工記錄-${selected}.json`);
 $('clear-review').onclick=()=>{$('review-note').value='';persist();};
}
function renderResearchCharts(){
 if(!detail)return;
 const full=detail.candles[frame];chartCount=Math.max(Math.min(8,full.length),Math.min(chartCount,full.length));chartOffset=Math.max(0,Math.min(chartOffset,Math.max(0,full.length-chartCount)));
 const logicalStart=full.length-chartOffset-chartCount,start=Math.max(0,Math.floor(logicalStart)),end=Math.min(full.length,Math.ceil(full.length-chartOffset));
 const bars=full.slice(start,end);
 if(!bars.length){$('chart').innerHTML='<div class="empty">沒有可驗證的 K 線</div>';return;}
 const width=Math.max(300,$('chart').clientWidth),left=12,right=70,plot=width-left-right,stride=plot/chartCount,x=i=>left+(i+start-logicalStart+.5)*stride;
 chartGeometry={width,left,right,plot,stride,start,logicalStart,total:full.length};
 const first=bars[0].time,last=bars.at(-1).time,annotation=detail.annotations?.[frame]||{};
 const research=detail.research||{},major=$('major-band').value,retail=$('retail-band').value;
 const own=research.ownership||[],chips=detail.chips.series||[];
 function bucket(d){let i=bars.findIndex(b=>b.time>=d);if(i<0)return null;const prior=i?bars[i-1].time:(frame==='day'?first:new Date(new Date(first+'T00:00:00Z')-(frame==='week'?7:32)*86400000).toISOString().slice(0,10));return d>=prior?i:null;}
 function points(rows,key='value'){const out=[];for(const r of rows){if(r.date<first||r.date>last)continue;let i=bucket(r.date);const value=typeof key==='function'?key(r):r[key];if(i!=null&&value!=null&&Number.isFinite(value)){if(i>0&&r.date!==bars[i].time)i=i-1+(new Date(r.date)-new Date(bars[i-1].time))/(new Date(bars[i].time)-new Date(bars[i-1].time));out.push({i,date:r.date,value});}}return out;}
 const panels=[['K 線 · 還原價格',[],false,'price'],['成交量（張）',bars.map((b,i)=>({i,date:b.time,value:b.volume/1000})),true,'volume'],
 ['外資持股（張）',points(research.foreign||[]),false,'foreign'],['外資逐日買賣超（張）',points(chips,r=>r.foreign_net==null?null:r.foreign_net/1000),true,'foreign-net'],
 ['投信持股（張）',points(research.trust_holdings||[]),false,'trust'],['融資餘額（張）',points(research.margin||[]),false,'margin'],
 [`大戶持股比例 · ${major}+ 張`,points(own,r=>r.major?.[major]),false,'major'],[`散戶持股比例 · ${retail} 張以下`,points(own,r=>r.retail?.[retail]),false,'retail'],
 ['股東總人數',points(own,'shareholders'),false,'shareholders']];
 let content='';
 for(const [title,pts,hist,id] of panels){
  const price=id==='price',height=price?340:112,top=18,bottom=22,ph=height-top-bottom;
  let vals=price?bars.flatMap(b=>[b.low,b.high,...['ma20','ma60','ma240'].map(k=>b[k]).filter(v=>v!=null)]):pts.map(p=>p.value);
  if(hist)vals.push(0);let lo=Math.min(...vals),hi=Math.max(...vals);const pad=(hi-lo)*.09||1;lo-=pad;hi+=pad;if(id==='volume')lo=0;
  const center=(hi+lo)/2,span=(hi-lo)/(chartYZoom[id]||1);lo=center-span/2;hi=center+span/2;if(id==='volume')lo=0;
  const y=v=>top+(hi-v)/(hi-lo)*ph;
  let svg=`<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="${esc(title)}"><g>`;
  if(price||pts.length){
   for(let j=0;j<=2;j++){const v=lo+(hi-lo)*j/2;svg+=`<path d="M ${left} ${y(v)} H ${width-right}" stroke="var(--line)" stroke-dasharray="2 4"/><text x="${width-right+6}" y="${y(v)+4}" fill="var(--muted)" font-size="10">${num(v,['major','retail'].includes(id)?2:price?1:0)}</text>`;}
   if(price){
    for(const [k,stroke]of [['ma20','#d7b967'],['ma60','#71a5f6'],['ma240','#ba8de2']]){let d='',started=false;bars.forEach((b,i)=>{if(b[k]!=null){d+=`${started?'L':'M'}${x(i)},${y(b[k])} `;started=true;}else started=false;});svg+=`<path d="${d}" fill="none" stroke="${stroke}" stroke-width="1.2"/>`;}
    bars.forEach((b,i)=>{const xx=x(i),stroke=b.close>=b.open?'var(--red)':'var(--green)',w=Math.max(.65,stride*.6);svg+=`<line x1="${xx}" x2="${xx}" y1="${y(b.high)}" y2="${y(b.low)}" stroke="${stroke}"/><rect x="${xx-w/2}" y="${Math.min(y(b.open),y(b.close))}" width="${w}" height="${Math.max(1,Math.abs(y(b.open)-y(b.close)))}" fill="${stroke}"/>`;});
    if($('show-trends').checked)for(const line of annotation.lines||[]){if(!line.points.every(p=>p.time>=first&&p.time<=last))continue;svg+=`<polyline points="${line.points.map(p=>`${x(bucket(p.time))},${y(p.price)}`).join(' ')}" fill="none" stroke="#c99afa" stroke-width="1.4" stroke-dasharray="5 3"><title>${esc(index.patterns[line.id]||line.id)}</title></polyline>`;}
    const events=$('show-candles').checked&&frame==='day'?(annotation.events||[]):[];
    const markers=new Map();for(const e of events){if(e.end<first||e.end>last)continue;const i=bucket(e.end);if(i!=null){if(!markers.has(i))markers.set(i,[]);markers.get(i).push(e);}}
    let labelX=-1000;for(const [i,es]of markers){const e=es[0],showLabel=stride>5&&x(i)-labelX>85;if(showLabel)labelX=x(i);svg+=`<g data-candle-marker><circle cx="${x(i)}" cy="${Math.max(9,y(bars[i].high)-9)}" r="3" fill="#e0c778"/><title>${esc(es.map(t=>`${t.name} · ${t.end}：${t.evidence}\n${t.engine||''} · ${JSON.stringify(t.metrics||{})}`).join('\n'))}</title>${showLabel?`<text x="${x(i)}" y="${Math.max(10,y(bars[i].high)-16)}" text-anchor="middle" font-size="8" fill="#e0c778">${esc(e.name)}${es.length>1?' +'+(es.length-1):''}</text>`:''}</g>`;}
    if(aiOverlay?.stock_id===selected&&aiOverlay.frequency===frame)for(const e of aiOverlay.observations){if(e.end<first||e.end>last)continue;const i=bucket(e.end);svg+=`<text x="${x(i)}" y="${Math.min(height-bottom,y(bars[i].low)+17)}" text-anchor="middle" font-size="10" fill="#6bcde3">AI · ${esc(index.patterns[e.pattern_id])}<title>${esc(e.evidence)}</title></text>`;}
   }else if(hist){for(const p of pts)svg+=`<rect x="${x(p.i)-Math.max(.7,stride*(id==='volume'?.6:frame==='day'?.6:frame==='week'?.11:.025))/2}" y="${Math.min(y(0),y(p.value))}" width="${Math.max(.7,stride*(id==='volume'?.6:frame==='day'?.6:frame==='week'?.11:.025))}" height="${Math.max(1,Math.abs(y(0)-y(p.value)))}" fill="${id==='volume'?(bars[Math.round(p.i)].close>=bars[Math.round(p.i)].open?'var(--red)':'var(--green)'):(p.value>=0?'var(--red)':'var(--green)')}" opacity=".7"/>`;
   }else{let d='';pts.forEach((p,i)=>{const prev=pts[i-1],gap=prev?(new Date(p.date)-new Date(prev.date))/86400000:Infinity;d+=`${gap<=(['major','retail','shareholders'].includes(id)?10:5)?'L':'M'}${x(p.i)},${y(p.value)} `;svg+=`<circle cx="${x(p.i)}" cy="${y(p.value)}" r="2" fill="var(--accent)"><title>${esc(p.date)} · ${num(p.value)}</title></circle>`;});svg+=`<path d="${d}" fill="none" stroke="var(--accent)" stroke-width="1.5"/>`;}
  }else svg+=`<text x="${width/2}" y="${height/2}" text-anchor="middle" fill="var(--muted)" font-size="11">${id==='trust'?'投信估計持股待匯入（XQ 等來源）；非每日公告餘額':'此區間沒有可驗證資料'}</text>`;
  for(const i of [0,Math.floor(bars.length/2),bars.length-1])svg+=`<text x="${x(i)}" y="${height-5}" text-anchor="${i===0?'start':i===bars.length-1?'end':'middle'}" fill="var(--muted)" font-size="9">${bars[i].time.slice(2)}</text>`;
  svg+=`<line class="sync-crosshair" y1="0" y2="${height-bottom}" stroke="var(--muted)" stroke-dasharray="3 4" visibility="hidden"/></g></svg>`;
  content+=`<section class="chart-pane" data-chart-pane="${id}"><div class="pane-title"><span>${esc(title)}</span><small>${price?(bars.at(-1).complete?'':'末根未完成'):pts.length?`${pts.length} 期 · 最後 ${pts.at(-1).date}`:'待補'} <b data-pane-value="${id}"></b></small></div>${svg}</section>`;
 }
 $('chart').innerHTML=content;$('chart-meta').textContent=`${Math.round(chartCount)} 根 · ${first} → ${last}`;
 $('chart-back').disabled=chartOffset>=full.length-chartCount-.01;$('chart-forward').disabled=chartOffset<.01;
 $('chart').dataset.visibleStart=first;$('chart').dataset.visibleEnd=last;$('chart').dataset.visibleCount=String(chartCount);
 $('chart').onpointermove=e=>{if(chartDrag){moveChartDrag(e);return;}const cursor=Math.max(0,Math.min(bars.length-1,(e.clientX-$('chart').getBoundingClientRect().left-left)/stride-.5+logicalStart-start)),i=Math.round(cursor);document.querySelectorAll('.sync-crosshair').forEach(line=>{line.setAttribute('x1',x(cursor));line.setAttribute('x2',x(cursor));line.setAttribute('visibility','visible');});$('hover-value').textContent=`K 棒截至 ${bars[i].time} 開 ${num(bars[i].open)} 高 ${num(bars[i].high)} 低 ${num(bars[i].low)} 收 ${num(bars[i].close)}`;for(const [,pts,,id]of panels){const nearest=pts.reduce((best,p)=>!best||Math.abs(p.i-cursor)<Math.abs(best.i-cursor)?p:best,null),v=nearest&&Math.abs(nearest.i-cursor)*stride<=8?nearest:null;document.querySelector(`[data-pane-value="${id}"]`).textContent=v?`${v.date} · ${num(v.value,1)}`:'';}};
 $('chart').onpointerleave=()=>{document.querySelectorAll('.sync-crosshair').forEach(l=>l.setAttribute('visibility','hidden'));$('hover-value').textContent='';};
}
function queueChart(){if(!chartFrameRequest)chartFrameRequest=requestAnimationFrame(()=>{chartFrameRequest=0;renderChart();});}
function zoomChart(factor,fraction=.5){
 if(!chartGeometry)return;
 const g=chartGeometry;
 // Preserve the newest edge only while already following it (PR27).
 if(chartOffset<.01)fraction=1;
 const anchor=g.total-chartOffset-chartCount+fraction*chartCount;
 chartCount=Math.max(Math.min(8,g.total),Math.min(g.total,chartCount*factor));
 chartOffset=Math.max(0,Math.min(g.total-chartCount,g.total-anchor-(1-fraction)*chartCount));
 $('chart-range').value='custom';queueChart();
}
function moveChartDrag(e){
 if(!chartDrag||chartDrag.pointerId!==e.pointerId)return;
 if(chartDrag.axis)chartYZoom[chartDrag.axis]=Math.max(.2,Math.min(8,chartDrag.zoom*Math.exp((e.clientY-chartDrag.y)/180)));
 else chartOffset=Math.max(0,Math.min(chartGeometry.total-chartCount,chartDrag.offset+(e.clientX-chartDrag.x)/chartDrag.stride));
 queueChart();
}
function resetChart(){chartCount=$('chart-range').value==='all'?detail?.candles[frame].length||120:Number($('chart-range').value)||120;chartOffset=0;for(const key of Object.keys(chartYZoom))delete chartYZoom[key];queueChart();}
function setupChartGestures(){
 const chart=$('chart');chart.tabIndex=0;
 chart.addEventListener('wheel',e=>{if(!chartGeometry||!e.target.closest('svg'))return;e.preventDefault();const g=chartGeometry,xx=e.clientX-chart.getBoundingClientRect().left;
  if(xx>g.width-g.right){const id=e.target.closest('[data-chart-pane]').dataset.chartPane;chartYZoom[id]=Math.max(.2,Math.min(8,(chartYZoom[id]||1)*Math.exp(-Math.max(-150,Math.min(150,e.deltaY))*.003)));queueChart();}
  else if(e.target.closest('[data-chart-pane]')?.dataset.chartPane==='price'&&(Math.abs(e.deltaX)>Math.abs(e.deltaY)||e.shiftKey)){chartOffset=Math.max(0,Math.min(g.total-chartCount,chartOffset+(e.deltaX||e.deltaY)/g.stride));queueChart();}
  else if(e.target.closest('[data-chart-pane]')?.dataset.chartPane==='price')zoomChart(Math.exp(Math.max(-160,Math.min(160,e.deltaY))*.002),Math.max(0,Math.min(1,(xx-g.left)/g.plot)));
 },{passive:false});
 chart.onpointerdown=e=>{if(!chartGeometry||e.button!==0||!e.target.closest('svg'))return;const g=chartGeometry,id=e.target.closest('[data-chart-pane]').dataset.chartPane;if(id!=='price'&&e.clientX-chart.getBoundingClientRect().left<=g.width-g.right)return;chartDrag={pointerId:e.pointerId,x:e.clientX,y:e.clientY,stride:g.stride,offset:chartOffset,axis:e.clientX-chart.getBoundingClientRect().left>g.width-g.right?id:null,zoom:chartYZoom[id]||1};chart.setPointerCapture(e.pointerId);chart.classList.add('dragging');};
 const end=e=>{if(chartDrag?.pointerId===e.pointerId){chartDrag=null;chart.classList.remove('dragging');if(chart.hasPointerCapture(e.pointerId))chart.releasePointerCapture(e.pointerId);}};
 chart.onpointerup=end;chart.onpointercancel=end;chart.onlostpointercapture=end;
 chart.ondblclick=e=>{if(e.target.closest('svg')){e.preventDefault();$('chart-range').value='120';resetChart();}};
 chart.onkeydown=e=>{if(!chartGeometry)return;if(['ArrowLeft','ArrowRight','+','=','-','Home','End'].includes(e.key)){e.preventDefault();if(e.key==='ArrowLeft')chartOffset+=chartCount*.15;else if(e.key==='ArrowRight')chartOffset-=chartCount*.15;else if(e.key==='Home')chartOffset=chartGeometry.total-chartCount;else if(e.key==='End')chartOffset=0;else{zoomChart(e.key==='-'?1.2:1/1.2);return;}queueChart();}};
 $('chart-zoom-in').onclick=()=>zoomChart(1/1.3);$('chart-zoom-out').onclick=()=>zoomChart(1.3);
 $('chart-reset').onclick=()=>{$('chart-range').value='120';resetChart();};
}
function renderHeatmap(){
 if(!index)return;const window=$('heat-window').value,metric=$('heat-metric').value,groups=new Map();
 for(const stock of index.stocks)for(const tag of stock.industry||[]){if(!groups.has(tag.id))groups.set(tag.id,{...tag,stocks:[],values:[]});const g=groups.get(tag.id);g.stocks.push(stock);let value=null;
  if(metric==='change')value=stock.change_pct;
  else if(stock.chips[window]?.complete&&stock.close!=null)value=stock.chips[window].values.institutional_total_net*stock.close/1e8;
  if(value!=null)g.values.push(value);
 }
 const chain=$('heat-chain').value,direction=$('heat-direction').value;
 const rows=[...groups.values()].filter(g=>!chain||g.chain===chain).map(g=>({...g,value:g.values.length?g.values.reduce((a,b)=>a+b,0)/(metric==='change'?g.values.length:1):null})).filter(g=>!direction||(direction==='in'?g.value>0:g.value<0)).sort((a,b)=>Math.abs(b.value||0)-Math.abs(a.value||0));
 $('heatmap').innerHTML=rows.map(g=>`<button class="heat-cell ${industryFilter===g.id?'chosen':''}" data-industry="${esc(g.id)}" style="background:${g.value==null?'var(--panel2)':g.value>=0?'rgba(185,65,81,'+Math.min(.7,.16+Math.abs(g.value)/(metric==='change'?12:100))+')':'rgba(32,132,109,'+Math.min(.7,.16+Math.abs(g.value)/(metric==='change'?12:100))+')'}"><small>${esc(g.chain)}</small><strong>${esc(g.name)}</strong><b>${g.value==null?'—':num(g.value)+(metric==='change'?'%':' 億')}</b><small>${g.values.length}/${g.stocks.length} 檔有資料</small></button>`).join('');
 $('industry-clear').textContent=industryFilter?'清除產業篩選 · '+(groups.get(industryFilter)?.name||''):'全部產業';
 $('heatmap').onclick=e=>{const cell=e.target.closest('[data-industry]');if(cell){industryFilter=cell.dataset.industry;route='all';document.querySelectorAll('[data-route]').forEach(b=>b.classList.toggle('active',b.dataset.route===route));$('search').value='';renderList();renderHeatmap();}};
}
function setupResearch(){
 for(const id of ['major-band','retail-band','show-candles','show-trends'])$(id).onchange=renderChart;
 $('chart-back').onclick=()=>{chartOffset+=chartCount/2;queueChart();};$('chart-forward').onclick=()=>{chartOffset=Math.max(0,chartOffset-chartCount/2);queueChart();};
 $('focus-chart').onclick=()=>{document.body.classList.toggle('chart-focus');$('focus-chart').textContent=document.body.classList.contains('chart-focus')?'回到清單':'展開圖表';window.scrollTo({top:0,behavior:'smooth'});renderChart();};
 $('mda-source').onchange=renderList;$('condition-filter').onchange=renderList;$('ax-filter').onchange=renderList;$('margin-behavior').onchange=renderList;for(const id of ['heat-window','heat-metric','heat-chain','heat-direction'])$(id).onchange=renderHeatmap;
 $('industry-clear').onclick=()=>{industryFilter='';renderList();renderHeatmap();};
}
setupResearch();
setupChartGestures();
