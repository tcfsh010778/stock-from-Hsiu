/* Chart observations and manual MDA review stay independent from entry/exit rules. */
let industryFilter='',chartOffset=0,aiOverlay=null,profileData=null;
function renderMda(){
 const table=detail.mda_table,src=detail.mda.sources||{};
 const trend=src.holder_six_weeks||{},foreign=src.foreign_consecutive||{},margin=src.margin_observation||{};
 $('mda-sources').innerHTML=`<div class="section-top"><h3>M 大：初篩來源 → 逐項檢查</h3><span class="badge warn">${esc(detail.mda.stage)}</span></div><p>${(detail.mda.source_labels||[]).map(x=>`<span class="badge">${esc(x)}</span>`).join(' ')||'未在本次初篩池'}</p><div class="metric-grid"><div class="metric"><span>六週股權增減（需 7 期端點）</span><strong>${trend.status==='measured'?esc(trend.rhythm):'歷史不足'}</strong><small>${trend.points?.length||0} / 7 期 · ${trend.net_change==null?'—':num(trend.net_change)+' 百分點'}</small></div><div class="metric"><span>外資連買</span><strong>${!foreign.latest_known?'待補資料':(foreign.at_least?'至少 ':'')+(foreign.sessions||0)+' 日'}</strong></div><div class="metric"><span>融資較前日（張）</span><strong>${num(margin.latest_change_lots,0)}</strong></div></div><table><thead><tr><th>觀察期間</th><th>融資變化（張）</th><th>增加日數</th><th>同期間股價</th></tr></thead><tbody>${Object.entries(margin.windows||{}).map(([n,w])=>`<tr><td>${n} 日</td><td>${num(w.change_lots,0)}</td><td>${w.increase_sessions??'—'}</td><td>${pct(w.price_change_pct)}</td></tr>`).join('')}</tbody></table><p class="muted">${esc(margin.note||'')} ${esc(margin.identity||'')}</p>`;
 if(!table){$('mda-checklist').innerHTML='<p class="muted">價格未驗證，完整選股表等待資料補齊。</p>';return;}
 const key=`mda-review:${table.version}:${selected}`;let saved={};try{saved=JSON.parse(localStorage.getItem(key)||'{}');}catch{}
 $('mda-checklist').innerHTML=`<p class="muted">${esc(table.note)} 判讀記錄保存在這個瀏覽器，可匯出備份。</p>${saved.data_date&&saved.data_date!==detail.data_date?'<p class="warn">這份人工判讀依據 '+esc(saved.data_date)+' 的資料，尚未依本次 '+esc(detail.data_date)+' 資料重新確認。</p>':''}<p class="muted">記錄依據：${esc(saved.data_date||'尚無記錄')} · 最後編輯 ${esc(saved.updated_at||'—')}</p><div class="table-actions"><button id="export-review">匯出這檔判讀</button><button id="clear-review">清除這檔記錄</button><button id="confirm-review-date">已依本次資料重新核對</button></div>${table.sections.map(section=>`<details class="review-section" ${['A甲','A乙','B1','B2'].includes(section.id)?'open':''}><summary>${esc(section.title)} <span>${section.rows.length} 項</span></summary><div class="review-rows">${section.rows.map(r=>`<div class="review-row"><div><strong>${esc(r.label)}</strong><small class="${r.status==='pass'?'good':r.status==='fail'?'down':'muted'}">${r.status==='pass'?'符合計算条件 · ':r.status==='fail'?'未符合計算條件 · ':''}${esc(r.evidence)}</small></div><select aria-label="${esc(r.label)} 判讀" data-check="${esc(r.id)}"><option value="">未判讀</option>${[['yes','符合'],['no','不符合'],['watch','待觀察'],['na','不適用']].map(([v,t])=>`<option value="${v}" ${saved[r.id]===v?'selected':''}>${t}</option>`).join('')}</select></div>`).join('')}</div></details>`).join('')}<label class="note-label">我的觀察與長期持有理由<textarea id="review-note" rows="4" maxlength="6000">${esc(saved.note||'')}</textarea></label><span class="muted" id="review-saved"></span>`;
 const persist=()=>{saved.note=$('review-note').value;saved.stock_id=selected;saved.table_version=table.version;saved.data_date=saved.data_date||detail.data_date;saved.last_edit_data_date=detail.data_date;saved.updated_at=new Date().toISOString();try{localStorage.setItem(key,JSON.stringify(saved));$('review-saved').textContent='已存於此瀏覽器';}catch{$('review-saved').textContent='儲存空間不可用，請匯出備份';}};
 document.querySelectorAll('[data-check]').forEach(el=>el.onchange=()=>{saved[el.dataset.check]=el.value;persist();});$('review-note').oninput=persist;
 $('export-review').onclick=()=>{persist();const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([JSON.stringify(saved,null,2)],{type:'application/json'}));a.download=`M大-${selected}-${detail.data_date}.json`;a.click();setTimeout(()=>URL.revokeObjectURL(a.href),1000);};
 $('confirm-review-date').onclick=()=>{saved.data_date=detail.data_date;persist();renderMda();};
 $('clear-review').onclick=()=>{try{localStorage.removeItem(key);}catch{}renderMda();};
}
function renderResearchCharts(){
 if(!detail)return;
 const full=detail.candles[frame],count=Number($('chart-range').value);chartOffset=Math.max(0,Math.min(chartOffset,Math.max(0,full.length-20)));
 const bars=full.slice(Math.max(0,full.length-count-chartOffset),full.length-chartOffset);
 if(!bars.length){$('chart').innerHTML='<div class="empty">沒有可驗證的 K 線</div>';return;}
 const width=Math.max(300,$('chart').clientWidth),left=12,right=70,plot=width-left-right,stride=plot/bars.length,x=i=>left+(i+.5)*stride;
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
  if(hist)vals.push(0);let lo=Math.min(...vals),hi=Math.max(...vals);const pad=(hi-lo)*.09||1;lo-=pad;hi+=pad;
  const y=v=>top+(hi-v)/(hi-lo)*ph;
  let svg=`<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="${esc(title)}"><g>`;
  if(price||pts.length){
   for(let j=0;j<=2;j++){const v=lo+(hi-lo)*j/2;svg+=`<path d="M ${left} ${y(v)} H ${width-right}" stroke="var(--line)" stroke-dasharray="2 4"/><text x="${width-right+6}" y="${y(v)+4}" fill="var(--muted)" font-size="10">${num(v,price||['major','retail'].includes(id)?1:0)}</text>`;}
   if(price){
    for(const [k,stroke]of [['ma20','#d7b967'],['ma60','#71a5f6'],['ma240','#ba8de2']]){let d='',started=false;bars.forEach((b,i)=>{if(b[k]!=null){d+=`${started?'L':'M'}${x(i)},${y(b[k])} `;started=true;}else started=false;});svg+=`<path d="${d}" fill="none" stroke="${stroke}" stroke-width="1.2"/>`;}
    bars.forEach((b,i)=>{const xx=x(i),stroke=b.close>=b.open?'var(--red)':'var(--green)',w=Math.max(.65,stride*.6);svg+=`<line x1="${xx}" x2="${xx}" y1="${y(b.high)}" y2="${y(b.low)}" stroke="${stroke}"/><rect x="${xx-w/2}" y="${Math.min(y(b.open),y(b.close))}" width="${w}" height="${Math.max(1,Math.abs(y(b.open)-y(b.close)))}" fill="${stroke}"/>`;});
    if($('show-trends').checked)for(const line of annotation.lines||[]){if(!line.points.every(p=>p.time>=first&&p.time<=last))continue;svg+=`<polyline points="${line.points.map(p=>`${x(bucket(p.time))},${y(p.price)}`).join(' ')}" fill="none" stroke="#c99afa" stroke-width="1.4" stroke-dasharray="5 3"><title>${esc(index.patterns[line.id]||line.id)}</title></polyline>`;}
    const events=$('show-candles').checked?(annotation.events||[]):[];
    const markers=new Map();for(const e of events){if(e.end<first||e.end>last)continue;const i=bucket(e.end);if(i!=null){if(!markers.has(i))markers.set(i,[]);markers.get(i).push(e);}}
    let labelX=-1000;for(const [i,es]of markers){const e=es[0],showLabel=stride>5&&x(i)-labelX>85;if(showLabel)labelX=x(i);svg+=`<g><circle cx="${x(i)}" cy="${Math.max(9,y(bars[i].high)-9)}" r="3" fill="#e0c778"/><title>${esc(es.map(t=>`${t.name} · ${t.end}：${t.evidence}`).join('\n'))}</title>${showLabel?`<text x="${x(i)}" y="${Math.max(10,y(bars[i].high)-16)}" text-anchor="middle" font-size="8" fill="#e0c778">${esc(e.name)}${es.length>1?' +'+(es.length-1):''}</text>`:''}</g>`;}
    if(aiOverlay?.stock_id===selected&&aiOverlay.frequency===frame)for(const e of aiOverlay.observations){if(e.end<first||e.end>last)continue;const i=bucket(e.end);svg+=`<text x="${x(i)}" y="${Math.min(height-bottom,y(bars[i].low)+17)}" text-anchor="middle" font-size="10" fill="#6bcde3">AI · ${esc(index.patterns[e.pattern_id])}<title>${esc(e.evidence)}</title></text>`;}
   }else if(hist){for(const p of pts)svg+=`<rect x="${x(p.i)-Math.max(.7,stride*(id==='volume'?.6:frame==='day'?.6:frame==='week'?.11:.025))/2}" y="${Math.min(y(0),y(p.value))}" width="${Math.max(.7,stride*(id==='volume'?.6:frame==='day'?.6:frame==='week'?.11:.025))}" height="${Math.max(1,Math.abs(y(0)-y(p.value)))}" fill="${id==='volume'?(bars[Math.round(p.i)].close>=bars[Math.round(p.i)].open?'var(--red)':'var(--green)'):(p.value>=0?'var(--red)':'var(--green)')}" opacity=".7"/>`;
   }else{let d='';pts.forEach((p,i)=>{const prev=pts[i-1],gap=prev?(new Date(p.date)-new Date(prev.date))/86400000:Infinity;d+=`${gap<=(['major','retail','shareholders'].includes(id)?10:5)?'L':'M'}${x(p.i)},${y(p.value)} `;svg+=`<circle cx="${x(p.i)}" cy="${y(p.value)}" r="2" fill="var(--accent)"><title>${esc(p.date)} · ${num(p.value)}</title></circle>`;});svg+=`<path d="${d}" fill="none" stroke="var(--accent)" stroke-width="1.5"/>`;}
  }else svg+=`<text x="${width/2}" y="${height/2}" text-anchor="middle" fill="var(--muted)" font-size="11">${id==='trust'?'尚無持股總張數來源；買賣超不冒充持股餘額':'此區間沒有可驗證資料'}</text>`;
  for(const i of [0,Math.floor(bars.length/2),bars.length-1])svg+=`<text x="${x(i)}" y="${height-5}" text-anchor="${i===0?'start':i===bars.length-1?'end':'middle'}" fill="var(--muted)" font-size="9">${bars[i].time.slice(2)}</text>`;
  svg+=`<line class="sync-crosshair" y1="0" y2="${height-bottom}" stroke="var(--muted)" stroke-dasharray="3 4" visibility="hidden"/></g></svg>`;
  content+=`<section class="chart-pane" data-chart-pane="${id}"><div class="pane-title"><span>${esc(title)}</span><small>${price?(bars.at(-1).complete?'':'末根未完成'):pts.length?`${pts.length} 期 · 最後 ${pts.at(-1).date}`:'待補'} <b data-pane-value="${id}"></b></small></div>${svg}</section>`;
 }
 $('chart').innerHTML=content;$('chart-meta').textContent=`${bars.length} 根 · ${first} → ${last}`;
 $('chart').onpointermove=e=>{const cursor=Math.max(0,Math.min(bars.length-1,(e.clientX-$('chart').getBoundingClientRect().left-left)/stride-.5)),i=Math.round(cursor);document.querySelectorAll('.sync-crosshair').forEach(line=>{line.setAttribute('x1',x(cursor));line.setAttribute('x2',x(cursor));line.setAttribute('visibility','visible');});$('hover-value').textContent=`K 棒截至 ${bars[i].time} 開 ${num(bars[i].open)} 高 ${num(bars[i].high)} 低 ${num(bars[i].low)} 收 ${num(bars[i].close)}`;for(const [,pts,,id]of panels){const nearest=pts.reduce((best,p)=>!best||Math.abs(p.i-cursor)<Math.abs(best.i-cursor)?p:best,null),v=nearest&&Math.abs(nearest.i-cursor)*stride<=8?nearest:null;document.querySelector(`[data-pane-value="${id}"]`).textContent=v?`${v.date} · ${num(v.value,1)}`:'';}};
 $('chart').onpointerleave=()=>{document.querySelectorAll('.sync-crosshair').forEach(l=>l.setAttribute('visibility','hidden'));$('hover-value').textContent='';};
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
 $('chart-back').onclick=()=>{chartOffset+=Math.floor(Number($('chart-range').value)/2);renderChart();};$('chart-forward').onclick=()=>{chartOffset=Math.max(0,chartOffset-Math.floor(Number($('chart-range').value)/2));renderChart();};
 $('focus-chart').onclick=()=>{document.body.classList.toggle('chart-focus');$('focus-chart').textContent=document.body.classList.contains('chart-focus')?'回到清單':'展開圖表';window.scrollTo({top:0,behavior:'smooth'});renderChart();};
 $('mda-source').onchange=renderList;$('margin-behavior').onchange=renderList;for(const id of ['heat-window','heat-metric','heat-chain','heat-direction'])$(id).onchange=renderHeatmap;
 $('industry-clear').onclick=()=>{industryFilter='';renderList();renderHeatmap();};
}
setupResearch();
