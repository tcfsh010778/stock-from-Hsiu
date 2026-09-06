(function(){'use strict';
const $=s=>document.querySelector(s),NS='http://www.w3.org/2000/svg';
const svg=(tag,attrs={},text)=>{const e=document.createElementNS(NS,tag);for(const[k,v]of Object.entries(attrs))e.setAttribute(k,v);if(text!==undefined)e.textContent=text;return e;};
let packet=null,input=[],kind='minute',source='',profile=null,sourceMeta=null;
const fmt=v=>Number(v).toLocaleString('zh-TW',{maximumFractionDigits:2});
function message(s){$('#vp-status').textContent=s;}
function useRows(rows,type,label){input=VolumeProfile.validate(rows,type);kind=type;source=label;const dates=input.map(r=>r.date).sort();$('#vp-start').value=dates[0];$('#vp-end').value=dates.at(-1);for(const key of ['#vp-start','#vp-end']){$(key).min=dates[0];$(key).max=dates.at(-1);}draw();}
function adjustedRows(){
 const start=$('#vp-start').value,end=$('#vp-end').value;
 if($('#vp-price-mode').value==='raw')return input.filter(r=>r.date>=start&&r.date<=end);
 const prices=new Map((packet.series||[]).map(r=>[r.date,r]));
 if(!packet.price_adjustment?.verified)throw Error('價格口徑尚未驗證；完成歷史重建後才能與還原日 K 對齊');
 return input.filter(r=>r.date>=start&&r.date<=end).map(r=>{const p=prices.get(r.date),factor=Number(p?.adjustment_factor);if(!p||!(factor>0))throw Error('選定資料超出可驗證還原因子的日期範圍');const out={...r};for(const k of kind==='ticks'?['price']:['open','high','low','close'])out[k]*=factor;return out;});
}
function draw(){
 const root=$('#vp-chart');root.replaceChildren();profile=null;
 try{
  if(!input.length){message('尚無分鐘／逐筆資料。可匯入自己的 CSV；日 K 不會自動轉成逐價成交量。');return;}
  const rows=adjustedRows();profile=VolumeProfile.calculate(rows,{kind,start:$('#vp-start').value,end:$('#vp-end').value,bins:Number($('#vp-bins').value),valueArea:Number($('#vp-va').value)/100});
  const p=profile,W=1120,H=480,l=60,r=100,t=28,b=44,span=Math.max(.00001,p.high-p.low),y=v=>t+(p.high-v)/span*(H-t-b),right=W-r;
  const rawMode=$('#vp-price-mode').value==='raw';
  const candleSource=rawMode?Object.entries(sourceMeta?.observed_daily_ohlc||{}).map(([date,row])=>({date,...row})):(packet.series||[]);
  const candles=candleSource.filter(r=>r.date>=p.start&&r.date<=p.end),cw=(W-l-r)*.61,x=i=>l+(i+.5)/Math.max(1,candles.length)*cw;
  root.append(svg('rect',{x:l,y:y(p.vah),width:W-l-r,height:y(p.val)-y(p.vah),fill:'#284761',opacity:.22}));
  for(let i=0;i<=5;i++){const price=p.low+span*i/5,yy=y(price);root.append(svg('line',{x1:l,x2:right,y1:yy,y2:yy,stroke:'#202a3a'}),svg('text',{x:right+8,y:yy+4,class:'axis'},fmt(price)));}
  // Show only daily candles fitting this profile range; the profile uses the same adjustment factors.
  candles.forEach((r,i)=>{const color=r.close>=r.open?'#ef5350':'#26a69a',xx=x(i);root.append(svg('line',{x1:xx,x2:xx,y1:y(Math.min(p.high,r.high)),y2:y(Math.max(p.low,r.low)),stroke:color}));const top=y(Math.min(p.high,Math.max(r.open,r.close))),bot=y(Math.max(p.low,Math.min(r.open,r.close)));if(bot>=top)root.append(svg('rect',{x:xx-3,y:top,width:6,height:Math.max(1,bot-top),fill:color}));if(candles.length<=12||i%Math.ceil(candles.length/8)===0)root.append(svg('text',{x:xx-23,y:H-17,class:'axis'},r.date.slice(5)));});
  const max=Math.max(...p.bins.map(b=>b.volume));p.bins.forEach(bin=>{const width=bin.volume/max*(W-l-r)*.32,bar=svg('rect',{x:right-width,y:y(bin.high),width,height:Math.max(.5,y(bin.low)-y(bin.high)-1),fill:bin.isPoc?'#f6c453':bin.inValueArea?'#4da3ff':'#45637f'});bar.append(svg('title',{},`${fmt(bin.low)}–${fmt(bin.high)}：${fmt(bin.volume)} 股`));root.append(bar);});
  for(const[name,value,color]of [['POC',p.poc,'#f6c453'],['VAH',p.vah,'#4da3ff'],['VAL',p.val,'#4da3ff']])root.append(svg('line',{x1:l,x2:right,y1:y(value),y2:y(value),stroke:color,'stroke-dasharray':'5 4'}),svg('text',{x:l+5,y:y(value)-4,fill:color,'font-size':12},`${name} ${fmt(value)}`));
  const note=kind==='ticks'?'逐筆成交按價格分箱':kind==='bins'?'5 分鐘 K 均勻分配 → 每日分箱 → 再分箱推估':'分鐘 K 區間均勻分配推估（不是逐筆成交真值）';
  message(`${source}｜${note}｜${p.start} 至 ${p.end}｜觀測成交量 ${fmt(p.totalVolume)} 股｜價值區實際 ${fmt(p.valueAreaActual*100)}%｜${rawMode?'原始成交價；左側為可得分鐘資料合成日 K':'與日 K 相同還原價口徑'}${sourceMeta?.partial?'｜資料不完整：不是全市場成交分布':''}`);
  const byDate=new Map();for(const row of p.rows)byDate.set(row.date,(byDate.get(row.date)||0)+row.volume);
  const official=packet.price_adjustment?.verified?new Map((packet.series||[]).map(r=>[r.date,r.volume])):new Map();
  $('#vp-coverage').textContent=[...byDate].map(([date,volume])=>`${date}：觀測 ${fmt(volume)} 股 / ${official.has(date)?`官方日量 ${fmt(official.get(date))} 股（${fmt(volume/official.get(date)*100)}%）`:'官方日量尚未核對'}${sourceMeta?.observations_by_date?`，${sourceMeta.observations_by_date[date]||0} 根來源 K 棒`:''}`).join('；');
  $('#vp-values').textContent=`POC ${fmt(p.poc)}　VAH ${fmt(p.vah)}　VAL ${fmt(p.val)}。橫條長度代表累積成交量；黃色為最大量價格分箱，不是買賣訊號。`;
 }catch(e){message(e.message);$('#vp-values').textContent='資料不足；不繪製無法驗證的成交分布。';}
}
window.addEventListener('stock-packets-ready',async event=>{
 packet=event.detail.find(p=>p.timeframe==='daily');
 for(const selector of ['#vp-start','#vp-end','#vp-bins','#vp-va','#vp-price-mode']){ $(selector).addEventListener('change',draw); $(selector).addEventListener('input',draw); }
 $('#vp-file').addEventListener('change',async event=>{try{const file=event.target.files[0];if(!file)return;if(file.size>25*1024*1024)throw Error('CSV 上限 25 MB');const type=$('#vp-kind').value,rows=VolumeProfile.csv(await file.text(),type,packet.stock_id);sourceMeta=null;useRows(rows,type,'本機 CSV（不會上傳；完整性由匯入來源決定）');}catch(e){message(e.message);}});
 $('#vp-export').addEventListener('click',()=>{if(!profile){message('先建立有效成交分布再匯出');return;}const {rows,...summary}=profile;const blob=new Blob([JSON.stringify({...summary,source,price_adjustment:$('#vp-price-mode').value==='raw'?{mode:'raw',verified:false}:packet.price_adjustment,coverage:sourceMeta?.coverage||'user_import_unverified_completeness'},null,2)],{type:'application/json'}),a=document.createElement('a'),url=URL.createObjectURL(blob);a.href=url;a.download=`${packet.stock_id}-volume-profile.json`;a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);});
 draw();
 try{const res=await fetch(`profiles/${encodeURIComponent(packet.stock_id)}.json`);if(res.ok){const p=await res.json();if(p.schema_version!=='1.1.0'||p.stock_id!==packet.stock_id||p.volume_unit!=='shares'||p.price_basis!=='raw'||p.kind!=='bins'||p.method!=='uniform_minute_range_estimate_then_daily_bins'||!Number.isFinite(Date.parse(p.retrieved_at)))throw Error('成交分布來源識別／口徑不一致');
 const rows=VolumeProfile.validate(p.rows,p.kind);if(rows.some(r=>r.date>p.data_date)||VolumeProfile.day(p.retrieved_at)<p.data_date)throw Error('成交分布來源日期不一致');sourceMeta=p;useRows(p.rows,p.kind,`${p.source} · ${p.interval} · 資料截至 ${p.data_date}`);}}catch(e){message(`分鐘資料載入失敗：${e.message}。仍可匯入 CSV。`);}
});
})();
