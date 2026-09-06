/* Deterministic fixed-range volume profile. No provider calls or order logic. */
(function(root,factory){const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.VolumeProfile=api;})(typeof globalThis!=='undefined'?globalThis:this,function(){
'use strict';
const number=v=>v!==null&&v!==''&&Number.isFinite(Number(v));
function instant(value){
 const s=String(value||'');if(!/^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:\d{2})?$/.test(s))throw Error('逐筆／分鐘時間須為 ISO 時間');
 const normalized=s.replace(' ','T')+(/(?:Z|[+-]\d{2}:\d{2})$/.test(s)?'':'+08:00'),valueMs=Date.parse(normalized);
 if(!Number.isFinite(valueMs))throw Error('無效時間');return {ms:valueMs,iso:new Date(valueMs).toISOString(),date:new Date(valueMs+8*3600000).toISOString().slice(0,10)};
}
function day(value){
 const s=String(value||''),m=s.match(/^(\d{4}-\d{2}-\d{2})(?:[ T]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:\d{2})?)?$/);
 if(!m)throw Error('日期須為 YYYY-MM-DD 或 ISO 時間');
 const d=new Date(m[1]+'T00:00:00Z');if(!Number.isFinite(+d)||d.toISOString().slice(0,10)!==m[1])throw Error('無效日期');
 if(s.length>10){const timestamp=s.replace(' ','T')+(/(?:Z|[+-]\d{2}:\d{2})$/.test(s)?'':'+08:00');const t=new Date(timestamp);if(!Number.isFinite(+t))throw Error('無效時間');return new Date(+t+8*3600000).toISOString().slice(0,10);}
 return m[1];
}
function validate(rows,kind){
 if(!['ticks','minute','bins'].includes(kind))throw Error('資料類型須為逐筆或分鐘 K');
 if(!Array.isArray(rows)||!rows.length||rows.length>250000)throw Error('資料須有 1–250000 列');
 const seen=new Set();
 return rows.map((r,i)=>{
 const timestamp=String(r.timestamp||r.date||''),date=day(timestamp);
  if(timestamp.length<=10&&kind!=='bins')throw Error('逐筆／分鐘資料需要含時間；不能匯入日 K 冒充');
  if(!number(r.volume)||+r.volume<0)throw Error(`第 ${i+1} 列成交量無效`);
  const parsed=kind==='bins'?null:instant(timestamp);let out={timestamp:parsed?parsed.iso:timestamp,date:parsed?parsed.date:date,volume:+r.volume};
  if(kind==='ticks'){if(!number(r.price)||+r.price<=0)throw Error(`第 ${i+1} 列價格無效`);out.price=+r.price;if(r.trade_id!==undefined&&r.trade_id!==''){out.trade_id=String(r.trade_id);const key=`trade:${out.trade_id}`;if(seen.has(key))throw Error('逐筆 trade_id 重複');seen.add(key);}}
  else{
   const key=kind==='minute'?`minute:${parsed.ms}`:null;if(key&&seen.has(key))throw Error('分鐘 K 時間重複');if(key)seen.add(key);
   for(const k of ['open','high','low','close']){if(!number(r[k])||+r[k]<=0)throw Error(`第 ${i+1} 列 ${k} 無效`);out[k]=+r[k];}
   if(out.high<Math.max(out.open,out.close,out.low)||out.low>Math.min(out.open,out.close))throw Error('K 線高低價不一致');
  }
  return out;
 }).sort((a,b)=>a.timestamp.localeCompare(b.timestamp));
}
function csv(text,kind,stockId){
 const lines=String(text).replace(/^\uFEFF/,'').trim().split(/\r?\n/);if(lines.length<2)throw Error('CSV 沒有資料');
 // Deliberately small numeric CSV contract: quoted delimiters are not accepted.
 const head=lines.shift().split(',').map(x=>x.trim());if(new Set(head).size!==head.length)throw Error('CSV 欄位重複');
 if(!head.includes('stock_id'))throw Error('CSV 必須包含 stock_id 欄位');
 const rows=lines.filter(x=>x.trim()).map(line=>{const cells=line.split(',').map(x=>x.trim());if(cells.length!==head.length)throw Error('CSV 欄數不一致');const r=Object.fromEntries(head.map((k,i)=>[k,cells[i]]));if(!r.stock_id||r.stock_id!==stockId)throw Error('CSV 股票代號與目前頁面不同');return r;});
 return validate(rows,kind);
}
function calculate(input,{kind='minute',start,end,bins=32,valueArea=.7}={}){
 const rows=validate(input,kind);start=day(start||rows[0].date);end=day(end||rows.at(-1).date);
 if(start>end)throw Error('起日不可晚於迄日');if(!Number.isInteger(bins)||bins<8||bins>120)throw Error('分箱數須介於 8–120');if(!(valueArea>0&&valueArea<=1))throw Error('價值區比例無效');
 const selected=rows.filter(r=>r.date>=start&&r.date<=end);if(!selected.length)throw Error('所選區間沒有資料');
 let lo=Infinity,hi=-Infinity,total=0;for(const r of selected){lo=Math.min(lo,kind==='ticks'?r.price:r.low);hi=Math.max(hi,kind==='ticks'?r.price:r.high);total+=r.volume;}
 if(!(total>0))throw Error('所選區間成交量為零');if(hi===lo){const pad=Math.max(lo*.0001,.000001);lo-=pad;hi+=pad;}
 const step=(hi-lo)/bins,volumes=Array(bins).fill(0),at=p=>Math.max(0,Math.min(bins-1,Math.floor((p-lo)/step)));
 for(const r of selected){
  if(kind==='ticks'||r.high===r.low){volumes[at(kind==='ticks'?r.price:r.close)]+=r.volume;continue;}
  const a=at(r.low),b=at(r.high);let assigned=0;
  for(let i=a;i<=b;i++){const weight=Math.max(0,Math.min(r.high,lo+(i+1)*step)-Math.max(r.low,lo+i*step))/(r.high-r.low);let v=i===b?r.volume-assigned:r.volume*weight;if(v<0&&Math.abs(v)<=Math.max(1,r.volume)*1e-12)v=0;if(v<0)throw Error('分箱產生負成交量');volumes[i]+=v;assigned+=v;}
 }
 const conserved=volumes.reduce((sum,value)=>sum+value,0),tolerance=Math.max(1,total)*1e-10;if(volumes.some(value=>value<0)||Math.abs(conserved-total)>tolerance)throw Error('分箱成交量未守恆');
 let poc=0;for(let i=1;i<bins;i++)if(volumes[i]>volumes[poc])poc=i;
 let left=poc,right=poc,sum=volumes[poc];
 // Expand contiguously from POC, largest adjacent bin first, through target share.
 // Ties choose the upper bin. This is a disclosed implementation, not TV parity.
 while(sum<total*valueArea&&(left>0||right<bins-1)){const down=left>0?volumes[left-1]:-1,up=right<bins-1?volumes[right+1]:-1;if(up>=down){right++;sum+=volumes[right];}else{left--;sum+=volumes[left];}}
 return {start,end,kind,method:kind==='ticks'?'trade_price_binned':kind==='bins'?'rebinned_minute_histogram_estimate':'uniform_minute_range_estimate',observationCount:selected.length,totalVolume:total,valueAreaActual:sum/total,poc:lo+(poc+.5)*step,val:lo+left*step,vah:lo+(right+1)*step,low:lo,high:hi,bins:volumes.map((volume,i)=>({low:lo+i*step,high:lo+(i+1)*step,volume,isPoc:i===poc,inValueArea:i>=left&&i<=right})),rows:selected};
}
return {day,validate,csv,calculate};
});
