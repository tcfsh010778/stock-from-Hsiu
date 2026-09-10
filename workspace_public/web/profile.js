/* Deterministic price-bin volume, entirely local. No AI-generated POC. */
function parseTicks(text,sid){
 const lines=text.replace(/^\uFEFF/,'').trim().split(/\r?\n/),head=lines.shift().split(',').map(x=>x.trim());
 const required=['stock_id','time','price','volume'];if(required.some(k=>!head.includes(k))||new Set(head).size!==head.length)throw Error('欄位必須包含 stock_id,time,price,volume，且不能重複。');
 if(lines.length>200000||!lines.length)throw Error('每次需 1 至 200,000 筆成交。');
 const ticks=[];let previous='';
 for(const line of lines){const cols=line.split(',');if(cols.length!==head.length)throw Error('CSV 欄位數不一致；請使用無千分位逗號的數字。');const r=Object.fromEntries(head.map((k,i)=>[k,cols[i].trim()]));
  if(r.stock_id!==sid)throw Error('CSV 股票代號與目前個股不同。');
  if(!/^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?$/.test(r.time)||!Number.isFinite(Date.parse(r.time.replace(' ','T')+'+08:00')))throw Error('時間請用 YYYY-MM-DD HH:mm:ss（台北時間）。');
  if(new Date(r.time.slice(0,10)+'T00:00:00Z').toISOString().slice(0,10)!==r.time.slice(0,10))throw Error('日期不存在。');const time=r.time.replace(' ','T');if(previous&&time<previous)throw Error('成交需依時間排序；不會自行補齊或重排。');previous=time;
  const price=Number(r.price),volume=Number(r.volume);if(!r.price||!r.volume||!Number.isFinite(price)||price<=0||!Number.isSafeInteger(volume)||volume<=0)throw Error('價格須為正數，成交股數須為正整數。');ticks.push({time,price,volume});
 }return ticks;
}
function volumeProfile(ticks,step){
 if(!Number.isFinite(step)||step<.01)throw Error('分箱寬度至少 0.01 元。');
 const bins=new Map();let total=0;for(const t of ticks){const bin=Math.floor((t.price+step*1e-9)/step);bins.set(bin,(bins.get(bin)||0)+t.volume);total+=t.volume;if(bins.size>1000)throw Error('超過 1,000 個價格區間，請調大分箱寬度。');if(!Number.isSafeInteger(total))throw Error('成交股數超過可精確計算範圍。');}
 const rows=[...bins].sort((a,b)=>a[0]-b[0]).map(([bin,volume])=>({low:bin*step,high:(bin+1)*step,volume}));
 const poc=rows.reduce((a,b)=>b.volume>a.volume?b:a);return {rows,poc,total};
}
function renderProfile(){
 if(!profileData||profileData.stock_id!==selected){$('profile-output').textContent='尚未匯入目前個股的逐筆成交資料。';return;}
 try{const result=volumeProfile(profileData.ticks,Number($('profile-step').value)),rows=result.rows.slice().reverse();
 $('profile-output').innerHTML=`<p><strong>${esc(selected)} · POC ${num(result.poc.low)}–${num(result.poc.high)} 元</strong> · ${num(result.total/1000,1)} 張 · ${profileData.ticks.length.toLocaleString()} 筆</p><p>${esc(profileData.ticks[0].time)} → ${esc(profileData.ticks.at(-1).time)}（台北時間）。區間為左含右不含；同量時取較低價區間。來源：使用者 CSV；未由網站驗證成交完整性。</p><div class="profile-bars">${rows.map(r=>`<div><span>${num(r.low)}–${num(r.high)}</span><i style="width:${r.volume/result.poc.volume*70}%;background:${r===result.poc?'#d7b967':'var(--accent)'}"></i><small>${num(r.volume/1000,1)} 張</small></div>`).join('')}</div>`;
 }catch(e){$('profile-output').textContent=e.message;}
}
if(typeof document!=='undefined'){
 $('profile-file').onchange=async event=>{const file=event.target.files[0];if(!file)return;try{if(file.size>15000000)throw Error('檔案上限 15 MB。');const sid=selected,ticks=parseTicks(await file.text(),sid);if(sid!==selected)throw Error('個股已切換，請重新匯入。');profileData={stock_id:sid,ticks};renderProfile();}catch(e){$('profile-output').textContent=e.message;}finally{event.target.value='';}};
 $('profile-step').onchange=renderProfile;$('profile-clear').onclick=()=>{profileData=null;renderProfile();};
}
if(typeof module!=='undefined')module.exports={parseTicks,volumeProfile};
