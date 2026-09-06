const assert=require('node:assert/strict');
const VP=require('../stock_v2_public/volume_profile.js');
const trades=[{timestamp:'2026-09-03T09:00:00+08:00',price:100,volume:200},{timestamp:'2026-09-04T09:00:00+08:00',price:101,volume:700},{timestamp:'2026-09-04T09:00:01+08:00',price:104,volume:100}];
const p=VP.calculate(trades,{kind:'ticks',bins:8});
assert.equal(p.totalVolume,1000);assert.equal(p.bins.reduce((s,b)=>s+b.volume,0),1000);assert.ok(p.poc>=101&&p.poc<101.5);assert.ok(p.val<=p.poc&&p.vah>p.poc);assert.ok(p.valueAreaActual>=.7);
assert.equal(VP.calculate(trades,{kind:'ticks',end:'2026-09-03'}).totalVolume,200);
const bars=[{timestamp:'2026-09-04T09:00:00+08:00',open:100,high:108,low:100,close:108,volume:800}];
assert.deepEqual(VP.calculate(bars,{bins:8}).bins.map(b=>Math.round(b.volume)),Array(8).fill(100));
assert.throws(()=>VP.calculate(bars.concat(bars)),/重複/);
assert.throws(()=>VP.calculate([{...bars[0],timestamp:'2026-09-04'}]),/需要含時間/);
assert.throws(()=>VP.csv('timestamp,price,volume,stock_id\n2026-09-04T09:00:00,1,2,2317','ticks','2330'),/股票代號/);
assert.throws(()=>VP.csv('timestamp,price,volume\n2026-09-04T09:00:00,1,2','ticks','2330'),/stock_id/);
assert.throws(()=>VP.calculate([
  {timestamp:'2026-09-04T09:00:00+08:00',open:1,high:2,low:1,close:2,volume:3},
  {timestamp:'2026-09-04T01:00:00Z',open:1,high:2,low:1,close:2,volume:3}
],{kind:'minute'}),/重複/);
assert.doesNotThrow(()=>VP.calculate([
  {timestamp:'2026-09-04T09:00:00+08:00',price:1,volume:2},
  {timestamp:'2026-09-04T09:00:00+08:00',price:1,volume:2}
],{kind:'ticks'})); // identical prints can be distinct trades when no trade_id exists
assert.throws(()=>VP.calculate([
  {timestamp:'2026-09-04T09:00:00+08:00',price:1,volume:2,trade_id:'a'},
  {timestamp:'2026-09-04T09:00:01+08:00',price:1,volume:2,trade_id:'a'}
],{kind:'ticks'}),/trade_id 重複/);
assert.throws(()=>VP.calculate([{...trades[0],price:Infinity}],{kind:'ticks'}),/價格無效/);
assert.throws(()=>VP.day('2026-02-30'),/無效日期/);
assert.equal(VP.day('2026-09-03T17:00:00Z'),'2026-09-04');
assert.throws(()=>VP.calculate(bars,{start:'2026-09-05'}),/起日/);
console.log('Volume Profile: conservation, fixed range, POC/VA, minute estimate, CSV and date boundaries passed');
