use crate::config::{self, Config};
use crate::history;
use crate::ripper;
use std::io::Write as _;
use std::sync::{Arc, RwLock};
use tiny_http::{Header, Method, Response, Server, StatusCode};

/// Embedded single-page HTML dashboard — full parity with Python autorip web UI.
const DASHBOARD_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AutoRip</title>
<style>
:root {
  --bg:#f6f8fa; --border:#d0d7de; --text:#1f2328; --text2:#4d5560; --text3:#656d76;
  --accent:#0969da; --green:#1a7f37; --yellow:#9a6700; --red:#cf222e; --blue:#0969da;
  --card:#fff; --log-bg:#fff; --log-text:#24292f; --log-border:#d0d7de; --chip:#eaeef2; --poster-bg:#e1e4e8;
}
body.dark {
  --bg:#0d1117; --border:#3d444d; --text:#f0f6fc; --text2:#d1d9e0; --text3:#9198a1;
  --accent:#79c0ff; --green:#56d364; --yellow:#e3b341; --red:#ff7b72; --blue:#79c0ff;
  --card:#151b23; --log-bg:#151b23; --log-text:#d1d9e0; --log-border:#3d444d; --chip:#262c36; --poster-bg:#262c36;
}
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family:-apple-system,system-ui,"Segoe UI",Roboto,sans-serif; background:var(--bg); color:var(--text); min-height:100vh; display:flex; flex-direction:column; }
.c { max-width:900px; margin:0 auto; padding:20px; width:100%; flex:1; display:flex; flex-direction:column; }
@keyframes p { 0%,100%{opacity:1} 50%{opacity:.3} }
.card { background:var(--card); border:1px solid var(--border); border-radius:12px; padding:16px; margin-bottom:16px; }
.card h2 { font-size:.7rem; color:var(--text3); margin-bottom:10px; text-transform:uppercase; font-weight:600; letter-spacing:1px; }
.log { background:var(--log-bg); border:1px solid var(--log-border); border-radius:8px; padding:12px; font-family:'SF Mono','Fira Code',monospace; font-size:.75rem; max-height:280px; overflow-y:auto; white-space:pre-wrap; word-break:break-all; line-height:1.6; color:var(--log-text); }
.log::-webkit-scrollbar { width:5px; } .log::-webkit-scrollbar-thumb { background:var(--border); border-radius:3px; }
.btn { background:var(--chip); border:1px solid var(--border); color:var(--text); padding:5px 12px; border-radius:6px; cursor:pointer; font-size:.78rem; text-decoration:none; }
.btn:hover { background:var(--border); }
.ok { color:var(--green); } .warn { color:var(--red); }
.headerbar { display:flex; align-items:center; gap:8px 12px; padding:12px 16px; flex-wrap:wrap; border-bottom:1px solid var(--border); background:var(--card); position:sticky; top:0; z-index:10; }
.nav { text-decoration:none; font-size:.85rem; color:var(--text3); padding:4px 0; border-bottom:2px solid transparent; cursor:pointer; background:none; border-top:none; border-left:none; border-right:none; }
.nav:hover { color:var(--text); } .nav.active { color:var(--text); border-bottom-color:var(--accent); font-weight:500; }
/* Now Playing card */
.np { display:flex; gap:20px; background:var(--card); border:1px solid var(--border); border-radius:12px; padding:20px; margin-bottom:16px; min-height:180px; }
.poster { width:120px; min-height:170px; border-radius:8px; background:var(--poster-bg); flex-shrink:0; object-fit:cover; box-shadow:0 2px 8px rgba(0,0,0,.1); }
.ph { width:120px; min-height:170px; border-radius:8px; background:var(--poster-bg); flex-shrink:0; display:flex; align-items:center; justify-content:center; }
.ph svg { width:40px; height:40px; opacity:.4; }
.nfo { flex:1; display:flex; flex-direction:column; justify-content:center; }
.mt { font-size:1.5rem; font-weight:600; color:var(--text); line-height:1.2; }
.my { font-size:.9rem; color:var(--text2); margin-top:4px; }
.mo { font-size:.8rem; color:var(--text2); margin-top:8px; line-height:1.5; display:-webkit-box; -webkit-line-clamp:3; -webkit-box-orient:vertical; overflow:hidden; }
.b { display:inline-block; padding:2px 8px; border-radius:4px; font-size:.7rem; font-weight:600; text-transform:uppercase; margin-left:8px; }
.b.uhd { background:#0969da18; color:var(--blue); border:1px solid #0969da33; }
.b.bluray { background:#1a7f3718; color:var(--green); border:1px solid #1a7f3733; }
.b.dvd { background:#9a670018; color:var(--yellow); border:1px solid #9a670033; }
.btn-stop { font-size:.65rem; padding:2px 8px; color:var(--text3); border-color:var(--border); background:var(--chip); white-space:nowrap; }
.btn-eject { font-size:.7rem; padding:3px 10px; color:var(--text3); }
.idle-msg { display:flex; flex-direction:column; align-items:center; justify-content:center; width:100%; min-height:160px; color:var(--text3); }
.idle-msg svg { width:48px; height:48px; opacity:.4; margin-bottom:12px; }
.idle-msg p { font-size:.85rem; }
/* Device tabs */
.dtab { display:inline-block; padding:6px 16px; font-size:.8rem; cursor:pointer; border:1px solid var(--border); border-bottom:none; border-radius:8px 8px 0 0; background:var(--chip); color:var(--text3); margin-right:4px; }
.dtab.active { background:var(--card); color:var(--text); font-weight:500; border-bottom:1px solid var(--card); margin-bottom:-1px; position:relative; z-index:1; }
.dtabs { border-bottom:1px solid var(--border); margin-bottom:16px; padding:0 4px; }
.actions { display:flex; gap:8px; align-items:center; margin-bottom:12px; }
/* History table */
table { width:100%; border-collapse:collapse; font-size:.8rem; margin-top:16px; display:block; overflow-x:auto; }
th { text-align:left; color:var(--text3); font-weight:600; font-size:.7rem; text-transform:uppercase; letter-spacing:.5px; padding:8px 10px; border-bottom:2px solid var(--border); }
td { padding:8px 10px; border-bottom:1px solid var(--border); }
tr:hover { background:var(--chip); }
/* System page */
.files { font-size:.8rem; line-height:1.8; }
.files span { color:var(--text2); }
/* Settings */
.setting { margin-bottom:16px; }
.setting label { display:block; font-size:.75rem; color:var(--text3); text-transform:uppercase; font-weight:600; letter-spacing:.5px; margin-bottom:4px; }
.setting input[type=text], .setting input[type=number] { width:100%; padding:8px 10px; border:1px solid var(--border); border-radius:6px; background:var(--log-bg); color:var(--text); font-size:.85rem; font-family:inherit; }
.setting input:focus { outline:none; border-color:var(--accent); }
.setting .hint { font-size:.7rem; color:var(--text3); margin-top:2px; }
.toggle { display:flex; align-items:center; gap:8px; font-size:.85rem; cursor:pointer; }
.toggle input { width:16px; height:16px; }
.section { display:none; } .section.active { display:flex; flex-direction:column; flex:1; }
@media(max-width:600px){ .c{padding:10px} .np{flex-direction:column;gap:12px} .poster,.ph{width:100%;min-height:auto;max-height:200px} .mt{font-size:1.2rem} }
</style>
</head>
<body>
<div class="c">
<div class="headerbar">
  <span style="font-size:1.1rem;color:var(--text3);font-weight:400;letter-spacing:3px;text-transform:uppercase">AUTORIP</span>
  <button class="nav active" data-tab="ripper">Ripper</button>
  <button class="nav" data-tab="history">History</button>
  <button class="nav" data-tab="system">System</button>
  <button class="nav" data-tab="settings">Settings</button>
  <button class="btn" style="margin-left:auto" onclick="toggleTheme()" id="thm"></button>
</div>

<!-- Ripper page -->
<div id="ripper" class="section active">
  <div id="dtabs"></div>
  <div id="np"></div>
  <div id="actions"></div>
  <div id="steps" style="margin-bottom:16px"></div>
  <div id="err"></div>
  <details style="margin-top:16px"><summary style="font-size:.7rem;color:var(--text3);text-transform:uppercase;font-weight:600;letter-spacing:1px;cursor:pointer;user-select:none">Log</summary>
  <div id="log" class="log" style="flex:1;max-height:none;margin-top:8px"></div></details>
</div>

<!-- History page -->
<div id="history" class="section">
  <div id="hi" style="margin-top:16px"></div>
</div>

<!-- System page -->
<div id="system" class="section">
  <div class="card" style="margin-top:16px"><h2>Data Files</h2><div id="files" class="files"></div></div>
  <div class="card"><h2>Move Queue</h2><div id="moves"></div></div>
  <div><h2 style="font-size:.7rem;color:var(--text3);text-transform:uppercase;font-weight:600;letter-spacing:1px;margin-bottom:8px">System Log</h2><div id="syslog" class="log" style="max-height:400px"></div></div>
</div>

<!-- Settings page -->
<div id="settings" class="section">
  <div style="margin-top:16px">
  <div id="settings-form"></div>
  <div style="position:sticky;bottom:0;padding:12px 0;background:var(--bg)">
  <button class="btn" id="savebtn" onclick="saveSettings()">Save</button>
  <span id="save-status" style="margin-left:8px;font-size:.8rem;color:var(--green)"></span>
  </div>
  </div>
</div>
</div>

<div style="text-align:center;padding:16px;font-size:.7rem"><a href="https://github.com/MattJackson/autorip" style="color:var(--text3);text-decoration:none" target="_blank">autorip</a></div>

<script>
/* ---- Theme ---- */
const _sun='<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41"/></svg>';
const _moon='<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>';
function toggleTheme(){document.body.classList.toggle('dark');localStorage.setItem('theme',document.body.classList.contains('dark')?'dark':'light');document.getElementById('thm').innerHTML=document.body.classList.contains('dark')?_sun:_moon}
(function(){
  const saved=localStorage.getItem('theme');
  if(saved==='dark'||(saved==null&&window.matchMedia('(prefers-color-scheme:dark)').matches))document.body.classList.add('dark');
  document.getElementById('thm').innerHTML=document.body.classList.contains('dark')?_sun:_moon;
})();

/* ---- Util ---- */
function esc(s){if(!s)return'';const d=document.createElement('div');d.textContent=s;return d.innerHTML}
function upd(id,html){const el=document.getElementById(id);if(el&&el._last!==html){el.innerHTML=html;el._last=html}}

/* ---- Navigation ---- */
document.querySelectorAll('.nav[data-tab]').forEach(btn=>{
  btn.addEventListener('click',function(){
    const tab=this.dataset.tab;
    document.querySelectorAll('.section').forEach(s=>s.classList.remove('active'));
    document.getElementById(tab).classList.add('active');
    document.querySelectorAll('.nav[data-tab]').forEach(b=>b.classList.remove('active'));
    this.classList.add('active');
    if(tab==='history')loadHistory();
    if(tab==='system')loadSystem();
    if(tab==='settings')loadSettings();
  });
});

/* ---- Browser notifications ---- */
if(typeof Notification!=='undefined'&&Notification.permission==='default')Notification.requestPermission();
function notify(title,body,icon){
  if(typeof Notification!=='undefined'&&Notification.permission==='granted'){
    try{new Notification(title,{body:body,icon:icon||''})}catch(e){}
  }
}

/* ---- Disc SVG icon ---- */
const D='<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="3"/></svg>';

/* ---- Codec/Resolution maps ---- */
const VM=[['HEVC Main10','4K HDR'],['HEVC Main','4K'],['HEVC','4K HDR'],['AVC High','HD'],['AVC','HD'],['MPEG2','SD']];
const AM=[['TRUEHD','TrueHD 7.1'],['DTSHD','DTS-HD MA'],['A_DTS','DTS-HD MA'],['AC3','Dolby Digital'],['AAC','AAC'],['LPCM','PCM'],['FLAC','FLAC']];
const RV={'3840x2160':'4K','1920x1080':'1080p','1280x720':'720p','720x480':'480p','720x576':'576p'};
function ml(v,m){if(!v)return'';for(const[k,l]of m)if(v.includes(k))return l;return''}

/* ---- Step-by-step progress ---- */
const ACTIVE_STATES=['ripping','scanning','detecting'];
let _lastStatus={};
let _activeTab=null;

function renderSteps(steps,progress,eta,speed){
  if(!steps||!steps.length)return'<div style="color:var(--text3);font-size:.85rem;padding:8px 0">Insert a disc to start ripping</div>';
  const icons={done:'\u2713',active:'\u25cf',pending:'\u25cb'};
  const colors={done:'var(--green)',active:'var(--accent)',pending:'var(--text3)'};
  return steps.map(st=>{
    let detail=st.detail||'';
    if(st.status==='active'&&st.name==='Ripping'&&progress){
      const p=parseInt(progress)||0;
      const spdStr=speed?' \u00b7 '+speed:'';
      const etaStr=eta?' \u00b7 '+eta+' remaining':'';
      detail='<div style="display:flex;align-items:center;gap:8px;margin-top:4px"><div style="flex:1;background:var(--chip);border-radius:3px;height:3px;overflow:hidden"><div style="background:var(--green);height:100%;width:'+p+'%;transition:width 1s"></div></div><span style="font-size:.75rem;color:var(--text2)">'+progress+spdStr+etaStr+'</span></div>';
    }else if(detail){detail=' \u2014 '+detail}
    const anim=st.status==='active'?';animation:p 1.5s infinite':'';
    return '<div style="display:flex;align-items:flex-start;gap:8px;padding:4px 0;font-size:.8rem"><span style="color:'+colors[st.status]+';font-size:.7rem;width:14px;text-align:center'+anim+'">'+icons[st.status]+'</span><span style="color:'+(st.status==='pending'?'var(--text3)':'var(--text)')+'">'+st.name+detail+'</span></div>';
  }).join('');
}

/* ---- Build steps from state ---- */
function buildSteps(s){
  const steps=[];
  const st=s.status;
  if(st==='idle')return[];
  if(st==='scanning'){
    steps.push({name:'Scanning',status:'active',detail:''});
    steps.push({name:'Ripping',status:'pending',detail:''});
    steps.push({name:'Verified',status:'pending',detail:''});
    steps.push({name:'Moving',status:'pending',detail:''});
    steps.push({name:'Done',status:'pending',detail:''});
  }else if(st==='ripping'){
    steps.push({name:'Scanning',status:'done',detail:'\u2713'});
    steps.push({name:'Ripping',status:'active',detail:''});
    steps.push({name:'Verified',status:'pending',detail:''});
    steps.push({name:'Moving',status:'pending',detail:''});
    steps.push({name:'Done',status:'pending',detail:''});
  }else if(st==='moving'){
    steps.push({name:'Scanning',status:'done',detail:'\u2713'});
    steps.push({name:'Ripping',status:'done',detail:'\u2713'});
    steps.push({name:'Verified',status:'done',detail:'\u2713'});
    steps.push({name:'Moving',status:'active',detail:''});
    steps.push({name:'Done',status:'pending',detail:''});
  }else if(st==='done'){
    steps.push({name:'Scanning',status:'done',detail:'\u2713'});
    steps.push({name:'Ripping',status:'done',detail:'\u2713'});
    steps.push({name:'Verified',status:'done',detail:'\u2713'});
    steps.push({name:'Moving',status:'done',detail:'\u2713'});
    steps.push({name:'Done',status:'done',detail:'\u2713'});
  }else if(st==='error'){
    steps.push({name:'Error',status:'active',detail:s.last_error||''});
  }
  return steps;
}

/* ---- Ripper page render ---- */
function handleState(data){
  const devs=Object.keys(data);
  if(!devs.length){
    upd('dtabs','');
    upd('np','<div class="np"><div class="idle-msg">'+D+'<p>No drives detected</p></div></div>');
    upd('actions','');upd('steps','');upd('err','');
    return;
  }
  const multi=devs.length>1;

  devs.forEach(dev=>{
    const s=data[dev];
    const prev=_lastStatus[dev];
    if(prev&&prev!==s.status){
      if(s.status==='done')notify('AutoRip',(s.tmdb_title||s.disc_name)+' \u2014 Complete',s.tmdb_poster);
      if(s.status==='error')notify('AutoRip',(s.tmdb_title||s.disc_name)+' \u2014 Error: '+(s.last_error||'unknown'),s.tmdb_poster);
    }
    _lastStatus[dev]=s.status;
  });

  if(!_activeTab||!devs.includes(_activeTab))_activeTab=devs[0];

  /* Device tabs */
  if(multi){
    const tabHtml=devs.map(dev=>{
      const s=data[dev];
      const active=ACTIVE_STATES.includes(s.status);
      const errState=s.status==='error';
      const dotColor=active?'var(--green)':errState?'var(--red)':'var(--text3)';
      const dotAnim=active?'animation:p 1.5s infinite;':'';
      const dot='<span style="display:inline-block;width:6px;height:6px;border-radius:50%;background:'+dotColor+';'+dotAnim+'margin-right:4px;vertical-align:middle"></span>';
      return '<span class="dtab'+(dev===_activeTab?' active':'')+'" onclick="_activeTab=\''+dev+'\';renderCurrent()">'+dot+dev+'</span>';
    }).join('');
    upd('dtabs','<div class="dtabs">'+tabHtml+'</div>');
  }else{upd('dtabs','')}

  window._stateData=data;
  renderCurrent();
}

function renderCurrent(){
  const data=window._stateData;
  if(!data)return;
  const dev=_activeTab;
  const s=data[dev];
  if(!s)return;

  /* Now Playing card */
  let card;
  const title=s.tmdb_title||s.disc_name;
  if(s.status==='idle'||!title){
    card='<div class="np"><div class="idle-msg">'+D+'<p>Insert a disc to start ripping</p></div></div>';
  }else{
    const img=s.tmdb_poster?'<img class="poster" src="'+esc(s.tmdb_poster)+'" alt="">':'<div class="ph">'+D+'</div>';
    const fmt=s.disc_format;
    const b=fmt&&fmt!=='unknown'?'<span class="b '+fmt+'">'+fmt+'</span>':'';
    const o=s.tmdb_overview?'<div class="mo">'+esc(s.tmdb_overview)+'</div>':'';
    const yr=s.tmdb_year>0?s.tmdb_year:'';
    card='<div class="np">'+img+'<div class="nfo"><div class="mt">'+esc(title)+'</div><div class="my">'+yr+' '+b+'</div>'+o+'</div></div>';
  }
  upd('np',card);

  /* Actions bar */
  const active=ACTIVE_STATES.includes(s.status);
  const hasDisc=s.status!=='idle';
  let btns='';
  if(active){
    btns='<button class="btn btn-stop" onclick="if(confirm(\'Stop the current rip?\')){this.disabled=true;fetch(\'/api/stop/'+dev+'\',{method:\'POST\'})}">Stop</button>';
    btns+='<button class="btn btn-eject" onclick="fetch(\'/api/eject/'+dev+'\',{method:\'POST\'})">Eject</button>';
  }else if(hasDisc&&s.status!=='error'){
    btns='<button class="btn" onclick="fetch(\'/api/rip/'+dev+'\',{method:\'POST\'})">Rip</button>';
    btns+='<button class="btn btn-eject" onclick="fetch(\'/api/eject/'+dev+'\',{method:\'POST\'})">Eject</button>';
  }
  const statusLabel=s.status||'idle';
  const dot=active?'var(--green)':hasDisc?'var(--accent)':'var(--text3)';
  const pulse=active?'animation:p 1.5s infinite;':'';
  upd('actions','<div class="actions"><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:'+dot+';vertical-align:middle;margin-right:6px;'+pulse+'"></span><span style="font-size:.8rem;color:var(--text2)">'+dev+' \u00b7 '+statusLabel+'</span><span style="margin-left:auto;display:flex;gap:6px">'+btns+'</span></div>');

  /* Steps */
  const steps=buildSteps(s);
  const progressStr=s.progress_pct>0?s.progress_pct+'%':'';
  const speedStr=s.speed_mbs>0?s.speed_mbs.toFixed(1)+' MB/s':'';
  const etaStr=s.eta||'';
  upd('steps',renderSteps(steps,progressStr,etaStr,speedStr));

  /* Error banner */
  const errHtml=s.errors>0?'<div style="background:var(--red);color:#fff;padding:8px 12px;border-radius:6px;font-size:.8rem;margin-bottom:8px">\u26a0 '+s.errors+' error'+(s.errors>1?'s':'')+': '+esc(s.last_error)+'</div>':'';
  upd('err',errHtml);

  /* Device log */
  loadDeviceLog(dev);
}

/* ---- Device log viewer ---- */
let _logTimer=null;
function loadDeviceLog(dev){
  clearTimeout(_logTimer);
  fetch('/api/logs/'+encodeURIComponent(dev)).then(r=>r.text()).then(text=>{
    const e=document.getElementById('log');
    if(e&&e._last!==text){
      const atBottom=e.scrollHeight-e.scrollTop-e.clientHeight<50;
      e.textContent=text;
      e._last=text;
      if(atBottom)e.scrollTop=e.scrollHeight;
    }
  }).catch(()=>{});
  _logTimer=setTimeout(()=>loadDeviceLog(dev),3000);
}

/* ---- SSE connection ---- */
let _es=null;
function connectSSE(){
  if(_es){_es.close();_es=null}
  _es=new EventSource('/events');
  _es.onmessage=function(e){try{handleState(JSON.parse(e.data))}catch(x){}};
  _es.onerror=function(){_es.close();_es=null;setTimeout(connectSSE,2000)};
}

/* ---- History page ---- */
function loadHistory(){
  fetch('/api/history').then(r=>r.json()).then(h=>{
    if(!h.length){document.getElementById('hi').innerHTML='<div style="color:var(--text2);font-size:.85rem;padding:20px">No rips yet</div>';return}
    let t='<table><tr><th></th><th>Title</th><th>Type</th><th>Date</th><th>Duration</th><th></th></tr>';
    h.forEach(i=>{
      const poster=i.poster_url?'<img src="'+esc(i.poster_url)+'" style="width:32px;height:48px;border-radius:4px;object-fit:cover" alt="">':'';
      const fmt=(i.format||'').toUpperCase();
      const fmtCls=(i.format||'').toLowerCase();
      const badge=fmt?'<span class="b '+fmtCls+'">'+fmt+'</span>':'';
      const dt=(i.date||i.timestamp||'').split('T')[0];
      const dl=i._file?'<a href="/api/history/'+i._file+'" class="btn" style="font-size:.65rem;padding:2px 6px;text-decoration:none">JSON</a>':'';
      t+='<tr><td>'+poster+'</td><td><strong>'+esc(i.title||i.disc_name||'Unknown')+'</strong></td><td>'+badge+'</td><td>'+esc(dt)+'</td><td>'+esc(i.duration||'')+'</td><td>'+dl+'</td></tr>';
    });
    t+='</table>';
    document.getElementById('hi').innerHTML=t;
  }).catch(()=>{
    document.getElementById('hi').innerHTML='<div style="color:var(--text2);font-size:.85rem;padding:20px">Could not load history</div>';
  });
}

/* ---- System page ---- */
function loadSystem(){
  fetch('/api/system').then(r=>r.json()).then(data=>{
    /* Data files */
    const filesEl=document.getElementById('files');
    if(data.files&&data.files.length){
      let fhtml='';
      data.files.forEach(f=>{
        const dot=f.present?'var(--green)':'var(--red)';
        const info=f.present?(f.size||'')+' \u00b7 '+(f.updated||''):'missing';
        fhtml+='<div><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:'+dot+';margin-right:8px;vertical-align:middle"></span>'+esc(f.name)+' <span>'+esc(info)+'</span></div>';
      });
      filesEl.innerHTML=fhtml;
    }else{
      filesEl.innerHTML='<div style="color:var(--text3);font-size:.8rem">No data files found</div>';
    }
    /* Move queue */
    const movesEl=document.getElementById('moves');
    if(data.move_queue&&data.move_queue.length){
      let mhtml='';
      data.move_queue.forEach(m=>{
        mhtml+='<div style="padding:4px 0;font-size:.8rem"><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--yellow);margin-right:8px;vertical-align:middle"></span>'+esc(m)+'</div>';
      });
      movesEl.innerHTML=mhtml;
    }else{
      movesEl.innerHTML='<div style="color:var(--text3);font-size:.8rem">No pending moves</div>';
    }
    /* System log */
    const logEl=document.getElementById('syslog');
    if(data.syslog){
      logEl.textContent=data.syslog;
      logEl.scrollTop=0;
    }else{
      logEl.textContent='No system log available';
    }
  }).catch(()=>{});
}

/* ---- Settings page ---- */
function loadSettings(){
  fetch('/api/settings').then(r=>r.json()).then(renderSettings).catch(()=>{});
}

function renderSettings(s){
  const groups=[
    {title:'Ripping',fields:[
      {key:'on_insert',label:'On Disc Insert',type:'radio',options:[{value:'nothing',label:'Do Nothing'},{value:'identify',label:'Identify'},{value:'rip',label:'Rip'}],hint:'What happens when a disc is inserted'},
      {key:'main_feature',label:'Main Feature Only',type:'bool',hint:'Rip longest title only'},
      {key:'min_length_secs',label:'Minimum Title Length (seconds)',type:'number',hint:'Shorter titles are skipped (600 = 10 min)'},
      {key:'auto_eject',label:'Auto Eject',type:'bool',hint:'Eject disc after rip completes'},
      {key:'abort_on_error',label:'Abort on Error',type:'bool',hint:'Stop rip on first disc read error'},
    ]},
    {title:'Output',fields:[
      {key:'output_dir',label:'Output Directory',type:'text',hint:'Where all ripped files go by default'},
      {key:'movie_dir',label:'Movies',type:'text',hint:'',indent:true,placeholder:'Same as output directory'},
      {key:'tv_dir',label:'TV Series',type:'text',hint:'',indent:true,placeholder:'Same as output directory'},
    ]},
    {title:'API Keys',fields:[
      {key:'tmdb_api_key',label:'TMDB API Key',type:'text',hint:'v3 API key from themoviedb.org'},
    ]},
  ];
  let html='';
  groups.forEach(g=>{
    html+='<div class="card"><h2>'+g.title+'</h2>';
    g.fields.forEach(f=>{
      const v=s[f.key]!=null?s[f.key]:'';
      const indent=f.indent?'margin-left:20px;border-left:2px solid var(--border);padding-left:12px':'';
      const ph=f.placeholder?' placeholder="'+f.placeholder+'"':'';
      if(f.type==='radio'){
        const opts=f.options.map(o=>'<label style="font-size:.85rem;cursor:pointer;display:inline-flex;align-items:center;gap:4px;margin-right:16px"><input type="radio" name="'+f.key+'" data-key="'+f.key+'" value="'+o.value+'" '+(v===o.value?'checked':'')+'>'+o.label+'</label>').join('');
        html+='<div class="setting" style="'+indent+'"><label>'+f.label+'</label><div style="margin-top:4px">'+opts+'</div>'+(f.hint?'<div class="hint">'+f.hint+'</div>':'')+'</div>';
      }else if(f.type==='bool'){
        html+='<div class="setting" style="'+indent+'"><label class="toggle"><input type="checkbox" data-key="'+f.key+'" '+(v?'checked':'')+'>'+f.label+'</label>'+(f.hint?'<div class="hint">'+f.hint+'</div>':'')+'</div>';
      }else{
        html+='<div class="setting" style="'+indent+'"><label>'+f.label+'</label><input type="'+f.type+'" data-key="'+f.key+'" value="'+esc(String(v))+'"'+ph+'>'+(f.hint?'<div class="hint">'+f.hint+'</div>':'')+'</div>';
      }
    });
    html+='</div>';
  });
  document.getElementById('settings-form').innerHTML=html;
}

function saveSettings(){
  const inputs=document.querySelectorAll('#settings-form [data-key]');
  const s={};
  inputs.forEach(el=>{
    if(el.type==='radio'){if(el.checked)s[el.dataset.key]=el.value}
    else if(el.type==='checkbox')s[el.dataset.key]=el.checked;
    else if(el.type==='number')s[el.dataset.key]=parseInt(el.value)||0;
    else s[el.dataset.key]=el.value;
  });
  fetch('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(s)})
  .then(r=>{if(r.ok){document.getElementById('save-status').textContent='Saved';setTimeout(()=>document.getElementById('save-status').textContent='',2000)}});
}

/* ---- Init ---- */
fetch('/api/state').then(r=>r.json()).then(data=>{handleState(data);connectSSE()}).catch(()=>setTimeout(connectSSE,1000));
</script>
</body>
</html>"##;

pub fn run(cfg: &Arc<RwLock<Config>>) {
    let port = cfg.read().unwrap().port;
    let addr = format!("0.0.0.0:{}", port);
    let server = match Server::http(&addr) {
        Ok(s) => Arc::new(s),
        Err(e) => {
            eprintln!("Web server failed to start on {}: {}", addr, e);
            return;
        }
    };
    eprintln!("Web UI: http://0.0.0.0:{}", port);

    for request in server.incoming_requests() {
        let cfg = Arc::clone(cfg);
        std::thread::spawn(move || {
            handle_request(request, &cfg);
        });
    }
}

fn handle_request(
    request: tiny_http::Request,
    cfg: &Arc<RwLock<Config>>,
) {
    let url = request.url().to_string();
    let is_get = *request.method() == Method::Get;
    let is_post = *request.method() == Method::Post;

    if is_get && (url == "/" || url == "/index.html") {
        serve_html(request);
    } else if is_get && url == "/api/state" {
        json_response(request, 200, &get_state_json());
    } else if is_get && url == "/api/history" {
        let history_dir = cfg.read().unwrap().history_dir();
        let items = history::load_recent(&history_dir, 50);
        let json = serde_json::to_string(&items).unwrap_or_else(|_| "[]".to_string());
        json_response(request, 200, &json);
    } else if is_get && url.starts_with("/api/history/") {
        let fname = url.trim_start_matches("/api/history/");
        handle_history_file(request, cfg, fname);
    } else if is_get && url == "/api/settings" {
        let c = cfg.read().unwrap();
        let json = serde_json::to_string(&*c).unwrap_or_else(|_| "{}".to_string());
        json_response(request, 200, &json);
    } else if is_post && url == "/api/settings" {
        handle_settings_post(request, cfg);
    } else if is_get && url == "/api/system" {
        handle_system_info(request, cfg);
    } else if is_get && url.starts_with("/api/logs/") {
        let device = url.trim_start_matches("/api/logs/");
        let device = percent_decode(device);
        handle_device_log(request, cfg, &device);
    } else if is_get && url == "/events" {
        handle_sse(request);
    } else if is_post && url.starts_with("/api/rip/") {
        let device = url.trim_start_matches("/api/rip/");
        let device = percent_decode(device);
        handle_rip(request, cfg, &device);
    } else if is_post && url.starts_with("/api/eject/") {
        let device = url.trim_start_matches("/api/eject/");
        let device = percent_decode(device);
        handle_eject(request, &device);
    } else if is_post && url.starts_with("/api/stop/") {
        let device = url.trim_start_matches("/api/stop/");
        let device = percent_decode(device);
        handle_stop(request, &device);
    } else {
        json_response(request, 404, r#"{"error":"not found"}"#);
    }
}

// ---------- Helpers ----------

fn serve_html(request: tiny_http::Request) {
    let header = Header::from_bytes(&b"Content-Type"[..], &b"text/html; charset=utf-8"[..]).unwrap();
    let response = Response::from_string(DASHBOARD_HTML).with_header(header);
    let _ = request.respond(response);
}

fn json_response(request: tiny_http::Request, status: u16, body: &str) {
    let header =
        Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
    let response = Response::from_string(body)
        .with_status_code(StatusCode(status))
        .with_header(header);
    let _ = request.respond(response);
}

fn text_response(request: tiny_http::Request, body: &str) {
    let header =
        Header::from_bytes(&b"Content-Type"[..], &b"text/plain; charset=utf-8"[..]).unwrap();
    let response = Response::from_string(body).with_header(header);
    let _ = request.respond(response);
}

fn get_state_json() -> String {
    let state = ripper::STATE.lock().unwrap();
    serde_json::to_string(&*state).unwrap_or_else(|_| "{}".to_string())
}

fn handle_history_file(request: tiny_http::Request, cfg: &Arc<RwLock<Config>>, fname: &str) {
    let fname = percent_decode(fname);
    // Only allow safe filenames
    if !fname.ends_with(".json") || fname.contains("..") || fname.contains('/') {
        json_response(request, 400, r#"{"error":"invalid filename"}"#);
        return;
    }
    let history_dir = cfg.read().unwrap().history_dir();
    let path = format!("{}/{}", history_dir, fname);
    match std::fs::read_to_string(&path) {
        Ok(content) => {
            let header = Header::from_bytes(
                &b"Content-Type"[..],
                &b"application/json"[..],
            ).unwrap();
            let disp = format!("attachment; filename=\"{}\"", fname);
            let disp_header = Header::from_bytes(
                &b"Content-Disposition"[..],
                disp.as_bytes(),
            ).unwrap();
            let response = Response::from_string(content)
                .with_header(header)
                .with_header(disp_header);
            let _ = request.respond(response);
        }
        Err(_) => {
            json_response(request, 404, r#"{"error":"not found"}"#);
        }
    }
}

fn handle_system_info(request: tiny_http::Request, cfg: &Arc<RwLock<Config>>) {
    let cfg = cfg.read().unwrap();

    // Data files check
    let data_dir = format!("{}/makemkv", cfg.autorip_dir);
    let data_files = ["KEYDB.cfg", "hkd.dat", "sdf.dat", "_private_data.tar"];
    let mut files_json = Vec::new();
    for name in &data_files {
        let path = format!("{}/{}", data_dir, name);
        let meta = std::fs::metadata(&path);
        if let Ok(m) = meta {
            let size = m.len();
            let size_str = if size > 1024 * 1024 {
                format!("{:.1} MB", size as f64 / (1024.0 * 1024.0))
            } else if size > 1024 {
                format!("{:.1} KB", size as f64 / 1024.0)
            } else {
                format!("{} B", size)
            };
            let modified = m.modified().ok().and_then(|t| {
                t.duration_since(std::time::UNIX_EPOCH).ok().map(|d| {
                    let secs = d.as_secs();
                    format_epoch_datetime(secs)
                })
            }).unwrap_or_default();
            files_json.push(serde_json::json!({
                "name": name,
                "present": true,
                "size": size_str,
                "updated": modified,
            }));
        } else {
            files_json.push(serde_json::json!({
                "name": name,
                "present": false,
                "size": null,
                "updated": null,
            }));
        }
    }

    // Move queue: find drives with status "done" or "moving"
    let move_queue: Vec<String> = {
        let state = ripper::STATE.lock().unwrap();
        state.values()
            .filter(|rs| rs.status == "done" || rs.status == "moving")
            .map(|rs| {
                let title = if rs.tmdb_title.is_empty() {
                    rs.disc_name.clone()
                } else {
                    rs.tmdb_title.clone()
                };
                format!("{} ({})", title, rs.status)
            })
            .collect()
    };

    // System log: last 50 lines
    let syslog_path = format!("{}/logs/system.log", cfg.autorip_dir);
    let syslog = std::fs::read_to_string(&syslog_path)
        .unwrap_or_default()
        .lines()
        .rev()
        .take(50)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join("\n");

    let body = serde_json::json!({
        "files": files_json,
        "move_queue": move_queue,
        "syslog": syslog,
    });

    json_response(request, 200, &body.to_string());
}

fn handle_device_log(request: tiny_http::Request, cfg: &Arc<RwLock<Config>>, device: &str) {
    // Validate device name
    if !device.chars().all(|c| c.is_ascii_alphanumeric()) {
        text_response(request, "invalid device");
        return;
    }
    let log_dir = cfg.read().unwrap().log_dir();
    let path = format!("{}/current_{}.log", log_dir, device);
    let content = std::fs::read_to_string(&path).unwrap_or_default();
    // Return last 200 lines
    let lines: Vec<&str> = content.lines().collect();
    let start = if lines.len() > 200 { lines.len() - 200 } else { 0 };
    let tail = lines[start..].join("\n");
    text_response(request, &tail);
}

fn handle_settings_post(mut request: tiny_http::Request, cfg: &Arc<RwLock<Config>>) {
    let mut body = String::new();
    if request.as_reader().read_to_string(&mut body).is_err() {
        json_response(request, 400, r#"{"ok":false,"error":"bad body"}"#);
        return;
    }
    let patch: serde_json::Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(_) => {
            json_response(request, 400, r#"{"ok":false,"error":"invalid json"}"#);
            return;
        }
    };

    {
        let mut c = cfg.write().unwrap();
        if let Some(v) = patch.get("output_dir").and_then(|v| v.as_str()) {
            c.output_dir = v.to_string();
        }
        if let Some(v) = patch.get("staging_dir").and_then(|v| v.as_str()) {
            c.staging_dir = v.to_string();
        }
        if let Some(v) = patch.get("movie_dir").and_then(|v| v.as_str()) {
            c.movie_dir = v.to_string();
        }
        if let Some(v) = patch.get("tv_dir").and_then(|v| v.as_str()) {
            c.tv_dir = v.to_string();
        }
        if let Some(v) = patch.get("tmdb_api_key").and_then(|v| v.as_str()) {
            c.tmdb_api_key = v.to_string();
        }
        if let Some(v) = patch.get("on_insert").and_then(|v| v.as_str()) {
            c.on_insert = v.to_string();
        }
        if let Some(v) = patch.get("main_feature").and_then(|v| v.as_bool()) {
            c.main_feature = v;
        }
        if let Some(v) = patch.get("auto_eject").and_then(|v| v.as_bool()) {
            c.auto_eject = v;
        }
        if let Some(v) = patch.get("abort_on_error").and_then(|v| v.as_bool()) {
            c.abort_on_error = v;
        }
        if let Some(v) = patch.get("min_length_secs").and_then(|v| v.as_u64()) {
            c.min_length_secs = v;
        }
        if let Some(v) = patch.get("port").and_then(|v| v.as_u64()) {
            c.port = v as u16;
        }
        config::save(&c);
    }

    json_response(request, 200, r#"{"ok":true}"#);
}

fn handle_sse(request: tiny_http::Request) {
    let headers = vec![
        Header::from_bytes(&b"Content-Type"[..], &b"text/event-stream"[..]).unwrap(),
        Header::from_bytes(&b"Cache-Control"[..], &b"no-cache"[..]).unwrap(),
        Header::from_bytes(&b"Connection"[..], &b"keep-alive"[..]).unwrap(),
        Header::from_bytes(&b"Access-Control-Allow-Origin"[..], &b"*"[..]).unwrap(),
    ];

    let mut response = Response::empty(200);
    for h in headers {
        response = response.with_header(h);
    }

    let mut stream = match request.upgrade("sse", response) {
        stream => stream,
    };

    let initial = format!("data: {}\n\n", get_state_json());
    if stream.write_all(initial.as_bytes()).is_err() {
        return;
    }
    let _ = stream.flush();

    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
        let frame = format!("data: {}\n\n", get_state_json());
        if stream.write_all(frame.as_bytes()).is_err() {
            break;
        }
        if stream.flush().is_err() {
            break;
        }
    }
}

fn handle_rip(request: tiny_http::Request, cfg: &Arc<RwLock<Config>>, device: &str) {
    let already = ripper::STATE.lock().map(|s| {
        s.get(device)
            .map(|r| r.status == "scanning" || r.status == "ripping")
            .unwrap_or(false)
    }).unwrap_or(false);

    if already {
        json_response(request, 409, r#"{"ok":false,"error":"already ripping"}"#);
        return;
    }

    let dev = device.to_string();
    let cfg = Arc::clone(cfg);
    std::thread::spawn(move || {
        ripper::STATE
            .lock()
            .map(|mut s| {
                s.insert(
                    dev.clone(),
                    ripper::RipState {
                        device: dev.clone(),
                        status: "idle".to_string(),
                        ..Default::default()
                    },
                );
            })
            .ok();
        let _ = cfg;
    });

    json_response(request, 200, r#"{"ok":true}"#);
}

fn handle_eject(request: tiny_http::Request, device: &str) {
    let device_path = format!("/dev/{}", device);
    crate::ripper::eject_drive(&device_path);
    ripper::STATE
        .lock()
        .map(|mut s| {
            s.insert(
                device.to_string(),
                ripper::RipState {
                    device: device.to_string(),
                    status: "idle".to_string(),
                    ..Default::default()
                },
            );
        })
        .ok();
    json_response(request, 200, r#"{"ok":true}"#);
}

fn handle_stop(request: tiny_http::Request, device: &str) {
    let existed = ripper::STATE
        .lock()
        .map(|mut s| {
            if let Some(rs) = s.get_mut(device) {
                rs.status = "idle".to_string();
                true
            } else {
                false
            }
        })
        .unwrap_or(false);

    if existed {
        // Set stop cooldown to suppress auto-rip for 15 seconds
        ripper::set_stop_cooldown(device);
        json_response(request, 200, r#"{"ok":true}"#);
    } else {
        json_response(request, 404, r#"{"ok":false,"error":"drive not found"}"#);
    }
}

fn percent_decode(s: &str) -> String {
    let mut result = Vec::new();
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(byte) = u8::from_str_radix(
                &String::from_utf8_lossy(&bytes[i + 1..i + 3]),
                16,
            ) {
                result.push(byte);
                i += 3;
                continue;
            }
        }
        result.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&result).to_string()
}

fn format_epoch_datetime(secs: u64) -> String {
    let days = (secs / 86400) as i64;
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    let time_of_day = secs % 86400;
    let hh = time_of_day / 3600;
    let mm = (time_of_day % 3600) / 60;
    format!("{:04}-{:02}-{:02} {:02}:{:02}", y, m, d, hh, mm)
}
