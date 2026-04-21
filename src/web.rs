use crate::config::{self, Config};
use crate::history;
use crate::ripper;
use std::io::{Read as _, Write as _};
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
.btn-stop, .btn-eject { font-size:.78rem; }
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
.setting { margin-bottom:18px; }
.setting label { display:block; font-size:13px; color:var(--text2); font-weight:500; margin-bottom:5px; }
.setting input[type=text], .setting input[type=number] { padding:8px 10px; border:1px solid var(--border); border-radius:6px; background:var(--log-bg); color:var(--text); font-size:13px; font-family:inherit; box-sizing:border-box; }
.setting input[type=text] { width:100%; }
.setting input[type=number] { width:120px; }
.setting input:focus { outline:none; border-color:var(--accent); }
.setting .hint { font-size:12px; color:var(--text3); margin-top:3px; line-height:1.4; }
.toggle { display:flex; align-items:center; gap:6px; font-size:13px; cursor:pointer; font-weight:400; color:var(--text); line-height:1; }
.toggle input[type=checkbox] { width:13px; height:13px; margin:0; flex-shrink:0; accent-color:var(--accent); }
#settings-form .card { margin-bottom:12px; }
#settings-form .card h2 { margin-bottom:14px; }
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
  <div class="card" style="margin-top:16px"><h2>Data Files</h2><div id="files" class="files" style="margin-bottom:12px"></div><div style="display:flex;align-items:center;gap:10px"><button class="btn" onclick="updateKeydb()">Update KEYDB</button><span id="keydb-status" style="font-size:.8rem"></span></div></div>
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

<div style="text-align:center;padding:16px;font-size:.7rem"><a href="https://github.com/freemkv/autorip" style="color:var(--text3);text-decoration:none" target="_blank">autorip v{VERSION}</a></div>

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
const ACTIVE_STATES=['ripping','scanning','detecting','verifying'];
let _lastStatus={};
let _activeTab=null;

function renderSteps(steps,progress,eta,speed){
  if(!steps||!steps.length)return'';
  const icons={done:'\u2713',active:'\u25cf',pending:'\u25cb'};
  const colors={done:'var(--green)',active:'var(--accent)',pending:'var(--text3)'};
  return steps.map(st=>{
    let detail=st.detail||'';
    if(st.status==='active'&&st.name==='Ripping'&&(progress||speed)){
      const p=parseInt(progress)||0;
      const spdStr=speed?' \u00b7 '+speed:'';
      const etaStr=eta?' \u00b7 '+eta+' remaining':'';
      const label=progress?progress+spdStr+etaStr:speed+(etaStr||'');
      detail='<div style="display:flex;align-items:center;gap:8px;margin-top:4px">'+(p>0?'<div style="flex:1;background:var(--chip);border-radius:3px;height:3px;overflow:hidden"><div style="background:var(--green);height:100%;width:'+p+'%;transition:width 1s"></div></div>':'')+'<span style="font-size:.75rem;color:var(--text2)">'+label+'</span></div>';
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
    steps.push({name:'Done',status:'pending',detail:''});
  }else if(st==='ripping'){
    steps.push({name:'Scanning',status:'done',detail:''});
    steps.push({name:'Ripping',status:'active',detail:''});
    steps.push({name:'Done',status:'pending',detail:''});
  }else if(st==='moving'||st==='done'){
    steps.push({name:'Scanning',status:'done',detail:''});
    steps.push({name:'Ripping',status:'done',detail:''});
    steps.push({name:'Done',status:'done',detail:''});
  }else if(st==='error'){
    steps.push({name:'Error',status:'active',detail:s.last_error||''});
  }
  return steps;
}

/* ---- Ripper page render ---- */
function handleState(data){
  const devs=Object.keys(data).filter(k=>!k.startsWith('_'));
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
  if(document.getElementById('system').classList.contains('active'))renderMoves();
}

function renderCurrent(){
  const data=window._stateData;
  if(!data)return;
  const dev=_activeTab;
  const s=data[dev];
  if(!s)return;

  /* Derived state */
  const verifying=data._verify&&data._verify.status==='running';
  const active=ACTIVE_STATES.includes(s.status)||verifying;
  const title=s.tmdb_title||s.disc_name;
  const scanned=!!title;
  const discIn=s.disc_present||scanned||active;

  /* Now Playing card */
  let card;
  if(!discIn){
    card='<div class="np"><div class="idle-msg">'+D+'<p>No disc</p></div></div>';
  }else if(!scanned){
    card='<div class="np"><div class="idle-msg">'+D+'<p>Disc detected</p></div></div>';
  }else{
    const img=s.tmdb_poster?'<img class="poster" src="'+esc(s.tmdb_poster)+'" alt="">':'<div class="ph">'+D+'</div>';
    const fmt=s.disc_format;
    const b=fmt&&fmt!=='unknown'?'<span class="b '+fmt+'">'+fmt+'</span>':'';
    const o=s.tmdb_overview?'<div class="mo">'+esc(s.tmdb_overview)+'</div>':'';
    const yr=s.tmdb_year>0?s.tmdb_year:'';
    const dur=s.duration?' \u00b7 '+esc(s.duration):'';
    const codecs=s.codecs?'<div class="mo" style="color:var(--text3);font-size:.75rem;margin-top:6px">'+esc(s.codecs)+'</div>':'';
    const ready=s.status==='idle'?'<div class="mo" style="color:var(--green)">Ready to rip</div>':'';
    card='<div class="np">'+img+'<div class="nfo"><div class="mt">'+esc(title)+'</div><div class="my">'+yr+dur+' '+b+'</div>'+o+codecs+ready+'</div></div>';
  }
  upd('np',card);

  /* Actions bar */
  let btns='';
  if(active){
    btns='<button class="btn btn-stop" onclick="if(confirm(\'Stop?\')){this.disabled=true;fetch(\'/api/stop/'+dev+'\',{method:\'POST\'})}">Stop</button>';
  }else if(scanned){
    btns='<button class="btn" style="background:var(--green);color:#fff;border-color:var(--green)" onclick="fetch(\'/api/rip/'+dev+'\',{method:\'POST\'})">Rip</button>';
    btns+='<button class="btn" onclick="fetch(\'/api/verify/'+dev+'\',{method:\'POST\'})">Verify</button>';
  }else if(discIn){
    btns='<button class="btn" onclick="fetch(\'/api/scan/'+dev+'\',{method:\'POST\'})">Scan</button>';
  }
  if(discIn&&!active)btns+='<button class="btn btn-eject" onclick="fetch(\'/api/eject/'+dev+'\',{method:\'POST\'})">Eject</button>';

  const statusLabel=verifying?'verifying':(s.status||'idle');
  const dot=active?'var(--green)':scanned?'var(--accent)':discIn?'var(--yellow)':'var(--text3)';
  const pulse=active?'animation:p 1.5s infinite;':'';
  upd('actions','<div class="actions"><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:'+dot+';vertical-align:middle;margin-right:6px;'+pulse+'"></span><span style="font-size:.8rem;color:var(--text2)">'+dev+' \u00b7 '+statusLabel+'</span><span style="margin-left:auto;display:flex;gap:6px">'+btns+'</span></div>');

  /* Steps */
  const steps=buildSteps(s);
  const progressStr=s.progress_pct>0?s.progress_pct+'%':(s.progress_gb>0?s.progress_gb.toFixed(1)+' GB':'');
  const speedStr=s.speed_mbs>=1?s.speed_mbs.toFixed(1)+' MB/s':s.speed_mbs>0?(s.speed_mbs*1024).toFixed(0)+' KB/s':'0 KB/s';
  const etaStr=s.eta||'';
  upd('steps',renderSteps(steps,progressStr,etaStr,speedStr));

  /* Error banner */
  let errHtml='';
  if(s.errors>0&&s.last_error){
    errHtml='<div style="background:var(--red);color:#fff;padding:8px 12px;border-radius:6px;font-size:.8rem;margin-bottom:8px">\u26a0 '+esc(s.last_error)+'</div>';
  }else if(s.errors>0){
    const errMb=(s.errors*2048/1048576).toFixed(1);
    const errSecs=(s.errors*2048/8250000).toFixed(1);
    errHtml='<div style="background:var(--yellow);color:#000;padding:8px 12px;border-radius:6px;font-size:.8rem;margin-bottom:8px">'+s.errors+' sector'+(s.errors>1?'s':'')+' skipped ('+errMb+' MB, ~'+errSecs+'s of video)</div>';
  }
  upd('err',errHtml);

  /* Verify / Disc Health */
  const v=data._verify;
  let vhtml='';
  if(v&&(v.status==='running'||v.status==='done')){
    vhtml+='<div class="card" style="margin-top:12px"><h2>Disc Health</h2>';
    /* Sector map bar */
    vhtml+='<div style="position:relative;height:12px;background:var(--chip);border-radius:3px;overflow:hidden;margin-bottom:8px">';
    if(v.status==='running'){
      const pct=v.progress_pct||0;
      vhtml+='<div style="position:absolute;left:0;top:0;height:100%;width:'+pct+'%;background:var(--green);transition:width 1s"></div>';
    }else{
      /* Full green background for completed */
      vhtml+='<div style="position:absolute;left:0;top:0;height:100%;width:100%;background:var(--green)"></div>';
    }
    /* Overlay bad/slow sectors */
    if(v.sector_map){
      v.sector_map.forEach(s=>{
        const color=s.status==='bad'?'var(--red)':s.status==='recovered'?'var(--yellow)':'var(--yellow)';
        vhtml+='<div style="position:absolute;left:'+s.offset_pct+'%;top:0;height:100%;width:'+Math.max(s.width_pct,0.3)+'%;background:'+color+'"></div>';
      });
    }
    vhtml+='</div>';
    /* Stats line */
    if(v.status==='running'){
      const spd=v.speed_mbs?v.speed_mbs.toFixed(1)+' MB/s':'';
      const done=v.sectors_done||0;
      const total=v.sectors_total||1;
      const pct=(done/total*100).toFixed(1);
      const goodCount=done-(v.bad||0)-(v.slow||0)-(v.recovered||0);
      const badMb=((v.bad||0)*2048/1048576).toFixed(1);
      const badSecs=((v.bad||0)*2048/8250000).toFixed(1);
      vhtml+='<div style="font-size:.8rem;color:var(--text)">Verifying... <strong>'+pct+'%</strong> \u00b7 '+spd+'</div>';
      let stats='<span style="color:var(--green)">'+goodCount.toLocaleString()+' good</span>';
      if(v.bad)stats+=' \u00b7 <span style="color:var(--red)">'+v.bad.toLocaleString()+' bad ('+badMb+' MB, ~'+badSecs+'s)</span>';
      if(v.slow)stats+=' \u00b7 <span style="color:var(--yellow)">'+v.slow.toLocaleString()+' slow</span>';
      if(v.recovered)stats+=' \u00b7 <span style="color:var(--accent)">'+v.recovered.toLocaleString()+' recovered</span>';
      stats+=' \u00b7 <span style="color:var(--text3)">'+total.toLocaleString()+' total</span>';
      vhtml+='<div style="font-size:.75rem;margin-top:4px">'+stats+'</div>';
    }else{
      const total=v.sectors_total||1;
      const pct=(((total-(v.bad||0))/total)*100).toFixed(v.bad>0?4:0);
      const elapsed=v.elapsed_secs||0;
      const m=Math.floor(elapsed/60);
      const s=Math.floor(elapsed%60);
      vhtml+='<div style="font-size:.8rem;color:var(--text);margin-bottom:4px"><strong>'+pct+'%</strong> readable in '+m+':'+String(s).padStart(2,'0')+'</div>';
      vhtml+='<div style="font-size:.75rem;color:var(--text2)">';
      vhtml+='Good: '+(v.good||0).toLocaleString();
      if(v.slow)vhtml+=' \u00b7 Slow: '+v.slow.toLocaleString();
      if(v.recovered)vhtml+=' \u00b7 Recovered: '+v.recovered.toLocaleString();
      if(v.bad)vhtml+=' \u00b7 <span style="color:var(--red)">Bad: '+v.bad.toLocaleString()+'</span>';
      vhtml+='</div>';
      /* Bad ranges */
      if(v.bad_ranges&&v.bad_ranges.length){
        v.bad_ranges.filter(r=>r.status==='bad').forEach(r=>{
          vhtml+='<div style="font-size:.75rem;color:var(--red);margin-top:4px">\u26a0 '+r.count+' bad sectors at '+r.gb_offset.toFixed(1)+' GB';
          if(r.chapter)vhtml+=' ('+esc(r.chapter)+')';
          vhtml+='</div>';
        });
      }
      if(!v.bad&&!v.slow)vhtml+='<div style="font-size:.8rem;color:var(--green);margin-top:4px">\u2713 Disc is perfect</div>';
    }
    vhtml+='</div>';
  }
  upd('err',errHtml+vhtml);

  /* Device log */
  loadDeviceLog(dev);
}

/* ---- Local time conversion for log lines ---- */
function utcToLocal(line){
  return line.replace(/^\[(\d{2}):(\d{2}):(\d{2})\]/,function(_,h,m,s){
    const now=new Date();
    const d=new Date(Date.UTC(now.getUTCFullYear(),now.getUTCMonth(),now.getUTCDate(),+h,+m,+s));
    return '['+String(d.getHours()).padStart(2,'0')+':'+String(d.getMinutes()).padStart(2,'0')+':'+String(d.getSeconds()).padStart(2,'0')+']';
  });
}

/* ---- Device log viewer ---- */
let _logTimer=null;
function loadDeviceLog(dev){
  clearTimeout(_logTimer);
  fetch('/api/logs/'+encodeURIComponent(dev)).then(r=>r.text()).then(text=>{
    const e=document.getElementById('log');
    const reversed=text.split('\n').filter(l=>l).map(utcToLocal).reverse().join('\n');
    if(e&&e._last!==reversed){
      e.textContent=reversed;
      e._last=reversed;
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
function fmtElapsed(s){if(!s)return'';s=+s;const h=Math.floor(s/3600),m=Math.floor((s%3600)/60);return h>0?h+'h '+m+'m':m+'m'}
function loadHistory(){
  fetch('/api/history').then(r=>r.json()).then(h=>{
    if(!h.length){document.getElementById('hi').innerHTML='<div style="color:var(--text2);font-size:.85rem;padding:20px">No rips yet</div>';return}
    let html='';
    h.forEach((i,idx)=>{
      const title=i.title||i.disc_name||'Unknown';
      const fmt=(i.format||'').toLowerCase();
      const badge=fmt&&fmt!=='unknown'?'<span class="b '+fmt+'">'+fmt.toUpperCase()+'</span>':'';
      const dt=(i.date||'').split('T')[0];
      const poster=i.poster_url?'<img src="'+esc(i.poster_url)+'" style="width:48px;height:72px;border-radius:6px;object-fit:cover;flex-shrink:0" alt="">':'<div style="width:48px;height:72px;border-radius:6px;background:var(--chip);flex-shrink:0"></div>';
      const elapsed=fmtElapsed(i.elapsed_secs);
      const size=i.size_gb?(+i.size_gb).toFixed(1)+' GB':'';
      const speed=i.speed_mbs?(+i.speed_mbs).toFixed(0)+' MB/s':'';
      const stats=[size,elapsed,speed].filter(x=>x).join(' \u00b7 ');
      const codecs=i.codecs||'';
      const dur=i.duration||'';
      const hasLog=!!i.log;

      html+='<div style="display:flex;gap:14px;padding:14px 0;border-bottom:1px solid var(--border);align-items:flex-start">';
      html+=poster;
      html+='<div style="flex:1;min-width:0">';
      html+='<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap"><strong style="font-size:.9rem">'+esc(title)+'</strong>'+badge+'</div>';
      html+='<div style="font-size:.75rem;color:var(--text3);margin-top:3px">'+esc(dt);
      if(dur)html+=' \u00b7 '+esc(dur);
      html+='</div>';
      if(stats)html+='<div style="font-size:.75rem;color:var(--text2);margin-top:2px">'+esc(stats)+'</div>';
      if(codecs)html+='<div style="font-size:.7rem;color:var(--text3);margin-top:2px">'+esc(codecs)+'</div>';
      if(hasLog)html+='<details style="margin-top:6px"><summary style="font-size:.7rem;color:var(--text3);cursor:pointer;user-select:none">Log</summary><div class="log" style="margin-top:4px;max-height:200px;font-size:.7rem">'+esc(i.log)+'</div></details>';
      html+='</div></div>';
    });
    document.getElementById('hi').innerHTML=html;
  }).catch(()=>{
    document.getElementById('hi').innerHTML='<div style="color:var(--text2);font-size:.85rem;padding:20px">Could not load history</div>';
  });
}

function updateKeydb(){
  const st=document.getElementById('keydb-status');
  st.textContent='Updating...';st.style.color='var(--text3)';
  fetch('/api/update-keydb',{method:'POST'}).then(r=>r.json()).then(data=>{
    if(data.ok){st.textContent='Updated: '+data.entries+' entries';st.style.color='var(--green)';loadSystem();}
    else{st.textContent=data.error||'Update failed';st.style.color='var(--red)';}
  }).catch(e=>{st.textContent='Network error';st.style.color='var(--red)';});
}
/* ---- Move queue with live progress ---- */
function renderMoves(){
  const el=document.getElementById('moves');
  if(!el)return;
  const data=window._stateData||{};
  const mv=data._move;
  let html='';
  let hasContent=false;
  /* Active move from dedicated move state */
  if(mv&&mv.name){
    hasContent=true;
    const pct=mv.progress_pct||0;
    const spdStr=mv.speed_mbs>=1?mv.speed_mbs.toFixed(1)+' MB/s':mv.speed_mbs>0?(mv.speed_mbs*1024).toFixed(0)+' KB/s':'';
    const etaStr=mv.eta?mv.eta+' remaining':'';
    const label=[pct+'%',spdStr,etaStr].filter(x=>x).join(' \u00b7 ');
    html+='<div style="padding:6px 0"><div style="display:flex;align-items:center;gap:8px;margin-bottom:4px"><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--green);animation:p 1.5s infinite;flex-shrink:0"></span><span style="font-size:.85rem;font-weight:500">'+esc(mv.name)+'</span></div>';
    html+='<div style="display:flex;align-items:center;gap:8px">';
    if(pct>0)html+='<div style="flex:1;background:var(--chip);border-radius:3px;height:3px;overflow:hidden"><div style="background:var(--green);height:100%;width:'+pct+'%;transition:width 1s"></div></div>';
    html+='<span style="font-size:.75rem;color:var(--text2)">'+label+'</span></div></div>';
  }
  /* Pending queue items */
  if(window._moveQueue){
    window._moveQueue.forEach(m=>{
      if(mv&&mv.name&&m.replace(/ \(moving\)/,'').replace(/ /g,'_').includes(mv.name.replace(/ /g,'_')))return;
      hasContent=true;
      html+='<div style="padding:4px 0;font-size:.8rem"><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--yellow);margin-right:8px;vertical-align:middle"></span>'+esc(m)+'</div>';
    });
  }
  if(!hasContent)html='<div style="color:var(--text3);font-size:.8rem">No pending moves</div>';
  upd('moves',html);
}

/* ---- System page ---- */
function loadSystem(){
  fetch('/api/system').then(r=>r.json()).then(data=>{
    /* Data files */
    const filesEl=document.getElementById('files');
    if(data.files&&data.files.length){
      let fhtml='';
      data.files.forEach(f=>{
        if(f.present){
          fhtml+='<div style="display:flex;align-items:center;gap:8px;padding:4px 0"><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--green);flex-shrink:0"></span><div><div>'+esc(f.name)+'</div><div style="font-size:.75rem;color:var(--text3)">'+esc(f.size||'')+' \u00b7 '+esc(f.updated||'')+'</div></div></div>';
        }else{
          fhtml+='<div style="display:flex;align-items:center;gap:8px;padding:4px 0"><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--text3);flex-shrink:0"></span><div><div>KEYDB.cfg <span style="color:var(--text3);font-size:.8rem">— not found</span></div><div style="font-size:.75rem;color:var(--text3)">Optional — needed for encrypted Blu-ray/UHD discs. Set URL in Settings then click Update.</div></div></div>';
        }
      });
      filesEl.innerHTML=fhtml;
    }else{
      filesEl.innerHTML='<div style="color:var(--text3);font-size:.8rem">No data files found</div>';
    }
    /* Move queue — store for renderMoves, then render */
    window._moveQueue=data.move_queue||[];
    renderMoves();
    /* System log */
    const logEl=document.getElementById('syslog');
    if(data.syslog){
      logEl.textContent=data.syslog.split('\n').map(utcToLocal).join('\n');
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
      {key:'on_insert',label:'On Disc Insert',type:'radio',options:[{value:'nothing',label:'Do Nothing'},{value:'scan',label:'Scan'},{value:'rip',label:'Rip'}],hint:'What happens when a disc is inserted'},
      {key:'main_feature',label:'Main Feature Only',type:'bool',hint:'Rip longest title only'},
      {key:'min_length_secs',label:'Minimum Title Length (seconds)',type:'number',hint:'Shorter titles are skipped (600 = 10 min)'},
      {key:'auto_eject',label:'Auto Eject',type:'bool',hint:'Eject disc after rip completes'},
      {key:'on_read_error',label:'On Read Error',type:'radio',options:[{value:'stop',label:'Stop'},{value:'skip',label:'Skip (zero-fill)'}],hint:'Stop aborts the rip. Skip zero-fills bad sectors and continues — use after Verify confirms damage is minor.'},
      {key:'output_format',label:'Output Format',type:'radio',options:[{value:'mkv',label:'MKV'},{value:'m2ts',label:'M2TS'},{value:'iso',label:'ISO (disc image)'},{value:'network',label:'Network'}],hint:'Format for ripped files'},
      {key:'network_target',label:'Network Target',type:'text',hint:'host:port for network output (e.g. 192.168.1.100:9000)',indent:true,placeholder:'192.168.1.100:9000',showIf:{key:'output_format',value:'network'}},
    ]},
    {title:'Output',fields:[
      {key:'output_dir',label:'Output Directory',type:'text',hint:'Where all ripped files go by default'},
      {key:'movie_dir',label:'Movies',type:'text',hint:'',indent:true,placeholder:'Same as output directory'},
      {key:'tv_dir',label:'TV Series',type:'text',hint:'',indent:true,placeholder:'Same as output directory'},
    ]},
    {title:'API Keys',fields:[
      {key:'tmdb_api_key',label:'TMDB API Key',type:'text',hint:'v3 API key from themoviedb.org'},
      {key:'keydb_url',label:'KEYDB Update URL',type:'text',hint:'HTTP URL to download KEYDB.cfg (zip, gz, or plain text)'},
    ]},
  ];
  let html='';
  groups.forEach(g=>{
    html+='<div class="card"><h2>'+g.title+'</h2>';
    g.fields.forEach(f=>{
      const v=s[f.key]!=null?s[f.key]:'';
      const indent=f.indent?'margin-left:20px;border-left:2px solid var(--border);padding-left:12px':'';
      const ph=f.placeholder?' placeholder="'+f.placeholder+'"':'';
      const hide=f.showIf&&s[f.showIf.key]!==f.showIf.value?'display:none;':'';
      const showAttr=f.showIf?' data-show-key="'+f.showIf.key+'" data-show-value="'+f.showIf.value+'"':'';
      if(f.type==='radio'){
        const opts=f.options.map(o=>'<label style="font-size:13px;cursor:pointer;display:inline-flex;align-items:center;gap:6px;margin-right:16px"><input type="radio" name="'+f.key+'" data-key="'+f.key+'" value="'+o.value+'" style="width:14px;height:14px;margin:0;accent-color:var(--accent)" onchange="toggleConditional()" '+(v===o.value?'checked':'')+'>'+o.label+'</label>').join('');
        html+='<div class="setting" style="'+indent+hide+'"'+showAttr+'><label>'+f.label+'</label><div style="margin-top:4px">'+opts+'</div>'+(f.hint?'<div class="hint">'+f.hint+'</div>':'')+'</div>';
      }else if(f.type==='bool'){
        html+='<div class="setting" style="'+indent+hide+'"'+showAttr+'><label class="toggle"><input type="checkbox" data-key="'+f.key+'" '+(v?'checked':'')+'> '+f.label+'</label>'+(f.hint?'<div class="hint">'+f.hint+'</div>':'')+'</div>';
      }else{
        html+='<div class="setting" style="'+indent+hide+'"'+showAttr+'><label>'+f.label+'</label><input type="'+f.type+'" data-key="'+f.key+'" value="'+esc(String(v))+'"'+ph+'>'+(f.hint?'<div class="hint">'+f.hint+'</div>':'')+'</div>';
      }
    });
    html+='</div>';
    /* Insert webhooks card after Output */
    if(g.title==='Output'){
      const hooks=(s.webhook_urls||[]).filter(u=>u);
      html+='<div class="card"><h2>Webhooks</h2>';
      html+='<div id="webhook-list">';
      hooks.forEach((u,i)=>{
        html+='<div style="display:flex;gap:6px;margin-bottom:6px;align-items:center"><input type="text" data-webhook="'+i+'" value="'+esc(u)+'" style="flex:1;padding:8px 10px;border:1px solid var(--border);border-radius:6px;background:var(--log-bg);color:var(--text);font-size:13px;font-family:inherit"><button class="btn" onclick="this.parentElement.remove()" style="padding:5px 8px;font-size:.75rem">X</button></div>';
      });
      html+='</div>';
      html+='<button class="btn" onclick="addWebhook()" style="font-size:.75rem;margin-top:4px">+ Add Webhook</button>';
      html+='<div style="font-size:12px;color:var(--text3);margin-top:8px;line-height:1.4">POST JSON on rip and move complete. Works with Discord, Jellyfin, n8n, or any HTTP endpoint.</div>';
      html+='</div>';
    }
  });
  document.getElementById('settings-form').innerHTML=html;
  toggleConditional();
}
function toggleConditional(){
  document.querySelectorAll('[data-show-key]').forEach(el=>{
    const k=el.dataset.showKey,v=el.dataset.showValue;
    const radio=document.querySelector('input[data-key="'+k+'"]:checked');
    el.style.display=radio&&radio.value===v?'':'none';
  });
}

function addWebhook(){
  const list=document.getElementById('webhook-list');
  const i=list.children.length;
  const div=document.createElement('div');
  div.style='display:flex;gap:6px;margin-bottom:6px;align-items:center';
  div.innerHTML='<input type="text" data-webhook="'+i+'" placeholder="https://discord.com/api/webhooks/..." style="flex:1;padding:8px 10px;border:1px solid var(--border);border-radius:6px;background:var(--log-bg);color:var(--text);font-size:13px;font-family:inherit"><button class="btn" onclick="this.parentElement.remove()" style="padding:5px 8px;font-size:.75rem">X</button>';
  list.appendChild(div);
  div.querySelector('input').focus();
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
  /* Collect webhook URLs */
  const hooks=[];
  document.querySelectorAll('#webhook-list input[data-webhook]').forEach(el=>{
    const v=el.value.trim();
    if(v)hooks.push(v);
  });
  s.webhook_urls=hooks;
  fetch('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(s)})
  .then(r=>{if(r.ok){document.getElementById('save-status').textContent='Saved';setTimeout(()=>document.getElementById('save-status').textContent='',2000)}});
}

/* ---- Init ---- */
fetch('/api/state').then(r=>r.json()).then(data=>{handleState(data);connectSSE()}).catch(()=>setTimeout(connectSSE,1000));
</script>
</body>
</html>"##;

pub fn run(cfg: &Arc<RwLock<Config>>) {
    let port = cfg.read().map(|c| c.port).unwrap_or(8080);
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

fn handle_request(request: tiny_http::Request, cfg: &Arc<RwLock<Config>>) {
    let url = request.url().to_string();
    let is_get = *request.method() == Method::Get;
    let is_post = *request.method() == Method::Post;

    if is_get && (url == "/" || url == "/index.html") {
        serve_html(request);
    } else if is_get && url == "/api/state" {
        json_response(request, 200, &get_state_json());
    } else if is_get && url == "/api/history" {
        let history_dir = cfg.read().map(|c| c.history_dir()).unwrap_or_default();
        let items = history::load_recent(&history_dir, 50);
        let json = serde_json::to_string(&items).unwrap_or_else(|_| "[]".to_string());
        json_response(request, 200, &json);
    } else if is_get && url.starts_with("/api/history/") {
        let fname = url.trim_start_matches("/api/history/");
        handle_history_file(request, cfg, fname);
    } else if is_get && url == "/api/settings" {
        let c = match cfg.read() {
            Ok(c) => c,
            Err(_) => return json_response(request, 500, "{}"),
        };
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
    } else if is_post && url.starts_with("/api/scan/") {
        let device = url.trim_start_matches("/api/scan/");
        let device = percent_decode(device);
        handle_scan(request, cfg, &device);
    } else if is_post && url.starts_with("/api/rip/") {
        let device = url.trim_start_matches("/api/rip/");
        let device = percent_decode(device);
        handle_rip(request, cfg, &device);
    } else if is_post && url == "/api/update-keydb" {
        handle_update_keydb(request, cfg);
    } else if is_post && url.starts_with("/api/eject/") {
        let device = url.trim_start_matches("/api/eject/");
        let device = percent_decode(device);
        handle_eject(request, &device);
    } else if is_post && url.starts_with("/api/stop/") {
        let device = url.trim_start_matches("/api/stop/");
        let device = percent_decode(device);
        handle_stop(request, &device);
    } else if is_post && url.starts_with("/api/verify/") {
        let device = url.trim_start_matches("/api/verify/");
        let device = percent_decode(device);
        let dev_path = format!("/dev/{}", device);
        if crate::verify::is_running() {
            json_response(request, 409, r#"{"error":"verify already running"}"#);
        } else {
            let keydb = cfg.read().ok().and_then(|c| c.keydb_path.clone());
            crate::verify::run_verify(&device, &dev_path, keydb);
            json_response(request, 200, r#"{"ok":true}"#);
        }
    } else {
        json_response(request, 404, r#"{"error":"not found"}"#);
    }
}

// ---------- Helpers ----------

fn serve_html(request: tiny_http::Request) {
    let header =
        Header::from_bytes(&b"Content-Type"[..], &b"text/html; charset=utf-8"[..]).unwrap();
    let html = DASHBOARD_HTML.replace("{VERSION}", env!("CARGO_PKG_VERSION"));
    let response = Response::from_string(html).with_header(header);
    let _ = request.respond(response);
}

fn json_response(request: tiny_http::Request, status: u16, body: &str) {
    let header = Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
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
    let state = match ripper::STATE.lock() {
        Ok(s) => s,
        Err(_) => return "{}".to_string(),
    };
    let move_state = crate::mover::MOVE_STATE
        .lock()
        .ok()
        .and_then(|ms| ms.clone());
    let verify_state = crate::verify::VERIFY_STATE
        .lock()
        .ok()
        .and_then(|vs| vs.clone());
    let mut obj = serde_json::to_value(&*state).unwrap_or_else(|_| serde_json::json!({}));
    if let Some(ms) = move_state {
        obj["_move"] = serde_json::to_value(&ms).unwrap_or_default();
    }
    if let Some(vs) = verify_state {
        obj["_verify"] = serde_json::to_value(&vs).unwrap_or_default();
    }
    obj.to_string()
}

fn handle_history_file(request: tiny_http::Request, cfg: &Arc<RwLock<Config>>, fname: &str) {
    let fname = percent_decode(fname);
    // Only allow safe filenames
    if !fname.ends_with(".json") || fname.contains("..") || fname.contains('/') {
        json_response(request, 400, r#"{"error":"invalid filename"}"#);
        return;
    }
    let history_dir = cfg.read().map(|c| c.history_dir()).unwrap_or_default();
    let path = format!("{}/{}", history_dir, fname);
    match std::fs::read_to_string(&path) {
        Ok(content) => {
            let header =
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
            let disp = format!("attachment; filename=\"{}\"", fname);
            let disp_header =
                Header::from_bytes(&b"Content-Disposition"[..], disp.as_bytes()).unwrap();
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

    // KEYDB status — check config path, then default path
    let keydb_paths: Vec<std::path::PathBuf> = [
        cfg.keydb_path.as_ref().map(std::path::PathBuf::from),
        Some(std::path::PathBuf::from(format!(
            "{}/freemkv/KEYDB.cfg",
            cfg.autorip_dir
        ))),
        libfreemkv::keydb::default_path().ok(),
    ]
    .into_iter()
    .flatten()
    .collect();

    let mut files_json = Vec::new();
    let mut found = false;
    for path in &keydb_paths {
        if let Ok(m) = std::fs::metadata(path) {
            let size = m.len();
            let size_str = if size > 1024 * 1024 {
                format!("{:.1} MB", size as f64 / (1024.0 * 1024.0))
            } else if size > 1024 {
                format!("{:.1} KB", size as f64 / 1024.0)
            } else {
                format!("{} B", size)
            };
            let modified = m
                .modified()
                .ok()
                .and_then(|t| {
                    t.duration_since(std::time::UNIX_EPOCH)
                        .ok()
                        .map(|d| format_epoch_datetime(d.as_secs()))
                })
                .unwrap_or_default();
            files_json.push(serde_json::json!({
                "name": format!("KEYDB.cfg ({})", path.display()),
                "present": true,
                "size": size_str,
                "updated": modified,
            }));
            found = true;
            break;
        }
    }
    if !found {
        files_json.push(serde_json::json!({
            "name": "KEYDB.cfg",
            "present": false,
        }));
    }

    // Move queue: scan staging for .done markers (pending moves)
    let move_queue: Vec<String> = std::fs::read_dir(&cfg.staging_dir)
        .ok()
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| e.path().is_dir() && e.path().join(".done").exists())
                .map(|e| {
                    let name = e.file_name().to_string_lossy().replace('_', " ");
                    format!("{} (moving)", name)
                })
                .collect()
        })
        .unwrap_or_default();

    // System log: last 50 lines
    let syslog_path = format!("{}/device_system.log", cfg.log_dir());
    let syslog = std::fs::read_to_string(&syslog_path)
        .unwrap_or_default()
        .lines()
        .rev()
        .take(50)
        .collect::<Vec<_>>()
        .join("\n");

    let body = serde_json::json!({
        "files": files_json,
        "move_queue": move_queue,
        "syslog": syslog,
    });

    json_response(request, 200, &body.to_string());
}

fn handle_device_log(request: tiny_http::Request, _cfg: &Arc<RwLock<Config>>, device: &str) {
    // Validate device name
    if !device.chars().all(|c| c.is_ascii_alphanumeric()) {
        text_response(request, "invalid device");
        return;
    }
    let lines = crate::log::get_device_log(device, 200);
    text_response(request, &lines.join("\n"));
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
        let mut c = match cfg.write() {
            Ok(c) => c,
            Err(_) => return json_response(request, 500, "{}"),
        };
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
        if let Some(v) = patch.get("keydb_url").and_then(|v| v.as_str()) {
            c.keydb_url = v.to_string();
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
        if let Some(v) = patch.get("on_read_error").and_then(|v| v.as_str()) {
            c.on_read_error = v.to_string();
        }
        // Legacy: migrate abort_on_error bool to on_read_error string
        if let Some(false) = patch.get("abort_on_error").and_then(|v| v.as_bool()) {
            c.on_read_error = "skip".to_string();
        }
        if let Some(v) = patch.get("output_format").and_then(|v| v.as_str()) {
            c.output_format = v.to_string();
        }
        if let Some(v) = patch.get("network_target").and_then(|v| v.as_str()) {
            c.network_target = v.to_string();
        }
        if let Some(v) = patch.get("min_length_secs").and_then(|v| v.as_u64()) {
            c.min_length_secs = v;
        }
        if let Some(v) = patch.get("port").and_then(|v| v.as_u64()) {
            c.port = v as u16;
        }
        if let Some(arr) = patch.get("webhook_urls").and_then(|v| v.as_array()) {
            c.webhook_urls = arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .filter(|s| !s.is_empty())
                .collect();
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

    let mut stream = request.upgrade("sse", response);

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

fn handle_scan(request: tiny_http::Request, cfg: &Arc<RwLock<Config>>, device: &str) {
    if ripper::STATE
        .lock()
        .map(|s| {
            s.get(device)
                .map(|r| r.status == "scanning" || r.status == "ripping")
                .unwrap_or(false)
        })
        .unwrap_or(false)
    {
        json_response(request, 409, r#"{"ok":false,"error":"busy"}"#);
        return;
    }

    let dev = device.to_string();
    let dev_path = format!("/dev/{}", device);
    let cfg = Arc::clone(cfg);
    ripper::update_state(
        &dev,
        ripper::RipState {
            device: dev.clone(),
            status: "scanning".to_string(),
            disc_present: true,
            ..Default::default()
        },
    );
    std::thread::spawn(move || {
        ripper::scan_disc(&cfg, &dev, &dev_path);
    });
    json_response(request, 200, r#"{"ok":true}"#);
}

fn handle_rip(request: tiny_http::Request, cfg: &Arc<RwLock<Config>>, device: &str) {
    let already = ripper::STATE
        .lock()
        .map(|s| {
            s.get(device)
                .map(|r| r.status == "scanning" || r.status == "ripping")
                .unwrap_or(false)
        })
        .unwrap_or(false);

    if already {
        json_response(request, 409, r#"{"ok":false,"error":"already ripping"}"#);
        return;
    }

    let dev = device.to_string();
    let dev_path = format!("/dev/{}", device);
    let cfg = Arc::clone(cfg);
    // Set scanning state before spawn — preserve TMDB info to prevent UI flash
    if let Ok(mut s) = ripper::STATE.lock() {
        if let Some(rs) = s.get_mut(&dev) {
            rs.status = "scanning".to_string();
        } else {
            s.insert(
                dev.clone(),
                ripper::RipState {
                    device: dev.clone(),
                    status: "scanning".to_string(),
                    disc_present: true,
                    ..Default::default()
                },
            );
        }
    }
    std::thread::spawn(move || {
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ripper::rip_disc(&cfg, &dev, &dev_path);
        }))
        .is_err()
        {
            crate::log::device_log(&dev, "Rip thread panicked");
            ripper::update_state(
                &dev,
                ripper::RipState {
                    device: dev.clone(),
                    status: "error".to_string(),
                    last_error: "Internal error (panic)".to_string(),
                    ..Default::default()
                },
            );
        }
    });

    json_response(request, 200, r#"{"ok":true}"#);
}

fn handle_update_keydb(request: tiny_http::Request, cfg: &Arc<RwLock<Config>>) {
    let keydb_url = cfg
        .read()
        .ok()
        .map(|c| c.keydb_url.clone())
        .unwrap_or_default();
    if keydb_url.is_empty() {
        json_response(
            request,
            400,
            r#"{"ok":false,"error":"No KEYDB URL configured. Set it in Settings."}"#,
        );
        return;
    }

    // Download via ureq (supports HTTPS) then save via libfreemkv
    let body = match ureq::get(&keydb_url).call() {
        Ok(resp) => {
            let mut buf = Vec::new();
            if resp
                .into_reader()
                .take(100 * 1024 * 1024)
                .read_to_end(&mut buf)
                .is_err()
            {
                json_response(
                    request,
                    500,
                    r#"{"ok":false,"error":"Failed to read response body."}"#,
                );
                return;
            }
            buf
        }
        Err(ureq::Error::Status(code, _)) => {
            let msg = format!(
                r#"{{"ok":false,"error":"Server returned HTTP {}. Check the URL in Settings."}}"#,
                code
            );
            json_response(request, 502, &msg);
            return;
        }
        Err(_) => {
            let msg = format!(
                r#"{{"ok":false,"error":"Could not connect to {}. Check the URL in Settings."}}"#,
                keydb_url.split('/').nth(2).unwrap_or(&keydb_url)
            );
            json_response(request, 502, &msg);
            return;
        }
    };

    match libfreemkv::keydb::save(&body) {
        Ok(result) => {
            let body = serde_json::json!({
                "ok": true,
                "entries": result.entries,
                "bytes": result.bytes,
                "path": result.path.display().to_string(),
            });
            json_response(request, 200, &body.to_string());
        }
        Err(_) => {
            json_response(
                request,
                500,
                r#"{"ok":false,"error":"Downloaded file is not a valid KEYDB. Check the URL."}"#,
            );
        }
    }
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
    // Signal the rip thread to stop
    ripper::request_stop(device);
    // Also stop verify if running
    crate::verify::request_stop();

    let existed = ripper::STATE
        .lock()
        .map(|mut s| {
            if let Some(rs) = s.get_mut(device) {
                rs.status = "idle".to_string();
                rs.disc_present = true;
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
            if let Ok(byte) = u8::from_str_radix(&String::from_utf8_lossy(&bytes[i + 1..i + 3]), 16)
            {
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
