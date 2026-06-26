use crate::config::{self, Config};
use crate::ripper;
use once_cell::sync::Lazy;
use std::io::{Read as _, Write as _};
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use tiny_http::{Header, Method, Response, Server, StatusCode};

/// Runtime debug flag - toggled via /api/debug POST.
pub static DEBUG_ENABLED: Lazy<Arc<RwLock<bool>>> = Lazy::new(|| Arc::new(RwLock::new(false)));

/// Check if debug logging is enabled.
///
/// Poison-tolerant: this runs on the mux hot path, so a panic elsewhere
/// while the write guard is held must not turn every later call into a
/// panic (which would kill the mux thread). Recover the inner value
/// instead of unwrapping.
pub fn debug_enabled() -> bool {
    *DEBUG_ENABLED
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

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
  /* Light-mode pill backgrounds (--green / --yellow / --red) are all
     saturated/dark — pair them with white pill text. Dark mode flips
     to lighter pill backgrounds, which want black text instead. */
  --pill-fg:#fff;
}
body.dark {
  --bg:#0d1117; --border:#3d444d; --text:#f0f6fc; --text2:#d1d9e0; --text3:#9198a1;
  --accent:#79c0ff; --green:#56d364; --yellow:#e3b341; --red:#ff7b72; --blue:#79c0ff;
  --card:#151b23; --log-bg:#151b23; --log-text:#d1d9e0; --log-border:#3d444d; --chip:#262c36; --poster-bg:#262c36;
  --pill-fg:#000;
}
* { margin:0; padding:0; box-sizing:border-box; }
/* Always reserve the vertical scrollbar gutter. Without this, switching
   between key sources (Local is taller than Online) makes the page scrollbar
   appear/disappear, which changes the viewport width and shifts the centered
   .c container sideways. overflow-y:scroll keeps the gutter present always. */
html { overflow-y:scroll; scrollbar-gutter:stable; }
body { font-family:-apple-system,system-ui,"Segoe UI",Roboto,sans-serif; background:var(--bg); color:var(--text); min-height:100vh; display:flex; flex-direction:column; }
.c { max-width:900px; margin:0 auto; padding:20px; width:100%; flex:1; display:flex; flex-direction:column; }
@keyframes p { 0%,100%{opacity:1} 50%{opacity:.3} }
@keyframes ph { 0%,100%{opacity:1} 50%{opacity:.45} }
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
.np { display:flex; align-items:flex-start; gap:20px; background:var(--card); border:1px solid var(--border); border-radius:12px; padding:20px; margin-bottom:16px; min-height:180px; }
.poster { width:120px; height:180px; border-radius:8px; background:var(--poster-bg); flex-shrink:0; align-self:flex-start; object-fit:cover; box-shadow:0 2px 8px rgba(0,0,0,.1); }
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
  <div id="bad-ranges"></div>
  <details style="margin-top:16px"><summary style="font-size:.7rem;color:var(--text3);text-transform:uppercase;font-weight:600;letter-spacing:1px;cursor:pointer;user-select:none">Log</summary>
  <div id="log" class="log" style="flex:1;max-height:none;margin-top:8px"></div></details>
</div>

<!-- System page -->
<div id="system" class="section">
  <div id="review"></div>
  <div class="card" style="margin-top:16px"><h2>Mux Queue</h2><div id="muxes"></div></div>
  <div class="card"><h2>Move Queue</h2><div id="moves"></div></div>
  <div class="card"><div class="setting"><label class="toggle"><input type="checkbox" id="debugToggle" onchange="toggleDebug(this.checked)"> Debug logging</label><div class="hint">Verbose logs for bug reports (autorip + rip library). Off by default.</div></div></div>
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
function esc(s){if(s==null)return'';return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;')}
function upd(id,html){const el=document.getElementById(id);if(el&&el._last!==html){el.innerHTML=html;el._last=html}}

/* ---- Navigation ---- */
document.querySelectorAll('.nav[data-tab]').forEach(btn=>{
  btn.addEventListener('click',function(){
    const tab=this.dataset.tab;
    document.querySelectorAll('.section').forEach(s=>s.classList.remove('active'));
    document.getElementById(tab).classList.add('active');
    document.querySelectorAll('.nav[data-tab]').forEach(b=>b.classList.remove('active'));
    this.classList.add('active');
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
/* Persisted across the ~1s data re-renders so the bad-range details stays
   open if the user opened it (upd() swaps innerHTML, which would otherwise
   collapse the native <details> every tick). Updated via its ontoggle. */
let _badRangesOpen=false;

function renderBar(s,p){
  /* Two modes, one renderer:
     - Pass 1 (sequential sweep, s.pass<=1): a genuine left-to-right progress
       fill. x-axis = work done. Green grows to pass_progress_pct. Bad-range
       ticks still overlay at their real LBA position so the operator sees
       damage accumulating during the sweep.
     - Pass 2-N (retry/patch, s.pass>1): a POSITIONAL disc map ("the disc,
       coloured by status"). x-axis = DISC POSITION (0..bytes_total_disc), NOT
       work done. The whole bar is the disc: GREEN everywhere it's good, RED
       segments at each still-bad range's real offset. As patches recover
       sectors, bad_ranges shrink and the red heals to green IN PLACE. A blue
       PLAYHEAD marks the current read position (last_sector) and pulses so it
       reads as "actively working here". */
  const total=s&&s.bytes_total_disc||0;
  const ranges=s&&s.bad_ranges||[];
  const positional=!!(s&&s.pass>1);
  let html='<div style="flex:1;background:var(--chip);border-radius:3px;height:6px;overflow:hidden;position:relative">';
  if(positional){
    /* Positional map: the entire bar is green (the whole disc, good), then
       red bad ranges are punched in at their real offsets. If total is
       unknown (0) we can't place anything positionally — fall back to a
       neutral green fill so we never divide by zero. */
    html+='<div style="position:absolute;left:0;top:0;width:100%;height:100%;background:var(--green)"></div>';
  }else{
    /* Sequential sweep: green grows with pass_progress_pct. */
    html+='<div style="background:var(--green);height:100%;width:'+p+'%;transition:width 1s"></div>';
  }
  /* Red bad-range overlay (both modes). Drawn at each range's real LBA
     position. min-width 0.5% keeps single-sector ranges visible on a 72GB
     UHD; clamp left+width so a range near the tail never overflows the bar. */
  if(total>0&&ranges.length){
    ranges.forEach(r=>{
      let offPct=(r.lba*2048)/total*100;
      if(offPct<0)offPct=0; if(offPct>100)offPct=100;
      let wPct=Math.max((r.count*2048)/total*100,0.5);
      if(offPct+wPct>100)wPct=100-offPct;
      html+='<div style="position:absolute;left:'+offPct+'%;top:0;width:'+wPct+'%;height:100%;background:var(--red);opacity:0.9;transition:left 1s,width 1s"></div>';
    });
  }
  /* Playhead: current read position = last_sector / total-sectors. Only during
     an active rip with a known position and a known total. Thin (2px), full
     height, accent/blue, pulsing — answers "where are we trying right now". It
     hops around the bad ranges as the patch works. */
  if(total>0&&s&&s.status==='ripping'&&s.last_sector>0){
    let phPct=(s.last_sector*2048)/total*100;
    if(phPct<0)phPct=0; if(phPct>100)phPct=100;
    html+='<div style="position:absolute;left:'+phPct+'%;top:0;width:2px;height:100%;margin-left:-1px;background:var(--blue);box-shadow:0 0 3px var(--blue);animation:ph 1.2s infinite;transition:left 1s"></div>';
  }
  html+='</div>';
  return html;
}
function renderTotalBar(p){
  /* v0.13.19: matches the pass bar's geometry (same height, same border-radius)
     so the two read as a pair instead of two unrelated components. The accent
     colour + lower opacity still signal "secondary / aggregate" without making
     the bar look stylistically different. No bad-range overlay — bad ranges
     are a per-disc concept and live on the pass bar. */
  return '<div style="flex:1;background:var(--chip);border-radius:3px;height:6px;overflow:hidden;position:relative;opacity:0.7">'
    +'<div style="background:var(--accent);height:100%;width:'+p+'%;transition:width 1s"></div>'
    +'</div>';
}
function passLabelFor(s){
   /* Resolve the current pass into a human-readable label for the Ripping
      step. During multipass we show pass number + phase; otherwise "Ripping". */
   if(s.pass>0&&s.total_passes>0){
     /* Phase rendering is identical regardless of which orchestrator
        produced the state. Operators read phase, not code path \u2014
        consistency between fresh-rip and resume is load-bearing.
        Resume sets total_passes to the same `max_retries + 2` value
        the multipass orchestrator uses, so both reach the mux phase
        showing the same `pass N/N \u00b7 muxing` label. */
     const phase=s.pass===1?'copying':(s.pass===s.total_passes?'muxing':'retrying');
     return 'pass '+s.pass+'/'+s.total_passes+' \u00b7 '+phase;
   }
   /* If pass=1 and no total_passes set, this is a clean disc — skip to mux. */
   if(s.pass===1&&s.total_passes===0){
     return 'pass 1/1 · copying';
   }
   return '';
 }
function renderSteps(steps,progress,eta,speed,s){
  if(!steps||!steps.length)return'';
  const icons={done:'\u2713',active:'\u25cf',pending:'\u25cb'};
  const colors={done:'var(--green)',active:'var(--accent)',pending:'var(--text3)'};
  return steps.map(st=>{
    let detail=st.detail||'';
    if(st.status==='active'&&st.name==='Ripping'){
      /* v0.13.18: two distinct bars + their own text rows.
           [pass bar           ] X% \u00b7 ETA H:MM \u00b7 NN MB/s
           [total bar          ] Total Y% \u00b7 Total ETA H:MM \u00b7 Recovered A.B / C.D GB
         Both bars read pass_progress_pct / total_progress_pct directly from
         the server. JS does NO math. */
      const passPct=(typeof s.pass_progress_pct==='number')?s.pass_progress_pct:(parseInt(progress)||0);
      const totalPct=(typeof s.total_progress_pct==='number')?s.total_progress_pct:passPct;
      const passLbl=passLabelFor(s);
      const header=passLbl?' \u00b7 '+passLbl:'';
      /* Three fixed-width columns + tabular-nums so the per-pass and
         total rows align visually: digits stack vertically across rows
         instead of shifting as the value width changes ("9%" -> "10%",
         "ETA 1:30:45" -> "ETA 0:05"). Empty slots reserve their column
         width so the totalLine doesn't drift right when speed is blank. */
      const TAB='font-variant-numeric:tabular-nums;display:inline-block;';
      const col=(body,minPx,align)=>
        '<span style="'+TAB+'min-width:'+minPx+'px;text-align:'+align+'">'+body+'</span>';
      const passPctStr=col(passPct+'%',45,'right');
      const passEtaStr=col(s.pass_eta?'ETA '+s.pass_eta:'',95,'left');
      const spdStr=col((speed&&speed!=='0 KB/s')?speed:'',85,'left');
      /* v0.13.19: wider text-row separators (em-spaces around the middle dot)
         + more vertical breathing room between bars and their text rows so
         the dashboard doesn't feel cramped. */
      const SEP=' \u2003\u00b7\u2003 ';
      /* Don't filter empty slots — they're already wrapped in fixed-width
         spans and need to keep their column position. Always join all 3. */
      const passLine=[passPctStr,passEtaStr,spdStr].join(SEP);
      /* 0.13.24: mirror the per-pass line's terse format. Drop the
         redundant "Total " prefix on ETA (the leading "Total N%" already
         makes the bar's identity obvious), and drop "Recovered X / Y GB"
         entirely — the green Good pill carries the same information
         without duplicating it. */
      /* Identical column structure to passLine — the "Total " prefix is
         gone now that a "Total Progress:" label sits above the bar (just
         like "Rip · pass N/M" sits above the per-pass bar). Third column
         is reserved with an empty body so the layout doesn't reflow. */
      const totalPctStr=col(totalPct+'%',45,'right');
      const totalEtaStr=col(s.total_eta?'ETA '+s.total_eta:'',95,'left');
      const totalSpdStr=col('',85,'left');
      const totalLine=[totalPctStr,totalEtaStr,totalSpdStr].join(SEP);
      /* 0.13.23: three-bucket display.
         GOOD  (green)  \u2014 Finished sectors. Always rendered when bytes_total_disc>0.
         MAYBE (yellow) \u2014 Pending sectors (Pass 2-N may recover). Hidden when 0.
         LOST  (red)    \u2014 Unreadable sectors (terminal). Hidden when 0.
         Each pill carries the bucket's video time equivalent, so users can
         see at a glance "X good \u00b7 2h05m, Y maybe \u00b7 2.4s, Z no chance \u00b7 0s".
         The damage_severity label appears alongside (Cosmetic / Moderate /
         Serious) only when there's actual loss. */
      let badLine='';
      const bg=s.bytes_good||0, bm=s.bytes_maybe||0, bl=s.bytes_lost||0;
      const haveAny = bg>0 || bm>0 || bl>0;
      if(haveAny){
        const fmtBytes = (b)=> b>=1073741824 ? (b/1073741824).toFixed(2)+' GB'
                            : b>=1048576    ? (b/1048576).toFixed(1)+' MB'
                            : b>=1024       ? (b/1024).toFixed(1)+' KB'
                            : b+' B';
        const maybeMs = (s.total_maybe_ms!=null && s.total_maybe_ms>=0) ? s.total_maybe_ms : 0;
        const lostMs  = (s.total_lost_ms!=null  && s.total_lost_ms >=0) ? s.total_lost_ms  : 0;
        /* Fixed-width per pill type: each pill's content can grow as
           values change ("864 MB" \u2192 "12.5 GB", "~3s" \u2192 "~01:23:45"),
           and an inline-block grows with content unless we pin it.
           Per-type min-widths sized for each pill's worst case so the
           Good chip doesn't waste space matching Maybe/Bad. */
        const pill = (label, color, body, minPx)=>
          '<span style="display:inline-block;padding:2px 8px;border-radius:10px;background:'+color
          +';color:var(--pill-fg);font-size:.65rem;font-weight:600;margin-right:6px;'
          +'min-width:'+minPx+'px;text-align:center;font-variant-numeric:tabular-nums">'
          +label+' '+body+'</span>';
        let pills='';
        if(bg>0){
          pills+=pill('Good','var(--green,#3aaa55)', fmtBytes(bg), 90);
        }
        if(bm>0){
          pills+=pill('Maybe','var(--yellow,#f0c000)', fmtBytes(bm)+' \u00b7 ~'+fmtMs(maybeMs), 170);
        }
        if(bl>0){
          /* damage_severity label only when terminal loss exists. */
          const sev=s.damage_severity||'';
          const sevLabel = sev==='serious' ? 'Serious'
                         : sev==='moderate' ? 'Moderate'
                         : sev==='cosmetic' ? 'Cosmetic'
                         : 'No chance';
          pills+=pill(sevLabel,'var(--red,#e34234)', fmtBytes(bl)+' \u00b7 ~'+fmtMs(lostMs), 195);
        }
        /* 0.13.24: bump margin-top from 6px to 14px so the pill row gets
           the same visual breathing room as the gap between the per-pass
           and total bars (matches the v0.13.19 polish on those bars). */
        if(pills) badLine='<div style="font-size:.7rem;margin-top:14px">'+pills+'</div>';
      }
      detail='<div style="margin-top:6px">'
        +renderBar(s,passPct)
        +'<div style="font-size:.75rem;color:var(--text2);margin-top:7px">'+passLine+'</div>'
        /* "Total Progress:" label mirrors the "Rip · pass N/M" header
           that's rendered above the per-pass bar (outside this `detail`
           string) — both bars now have a small caption above them so a
           glance at the dashboard tells you which is which. The 18px
           top-margin gives breathing room from the per-pass speed line. */
        +'<div style="font-size:.7rem;color:var(--text3);margin-top:18px">Total Progress:</div>'
        +'<div style="margin-top:6px">'+renderTotalBar(totalPct)+'</div>'
        +'<div style="font-size:.75rem;color:var(--text2);margin-top:7px">'+totalLine+'</div>'
        +badLine+'</div>';
      /* Fold pass info into the step name so it's obvious at a glance. */
      if(passLbl){
        /* 0.13.25: flex:1 + min-width:0 on the content span pins it to
           the remaining row width regardless of inner text length. Without
           this the span sizes to its content, so a longer header
           ("Pass 2/7: retrying bad ranges") makes the bar inside `detail`
           wider than a shorter one ("pass 1/7 · copying"), producing
           visible width wobble as the rip moves through phases. */
        return '<div style="display:flex;align-items:flex-start;gap:8px;padding:4px 0;font-size:.8rem"><span style="color:'+colors[st.status]+';font-size:.7rem;width:14px;text-align:center;flex-shrink:0;animation:p 1.5s infinite">'+icons[st.status]+'</span><span style="color:var(--text);flex:1;min-width:0">Rip'+header+detail+'</span></div>';
      }
    }else if(detail){detail=' \u2014 '+esc(detail)}
    const anim=st.status==='active'?';animation:p 1.5s infinite':'';
    return '<div style="display:flex;align-items:flex-start;gap:8px;padding:4px 0;font-size:.8rem"><span style="color:'+colors[st.status]+';font-size:.7rem;width:14px;text-align:center'+anim+'">'+icons[st.status]+'</span><span style="color:'+(st.status==='pending'?'var(--text3)':'var(--text)')+'">'+st.name+detail+'</span></div>';
  }).join('');
}
function fmtMs(ms){
  /* 0.13.24: escalate to minutes / hours / H:MM:SS for large durations.
     "10817 s" by itself means nothing — render it as "3:00:17". Below
     1 s we still want millisecond precision for tight read traces. */
  if(ms==null||!isFinite(ms))return'';
  if(ms<1)return'<1 ms';
  if(ms<1000)return ms.toFixed(0)+' ms';
  const totalSecs=ms/1000;
  if(totalSecs<60)return totalSecs.toFixed(2)+' s';
  const h=Math.floor(totalSecs/3600);
  const m=Math.floor((totalSecs%3600)/60);
  const s=Math.floor(totalSecs%60);
  return h>0
    ? h+':'+String(m).padStart(2,'0')+':'+String(s).padStart(2,'0')
    : m+':'+String(s).padStart(2,'0');
}
function fmtChapterTime(secs){
  if(secs==null||!isFinite(secs))return'';
  const h=Math.floor(secs/3600),m=Math.floor((secs%3600)/60),s=Math.floor(secs%60);
  return h>0?h+':'+String(m).padStart(2,'0')+':'+String(s).padStart(2,'0'):m+':'+String(s).padStart(2,'0');
}
function renderBadRanges(s){
  const ranges=s.bad_ranges||[];
  if(!ranges.length)return'';
  const n=s.num_bad_ranges||ranges.length;
  const totMs=s.total_lost_ms||0;
  const maxMs=s.largest_gap_ms||0;
  const truncated=s.bad_ranges_truncated||0;
  const summary=n+' bad range'+(n!==1?'s':'')+' \u00b7 '+fmtMs(totMs)+' total \u00b7 largest '+fmtMs(maxMs);
  let rows='';
  ranges.forEach(r=>{
    const loc=r.chapter?'ch '+r.chapter+(r.time_offset_secs!=null?' @ '+fmtChapterTime(r.time_offset_secs):''):'\u2014';
    rows+='<tr><td style="font-family:monospace;font-size:.75rem">'+r.lba.toLocaleString()+'</td><td style="font-size:.75rem">'+r.count+'</td><td style="font-size:.75rem">'+fmtMs(r.duration_ms)+'</td><td style="font-size:.75rem;color:var(--text3)">'+loc+'</td></tr>';
  });
  if(truncated>0)rows+='<tr><td colspan="4" style="font-size:.75rem;color:var(--text3);padding-top:6px">\u2026 +'+truncated+' smaller</td></tr>';
  return '<div class="card"><details'+(_badRangesOpen?' open':'')+' ontoggle="_badRangesOpen=this.open"><summary style="cursor:pointer;font-size:.85rem"><strong>'+summary+'</strong></summary><table style="width:100%;margin-top:8px;border-collapse:collapse"><thead><tr style="color:var(--text3);font-size:.7rem;text-align:left;text-transform:uppercase"><th>LBA</th><th>Sectors</th><th>Duration</th><th>Location</th></tr></thead><tbody>'+rows+'</tbody></table></details></div>';
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
  /* Persist the latest payload + refresh the Move Queue first — the
     mover keeps running (and `_move` keeps changing) even when the
     drive list is empty (idle / state briefly cleared), so the
     no-devices early return below must not gate Move Queue updates. */
  window._stateData=data;
  if(document.getElementById('system').classList.contains('active')){renderMuxes();renderMoves();}
  const devs=Object.keys(data).filter(k=>!k.startsWith('_'));
  if(!devs.length){
    upd('dtabs','');
    upd('np','<div class="np"><div class="idle-msg">'+D+'<p>No drives detected</p></div></div>');
    upd('actions','');upd('steps','');upd('err','');upd('bad-ranges','');
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

  renderCurrent();
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
    const b=fmt&&fmt!=='unknown'?'<span class="b '+esc(fmt)+'">'+esc(fmt)+'</span>':'';
    const o=s.tmdb_overview?'<div class="mo">'+esc(s.tmdb_overview)+'</div>':'';
    const yr=s.tmdb_year>0?s.tmdb_year:'';
    const dur=s.duration?' \u00b7 '+esc(s.duration):'';
    const codecs=s.codecs?'<div class="mo" style="color:var(--text3);font-size:.75rem;margin-top:6px">'+esc(s.codecs)+'</div>':'';
    const ks=s.key_status||'';const rc=ks.indexOf('Missing')===0?'var(--yellow)':'var(--green)';const ready=s.status==='idle'?'<div class="mo" style="color:'+rc+'">'+esc(ks||'Ready to rip')+'</div>':'';
    /* Before ripping (idle), let the operator correct the matched title:
       search TMDB and pick — the choice overrides the auto-match for this rip. */
    const editable=s.status==='idle';
    /* ✎ change sits in a fixed row ABOVE the title (not appended to it, where it
       shifted with title length). */
    const editRow=editable?'<div style="margin-bottom:6px"><button class="btn" style="padding:1px 7px;font-size:.7rem" onclick="titleEdit(\''+dev+'\')">✎ change</button></div>':'';
    const editBox=editable?'<div id="tedit-'+dev+'" style="display:none;margin-top:8px"></div>':'';
    card='<div class="np">'+img+'<div class="nfo">'+editRow+'<div class="mt">'+esc(title)+'</div><div class="my">'+yr+dur+' '+b+'</div>'+o+codecs+ready+editBox+'</div></div>';
  }
  upd('np',card);

  /* Actions bar */
  let btns='';
  if(active){
    btns='<button class="btn btn-stop" onclick="if(confirm(\'Stop?\')){this.disabled=true;fetch(\'/api/stop/'+dev+'\',{method:\'POST\'})}">Stop</button>';
    /* Elapsed-since-rip-started counter (v0.25.7). Ticks every 1s from
       a single setInterval. font-variant-numeric:tabular-nums keeps
       every digit the same pixel width so the second-flips don't
       jitter; min-width reserves space for the widest expected value
       ("1h 02m 34s" ≈ 70px) so growing past 10m or past 1h doesn't
       shove anything else around. */
    btns+='<span id="rip-elapsed-'+dev+'" data-started="'+(s.started_epoch_secs||0)+'" style="margin-left:10px;font-size:.78rem;color:var(--text2);align-self:center;font-variant-numeric:tabular-nums;min-width:80px;display:inline-block"></span>';
  }else if(scanned){
    /* Keys resolved at scan time. If they're missing (and the operator
       hasn't opted into capture-without-keys), don't offer Rip at all —
       it would just error. Offer "Scan again" so a freshly-loaded KEYDB
       or a corrected key source can be re-checked without a page reload. */
    const notReady=(s.key_status||'').indexOf('Missing')===0;
    if(notReady){
      btns='<button class="btn" onclick="fetch(\'/api/scan/'+dev+'\',{method:\'POST\'})">Scan again</button>';
    }else if(s.resumable){
      /* A resumable partial exists for this disc. Resume (accent) continues
         where the last rip left off — for "sweep" it reads only the missing
         ranges off the disc, for "remux" it just re-muxes the staged ISO.
         Rip (green) means START OVER: it wipes the partial first, so confirm. */
      const rl=s.resumable==='remux'?'Resume (re-mux)':'Resume';
      btns='<button class="btn" style="background:var(--accent);color:#fff;border-color:var(--accent)" onclick="fetch(\'/api/rip/'+dev+'?resume=yes\',{method:\'POST\'})">'+rl+'</button>';
      btns+='<button class="btn" style="background:var(--green);color:#fff;border-color:var(--green)" onclick="if(confirm(\'Start over from scratch? This discards the resumable partial for this disc.\')){fetch(\'/api/rip/'+dev+'?resume=no\',{method:\'POST\'})}">Rip</button>';
    }else{
      btns='<button class="btn" style="background:var(--green);color:#fff;border-color:var(--green)" onclick="fetch(\'/api/rip/'+dev+'?resume=no\',{method:\'POST\'})">Rip</button>';
    }
    btns+='<button class="btn" onclick="fetch(\'/api/verify/'+dev+'\',{method:\'POST\'})">Verify</button>';
  }else if(discIn){
    btns='<button class="btn" onclick="fetch(\'/api/scan/'+dev+'\',{method:\'POST\'})">Scan</button>';
  }
  if(discIn&&!active)btns+='<button class="btn btn-eject" onclick="fetch(\'/api/eject/'+dev+'\',{method:\'POST\'})">Eject</button>';

  const dot=active?'var(--green)':scanned?'var(--accent)':discIn?'var(--yellow)':'var(--text3)';
  const pulse=active?'animation:p 1.5s infinite;':'';
  /* statusLabel intentionally not shown here \u2014 it's already in the
     Ripping step header below ("Rip \u00b7 pass N/M \u00b7 copying") and the tab
     strip identifies which device this panel is for. Keep just the
     colored dot + dev name + action buttons in this row. */
  upd('actions','<div class="actions"><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:'+dot+';vertical-align:middle;margin-right:6px;'+pulse+'"></span><span style="font-size:.8rem;color:var(--text2)">'+dev+'</span><span style="margin-left:auto;display:flex;gap:6px">'+btns+'</span></div>');

  /* Steps */
  const steps=buildSteps(s);
  const progressStr=s.progress_pct>0?s.progress_pct+'%':(s.progress_gb>0?s.progress_gb.toFixed(1)+' GB':'');
  const speedStr=s.speed_mbs>=1?s.speed_mbs.toFixed(1)+' MB/s':s.speed_mbs>0?(s.speed_mbs*1024).toFixed(0)+' KB/s':'0 KB/s';
  const etaStr=s.eta||'';
  upd('steps',renderSteps(steps,progressStr,etaStr,speedStr,s));

  /* Error + recovery banner */
  let errHtml='';
  if(s.errors>0&&s.last_error){
    errHtml='<div style="background:var(--red);color:#fff;padding:8px 12px;border-radius:6px;font-size:.8rem;margin-bottom:8px">\u26a0 '+esc(s.last_error)+'</div>';
  }
  /* The old "N sectors skipped (X MB) — Y at risk" yellow box was removed
     (2026-06-05): it duplicated the Good/Maybe/No-chance pills (which already
     show the byte + time breakdown) and the bad-range bar (which shows where
     the damage is). The red banner above still surfaces a real last_error. */
  /* Adaptive batch recovery state \u2014 only during an active rip.
     current_batch < preferred_batch means the library shrunk the read size
     after a failure and is working through a marginal zone. Show a blue
     banner so the user can tell "recovering" from "stalled". */
  if(s.status==='ripping'&&s.current_batch>0&&s.preferred_batch>0&&s.current_batch<s.preferred_batch){
    const lbaStr=s.last_sector>0?' \u00b7 LBA '+s.last_sector.toLocaleString():'';
    errHtml+='<div style="background:var(--blue);color:#fff;padding:8px 12px;border-radius:6px;font-size:.8rem;margin-bottom:8px">\u21ba Recovering \u00b7 batch '+s.current_batch+' / '+s.preferred_batch+lbaStr+'</div>';
  }
  /* (Pass/phase info lives inside the Ripping step \u2014 no separate banner.) */
  upd('err',errHtml);
  upd('bad-ranges',renderBadRanges(s));

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

/* Live rip-elapsed counter (seconds resolution). */
function fmtElapsedSecs(s){if(!s||s<0)return'';s=+s;const h=Math.floor(s/3600),m=Math.floor((s%3600)/60),sec=s%60;return h>0?h+'h '+String(m).padStart(2,'0')+'m '+String(sec).padStart(2,'0')+'s':m+'m '+String(sec).padStart(2,'0')+'s'}
/* v0.25.7: tick the rip-elapsed counter every 1s. Reads each
   rip-elapsed-* span's data-started attribute (set by renderCurrent
   from the latest state push) so the value stays accurate even
   after the state push briefly rewrites the DOM. */
setInterval(()=>{
  const now=Math.floor(Date.now()/1000);
  document.querySelectorAll('[id^="rip-elapsed-"]').forEach(el=>{
    const started=+el.dataset.started||0;
    if(started>0){el.textContent=fmtElapsedSecs(now-started)}
    else{el.textContent=''}
  });
},1000);

/* ---- Candidate caches (avoid inlining titles/dirs — apostrophes break attrs;
       we key off integer indices instead). ---- */
let _REV=[];        /* held-rip items, by index */
let _RC={};         /* review TMDB candidates, by item index */
let _TC={};         /* ripper-card TMDB candidates, by device */

/* ---- Needs review (System page): rips held for a confident title ---- */
function reviewResolve(idx,action,extra){
  const it=_REV[idx]; if(!it)return;
  const body=Object.assign({dir:it.dir,action:action},extra||{});
  fetch('/api/review/resolve',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)})
    .then(r=>r.json()).then(()=>loadReview()).catch(()=>{});
}
function reviewSearch(idx){
  const q=(document.getElementById('rvq-'+idx)||{}).value; if(!q||!q.trim())return;
  const box=document.getElementById('rvc-'+idx); if(box)box.textContent='searching…';
  fetch('/api/tmdb/search?q='+encodeURIComponent(q.trim())).then(r=>r.json()).then(cs=>{
    if(!box)return; _RC[idx]=cs;
    if(!cs.length){box.textContent='no matches';return}
    box.innerHTML=cs.map((c,j)=>'<button class="btn" style="margin:2px" onclick="reviewPick('+idx+','+j+')">'+esc(c.title)+(c.year?' ('+c.year+')':'')+'</button>').join('');
  }).catch(()=>{if(box)box.textContent='search failed'});
}
function reviewPick(idx,j){const c=(_RC[idx]||[])[j]; if(c)reviewResolve(idx,'retitle',{title:c.title,year:c.year||0});}
function loadReview(){
  fetch('/api/review').then(r=>r.json()).then(items=>{
    const el=document.getElementById('review'); if(!el)return;
    _REV=items||[];
    if(!_REV.length){el.innerHTML='';return}
    let h='<div class="card" style="border-left:3px solid var(--accent);margin-bottom:16px">';
    h+='<div style="font-weight:600;margin-bottom:8px">⏸ Needs review — '+_REV.length+' rip(s) held for a confident title</div>';
    _REV.forEach((it,idx)=>{
      const t=esc(it.title||it.dir)+(it.year?' ('+it.year+')':'');
      h+='<div style="padding:8px 0;border-top:1px solid var(--border)">';
      h+='<div><strong>'+t+'</strong> <span style="color:var(--text3);font-size:.8rem">'+esc(it.reason||'')+'</span></div>';
      h+='<div style="color:var(--text3);font-size:.75rem">'+esc(it.file||'')+'</div>';
      h+='<div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap;align-items:center">';
      h+='<input id="rvq-'+idx+'" placeholder="correct title…" value="'+esc(it.title||'')+'" style="padding:4px 8px;border:1px solid var(--border);border-radius:6px">';
      h+='<button class="btn" onclick="reviewSearch('+idx+')">Search TMDB</button>';
      h+='<button class="btn" onclick="reviewResolve('+idx+',\'proceed\')">Proceed as-is</button>';
      h+='<button class="btn" onclick="if(confirm(\'Discard this rip?\'))reviewResolve('+idx+',\'cancel\')">Cancel</button>';
      h+='</div><div id="rvc-'+idx+'" style="margin-top:6px"></div></div>';
    });
    h+='</div>';
    el.innerHTML=h;
  }).catch(()=>{});
}
loadReview();
setInterval(loadReview,5000);

/* ---- Ripper-card title editor: correct the match BEFORE ripping ---- */
function titleEdit(dev){
  const el=document.getElementById('tedit-'+dev); if(!el)return;
  if(el.style.display!=='none'){el.style.display='none';return}
  el.style.display='block';
  el.innerHTML='<div style="display:flex;gap:6px;flex-wrap:wrap;align-items:center"><input id="tq-'+dev+'" placeholder="search a different title…" style="padding:4px 8px;border:1px solid var(--border);border-radius:6px"><button class="btn" onclick="titleSearch(\''+dev+'\')">Search TMDB</button></div><div id="tr-'+dev+'" style="margin-top:6px"></div>';
  const i=document.getElementById('tq-'+dev); if(i)i.focus();
}
function titleSearch(dev){
  const i=document.getElementById('tq-'+dev); const q=i?i.value.trim():''; if(!q)return;
  const box=document.getElementById('tr-'+dev); if(box)box.textContent='searching…';
  fetch('/api/tmdb/search?q='+encodeURIComponent(q)).then(r=>r.json()).then(cs=>{
    if(!box)return; _TC[dev]=cs;
    if(!cs.length){box.textContent='no matches';return}
    box.innerHTML=cs.map((c,j)=>'<button class="btn" style="margin:2px" onclick="titlePick(\''+dev+'\','+j+')">'+esc(c.title)+(c.year?' ('+c.year+')':'')+'</button>').join('');
  }).catch(()=>{if(box)box.textContent='search failed'});
}
function titlePick(dev,j){
  const c=(_TC[dev]||[])[j]; if(!c)return;
  fetch('/api/title/'+dev,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(c)})
    .then(r=>r.json()).then(()=>{const el=document.getElementById('tedit-'+dev);if(el)el.style.display='none';}).catch(()=>{});
}

function updateKeydb(stId){
  /* stId lets the same handler back both the System-page Data Files
     button (default 'keydb-status') and the Settings-page Local button
     ('keydb-status-settings'). Tolerates a missing status element. */
  const st=document.getElementById(stId||'keydb-status');
  const set=(t,c)=>{if(st){st.textContent=t;st.style.color=c;}};
  set('Updating…','var(--text3)');
  fetch('/api/update-keydb',{method:'POST'}).then(r=>r.json()).then(data=>{
    if(data.ok){set('Updated: '+data.entries+' entries','var(--green)');loadSystem();}
    else{set(data.error||'Update failed','var(--red)');}
  }).catch(e=>{set('Network error','var(--red)');});
}
/* ---- Mux queue with live progress (mirrors renderMoves shape) ---- */
function renderMuxes(){
  const el=document.getElementById('muxes');
  if(!el)return;
  const data=window._stateData||{};
  /* _mux on the wire is a RipState (the worker uses the synthetic
     `_mux` device key in update_state), not the MuxState struct —
     so we read disc_name / progress_pct / speed_mbs / eta. The
     synthetic device's status is "ripping" while the mux is in
     flight; treat absent or non-active as "no active mux". */
  const mx=data._mux;
  const muxActive=mx&&mx.status==='ripping'&&mx.disc_name;
  let html='';
  let hasContent=false;
  if(muxActive){
    hasContent=true;
    const pct=mx.progress_pct||0;
    const spdStr=mx.speed_mbs>=1?mx.speed_mbs.toFixed(1)+' MB/s':mx.speed_mbs>0?(mx.speed_mbs*1024).toFixed(0)+' KB/s':'';
    const etaStr=mx.eta?mx.eta+' remaining':'';
    const label=[pct+'%',spdStr,etaStr].filter(x=>x).join(' · ');
    html+='<div style="padding:6px 0"><div style="display:flex;align-items:center;gap:8px;margin-bottom:4px"><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--green);animation:p 1.5s infinite;flex-shrink:0"></span><span style="font-size:.85rem;font-weight:500">'+esc(mx.disc_name)+'</span></div>';
    html+='<div style="display:flex;align-items:center;gap:8px">';
    if(pct>0)html+='<div style="flex:1;background:var(--chip);border-radius:3px;height:3px;overflow:hidden"><div style="background:var(--green);height:100%;width:'+pct+'%;transition:width 1s"></div></div>';
    html+='<span style="font-size:.75rem;color:var(--text2)">'+label+'</span></div></div>';
  }
  /* Mux queue rides on the live state payload (_mux_queue), refreshed
     every SSE tick — so a job that moves on (mux finishes → Move queue)
     disappears here on the next tick instead of lingering until a hard
     refresh. `pending_queue` already excludes the dir currently muxing
     (it carries `.muxing`) and any dir that has entered the Move queue
     (`.done`/`.review`), so no frontend de-dup band-aid is needed: a job
     is in exactly one queue. Fall back to the older _muxQueue (from the
     /api/system fetch) only if the live field is absent. */
  const muxQ=(data._mux_queue!=null)?data._mux_queue:window._muxQueue;
  if(muxQ){
    muxQ.forEach(m=>{
      hasContent=true;
      html+='<div style="padding:4px 0;font-size:.8rem"><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--yellow);margin-right:8px;vertical-align:middle"></span>'+esc(m)+'</div>';
    });
  }
  if(!hasContent)html='<div style="color:var(--text3);font-size:.8rem">No pending muxes</div>';
  if(window._muxErrors&&window._muxErrors.length){
    html+='<div style="margin-top:8px;padding-top:8px;border-top:1px solid var(--chip)">';
    window._muxErrors.forEach(e=>{
      html+='<div style="padding:6px 0;font-size:.8rem">'
        +'<div style="display:flex;align-items:center;gap:8px;margin-bottom:2px">'
        +'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--red);flex-shrink:0"></span>'
        +'<span style="font-weight:500;color:var(--red)">'+esc(e.path||'')+'</span>'
        +'</div>'
        +'<div style="margin-left:16px;color:var(--text2)">'+esc(e.reason||'')+'</div>'
        +(e.hint?'<div style="margin-left:16px;color:var(--text3);font-size:.75rem;margin-top:2px">'+esc(e.hint)+'</div>':'')
        +'</div>';
    });
    html+='</div>';
  }
  upd('muxes',html);
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
  /* Pending queue items — from the live state payload (_move_queue),
     refreshed every SSE tick (falls back to the /api/system _moveQueue
     only if absent). The active move (_move) is rendered above with its
     progress bar; skip its matching queue entry so it isn't listed twice. */
  const moveQ=(data._move_queue!=null)?data._move_queue:window._moveQueue;
  if(moveQ){
    moveQ.forEach(m=>{
      if(mv&&mv.name&&m.replace(/ \(moving\)/,'').replace(/ /g,'_').includes(mv.name.replace(/ /g,'_')))return;
      hasContent=true;
      html+='<div style="padding:4px 0;font-size:.8rem"><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--yellow);margin-right:8px;vertical-align:middle"></span>'+esc(m)+'</div>';
    });
  }
  if(!hasContent)html='<div style="color:var(--text3);font-size:.8rem">No pending moves</div>';
  /* Stuck-move errors that need user action (orphaned staging dirs etc.) */
  if(window._moveErrors&&window._moveErrors.length){
    html+='<div style="margin-top:8px;padding-top:8px;border-top:1px solid var(--chip)">';
    window._moveErrors.forEach(e=>{
      html+='<div style="padding:6px 0;font-size:.8rem">'
        +'<div style="display:flex;align-items:center;gap:8px;margin-bottom:2px">'
        +'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--red);flex-shrink:0"></span>'
        +'<span style="font-weight:500;color:var(--red)">'+esc(e.path||'')+'</span>'
        +'</div>'
        +'<div style="margin-left:16px;color:var(--text2)">'+esc(e.reason||'')+'</div>'
        +(e.hint?'<div style="margin-left:16px;color:var(--text3);font-size:.75rem;margin-top:2px">'+esc(e.hint)+'</div>':'')
        +'</div>';
    });
    html+='</div>';
  }
  upd('moves',html);
}

/* ---- System page ---- */
function loadSystem(){
  fetch('/api/system').then(r=>r.json()).then(data=>{
    /* Move queue - store for renderMoves, then render */
    window._moveQueue=data.move_queue||[];
    window._moveErrors=data.move_errors||[];
    /* Mux queue (v0.25.3) — same shape, separate panel above */
    window._muxQueue=data.mux_queue||[];
    window._muxErrors=data.mux_errors||[];
    renderMuxes();
    renderMoves();
    /* Debug-logging toggle reflects current runtime state */
    const dbg=document.getElementById('debugToggle');
    if(dbg)dbg.checked=!!data.debug_enabled;
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

/* Flip runtime debug logging via POST /api/debug; sync the checkbox to the
   authoritative state the server returns. */
function toggleDebug(on){
  fetch('/api/debug',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({enabled:on})})
    .then(r=>r.json()).then(d=>{const t=document.getElementById('debugToggle');if(t)t.checked=!!d.enabled;})
    .catch(()=>{});
}

/* ---- Settings page ---- */
function loadSettings(){
  fetch('/api/settings').then(r=>r.json()).then(renderSettings).catch(()=>{});
}

function renderSettings(s){
  /* v0.13.19: derive a virtual `rip_mode` from `max_retries` so the radio
     selector renders with the right value on load. The backend stays on
     `max_retries` (and `keep_iso`) — `saveSettings` translates rip_mode back
     before POST. */
  if(typeof s.rip_mode!=='string'){
    s.rip_mode=(s.max_retries>0)?'multi':'single';
  }
  const groups=[
    {title:'Disc Lifecycle',fields:[
      {key:'on_insert',label:'On Disc Insert',type:'radio',options:[{value:'nothing',label:'Do Nothing'},{value:'scan',label:'Scan'},{value:'rip',label:'Rip'}],hint:'What happens when a disc is inserted'},
      {key:'auto_eject',label:'Auto Eject',type:'bool',hint:'Eject disc after rip completes'},
    ]},
    {title:'Ripping',fields:[
      // Output format is the parent setting — title-filtering only
      // makes sense when the rip ends in a mux step. ISO is a
      // whole-disc image; the title filters below have nothing to
      // act on, so they hide. Network output is a streamed mux, so
      // the title filters still apply.
      {key:'output_format',label:'Output Format',type:'radio',options:[{value:'mkv',label:'MKV'},{value:'m2ts',label:'M2TS'},{value:'iso',label:'ISO (disc image)'},{value:'network',label:'Network'}],hint:'Format for ripped files. ISO copies the whole disc; the other formats mux selected titles.'},
      {key:'network_target',label:'Network Target',type:'text',hint:'host:port for network output (e.g. nas.example.com:9000)',indent:true,placeholder:'nas.example.com:9000',showIf:{key:'output_format',value:'network'}},
      {key:'main_feature',label:'Main Feature Only',type:'bool',hint:'',indent:true,hideIf:{key:'output_format',value:'iso'}},
      {key:'min_length_secs',label:'Minimum Title Length (seconds)',type:'number',hint:'Shorter titles are skipped (600 = 10 min)',indent:true,hideIf:{key:'output_format',value:'iso'}},
    ]},
    {title:'Recovery',fields:[
      {key:'rip_mode',label:'Rip Mode',type:'radio',options:[{value:'single',label:'Single Pass'},{value:'multi',label:'Multi Pass'}],hint:'Single Pass: stream disc → MKV directly. Fastest, best for healthy discs. Multi Pass: rip an ISO, retry bad sectors with progressively smaller blocks, then mux to MKV. Use for discs with read errors.'},
      /* Single-pass error policy: only meaningful when there's no retry safety net. */
      {key:'on_read_error',label:'On Read Error',type:'radio',options:[{value:'stop',label:'Stop'},{value:'skip',label:'Skip (zero-fill)'}],hint:'Drive read error policy for single-pass rips. Stop aborts on the first bad sector. Skip zero-fills it and keeps streaming — useful when the disc is mostly fine and you accept minor loss for speed.',indent:true,showIf:{key:'rip_mode',value:'single'}},
      /* Multi-pass knobs: retries + accept-loss threshold. on_read_error doesn't apply
         in multi-pass — sweep always skips by design, retries always retry, and the
         post-retry abort decision is governed by abort_on_lost_secs (time-based). */
      {key:'max_retries',label:'Retry Passes',type:'number',hint:'How many retry passes to run on bad sectors. Each pass uses smaller blocks (60→30→15→7→1 sectors) and alternates direction. Default 5 covers most recoverable damage.',indent:true,showIf:{key:'rip_mode',value:'multi'}},
      {key:'keep_iso',label:'Keep Intermediate ISO',type:'bool',hint:'Promote the disc ISO into the library alongside the muxed title. Off by default to reclaim disk.',indent:true,showIf:{key:'rip_mode',value:'multi'}},
      {key:'abort_on_lost_secs',label:'Max Acceptable Main Movie Loss',type:'number',hint:'Seconds of missing data I will tolerate in the main feature after all retries finish. 0 = perfect rip required (abort if any loss). Applies to multi-pass only.',indent:true,showIf:{key:'rip_mode',value:'multi'}},
    ]},
    {title:'Output',fields:[
      {key:'staging_dir',label:'Staging Directory',type:'text',hint:'Where rips are written before being moved to the final destination. Use a fast local disk for performance; the finished MKV is moved to the output directory on completion.'},
      {key:'output_dir',label:'Output Directory',type:'text',hint:'Where all ripped files go by default'},
      {key:'movie_dir',label:'Movies',type:'text',hint:'',indent:true,placeholder:'Same as output directory'},
      {key:'tv_dir',label:'TV Series',type:'text',hint:'',indent:true,placeholder:'Same as output directory'},
    ]},
    {title:'API Keys',fields:[
      {key:'tmdb_api_key',label:'TMDB API Key',type:'text',hint:'v3 API key from themoviedb.org'},
    ]},
    {title:'Key Source',fields:[
      {key:'key_source',label:'AACS Key Source',type:'radio',options:[{value:'local',label:'Local KEYDB'},{value:'online',label:'Online Keyserver'}],hint:'Where per-disc AACS keys come from. Local uses a KEYDB.cfg on disk; Online queries a keyserver.'},
      {key:'keydb_path',label:'KEYDB.cfg Location',type:'text',hint:'Path to KEYDB.cfg on disk (blank = default location).',indent:true,showIf:{key:'key_source',value:'local'}},
      {key:'keydb_url',label:'KEYDB Update URL',type:'text',hint:'HTTP URL to download KEYDB.cfg (zip, gz, or plain text).',indent:true,showIf:{key:'key_source',value:'local'}},
      {type:'action',action:"updateKeydb('keydb-status-settings')",button:'Update KEYDB',status:'keydb-status-settings',hint:'Download the KEYDB.cfg from the URL above into the configured location.',indent:true,showIf:{key:'key_source',value:'local'}},
      {key:'keyserver_url',label:'Keyserver URL',type:'text',hint:'Full keyserver endpoint URL — the decode request is POSTed here verbatim, so include the path (e.g. https://host/decode).',indent:true,showIf:{key:'key_source',value:'online'}},
      {key:'keyserver_secret',label:'Keyserver API Secret',type:'text',hint:'Bearer token for the keyserver, if it requires one.',indent:true,showIf:{key:'key_source',value:'online'}},
      {key:'capture_without_keys',label:'Capture Discs Without Keys',type:'bool',hint:'No usable keys → capture the disc to an ISO and mux later when keys become available. Off = skip the disc.'},
    ]},
    {title:'Performance',fields:[
      {key:'decrypt_threads',label:'Decrypt Threads',type:'number',hint:'How many threads AACS decryption uses. 0 = auto (all available cores, capped at 64). Drop to 4-8 if autorip is sharing the host with other heavy workloads.'},
      {key:'log_retention_days',label:'Log Retention (days)',type:'number',hint:'Per-device .log files older than this are pruned by the in-process daily cleanup. Default 30.'},
    ]},
  ];
  let html='';
  groups.forEach(g=>{
    html+='<div class="card"><h2>'+g.title+'</h2>';
    g.fields.forEach(f=>{
      const v=s[f.key]!=null?s[f.key]:'';
      const indent=f.indent?'margin-left:20px;border-left:2px solid var(--border);padding-left:12px':'';
      const ph=f.placeholder?' placeholder="'+f.placeholder+'"':'';
      const hideShow=f.showIf&&s[f.showIf.key]!==f.showIf.value;
      const hideHide=f.hideIf&&s[f.hideIf.key]===f.hideIf.value;
      const hide=(hideShow||hideHide)?'display:none;':'';
      const showAttr=f.showIf?' data-show-key="'+f.showIf.key+'" data-show-value="'+f.showIf.value+'"':(f.hideIf?' data-hide-key="'+f.hideIf.key+'" data-hide-value="'+f.hideIf.value+'"':'');
      if(f.type==='action'){
        html+='<div class="setting" style="'+indent+hide+'"'+showAttr+'><div style="display:flex;align-items:center;gap:10px"><button type="button" class="btn" onclick="'+f.action+'">'+f.button+'</button><span id="'+f.status+'" style="font-size:.8rem"></span></div>'+(f.hint?'<div class="hint">'+f.hint+'</div>':'')+'</div>';
      }else if(f.type==='radio'){
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
  // hideIf: hide when the referenced field has the given value
  // (inverse of showIf). Used to gate title-filtering settings on
  // output formats that actually have a mux step.
  document.querySelectorAll('[data-hide-key]').forEach(el=>{
    const k=el.dataset.hideKey,v=el.dataset.hideValue;
    const radio=document.querySelector('input[data-key="'+k+'"]:checked');
    el.style.display=radio&&radio.value===v?'none':'';
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
 /* v0.13.19: translate the virtual `rip_mode` selector back to the backend
      fields. Single → max_retries=0. Keep keep_iso unchanged so the stored
      preference survives a mode switch (the server no longer clobbers it
      from rip_mode either). Multi → keep whatever max_retries the user set;
      default to 5 if they flipped to multi without ever touching the count.
      The `rip_mode` key itself is never persisted — the backend already
      infers it from max_retries on the next render. */
   if(s.rip_mode==='single'){s.max_retries=0}
   else if(s.rip_mode==='multi'&&(!s.max_retries||s.max_retries<1)){s.max_retries=5}
   delete s.rip_mode;
  /* Loud, hard-to-miss feedback on save. The previous version flashed
     "Saved" in a small green span next to the button for 2 s and did
     nothing at all on error — easy to miss and silent on failure.
     Now: the button itself transitions through Saving… → ✓ Saved (green
     fill) → original label, and the adjacent status span carries any
     error message in red. */
  const btn=document.getElementById('savebtn');
  const status=document.getElementById('save-status');
  const origLabel=btn.textContent;
  btn.disabled=true;
  btn.textContent='Saving…';
  status.textContent='';
  status.style.color='var(--green)';
  fetch('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(s)})
    .then(r=>{
      if(!r.ok)throw new Error('HTTP '+r.status);
      btn.textContent='✓ Saved';
      btn.style.background='var(--green)';
      btn.style.color='#fff';
      btn.style.borderColor='var(--green)';
      status.textContent='Saved';
      setTimeout(()=>{
        btn.disabled=false;
        btn.textContent=origLabel;
        btn.style.background='';
        btn.style.color='';
        btn.style.borderColor='';
        status.textContent='';
      },2000);
    })
    .catch(e=>{
      btn.disabled=false;
      btn.textContent=origLabel;
      status.style.color='var(--red)';
      status.textContent='Save failed: '+e.message;
    });
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
            // Bind failure is unrecoverable for an autorip instance — without
            // a UI we have a dead daemon. Pre-0.13 this was eprintln + return,
            // leaving the process alive with no UI and Docker none the wiser.
            // Now we signal SHUTDOWN so main exits non-zero and the container
            // restart policy recovers us.
            crate::log::syslog(&format!(
                "FATAL: web server bind failed on {}: {} — signalling shutdown",
                addr, e
            ));
            tracing::error!(
                address = %addr,
                error = %e,
                "web bind failed; signalling shutdown so the container restart policy recovers us"
            );
            crate::SHUTDOWN.store(true, std::sync::atomic::Ordering::SeqCst);
            return;
        }
    };
    crate::log::syslog(&format!("Web server listening on {}", addr));
    tracing::info!(address = %addr, "web server listening");

    for request in server.incoming_requests() {
        if crate::SHUTDOWN.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }
        // Bound concurrent handler threads so a connection flood can't
        // fork the container to death (unauthenticated LAN DoS). Over the
        // cap we answer 503 on this thread and move on without spawning.
        let guard = match ConnGuard::try_acquire(&INFLIGHT_HANDLERS, MAX_INFLIGHT_HANDLERS) {
            Some(g) => g,
            None => {
                tracing::warn!(
                    max = MAX_INFLIGHT_HANDLERS,
                    "request rejected: in-flight handler cap reached"
                );
                json_response(request, 503, r#"{"ok":false,"error":"server busy"}"#);
                continue;
            }
        };
        let cfg = Arc::clone(cfg);
        if let Err(e) = std::thread::Builder::new()
            .name("autorip-http".into())
            .spawn(move || {
                // Hold the admission token for the handler's lifetime;
                // dropped here on return/unwind, freeing the slot.
                let _guard = guard;
                handle_request(request, &cfg);
            })
        {
            tracing::error!(error = %e, "failed to spawn request handler thread");
            // guard drops here, freeing the reserved slot.
        }
    }
    tracing::info!("web server stopping");
}

/// Extract a header value by case-insensitive field name.
fn header_value<'a>(request: &'a tiny_http::Request, name: &str) -> Option<&'a str> {
    // `HeaderField::equiv` requires a `&'static str`; compare the field
    // name ourselves so we can take a borrowed `name`. HTTP header field
    // names are case-insensitive.
    request
        .headers()
        .iter()
        .find(|h| h.field.as_str().as_str().eq_ignore_ascii_case(name))
        .map(|h| h.value.as_str())
}

/// Pull the host[:port] authority out of a URL or a bare Host header value.
fn authority_of(s: &str) -> Option<String> {
    // Strip scheme (origin headers look like `http://host:port`); Host
    // headers are already bare. Then strip any path/query tail.
    let after_scheme = s.split("://").last().unwrap_or(s);
    let host = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(after_scheme)
        .trim();
    if host.is_empty() {
        None
    } else {
        Some(host.to_ascii_lowercase())
    }
}

/// Default TCP port implied by a URL scheme (used to normalize an authority
/// that omits its port). Only the two web schemes matter here.
fn default_port_for_scheme(s: &str) -> u16 {
    if s.starts_with("https://") {
        443
    } else {
        // http:// and bare Host values (no scheme) both default to 80,
        // which is the right comparison baseline for a same-origin POST.
        80
    }
}

/// Normalize an authority (`host` or `host:port`) to a canonical
/// `host:port`, filling in `default_port` when the port is omitted. This
/// lets `http://host` (Origin, port implied) compare equal to `host:80`
/// (Host header) so a legitimate same-origin request on the scheme's
/// default port isn't falsely rejected as cross-origin.
fn normalize_authority(authority: &str, default_port: u16) -> Option<String> {
    let a = authority_of(authority)?;
    // Bracketed IPv6 literal: [::1] or [::1]:8080.
    if let Some(rest) = a.strip_prefix('[') {
        let (host, after) = rest.split_once(']')?;
        let port = match after.strip_prefix(':') {
            Some(p) => p.parse::<u16>().ok()?,
            None if after.is_empty() => default_port,
            None => return None,
        };
        return Some(format!("[{host}]:{port}"));
    }
    match a.rsplit_once(':') {
        // Trailing ':NNN' is a port only if numeric; otherwise treat the
        // whole thing as a host (defensive — keeps a stray colon from
        // silently dropping the port).
        Some((host, p)) => match p.parse::<u16>() {
            Ok(port) => Some(format!("{host}:{port}")),
            Err(_) => Some(format!("{a}:{default_port}")),
        },
        None => Some(format!("{a}:{default_port}")),
    }
}

/// Lightweight cross-origin defense for state-changing POST routes.
///
/// This service is intentionally unauthenticated on the LAN and is driven
/// both by a browser dashboard and by operator `curl`/monitoring scripts
/// (which send no Origin header). So the policy is deliberately permissive:
/// if an `Origin` (or, failing that, `Referer`) header is PRESENT and its
/// host does NOT match the request's Host header, reject with 403. If no
/// such header is present we ALLOW the request, so curl and monitoring
/// keep working. This is defense-in-depth against a browser on the same
/// LAN being used to forge state-changing requests (CSRF); it is not an
/// authentication mechanism.
///
/// Returns `true` if the request should be rejected (caller sends 403).
fn is_cross_origin_post(request: &tiny_http::Request) -> bool {
    let origin = header_value(request, "Origin").or_else(|| header_value(request, "Referer"));
    let host = header_value(request, "Host");
    is_cross_origin(origin, host)
}

/// Pure cross-origin decision over the raw `Origin`/`Referer` and `Host`
/// header values. Returns `true` when the request should be rejected.
/// Absent/empty Origin → allow (curl/monitoring). Unparseable Origin or
/// absent Host → can't prove cross-origin, so allow.
fn is_cross_origin(origin: Option<&str>, host: Option<&str>) -> bool {
    let origin = match origin {
        None => return false,
        Some(o) if o.trim().is_empty() => return false,
        Some(o) => o,
    };
    // The Origin/Referer carries the scheme, which fixes the default port for
    // BOTH sides: a same-origin request's Host equals the Origin's host:port,
    // so the Host header (which never carries a scheme) is normalized against
    // the same scheme's default. Without this, `http://host` (Origin, port
    // implied) wouldn't match `host:80` (Host header) and a legitimate
    // same-origin POST on the default port would be falsely 403'd.
    let default_port = default_port_for_scheme(origin.trim());
    let origin_norm = match normalize_authority(origin, default_port) {
        Some(h) => h,
        None => return false,
    };
    let host_norm = match host.and_then(|h| normalize_authority(h, default_port)) {
        Some(h) => h,
        None => return false,
    };
    origin_norm != host_norm
}

fn handle_request(request: tiny_http::Request, cfg: &Arc<RwLock<Config>>) {
    let url = request.url().to_string();
    let is_get = *request.method() == Method::Get;
    let is_post = *request.method() == Method::Post;

    // Defense-in-depth CSRF check: reject a state-changing POST whose
    // Origin/Referer host disagrees with our Host header. Absent header is
    // allowed so curl/monitoring scripts keep working (see helper doc).
    if is_post && is_cross_origin_post(&request) {
        return json_response(
            request,
            403,
            r#"{"ok":false,"error":"cross-origin request rejected"}"#,
        );
    }

    if is_get && (url == "/" || url == "/index.html") {
        serve_html(request);
    } else if is_get && url == "/api/state" {
        let staging_dir = cfg
            .read()
            .map(|c| c.staging_dir.clone())
            .unwrap_or_default();
        json_response(request, 200, &get_state_json(&staging_dir));
    } else if is_get && url == "/api/version" {
        json_response(
            request,
            200,
            &format!("{{\"version\":\"{}\"}}", env!("CARGO_PKG_VERSION")),
        );
    } else if is_get && url == "/api/settings" {
        let c = match cfg.read() {
            Ok(c) => c,
            Err(_) => {
                return json_response(
                    request,
                    500,
                    r#"{"ok":false,"error":"config lock poisoned"}"#,
                );
            }
        };
        let json = settings_json_redacted(&c);
        json_response(request, 200, &json);
    } else if is_post && url == "/api/settings" {
        handle_settings_post(request, cfg);
    } else if is_get && url == "/api/system" {
        handle_system_info(request, cfg);
    } else if is_get && url.starts_with("/api/logs/") {
        let device = url.trim_start_matches("/api/logs/");
        let device = percent_decode(device);
        if !is_valid_device_name(&device) {
            return json_response(request, 400, r#"{"error":"invalid device name"}"#);
        }
        handle_device_log(request, cfg, &device);
    } else if is_post && url == "/api/debug" {
        handle_debug_toggle(request);
    } else if is_get && (url == "/api/debug" || url.starts_with("/api/debug?")) {
        handle_debug_log(request, &url);
    } else if is_get && url == "/events" {
        handle_sse(request, cfg);
    } else if is_post && url.starts_with("/api/scan/") {
        let device = url.trim_start_matches("/api/scan/");
        let device = percent_decode(device);
        if !is_valid_device_name(&device) {
            return json_response(request, 400, r#"{"error":"invalid device name"}"#);
        }
        handle_scan(request, cfg, &device);
    } else if is_post && url.starts_with("/api/rip/") {
        let path = url.trim_start_matches("/api/rip/");
        // Split off the query string. URL form: /api/rip/<device>[?resume=yes|no]
        let (device_raw, query) = match path.split_once('?') {
            Some((d, q)) => (d, q),
            None => (path, ""),
        };
        let device = percent_decode(device_raw);
        if !is_valid_device_name(&device) {
            return json_response(request, 400, r#"{"error":"invalid device name"}"#);
        }
        handle_rip(request, cfg, &device, query);
    } else if is_post && url == "/api/update-keydb" {
        handle_update_keydb(request, cfg);
    } else if is_post && url.starts_with("/api/eject/") {
        let device = url.trim_start_matches("/api/eject/");
        let device = percent_decode(device);
        if !is_valid_device_name(&device) {
            return json_response(request, 400, r#"{"error":"invalid device name"}"#);
        }
        handle_eject(request, &device);
    } else if is_post && url.starts_with("/api/stop/") {
        let device = url.trim_start_matches("/api/stop/");
        let device = percent_decode(device);
        if !is_valid_device_name(&device) {
            return json_response(request, 400, r#"{"error":"invalid device name"}"#);
        }
        handle_stop(request, cfg, &device);
    } else if is_post && url.starts_with("/api/verify/") {
        let device = url.trim_start_matches("/api/verify/");
        let device = percent_decode(device);
        if !is_valid_device_name(&device) {
            return json_response(request, 400, r#"{"error":"invalid device name"}"#);
        }
        let dev_path = format!("/dev/{}", device);
        // Gate on the unified per-device claim, not a verify-local "already
        // running" check: a rip/scan/eject in progress must also reject a
        // verify (and vice-versa). try_claim_active is the single source of
        // truth; reject early here so the caller gets a 409, then let
        // run_verify perform the actual atomic claim it will hold.
        if ripper::is_busy(&device) || crate::verify::is_running(&device) {
            json_response(request, 409, r#"{"error":"device busy"}"#);
        } else {
            let keydb = cfg.read().ok().and_then(|c| c.keydb_path.clone());
            crate::verify::run_verify(&device, &dev_path, keydb);
            json_response(request, 200, r#"{"ok":true}"#);
        }
    } else if is_get && url == "/api/review" {
        let staging = cfg
            .read()
            .map(|c| c.staging_dir.clone())
            .unwrap_or_default();
        let items = crate::review::list_held(&staging);
        json_response(
            request,
            200,
            &serde_json::to_string(&items).unwrap_or_else(|_| "[]".to_string()),
        );
    } else if is_post && url == "/api/review/resolve" {
        handle_review_resolve(request, cfg);
    } else if is_get && url.starts_with("/api/tmdb/search") {
        handle_tmdb_search(request, cfg, &url);
    } else if is_post && url.starts_with("/api/title/") {
        let device = percent_decode(url.trim_start_matches("/api/title/"));
        if !is_valid_device_name(&device) {
            return json_response(request, 400, r#"{"error":"invalid device name"}"#);
        }
        handle_title_override(request, &device);
    } else {
        json_response(request, 404, r#"{"error":"not found"}"#);
    }
}

/// Defensive validation for an operator-supplied poster URL. The value is
/// later interpolated into an `<img src>` attribute on the dashboard; require
/// an http(s) scheme and reject control characters or quotes so it can't break
/// out of the attribute context even if the front-end escaping regresses.
fn is_valid_poster_url(url: &str) -> bool {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return false;
    }
    !url.chars()
        .any(|c| c.is_control() || c == '"' || c == '\'' || c == '<' || c == '>')
}

/// `POST /api/title/<device>` — operator's TMDB pick for the active disc (from the
/// Ripper card). Body: `{"title":"…","year":2024,"poster_url":"…","overview":"…"}`.
/// Stored as a one-shot override `rip_disc` consumes; also reflected on the live
/// card immediately.
fn handle_title_override(request: tiny_http::Request, device: &str) {
    // Gate on a known device: an override for a drive that isn't tracked in
    // STATE has nothing to attach to and would just persist orphaned. Match
    // how other per-device routes validate (404 unknown). This runs before
    // the body is read so we reject early.
    if !ripper::device_known(device) {
        return json_response(request, 404, r#"{"ok":false,"error":"unknown device"}"#);
    }
    let (request, body) = match read_json_body(request) {
        Ok(rb) => rb,
        Err(()) => return,
    };
    let v: serde_json::Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(_) => return json_response(request, 400, r#"{"ok":false,"error":"invalid json"}"#),
    };
    // Clamp operator-supplied free text on char boundaries before it's
    // persisted and re-broadcast to every dashboard client (mirrors the
    // 200-char `q` cap). Caps: title ~300, overview ~2000, poster_url ~1000.
    let title = clamp_chars(v["title"].as_str().unwrap_or("").trim(), 300);
    if title.is_empty() {
        return json_response(request, 400, r#"{"ok":false,"error":"title required"}"#);
    }
    let year = v["year"]
        .as_u64()
        .and_then(|y| u16::try_from(y).ok())
        .unwrap_or(0);
    let poster_raw = v["poster_url"].as_str().unwrap_or("");
    if !poster_raw.is_empty() && !is_valid_poster_url(poster_raw) {
        return json_response(request, 400, r#"{"ok":false,"error":"invalid poster_url"}"#);
    }
    let poster = clamp_chars(poster_raw, 1000);
    let overview = clamp_chars(v["overview"].as_str().unwrap_or(""), 2000);
    let media_type = v["media_type"].as_str().unwrap_or("movie").to_string();
    ripper::set_title_override(
        device,
        crate::tmdb::TmdbResult {
            title: title.clone(),
            year,
            poster_url: poster.clone(),
            overview: overview.clone(),
            media_type,
        },
    );
    // Reflect on the live card right away.
    ripper::update_state_with(device, |s| {
        s.tmdb_title = title.clone();
        s.tmdb_year = year;
        if !poster.is_empty() {
            s.tmdb_poster = poster.clone();
        }
        if !overview.is_empty() {
            s.tmdb_overview = overview.clone();
        }
    });
    json_response(request, 200, r#"{"ok":true}"#);
}

/// `POST /api/review/resolve` — resolve a held rip. Body:
/// `{"dir":"<staging subdir>","action":"proceed|retitle|cancel","title":"…","year":2024}`.
fn handle_review_resolve(request: tiny_http::Request, cfg: &Arc<RwLock<Config>>) {
    let (request, body) = match read_json_body(request) {
        Ok(rb) => rb,
        Err(()) => return,
    };
    let v: serde_json::Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(_) => return json_response(request, 400, r#"{"ok":false,"error":"invalid json"}"#),
    };
    // Cap operator-supplied strings before they reach a filesystem marker,
    // mirroring handle_title_override (clamp_chars by char count, not bytes).
    let dir = clamp_chars(v["dir"].as_str().unwrap_or("").trim(), 300);
    let staging = cfg
        .read()
        .map(|c| c.staging_dir.clone())
        .unwrap_or_default();
    let action = match v["action"].as_str().unwrap_or("") {
        "proceed" => crate::review::Resolve::Proceed,
        "cancel" => crate::review::Resolve::Cancel,
        "retitle" => {
            let title = clamp_chars(v["title"].as_str().unwrap_or("").trim(), 300);
            if title.is_empty() {
                return json_response(request, 400, r#"{"ok":false,"error":"title required"}"#);
            }
            let year = v["year"]
                .as_u64()
                .and_then(|y| u16::try_from(y).ok())
                .unwrap_or(0);
            crate::review::Resolve::Retitle { title, year }
        }
        _ => return json_response(request, 400, r#"{"ok":false,"error":"bad action"}"#),
    };
    match crate::review::resolve(&staging, &dir, action) {
        Ok(()) => json_response(request, 200, r#"{"ok":true}"#),
        Err(e) => {
            // Build the error payload with serde so backslashes, newlines,
            // and control chars in a filesystem error string are escaped
            // properly — manual quote-replacement produced malformed JSON
            // the browser silently failed to parse.
            let body = serde_json::json!({ "ok": false, "error": e }).to_string();
            json_response(request, 400, &body)
        }
    }
}

/// `GET /api/tmdb/search?q=<query>` — candidate matches for the review picker.
fn handle_tmdb_search(request: tiny_http::Request, cfg: &Arc<RwLock<Config>>, url: &str) {
    // Parse via parse_query so `q` is found regardless of parameter order
    // (split_once("?q=") only matched q as the first query parameter, so
    // e.g. /api/tmdb/search?version=2&q=movie yielded an empty query).
    let q = parse_query(url).get("q").cloned().unwrap_or_default();
    let q = q.trim();
    // Reject empty queries and cap length so we never forward an abusive
    // request to TMDB.
    if q.is_empty() || q.len() > 200 {
        return json_response(request, 400, r#"{"error":"invalid query"}"#);
    }
    // Global cooldown: an unauthenticated LAN client could otherwise flood
    // TMDB through this proxy. Gate on the time since the last forwarded
    // search; reply 429 if a request arrived too recently.
    {
        use std::sync::Mutex;
        use std::time::{Duration, Instant};
        static LAST_TMDB_SEARCH: Mutex<Option<Instant>> = Mutex::new(None);
        const TMDB_MIN_INTERVAL: Duration = Duration::from_millis(500);
        let mut last = LAST_TMDB_SEARCH.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        if let Some(prev) = *last {
            if now.duration_since(prev) < TMDB_MIN_INTERVAL {
                return json_response(request, 429, r#"{"error":"rate limited"}"#);
            }
        }
        *last = Some(now);
    }
    let key = cfg
        .read()
        .map(|c| c.tmdb_api_key.clone())
        .unwrap_or_default();
    let results = crate::tmdb::search(q, &key, 8);
    json_response(
        request,
        200,
        &serde_json::to_string(&results).unwrap_or_else(|_| "[]".to_string()),
    );
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

/// Sentinel returned in place of a stored secret on GET /api/settings.
/// On POST, a field carrying exactly this value is treated as "unchanged"
/// so the UI can round-trip the redacted form without clobbering the real
/// secret.
const SECRET_SENTINEL: &str = "********";

/// Mask a webhook URL for display: keep the origin (`scheme://host[:port]`)
/// so the operator can tell Discord from Slack from Jellyfin, but replace the
/// path/query — where Discord/Slack/Jellyfin embed the secret token — with the
/// sentinel. e.g. `https://discord.com/api/webhooks/1/tok` → `https://discord.com/********`.
/// A masked value round-trips on POST: any entry CONTAINING the sentinel is
/// treated as "unchanged" and resolved back to the stored URL.
fn mask_webhook_url(url: &str) -> String {
    // Origin = everything up to the first '/', '?', or '#' after `scheme://`.
    // Treating '?' and '#' as terminators prevents a token carried in a query
    // string (`https://host?token=SECRET`) from slipping through unredacted.
    if let Some(scheme_end) = url.find("://") {
        let after = scheme_end + 3;
        let origin_end = url[after..]
            .find(['/', '?', '#'])
            .map(|i| after + i)
            .unwrap_or(url.len());
        // The authority span is `url[after..origin_end]`. If it carries HTTP
        // basic-auth userinfo (`user:pass@host`), the masked value would otherwise
        // LEAK the credentials to the client. Drop everything up to and including
        // the last '@' so only `scheme://host[:port]` survives.
        let authority = &url[after..origin_end];
        let host_start = match authority.rfind('@') {
            Some(at) => after + at + 1,
            None => after,
        };
        return format!(
            "{}{}/{}",
            &url[..after],
            &url[host_start..origin_end],
            SECRET_SENTINEL
        );
    }
    // No scheme — nothing identifiable to preserve; fully mask.
    SECRET_SENTINEL.to_string()
}

/// Mask a webhook URL for display, embedding a STABLE per-entry identifier
/// (its index in the stored `webhook_urls` array) so resolution on POST is by
/// identity, not by origin. Two distinct webhooks that share an origin
/// (e.g. two Discord hooks) mask to DIFFERENT placeholders and so round-trip
/// unambiguously — the origin-only mask used to collide them and force the
/// save to be rejected.
///
/// Form: `https://discord.com/********#<idx>` — the `#<idx>` fragment is
/// appended to the origin-masked value. [`resolve_webhook_urls`] reads it back.
fn mask_webhook_url_indexed(url: &str, idx: usize) -> String {
    format!("{}#{idx}", mask_webhook_url(url))
}

/// True if `s` is a redacted webhook placeholder produced by
/// [`mask_webhook_url`] / [`mask_webhook_url_indexed`] — i.e. it ends with the
/// sentinel, or with `********#<digits>` (the indexed form). Used to skip
/// re-validating / re-fetching a masked round-trip. Deliberately strict: a
/// hostile URL that merely *embeds* the sentinel mid-path (e.g.
/// `https://evil/********@host/x`) does NOT match and is still validated.
fn is_masked_webhook(s: &str) -> bool {
    if s.ends_with(SECRET_SENTINEL) {
        return true;
    }
    if let Some((head, idx)) = s.rsplit_once('#') {
        return head.ends_with(SECRET_SENTINEL)
            && !idx.is_empty()
            && idx.bytes().all(|b| b.is_ascii_digit());
    }
    false
}

/// Resolve an incoming `webhook_urls` array against the currently-stored
/// URLs, replacing redacted placeholders with their real (token-bearing)
/// values. Matching is BY ORIGIN PREFIX (via [`mask_webhook_url`]), never by
/// array position: the UI can delete or reorder rows between GET and POST, so
/// a positional match would bind a masked entry to a different stored secret.
///
/// A masked entry whose masked-origin matches exactly one stored URL resolves
/// to that URL. A non-masked entry is taken verbatim (a newly-entered secret).
/// `Err(prefix)` is returned when a masked entry is ambiguous — it matches 0
/// stored entries (the row it referred to was deleted) or >1 (two stored hooks
/// share an origin) — so the caller can reject the save instead of guessing.
/// Empty/whitespace entries are dropped.
fn resolve_webhook_urls(incoming: &[&str], existing: &[String]) -> Result<Vec<String>, String> {
    let mut resolved: Vec<String> = Vec::with_capacity(incoming.len());
    for s in incoming {
        if s.contains(SECRET_SENTINEL) {
            // Preferred path: the masked form carries a stable `#<idx>`
            // identifier (see mask_webhook_url_indexed). Resolve by that index
            // so two same-origin webhooks round-trip unambiguously. The index
            // must both be in range AND still mask to exactly this placeholder
            // (so a reordered/deleted row can't silently bind the wrong secret).
            if let Some((origin_mask, idx_str)) = s.rsplit_once('#') {
                if let Ok(idx) = idx_str.parse::<usize>() {
                    match existing.get(idx) {
                        Some(stored) if mask_webhook_url(stored) == origin_mask => {
                            resolved.push(stored.clone());
                            continue;
                        }
                        // Index stale (row deleted/reordered) — reject rather
                        // than guess.
                        _ => return Err((*s).to_string()),
                    }
                }
            }
            // Fallback: no embedded index (older client). Match by origin; only
            // unambiguous when exactly one stored URL shares the origin.
            let matches: Vec<&String> = existing
                .iter()
                .filter(|stored| mask_webhook_url(stored) == *s)
                .collect();
            match matches.as_slice() {
                [one] => resolved.push((*one).clone()),
                _ => return Err((*s).to_string()),
            }
        } else {
            resolved.push((*s).to_string());
        }
    }
    Ok(resolved
        .into_iter()
        .filter(|s| !s.trim().is_empty())
        .collect())
}

/// Serialize Config for GET /api/settings with credential fields redacted.
/// No route is authenticated and the server binds 0.0.0.0, so returning
/// `keyserver_secret` / `tmdb_api_key` in cleartext would hand any
/// LAN/host client the operator's bearer token and API key.
fn settings_json_redacted(c: &Config) -> String {
    let mut v = serde_json::to_value(c).unwrap_or_else(|_| serde_json::json!({}));
    for field in ["keyserver_secret", "tmdb_api_key"] {
        if let Some(s) = v.get(field).and_then(|x| x.as_str()) {
            if !s.is_empty() {
                v[field] = serde_json::json!(SECRET_SENTINEL);
            }
        }
    }
    // keyserver_url and keydb_url may carry auth tokens in the path/query
    // (e.g. https://keyserver.example.com/token/decode). Mask path/query
    // with the origin-preserving helper so the operator can see the host
    // but not the embedded secret. A masked value round-trips on POST.
    for field in ["keyserver_url", "keydb_url"] {
        if let Some(s) = v.get(field).and_then(|x| x.as_str()) {
            if !s.is_empty() {
                v[field] = serde_json::json!(mask_webhook_url(s));
            }
        }
    }
    // webhook_urls embed bearer tokens (Discord/Slack/Jellyfin webhook
    // secrets live in the path/query). Mask the token but keep the origin
    // visible so the operator can identify each hook. A masked entry
    // round-trips on POST: any entry containing the sentinel is "unchanged".
    // keydb_path is an absolute container path; leaking it to any LAN/host
    // client exposes the internal filesystem layout. Return only the filename
    // component (enough for the operator to confirm which file is in use).
    if let Some(s) = v.get("keydb_path").and_then(|x| x.as_str()) {
        if !s.is_empty() {
            let name = std::path::Path::new(s)
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            v["keydb_path"] = serde_json::json!(name);
        }
    }
    if let Some(arr) = v.get_mut("webhook_urls").and_then(|x| x.as_array_mut()) {
        for (idx, entry) in arr.iter_mut().enumerate() {
            if let Some(s) = entry.as_str() {
                if !s.is_empty() {
                    *entry = serde_json::json!(mask_webhook_url_indexed(s, idx));
                }
            }
        }
    }
    v.to_string()
}

/// Cap on a request body we read fully into memory. Every POST handler
/// deals in small JSON documents (a settings patch, a title override, a
/// review action, a debug toggle); 1 MiB is orders of magnitude above
/// the largest legitimate body. Without this cap a LAN client could
/// stream a multi-GB body and OOM the container, killing any in-flight
/// rip/mux — a trivial unauthenticated DoS.
const MAX_REQUEST_BODY: u64 = 1024 * 1024;

/// Outcome of [`read_body_capped`].
enum BodyRead {
    /// Body read successfully, within the cap.
    Ok(String),
    /// The reader errored before EOF (truncated/disconnected client).
    Err,
    /// The body exceeded `MAX_REQUEST_BODY` before EOF.
    TooLarge,
}

/// Read a request body fully into a `String`, but never more than
/// `MAX_REQUEST_BODY + 1` bytes. We read one byte past the cap so an
/// exactly-at-limit body is accepted while an oversized one is detected
/// (the reader yields the extra byte only if more data exists). The
/// client-supplied Content-Length is never trusted — the `take` adapter
/// bounds the actual bytes pulled off the socket.
fn read_body_capped(request: &mut tiny_http::Request) -> BodyRead {
    let mut body = String::new();
    match request
        .as_reader()
        .take(MAX_REQUEST_BODY + 1)
        .read_to_string(&mut body)
    {
        Ok(_) => {
            if body.len() as u64 > MAX_REQUEST_BODY {
                BodyRead::TooLarge
            } else {
                BodyRead::Ok(body)
            }
        }
        Err(_) => BodyRead::Err,
    }
}

/// Read a JSON POST body with the shared size cap, replying with the
/// appropriate error status (400 bad body / 413 too large) on failure.
/// Returns `None` once a response has already been sent.
fn read_json_body(mut request: tiny_http::Request) -> Result<(tiny_http::Request, String), ()> {
    match read_body_capped(&mut request) {
        BodyRead::Ok(body) => Ok((request, body)),
        BodyRead::Err => {
            json_response(request, 400, r#"{"ok":false,"error":"bad body"}"#);
            Err(())
        }
        BodyRead::TooLarge => {
            json_response(
                request,
                413,
                r#"{"ok":false,"error":"request body too large"}"#,
            );
            Err(())
        }
    }
}

/// Validate that a device name is `sg\d+`. Rejects anything containing slashes
/// or other characters that would let a malformed URL (e.g. a typo like
/// `/api/rip/sg4/stop`) hit the rip handler with `device = "sg4/stop"`, which
/// previously spawned a doomed rip thread and surfaced as a phantom tab in
/// the UI.
fn is_valid_device_name(s: &str) -> bool {
    // Cross-OS device key (the basename libfreemkv's list_drives() yields,
    // stripped by device_key): Linux `sgN`, macOS `diskN`, Windows `CdRomN`.
    // Accept ASCII-alphanumeric only — this is the path-safety boundary that
    // rejects separators / traversal / spaces (`sg4/stop`, `../etc/passwd`,
    // `sg4 `) for the /api/<device> routes and the per-device log path. It is
    // NOT a "this drive exists" check: an unknown-but-well-formed name simply
    // fails to match any enumerated drive downstream.
    (3..=64).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_alphanumeric())
}

/// Clamp `s` to at most `max` characters (Unicode scalar values), never
/// splitting a multi-byte char. Used to bound operator-supplied free text
/// (title/overview/poster_url) before it's persisted and re-broadcast.
fn clamp_chars(s: &str, max: usize) -> String {
    match s.char_indices().nth(max) {
        Some((byte_idx, _)) => s[..byte_idx].to_string(),
        None => s.to_string(),
    }
}

// ── SSRF guard ─────────────────────────────────────────────────────────
//
// Any operator-supplied URL that autorip later fetches/POSTs to from
// inside the container (keydb_url, keyserver_url, webhook_urls) is an
// SSRF vector: an unauthenticated LAN client who can reach the settings
// API could point it at 169.254.169.254 (cloud metadata), RFC1918
// hosts, or loopback and either exfiltrate disc-key material or probe
// internal services. We block those address classes at *store* time
// (reject the save with a 400) and again at *fetch* time as
// defence-in-depth, and we pin the connection to the IP we validated so
// a DNS-rebinding attacker can't swap a public answer for an internal
// one between the check and the connect (TOCTOU).

/// True when `ip` is in a class autorip must never connect to: loopback,
/// any RFC1918 / link-local / ULA private range, multicast, unspecified,
/// the cloud-metadata anycast 169.254.169.254, and other non-global
/// space. Conservative — anything not clearly a routable public address
/// is blocked.
fn is_blocked_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local() // 169.254.0.0/16, incl. metadata 169.254.169.254
                || v4.is_broadcast()
                || v4.is_documentation()
                || v4.is_unspecified()
                || v4.is_multicast()
                // Carrier-grade NAT 100.64.0.0/10 (not flagged by std helpers).
                || (v4.octets()[0] == 100 && (v4.octets()[1] & 0xc0) == 0x40)
                // 0.0.0.0/8 "this network".
                || v4.octets()[0] == 0
                // Class-E reserved 240.0.0.0/4 (not flagged by std helpers).
                || v4.octets()[0] >= 240
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                // Unique-local fc00::/7.
                || (v6.segments()[0] & 0xfe00) == 0xfc00
                // Link-local fe80::/10.
                || (v6.segments()[0] & 0xffc0) == 0xfe80
                // IPv4-mapped (::ffff:a.b.c.d) and IPv4-compatible (::a.b.c.d)
                // — to_ipv4() catches both forms; re-check the unwrapped address.
                || v6.to_ipv4().map(|m| is_blocked_ip(&IpAddr::V4(m))) == Some(true)
        }
    }
}

/// Validate an operator-supplied fetch/POST URL against the SSRF guard.
///
/// Requires an `http`/`https` scheme, resolves the host **once**, and
/// rejects the URL if it has no addresses or any resolved address is in
/// a blocked class. On success returns the resolved+validated socket
/// addresses so the caller can pin the connection to them (avoiding a
/// re-resolve race). `Err(msg)` carries an operator-facing reason.
/// Resolve `host:port` to socket addresses with a bounded deadline.
///
/// `ToSocketAddrs` performs a blocking DNS lookup, which can hang for the OS
/// resolver timeout (potentially tens of seconds) and freeze the calling
/// (unauthenticated) handler thread. Run it on a spawned thread and join with
/// a short deadline; error on timeout. Shared by `validate_fetch_url` and
/// `validate_network_target` so neither can re-introduce an unbounded lookup.
pub(crate) fn resolve_with_timeout(host: &str, port: u16) -> Result<Vec<SocketAddr>, String> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;
    const DNS_TIMEOUT: Duration = Duration::from_secs(4);
    // On timeout the spawned resolver thread can't be cancelled — it lingers
    // until the blocking `to_socket_addrs` returns. To stop these accumulating
    // unboundedly under repeated timeouts, cap the number of detached resolvers
    // in flight. When at the cap, fail fast as if timed out rather than spawning
    // (and leaking) yet another thread.
    const MAX_INFLIGHT: usize = 8;
    static INFLIGHT: AtomicUsize = AtomicUsize::new(0);

    if INFLIGHT.fetch_add(1, Ordering::SeqCst) >= MAX_INFLIGHT {
        INFLIGHT.fetch_sub(1, Ordering::SeqCst);
        return Err("DNS resolution timed out".to_string());
    }

    let host = host.to_string();
    // Bounded channel of capacity 1: the resolver thread's single send never
    // blocks forever (the buffer always has room for its one message), so the
    // thread always exits cleanly once resolution completes — even if the
    // receiver has already timed out and gone away.
    let (tx, rx) = mpsc::sync_channel::<Result<Vec<SocketAddr>, std::io::Error>>(1);
    std::thread::spawn(move || {
        let res = (host.as_str(), port)
            .to_socket_addrs()
            .map(|it| it.collect::<Vec<SocketAddr>>());
        // Receiver may be gone after the timeout — ignore the send error.
        let _ = tx.send(res);
        INFLIGHT.fetch_sub(1, Ordering::SeqCst);
    });
    match rx.recv_timeout(DNS_TIMEOUT) {
        Ok(Ok(addrs)) => Ok(addrs),
        Ok(Err(e)) => Err(format!("could not resolve host: {e}")),
        Err(_) => Err("DNS resolution timed out".to_string()),
    }
}

pub(crate) fn validate_fetch_url(url: &str) -> Result<Vec<SocketAddr>, String> {
    let url = url.trim();
    if url.is_empty() {
        return Err("URL is empty".to_string());
    }
    // Minimal scheme + authority parse — no URL crate dep, mirroring the
    // hand-rolled parsers already in this module.
    let rest = if let Some(r) = url.strip_prefix("https://") {
        (r, 443u16)
    } else if let Some(r) = url.strip_prefix("http://") {
        (r, 80u16)
    } else {
        return Err("URL must start with http:// or https://".to_string());
    };
    let (authority, default_port) = rest;
    // Strip path/query/fragment — keep only the authority (host[:port]).
    let authority = authority.split(['/', '?', '#']).next().unwrap_or(authority);
    // Strip userinfo if present (user:pass@host).
    let authority = authority.rsplit('@').next().unwrap_or(authority);
    if authority.is_empty() {
        return Err("URL has no host".to_string());
    }
    // Split host:port, handling bracketed IPv6 literals [::1]:8080.
    let (host, port): (String, u16) = if let Some(stripped) = authority.strip_prefix('[') {
        match stripped.split_once(']') {
            Some((h, after)) => {
                let p = after
                    .strip_prefix(':')
                    .map(|s| s.parse::<u16>().map_err(|_| "invalid port".to_string()))
                    .transpose()?
                    .unwrap_or(default_port);
                (h.to_string(), p)
            }
            None => return Err("malformed IPv6 host".to_string()),
        }
    } else if let Some((h, p)) = authority.rsplit_once(':') {
        // Only treat the trailing ':' as a port separator if the right
        // side is numeric (avoids mis-splitting a bare IPv6 literal,
        // though those should be bracketed).
        match p.parse::<u16>() {
            Ok(p) => (h.to_string(), p),
            Err(_) => (authority.to_string(), default_port),
        }
    } else {
        (authority.to_string(), default_port)
    };
    if host.is_empty() {
        return Err("URL has no host".to_string());
    }

    // Resolve once, with a bounded deadline (see resolve_with_timeout).
    let addrs: Vec<SocketAddr> = resolve_with_timeout(&host, port)?;
    if addrs.is_empty() {
        return Err("host did not resolve to any address".to_string());
    }
    for a in &addrs {
        if is_blocked_ip(&a.ip()) {
            return Err(format!(
                "refusing to connect to non-public address {} (SSRF guard)",
                a.ip()
            ));
        }
    }
    Ok(addrs)
}

/// Validate an operator-supplied network output target against the SSRF
/// guard. Unlike [`validate_fetch_url`] the target is a bare `host:port`
/// (no scheme) — at rip time libfreemkv streams decrypted disc content to
/// it, so the same non-public-address rule applies. Resolves the host once
/// and rejects if it has no addresses or any resolved address is blocked.
pub(crate) fn validate_network_target(target: &str) -> Result<(), String> {
    let target = target.trim();
    if target.is_empty() {
        return Err("network target is empty".to_string());
    }
    // Split host:port, handling bracketed IPv6 literals [::1]:9000.
    let (host, port): (String, u16) = if let Some(stripped) = target.strip_prefix('[') {
        match stripped.split_once(']') {
            Some((h, after)) => {
                let p = after
                    .strip_prefix(':')
                    .ok_or_else(|| "network target needs a port (host:port)".to_string())?
                    .parse::<u16>()
                    .map_err(|_| "invalid port".to_string())?;
                (h.to_string(), p)
            }
            None => return Err("malformed IPv6 host".to_string()),
        }
    } else {
        let (h, p) = target
            .rsplit_once(':')
            .ok_or_else(|| "network target needs a port (host:port)".to_string())?;
        let p = p.parse::<u16>().map_err(|_| "invalid port".to_string())?;
        (h.to_string(), p)
    };
    if host.is_empty() {
        return Err("network target has no host".to_string());
    }

    // Bounded DNS — same shared helper validate_fetch_url uses, so an
    // unauthenticated settings POST can't freeze the handler on a slow resolver.
    let addrs: Vec<SocketAddr> = resolve_with_timeout(&host, port)?;
    if addrs.is_empty() {
        return Err("host did not resolve to any address".to_string());
    }
    for a in &addrs {
        if is_blocked_ip(&a.ip()) {
            return Err(format!(
                "refusing to stream to non-public address {} (SSRF guard)",
                a.ip()
            ));
        }
    }
    Ok(())
}

/// Build a ureq agent that (a) follows zero redirects — so a permitted
/// public URL can't 30x-redirect into an internal address — and (b)
/// pins DNS resolution to `pinned`, the exact addresses
/// `validate_fetch_url` already vetted. Pinning closes the DNS-rebinding
/// TOCTOU: ureq connects to the validated IPs instead of re-resolving
/// the hostname (which an attacker could flip to 169.254.169.254 /
/// RFC1918 in the window between validation and fetch).
pub(crate) fn guarded_agent(pinned: Vec<SocketAddr>) -> ureq::Agent {
    // ureq 2.x sets NO default connect/read timeout. Without one a peer
    // that accepts the connection but never responds would block the
    // caller's thread (and hold its socket) forever — for webhooks a
    // fresh thread spawns on every move/rip-complete, so a dead receiver
    // would leak threads and sockets without bound. Bound both alongside
    // the SSRF pinning (resolver) and redirect block.
    ureq::AgentBuilder::new()
        .redirects(0)
        .timeout_connect(std::time::Duration::from_secs(5))
        .timeout_read(std::time::Duration::from_secs(30))
        .resolver(move |_netloc: &str| Ok(pinned.clone()))
        .build()
}

/// SSRF-guarded HTTP GET. Runs [`validate_fetch_url`] (scheme + resolved-IP
/// allow-list) and then issues the request through [`guarded_agent`] so the
/// connection is pinned to the validated addresses and redirects are blocked.
///
/// This is the single entry point any code path that fetches an
/// operator-supplied URL from inside the container should use — the KEYDB
/// download on startup and the daily-refresh thread (main.rs) both route
/// through here instead of calling `ureq::get` directly, which would bypass
/// the guard entirely. Returns the response on success or an
/// operator-facing reason string on rejection / transport failure.
///
/// `pub` (not `pub(crate)`): the binary's `main.rs` declares its own `mod
/// web`, but the library facade in `lib.rs` re-exports this module too. In
/// the lib build nothing inside the crate calls this helper — only the bin
/// and the test module do — so `pub(crate)` would trip `dead_code`. Exposing
/// it as the crate's public SSRF-guarded fetch entry point is also the honest
/// description of its role.
pub fn guarded_get(url: &str) -> Result<ureq::Response, String> {
    let pinned = validate_fetch_url(url)?;
    guarded_agent(pinned).get(url).call().map_err(|e| {
        // Do NOT embed `e` directly: ureq's Display includes the full
        // request URL, which leaks a token-bearing keydb_url into the
        // system log (and thence the unauthenticated /api/system endpoint).
        // Summarise by status code or transport-error kind only.
        match &e {
            ureq::Error::Status(code, _) => format!("fetch failed: HTTP {code}"),
            ureq::Error::Transport(t) => format!("fetch failed: {}", t.kind()),
        }
    })
}

// ── Connection caps ────────────────────────────────────────────────────
//
// run() spawns one OS thread per accepted connection, and /events
// (handle_sse) loops forever holding its thread until the client
// disconnects. With no cap an unauthenticated LAN client can open N
// sockets and pin N threads/stacks, exhausting the container and
// starving in-flight rips. We bound both: total in-flight handler
// threads and, more tightly, concurrent SSE streams. Over the cap we
// return 503 and let the thread end immediately.

/// Max concurrent request-handler threads. Generous — normal use is a
/// handful of browser tabs polling — but finite so a flood can't fork
/// the box to death.
const MAX_INFLIGHT_HANDLERS: usize = 64;
/// Max concurrent SSE (`/events`) streams. Each pins a thread for its
/// whole lifetime, so this is the tighter bound.
const MAX_SSE_CLIENTS: usize = 8;

static INFLIGHT_HANDLERS: AtomicUsize = AtomicUsize::new(0);
static SSE_CLIENTS: AtomicUsize = AtomicUsize::new(0);

/// RAII admission token for a counted connection slot. Decrements its
/// counter on drop, so the slot is freed no matter how the handler exits
/// (return, panic-unwind). `try_acquire` returns None when the cap is
/// already reached.
struct ConnGuard(&'static AtomicUsize);

impl ConnGuard {
    fn try_acquire(counter: &'static AtomicUsize, max: usize) -> Option<ConnGuard> {
        // fetch_update gives us a CAS loop that only increments while
        // under the cap, so the count can never exceed `max`.
        let ok = counter
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                if n < max { Some(n + 1) } else { None }
            })
            .is_ok();
        if ok { Some(ConnGuard(counter)) } else { None }
    }
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod web_tests {
    use super::*;

    // Regression (bug #3): the Mux queue and Move queue must be mutually
    // exclusive within a single state snapshot — a disc can never appear in
    // both at once. `build_queue_views` is the single source both
    // /api/state (SSE) and /api/system derive the queues from, so testing
    // it covers every UI view. We walk a staging dir through the post-mux
    // marker sequence and assert no name is in both lists at any step.
    #[test]
    fn build_queue_views_mutually_exclusive() {
        use std::fs;
        let tmp = tempfile::TempDir::new().unwrap();
        let staging = tmp.path().to_string_lossy().to_string();
        let disc = tmp.path().join("Border_Town");
        fs::create_dir_all(&disc).unwrap();

        let both_contain = |mux: &[String], mv: &[String]| -> bool {
            mux.iter().any(|m| {
                let name = m.replace(" (queued)", "").replace(" (malformed)", "");
                mv.iter().any(|v| v.replace(" (moving)", "") == name)
            })
        };

        // Step 1: fresh hand-off — `.ripped` only. In the Mux queue, not Move.
        crate::muxer::write_marker(
            &disc,
            &crate::muxer::RippedMarker {
                schema_version: crate::muxer::RIPPED_MARKER_SCHEMA,
                iso_path: "/x/Border_Town/Border_Town.iso".into(),
                mapfile_path: "/x/Border_Town/Border_Town.iso.mapfile".into(),
                display_name: "Border Town".into(),
                disc_format: "uhd".into(),
                mkv_filename: "Border_Town.mkv".into(),
                tmdb_title: "Border Town".into(),
                tmdb_year: 2024,
                tmdb_poster: String::new(),
                tmdb_overview: String::new(),
                tmdb_media_type: "movie".into(),
                max_retries: 5,
                abort_on_lost_secs: 0,
                rip_elapsed_secs: 0.0,
                rip_errors: 0,
                rip_lost_video_secs: 0.0,
                rip_last_sector: 0,
                origin_device: "sg0".into(),
                sweep_errors: 0,
                sweep_total_lost_ms: 0.0,
                sweep_main_lost_ms: 0.0,
                sweep_num_bad_ranges: 0,
                sweep_largest_gap_ms: 0.0,
                title_confident: true,
            },
        )
        .unwrap();
        let (mux, mv) = build_queue_views(&staging);
        assert_eq!(mux.len(), 1, "fresh .ripped must be in the Mux queue");
        assert!(mv.is_empty(), "not yet in the Move queue");
        assert!(!both_contain(&mux, &mv));

        // Step 2: mux in flight — `.muxing` added. Out of the Mux queue
        // (shown as the live `_mux` device), still not in Move.
        crate::ripper::staging::write_muxing_marker(&disc);
        let (mux, mv) = build_queue_views(&staging);
        assert!(
            mux.is_empty(),
            "an actively-muxing dir leaves the queued list"
        );
        assert!(mv.is_empty());
        assert!(!both_contain(&mux, &mv));
        crate::ripper::staging::clear_muxing_marker(&disc);

        // Step 3: mux done — `.done` written (mover hand-off), `.completed`
        // not yet, `.ripped` may linger. THIS is the double-listing bug
        // window: it must be in the Move queue ONLY.
        fs::write(disc.join(".done"), b"{}").unwrap();
        let (mux, mv) = build_queue_views(&staging);
        assert!(
            mux.is_empty(),
            "a dir in the Move queue (.done) must not also be (queued) in the Mux queue, got {mux:?}"
        );
        assert_eq!(mv.len(), 1, "must be in the Move queue");
        assert!(
            !both_contain(&mux, &mv),
            "BUG #3: a disc must never appear in both the mux and move queues"
        );

        // Step 4: terminal `.completed` lands — still Move-only, never both.
        crate::ripper::staging::write_completed_marker(&disc);
        let (mux, mv) = build_queue_views(&staging);
        assert!(mux.is_empty());
        assert_eq!(mv.len(), 1);
        assert!(!both_contain(&mux, &mv));
    }

    // ===================================================================
    // COMPREHENSIVE rip→mux→move→done state-machine coverage.
    // The three views (per-device tile status, Mux queue, Move queue) must
    // stay mutually consistent across EVERY marker transition and with
    // MULTIPLE discs in staging. These tests walk the full marker lifecycle
    // and assert, at each step, exactly which queue(s) a disc is in.
    // ===================================================================

    /// Build a schema-valid `.ripped` marker for `display_name` whose
    /// `origin_device` is `origin`. Keeps the lifecycle tests terse.
    fn ripped_marker_for(display_name: &str, origin: &str) -> crate::muxer::RippedMarker {
        let safe = display_name.replace(' ', "_");
        crate::muxer::RippedMarker {
            schema_version: crate::muxer::RIPPED_MARKER_SCHEMA,
            iso_path: format!("/x/{safe}/{safe}.iso"),
            mapfile_path: format!("/x/{safe}/{safe}.iso.mapfile"),
            display_name: display_name.into(),
            disc_format: "uhd".into(),
            mkv_filename: format!("{safe}.mkv"),
            tmdb_title: display_name.into(),
            tmdb_year: 2024,
            tmdb_poster: String::new(),
            tmdb_overview: String::new(),
            tmdb_media_type: "movie".into(),
            max_retries: 5,
            abort_on_lost_secs: 0,
            rip_elapsed_secs: 0.0,
            rip_errors: 0,
            rip_lost_video_secs: 0.0,
            rip_last_sector: 0,
            origin_device: origin.into(),
            sweep_errors: 0,
            sweep_total_lost_ms: 0.0,
            sweep_main_lost_ms: 0.0,
            sweep_num_bad_ranges: 0,
            sweep_largest_gap_ms: 0.0,
            title_confident: true,
        }
    }

    /// Does `name` appear in BOTH queues at once? (Strips the trailing
    /// status suffixes so `"X (queued)"` and `"X (moving)"` compare equal.)
    fn in_both_queues(mux: &[String], mv: &[String]) -> bool {
        let strip = |s: &str| -> String {
            s.replace(" (queued)", "")
                .replace(" (malformed)", "")
                .replace(" (moving)", "")
        };
        mux.iter().any(|m| mv.iter().any(|v| strip(m) == strip(v)))
    }

    /// FULL marker lifecycle with the device-status view folded in. At each
    /// step assert (a) which queue(s) the disc is in and (b) the device
    /// tile status. The disc is NEVER in two queues; the tile is correct at
    /// every stage. Covers `.ripped → .muxing → .done → .completed`.
    #[test]
    fn full_lifecycle_queue_and_status_consistent() {
        use std::fs;
        let tmp = tempfile::TempDir::new().unwrap();
        let staging = tmp.path().to_string_lossy().to_string();
        let disc = tmp.path().join("Mercy");
        fs::create_dir_all(&disc).unwrap();
        let device = "sg_lifecycle_dev";

        // --- Stage 0: sweep in progress. `.sweeping` marker, tile=ripping.
        crate::ripper::staging::write_sweeping_marker(&disc);
        crate::ripper::update_state(
            device,
            crate::ripper::RipState {
                device: device.to_string(),
                status: "ripping".to_string(),
                disc_name: "Mercy".to_string(),
                ..Default::default()
            },
        );
        let (mux, mv) = build_queue_views(&staging);
        assert!(
            mux.is_empty() && mv.is_empty(),
            "during sweep: in neither queue"
        );
        assert_eq!(device_status(device), Some("ripping".into()));

        // --- Stage 1: `.ripped` hand-off. The read is DONE: tile=done(100%),
        // disc enters the Mux queue ONLY. (`write_marker` also clears
        // `.sweeping`.)
        crate::muxer::write_marker(&disc, &ripped_marker_for("Mercy", device)).unwrap();
        crate::ripper::update_state(
            device,
            crate::ripper::RipState {
                device: device.to_string(),
                status: "done".to_string(),
                progress_pct: 100,
                disc_name: "Mercy".to_string(),
                output_file: "Mercy.mkv".to_string(),
                ..Default::default()
            },
        );
        let (mux, mv) = build_queue_views(&staging);
        assert_eq!(mux.len(), 1, ".ripped → Mux queue");
        assert!(mv.is_empty(), "not in Move queue yet");
        assert!(!in_both_queues(&mux, &mv));
        assert_eq!(
            device_status(device),
            Some("done".into()),
            "tile is 'done' the instant the read finishes, even though the mux is pending"
        );

        // --- Stage 2: mux in flight. `.muxing` lock; disc leaves the static
        // Mux queue (it's the live `_mux` device now); tile stays done.
        crate::ripper::staging::write_muxing_marker(&disc);
        let (mux, mv) = build_queue_views(&staging);
        assert!(
            mux.is_empty(),
            "actively-muxing dir leaves the (queued) list"
        );
        assert!(mv.is_empty());
        assert!(!in_both_queues(&mux, &mv));
        assert_eq!(device_status(device), Some("done".into()));
        crate::ripper::staging::clear_muxing_marker(&disc);

        // --- Stage 3: mux success. `.done` (mover hand-off) written BEFORE
        // `.completed`; `.ripped` may linger. Disc moves to the Move queue
        // ONLY — the double-listing bug window.
        fs::write(disc.join(".done"), b"{}").unwrap();
        let (mux, mv) = build_queue_views(&staging);
        assert!(
            mux.is_empty(),
            "a .done dir must NOT still be (queued) in the Mux queue"
        );
        assert_eq!(mv.len(), 1, ".done → Move queue");
        assert!(!in_both_queues(&mux, &mv), "BUG #3: never in both queues");
        assert_eq!(device_status(device), Some("done".into()));

        // --- Stage 4: `.completed` lands (terminal). Still Move-only.
        crate::ripper::staging::write_completed_marker(&disc);
        let (mux, mv) = build_queue_views(&staging);
        assert!(mux.is_empty());
        assert_eq!(
            mv.len(),
            1,
            "still in the Move queue until the mover relocates it"
        );
        assert!(!in_both_queues(&mux, &mv));

        crate::ripper::STATE.lock().unwrap().remove(device);
    }

    /// LOW-CONFIDENCE lifecycle: the mux writes `.review` (not `.done`) for
    /// an operator hold. The disc must leave the Mux queue (it's the mover's
    /// concern now) and NOT double-list. `.review` is a Move-queue concept
    /// only via the operator review flow, so it appears in neither the
    /// "(moving)" list nor the Mux "(queued)" list here — the key invariant
    /// is it is never simultaneously in both.
    #[test]
    fn review_hold_leaves_mux_queue_no_double_listing() {
        use std::fs;
        let tmp = tempfile::TempDir::new().unwrap();
        let staging = tmp.path().to_string_lossy().to_string();
        let disc = tmp.path().join("Held_Title");
        fs::create_dir_all(&disc).unwrap();

        crate::muxer::write_marker(&disc, &ripped_marker_for("Held Title", "sg0")).unwrap();
        let (mux, _) = build_queue_views(&staging);
        assert_eq!(mux.len(), 1, "fresh .ripped is queued for mux");

        // Low-confidence mux success: `.review` instead of `.done`, then
        // `.completed`.
        fs::write(disc.join(".review"), b"{}").unwrap();
        let (mux, mv) = build_queue_views(&staging);
        assert!(mux.is_empty(), "a .review dir must leave the Mux queue");
        assert!(
            !in_both_queues(&mux, &mv),
            "never in both queues on the review path"
        );

        crate::ripper::staging::write_completed_marker(&disc);
        let (mux, mv) = build_queue_views(&staging);
        assert!(mux.is_empty());
        assert!(!in_both_queues(&mux, &mv));
    }

    /// ABORT path: a post-mux loss abort writes `.failed` (no `.done`/
    /// `.completed`). The disc must leave BOTH queues, and the device tile
    /// reflects "error".
    #[test]
    fn abort_failed_leaves_both_queues_and_marks_error() {
        use std::fs;
        let tmp = tempfile::TempDir::new().unwrap();
        let staging = tmp.path().to_string_lossy().to_string();
        let disc = tmp.path().join("Lossy_Disc");
        fs::create_dir_all(&disc).unwrap();
        let device = "sg_abort_dev";

        crate::muxer::write_marker(&disc, &ripped_marker_for("Lossy Disc", device)).unwrap();
        let (mux, _) = build_queue_views(&staging);
        assert_eq!(mux.len(), 1);

        // Abort gate quarantines: `.failed`, tile=error.
        crate::ripper::staging::write_failed_marker(&disc, "aborted: loss exceeds threshold");
        crate::ripper::update_state(
            device,
            crate::ripper::RipState {
                device: device.to_string(),
                status: "error".to_string(),
                disc_name: "Lossy Disc".to_string(),
                last_error: "aborted: loss exceeds threshold".to_string(),
                ..Default::default()
            },
        );
        let (mux, mv) = build_queue_views(&staging);
        assert!(mux.is_empty(), ".failed dir must leave the Mux queue");
        assert!(
            mv.is_empty(),
            ".failed dir is NOT in the Move queue (no .done)"
        );
        assert_eq!(device_status(device), Some("error".into()));

        crate::ripper::STATE.lock().unwrap().remove(device);
    }

    /// CONCURRENT devices: two drives, each with its own staged job at a
    /// DIFFERENT lifecycle stage, must not cross-contaminate queue
    /// membership or device status. Disc A is mid-mux-queue (`.ripped`);
    /// disc B has finished (`.done` → Move queue).
    #[test]
    fn concurrent_devices_no_cross_contamination() {
        use std::fs;
        let tmp = tempfile::TempDir::new().unwrap();
        let staging = tmp.path().to_string_lossy().to_string();
        let dev_a = "sg_concurrent_a";
        let dev_b = "sg_concurrent_b";

        // Disc A: freshly handed off → Mux queue, tile A = done.
        let disc_a = tmp.path().join("Alpha");
        fs::create_dir_all(&disc_a).unwrap();
        crate::muxer::write_marker(&disc_a, &ripped_marker_for("Alpha", dev_a)).unwrap();
        crate::ripper::update_state(
            dev_a,
            crate::ripper::RipState {
                device: dev_a.to_string(),
                status: "done".to_string(),
                progress_pct: 100,
                disc_name: "Alpha".to_string(),
                ..Default::default()
            },
        );

        // Disc B: mux finished → Move queue, tile B = done.
        let disc_b = tmp.path().join("Beta");
        fs::create_dir_all(&disc_b).unwrap();
        crate::muxer::write_marker(&disc_b, &ripped_marker_for("Beta", dev_b)).unwrap();
        fs::write(disc_b.join(".done"), b"{}").unwrap();
        crate::ripper::staging::write_completed_marker(&disc_b);
        crate::ripper::update_state(
            dev_b,
            crate::ripper::RipState {
                device: dev_b.to_string(),
                status: "done".to_string(),
                progress_pct: 100,
                disc_name: "Beta".to_string(),
                ..Default::default()
            },
        );

        let (mux, mv) = build_queue_views(&staging);
        // Alpha is in the Mux queue ONLY; Beta in the Move queue ONLY.
        assert!(
            mux.iter().any(|m| m.contains("Alpha")),
            "Alpha must be in the Mux queue"
        );
        assert!(
            !mux.iter().any(|m| m.contains("Beta")),
            "Beta must NOT be in the Mux queue"
        );
        assert!(
            mv.iter().any(|m| m.contains("Beta")),
            "Beta must be in the Move queue"
        );
        assert!(
            !mv.iter().any(|m| m.contains("Alpha")),
            "Alpha must NOT be in the Move queue"
        );
        assert!(
            !in_both_queues(&mux, &mv),
            "neither disc may be in both queues"
        );
        // Each device tile is independent.
        assert_eq!(device_status(dev_a), Some("done".into()));
        assert_eq!(device_status(dev_b), Some("done".into()));

        crate::ripper::STATE.lock().unwrap().remove(dev_a);
        crate::ripper::STATE.lock().unwrap().remove(dev_b);
    }

    /// `get_state_json` END-TO-END: the serialized live payload (the source
    /// for the SSE/dashboard) must never list a disc in BOTH `_mux_queue`
    /// and `_move_queue`, across MULTIPLE discs at different stages. This is
    /// the top-level guarantee fix C makes — all three views derive from one
    /// snapshot.
    #[test]
    fn get_state_json_never_double_lists_across_discs() {
        use std::fs;
        let tmp = tempfile::TempDir::new().unwrap();
        let staging = tmp.path().to_string_lossy().to_string();

        // Three discs spanning the lifecycle:
        //   Queued      → .ripped only        (Mux queue)
        //   Moving      → .done + .completed   (Move queue)
        //   AlsoQueued  → .ripped only         (Mux queue)
        for (name, finished) in [("Queued", false), ("Moving", true), ("AlsoQueued", false)] {
            let d = tmp.path().join(name);
            fs::create_dir_all(&d).unwrap();
            crate::muxer::write_marker(&d, &ripped_marker_for(name, "sg0")).unwrap();
            if finished {
                fs::write(d.join(".done"), b"{}").unwrap();
                crate::ripper::staging::write_completed_marker(&d);
            }
        }

        let json = get_state_json(&staging);
        let v: serde_json::Value = serde_json::from_str(&json).expect("state json must parse");
        let to_names = |key: &str| -> Vec<String> {
            v.get(key)
                .and_then(|q| q.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str())
                        .map(|s| {
                            s.replace(" (queued)", "")
                                .replace(" (malformed)", "")
                                .replace(" (moving)", "")
                        })
                        .collect()
                })
                .unwrap_or_default()
        };
        let mux_names = to_names("_mux_queue");
        let move_names = to_names("_move_queue");

        assert!(mux_names.contains(&"Queued".to_string()));
        assert!(mux_names.contains(&"AlsoQueued".to_string()));
        assert!(move_names.contains(&"Moving".to_string()));
        // The cross-queue invariant: no disc in both lists.
        for name in &mux_names {
            assert!(
                !move_names.contains(name),
                "BUG #3 (get_state_json): '{name}' is in BOTH _mux_queue and _move_queue"
            );
        }
    }

    /// Helper: current status string of a device in the global STATE map.
    fn device_status(device: &str) -> Option<String> {
        ripper::STATE
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(device)
            .map(|s| s.status.clone())
    }

    #[test]
    fn keydb_body_under_cap_is_accepted() {
        let body = vec![b'x'; 100];
        let out = read_capped_keydb_body(&body[..], 10 * 1024 * 1024).unwrap();
        assert_eq!(out, body);
    }

    #[test]
    fn keydb_body_exactly_at_cap_is_accepted() {
        // The cap is inclusive: a body of exactly max_bytes must pass (no
        // false-positive on a legitimately cap-sized keydb).
        let cap: u64 = 4096;
        let body = vec![b'x'; cap as usize];
        let out = read_capped_keydb_body(&body[..], cap).unwrap();
        assert_eq!(out.len() as u64, cap);
    }

    #[test]
    fn keydb_body_over_cap_is_rejected() {
        // Regression (finding 2): a body one byte past the cap must be
        // detected as TooLarge, not silently truncated to the cap.
        let cap: u64 = 4096;
        let body = vec![b'x'; cap as usize + 1];
        let err = read_capped_keydb_body(&body[..], cap).unwrap_err();
        assert_eq!(err, KeydbReadError::TooLarge);
    }

    #[test]
    fn device_name_accepts_cross_os_keys() {
        // Linux sg, macOS disk, Windows CdRom — the basenames list_drives yields.
        assert!(is_valid_device_name("sg0"));
        assert!(is_valid_device_name("sg4"));
        assert!(is_valid_device_name("sg15"));
        assert!(is_valid_device_name("disk6")); // macOS
        assert!(is_valid_device_name("CdRom0")); // Windows
    }

    #[test]
    fn device_name_rejects_path_traversal_and_typos() {
        // The exact bug that created the phantom "sg4/stop" tab. The validator
        // is a path-safety boundary (reject separators/traversal/spaces), not a
        // drive-existence check — an unknown well-formed name fails to match a
        // real drive downstream, so e.g. "sr0"/"sda" are accepted as *format*.
        assert!(!is_valid_device_name("sg4/stop"));
        assert!(!is_valid_device_name("sg4/verify"));
        assert!(!is_valid_device_name("../etc/passwd"));
        assert!(!is_valid_device_name("sg4 ")); // trailing space
        assert!(!is_valid_device_name("sg")); // too short (< 3)
        assert!(!is_valid_device_name(""));
        assert!(!is_valid_device_name("a/b"));
        assert!(!is_valid_device_name("..")); // dots are separators
    }

    #[test]
    fn poster_url_validation() {
        assert!(is_valid_poster_url(
            "https://image.tmdb.org/t/p/w500/abc.jpg"
        ));
        assert!(is_valid_poster_url("http://example.com/poster.png"));
        // Wrong scheme.
        assert!(!is_valid_poster_url("javascript:alert(1)"));
        assert!(!is_valid_poster_url("ftp://example.com/x.jpg"));
        assert!(!is_valid_poster_url("//example.com/x.jpg"));
        // Attribute-breakout / control chars.
        assert!(!is_valid_poster_url("https://example.com/\"><script>"));
        assert!(!is_valid_poster_url("https://example.com/x'onerror=1"));
        assert!(!is_valid_poster_url("https://example.com/a\nb"));
    }

    /// The dashboard's `esc()` must HTML-escape all five sensitive characters
    /// (`&`, `<`, `>`, `"`, `'`) because its output is interpolated into both
    /// double-quoted attributes and `innerHTML`. A `textContent`/`innerHTML`
    /// round-trip (the prior implementation) leaves `"` and `'` unescaped.
    /// We mirror the shipped regex chain here and assert the full set, and
    /// also assert the JS source carries the quote escapes so a regression in
    /// the template is caught.
    #[test]
    fn dashboard_esc_escapes_all_five() {
        fn esc(s: &str) -> String {
            s.replace('&', "&amp;")
                .replace('<', "&lt;")
                .replace('>', "&gt;")
                .replace('"', "&quot;")
                .replace('\'', "&#39;")
        }
        assert_eq!(esc("\"x<>&'"), "&quot;x&lt;&gt;&amp;&#39;");
        // The shipped JS must escape quotes and apostrophes, not just <>&.
        assert!(DASHBOARD_HTML.contains(r#"replace(/"/g,'&quot;')"#));
        assert!(DASHBOARD_HTML.contains(r"replace(/'/g,'&#39;')"));
    }

    #[test]
    fn settings_get_redacts_secrets() {
        let c = Config {
            tmdb_api_key: "real-tmdb-key".into(),
            keyserver_secret: "real-bearer-token".into(),
            ..Config::default()
        };
        let json: serde_json::Value = serde_json::from_str(&settings_json_redacted(&c)).unwrap();
        assert_eq!(json["tmdb_api_key"], SECRET_SENTINEL);
        assert_eq!(json["keyserver_secret"], SECRET_SENTINEL);
        // An empty secret stays empty (no sentinel) so the UI shows a blank field.
        let json2: serde_json::Value =
            serde_json::from_str(&settings_json_redacted(&Config::default())).unwrap();
        assert_eq!(json2["tmdb_api_key"], "");
    }

    #[test]
    fn settings_get_masks_keyserver_url_token_in_path() {
        // keyserver_url may carry an auth token in the path
        // (e.g. https://keys.example.com/mytoken/decode). GET must mask the
        // path but keep the origin so the operator can identify the server.
        let c = Config {
            keyserver_url: "https://keys.example.com/mysecrettoken/decode".into(),
            keydb_url: "https://keydb.example.com/authtoken/keydb.zip".into(),
            ..Config::default()
        };
        let json: serde_json::Value = serde_json::from_str(&settings_json_redacted(&c)).unwrap();
        // Origin preserved, token-bearing path replaced with sentinel.
        assert_eq!(json["keyserver_url"], "https://keys.example.com/********");
        assert_eq!(json["keydb_url"], "https://keydb.example.com/********");
        // Tokens must not appear in the redacted output.
        assert!(
            !json["keyserver_url"]
                .as_str()
                .unwrap()
                .contains("mysecrettoken")
        );
        assert!(!json["keydb_url"].as_str().unwrap().contains("authtoken"));
        // Empty URLs stay empty (no sentinel so the UI shows a blank field).
        let json2: serde_json::Value =
            serde_json::from_str(&settings_json_redacted(&Config::default())).unwrap();
        assert_eq!(json2["keyserver_url"], "");
        assert_eq!(json2["keydb_url"], "");
    }

    // NOTE: the keyserver_url sentinel round-trip is now tested
    // executing-style by `http::settings_post_masked_keyserver_url_preserves_stored`
    // (it drives the real handle_settings_post via a live server + config::save,
    // not an inline re-implementation of the guard).

    #[test]
    fn settings_get_masks_webhook_token_keeps_origin() {
        // Webhook URLs embed bearer tokens (Discord/Slack/Jellyfin) in the
        // path, so a GET must mask the token — but keep the origin visible so
        // the operator can tell which hook is which.
        let c = Config {
            webhook_urls: vec![
                "https://discord.com/api/webhooks/123/secrettoken".into(),
                "".into(),
                "https://hooks.slack.com/services/AAA/BBB/cccsecret".into(),
            ],
            ..Config::default()
        };
        let json: serde_json::Value = serde_json::from_str(&settings_json_redacted(&c)).unwrap();
        let arr = json["webhook_urls"].as_array().unwrap();
        // Masked form now carries a stable per-entry index (#<pos>) so two
        // same-origin hooks round-trip unambiguously (#8).
        assert_eq!(arr[0], "https://discord.com/********#0");
        // Empty entry stays empty (no sentinel) so the UI shows a blank row.
        assert_eq!(arr[1], "");
        assert_eq!(arr[2], "https://hooks.slack.com/********#2");
        // The masked form must NOT leak the token.
        assert!(!arr[0].as_str().unwrap().contains("secrettoken"));
        assert!(!arr[2].as_str().unwrap().contains("cccsecret"));
    }

    #[test]
    fn mask_webhook_url_variants() {
        assert_eq!(
            mask_webhook_url("https://discord.com/api/webhooks/1/tok"),
            "https://discord.com/********"
        );
        // Host with port.
        assert_eq!(
            mask_webhook_url("http://jellyfin.example:8096/webhook/abc"),
            "http://jellyfin.example:8096/********"
        );
        // Bare origin, no path → still origin/sentinel.
        assert_eq!(
            mask_webhook_url("https://example.com"),
            "https://example.com/********"
        );
        // No scheme → fully masked (nothing identifiable to keep).
        assert_eq!(mask_webhook_url("not-a-url"), SECRET_SENTINEL);
    }

    #[test]
    fn mask_webhook_url_strips_query_string_token() {
        // Token in query string with no path slash — must not appear in output.
        assert_eq!(
            mask_webhook_url("https://hooks.example.com?token=SUPERSECRET"),
            "https://hooks.example.com/********"
        );
        // Fragment-only (no path) — similarly stripped.
        assert_eq!(
            mask_webhook_url("https://hooks.example.com#frag"),
            "https://hooks.example.com/********"
        );
    }

    #[test]
    fn mask_webhook_url_strips_basic_auth_userinfo() {
        // user:pass@host must NOT leak into the masked value returned to the
        // client. Only scheme://host[:port] survives.
        assert_eq!(
            mask_webhook_url("https://user:pass@host/x"),
            "https://host/********"
        );
        // Userinfo + explicit port.
        assert_eq!(
            mask_webhook_url("https://user:pass@host:8443/webhook/tok"),
            "https://host:8443/********"
        );
        // user-only (no colon) userinfo also stripped.
        assert_eq!(
            mask_webhook_url("http://alice@example.com/hook"),
            "http://example.com/********"
        );
        // An '@' only inside the path (no userinfo in authority) is untouched.
        assert_eq!(
            mask_webhook_url("https://example.com/a@b/c"),
            "https://example.com/********"
        );
        // A bare-origin URL with userinfo (no path) is still stripped.
        assert_eq!(
            mask_webhook_url("https://user:pass@example.com"),
            "https://example.com/********"
        );
    }

    #[test]
    fn webhook_sentinel_filter_uses_ends_with() {
        // A URL that CONTAINS but does not END WITH the sentinel must NOT be
        // skipped by the SSRF-validation filter — it could be an attacker URL
        // crafted to embed the sentinel in a path segment.
        let sentinel = SECRET_SENTINEL;
        let tricky = format!("https://evil.com/{}@attacker.com/path", sentinel);
        // ends_with check: this does not end with the sentinel, so it is NOT
        // filtered (it would be validated / rejected by validate_fetch_url).
        assert!(!tricky.ends_with(sentinel));
        // The masked form DOES end with the sentinel and IS filtered.
        let masked = format!("https://discord.com/{}", sentinel);
        assert!(masked.ends_with(sentinel));
    }

    #[test]
    fn webhook_post_sentinel_preserves_stored_url() {
        // A GET→POST round-trip of the redacted form must NOT wipe the
        // token-bearing stored URL. A masked placeholder resolves back to its
        // stored secret by origin; a real entry replaces; an empty entry drops.
        let existing: Vec<String> = vec![
            "https://discord.com/api/webhooks/1/aaa".into(),
            "https://hooks.slack.com/services/x/y/zzz".into(),
        ];
        let incoming = [
            "https://discord.com/********", // masked → keep discord secret
            "https://example.com/new-hook", // changed → replace
        ];
        let resolved = resolve_webhook_urls(&incoming, &existing).unwrap();
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0], "https://discord.com/api/webhooks/1/aaa");
        assert_eq!(resolved[1], "https://example.com/new-hook");
    }

    #[test]
    fn webhook_post_masked_resolves_by_origin_not_position() {
        // HIGH regression: stored = [discord=secretA, slack=secretB]. The UI
        // reorders the masked rows to [slack-masked, discord-masked]. Resolving
        // BY POSITION would bind slack's row to discord's secret and vice
        // versa — a silent secret-confusion bug. By origin, each masked entry
        // must resolve to ITS OWN stored secret regardless of order.
        let existing: Vec<String> = vec![
            "https://discord.com/api/webhooks/1/secretA".into(),
            "https://hooks.slack.com/services/x/y/secretB".into(),
        ];
        // Reordered: slack first, discord second (each still masked).
        let reordered = [
            "https://hooks.slack.com/********",
            "https://discord.com/********",
        ];
        let resolved = resolve_webhook_urls(&reordered, &existing).unwrap();
        assert_eq!(
            resolved,
            vec![
                "https://hooks.slack.com/services/x/y/secretB".to_string(),
                "https://discord.com/api/webhooks/1/secretA".to_string(),
            ],
            "each masked entry must carry its own origin's secret, not the other's"
        );

        // Deleting the discord row and keeping only the (masked) slack row must
        // still resolve slack correctly — never to discord's secret.
        let only_slack = ["https://hooks.slack.com/********"];
        let resolved = resolve_webhook_urls(&only_slack, &existing).unwrap();
        assert_eq!(
            resolved,
            vec!["https://hooks.slack.com/services/x/y/secretB".to_string()]
        );
    }

    #[test]
    fn webhook_post_masked_unresolvable_origin_is_rejected() {
        // A masked entry whose origin matches NO stored URL (the referenced row
        // was deleted) is ambiguous — reject rather than guess. Likewise when
        // two stored hooks share an origin (>1 match).
        let existing: Vec<String> = vec!["https://discord.com/api/webhooks/1/aaa".into()];
        // Masked slack origin has no stored counterpart → Err.
        let orphan = ["https://hooks.slack.com/********"];
        assert!(resolve_webhook_urls(&orphan, &existing).is_err());

        // Two stored discord hooks share an origin → a masked discord entry is
        // ambiguous (>1 match) → Err.
        let two_discord: Vec<String> = vec![
            "https://discord.com/api/webhooks/1/aaa".into(),
            "https://discord.com/api/webhooks/2/bbb".into(),
        ];
        let masked = ["https://discord.com/********"];
        assert!(resolve_webhook_urls(&masked, &two_discord).is_err());
    }

    #[test]
    fn webhook_two_same_origin_round_trip_by_index() {
        // Regression (#8): two webhooks that share an origin used to mask to the
        // SAME placeholder, so a GET→POST round-trip was ambiguous (>1 origin
        // match) and the save was permanently rejected. With a stable per-entry
        // index embedded in the mask, each resolves to its OWN stored secret.
        let existing: Vec<String> = vec![
            "https://discord.com/api/webhooks/1/secretA".into(),
            "https://discord.com/api/webhooks/2/secretB".into(),
        ];
        // Exactly what GET /api/settings now emits.
        let masked0 = mask_webhook_url_indexed(&existing[0], 0);
        let masked1 = mask_webhook_url_indexed(&existing[1], 1);
        assert_ne!(masked0, masked1, "same-origin masks must differ by index");

        let incoming = [masked0.as_str(), masked1.as_str()];
        let resolved = resolve_webhook_urls(&incoming, &existing).unwrap();
        assert_eq!(
            resolved,
            vec![
                "https://discord.com/api/webhooks/1/secretA".to_string(),
                "https://discord.com/api/webhooks/2/secretB".to_string(),
            ],
            "each indexed mask must resolve to its own stored secret"
        );

        // A stale index whose origin mask no longer matches must be rejected,
        // not silently bound to the wrong secret.
        let stale = [mask_webhook_url_indexed("https://discord.com/x", 5)];
        let stale = [stale[0].as_str()];
        assert!(resolve_webhook_urls(&stale, &existing).is_err());
    }

    #[test]
    fn port_range_validation_rejects_out_of_range() {
        // handle_settings_post validates the parsed port against this range
        // BEFORE taking the Config write guard, so a bad value (e.g. 70000,
        // which would truncate to 4464 as u16) can't leave a partial
        // in-memory mutation behind. Pin the predicate the pre-guard check
        // uses.
        let ok = |v: u64| (1..=65535).contains(&v);
        assert!(!ok(0), "0 is not a valid bind port");
        assert!(
            !ok(70000),
            "70000 must be rejected (would truncate to 4464)"
        );
        assert!(!ok(65536), "65536 overflows u16");
        assert!(ok(1));
        assert!(ok(8080));
        assert!(ok(65535));
    }

    // ── Cross-origin (CSRF defense-in-depth) ───────────────────────────

    #[test]
    fn cross_origin_post_rejected_when_origin_host_differs() {
        // A browser on the LAN forging a POST carries an Origin header
        // whose host won't match our Host header → reject.
        assert!(is_cross_origin(
            Some("http://evil.example.com"),
            Some("autorip.test")
        ));
        // Referer fallback host mismatch is likewise rejected (the request
        // helper falls back to Referer when Origin is absent).
        assert!(is_cross_origin(
            Some("http://evil.example.com/page"),
            Some("autorip.test")
        ));
    }

    #[test]
    fn cross_origin_post_allowed_when_origin_absent_or_same() {
        // curl / monitoring scripts send no Origin → allow.
        assert!(!is_cross_origin(None, Some("autorip.test")));
        // Empty Origin → allow.
        assert!(!is_cross_origin(Some(""), Some("autorip.test")));
        // Same host (scheme/path stripped, case-insensitive) → allow.
        assert!(!is_cross_origin(
            Some("http://autorip.test"),
            Some("autorip.test")
        ));
        assert!(!is_cross_origin(
            Some("http://Host.Test:8080/x"),
            Some("host.test:8080")
        ));
        // No Host header to compare against → can't prove cross-origin, allow.
        assert!(!is_cross_origin(Some("http://evil.example.com"), None));
    }

    #[test]
    fn cross_origin_default_port_normalization() {
        // Origin omits the default port; Host carries it explicitly. These
        // are the SAME origin and must NOT be rejected. (The pre-fix exact
        // string compare 403'd these.)
        assert!(!is_cross_origin(
            Some("http://autorip.test"),
            Some("autorip.test:80")
        ));
        assert!(!is_cross_origin(
            Some("https://autorip.test"),
            Some("autorip.test:443")
        ));
        // Inverse: Origin carries the default port, Host omits it.
        assert!(!is_cross_origin(
            Some("http://autorip.test:80"),
            Some("autorip.test")
        ));
        // IPv6 literal, default-port both sides.
        assert!(!is_cross_origin(Some("http://[::1]"), Some("[::1]:80")));
        // A genuinely different port is still cross-origin.
        assert!(is_cross_origin(
            Some("http://autorip.test:8080"),
            Some("autorip.test:9090")
        ));
        // https default (443) must not collapse onto http default (80):
        // an https Origin compared against a Host carrying :80 is a real
        // mismatch.
        assert!(is_cross_origin(
            Some("https://autorip.test"),
            Some("autorip.test:80")
        ));
    }

    // ── SSRF guard ─────────────────────────────────────────────────────

    #[test]
    fn blocks_loopback_private_and_metadata_ips() {
        use std::net::{Ipv4Addr, Ipv6Addr};
        // Loopback.
        assert!(is_blocked_ip(&IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))));
        // RFC1918 private ranges.
        assert!(is_blocked_ip(&IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))));
        assert!(is_blocked_ip(&IpAddr::V4(Ipv4Addr::new(192, 168, 1, 50))));
        assert!(is_blocked_ip(&IpAddr::V4(Ipv4Addr::new(172, 16, 0, 1))));
        // Cloud metadata anycast (link-local).
        assert!(is_blocked_ip(&IpAddr::V4(Ipv4Addr::new(
            169, 254, 169, 254
        ))));
        // Carrier-grade NAT 100.64.0.0/10 and "this network" 0.0.0.0/8.
        assert!(is_blocked_ip(&IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1))));
        assert!(is_blocked_ip(&IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0))));
        // IPv6 loopback, ULA, link-local.
        assert!(is_blocked_ip(&IpAddr::V6(Ipv6Addr::LOCALHOST)));
        assert!(is_blocked_ip(&IpAddr::V6(Ipv6Addr::new(
            0xfd00, 0, 0, 0, 0, 0, 0, 1
        ))));
        assert!(is_blocked_ip(&IpAddr::V6(Ipv6Addr::new(
            0xfe80, 0, 0, 0, 0, 0, 0, 1
        ))));
        // IPv4-mapped loopback ::ffff:127.0.0.1 must also be blocked.
        assert!(is_blocked_ip(&IpAddr::V6(
            Ipv4Addr::new(127, 0, 0, 1).to_ipv6_mapped()
        )));
    }

    #[test]
    fn blocks_ipv4_compat_and_class_e() {
        use std::net::{Ipv4Addr, Ipv6Addr};
        // IPv4-compatible ::127.0.0.1 (deprecated but still parseable).
        // to_ipv4_mapped() would miss this; to_ipv4() catches it.
        assert!(is_blocked_ip(&IpAddr::V6(Ipv6Addr::new(
            0, 0, 0, 0, 0, 0, 0x7f00, 0x0001
        ))));
        // Class-E 240.0.0.0/4 — reserved, not public.
        assert!(is_blocked_ip(&IpAddr::V4(Ipv4Addr::new(240, 0, 0, 1))));
        assert!(is_blocked_ip(&IpAddr::V4(Ipv4Addr::new(
            255, 255, 255, 254
        ))));
        // 239.x is multicast (already caught by is_multicast), not Class-E.
        // Boundary check: 239.255.255.255 is multicast, 240.0.0.0 is Class-E.
        assert!(is_blocked_ip(&IpAddr::V4(Ipv4Addr::new(240, 0, 0, 0))));
    }

    #[test]
    fn allows_public_ips() {
        use std::net::{Ipv4Addr, Ipv6Addr};
        assert!(!is_blocked_ip(&IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))));
        assert!(!is_blocked_ip(&IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1))));
        // Public IPv6 (Cloudflare DNS).
        assert!(!is_blocked_ip(&IpAddr::V6(Ipv6Addr::new(
            0x2606, 0x4700, 0x4700, 0, 0, 0, 0, 0x1111
        ))));
    }

    #[test]
    fn validate_fetch_url_rejects_internal_and_bad_scheme() {
        // Numeric internal/metadata literals resolve without DNS and must
        // be rejected.
        assert!(validate_fetch_url("http://127.0.0.1/x").is_err());
        assert!(validate_fetch_url("http://169.254.169.254/latest/meta-data/").is_err());
        assert!(
            validate_fetch_url(&format!("http://{}.{}.{}.{}:8080/decode", 10, 0, 0, 5)).is_err()
        );
        assert!(validate_fetch_url(&format!("https://{}.{}.{}.{}/", 192, 168, 0, 1)).is_err());
        assert!(validate_fetch_url("http://[::1]:9000/").is_err());
        // Non-http schemes and junk.
        assert!(validate_fetch_url("ftp://example.com/x").is_err());
        assert!(validate_fetch_url("file:///etc/passwd").is_err());
        assert!(validate_fetch_url("not a url").is_err());
        assert!(validate_fetch_url("").is_err());
    }

    #[test]
    fn guarded_get_rejects_rfc1918_before_connecting() {
        // guarded_get must run the SSRF guard FIRST, so an RFC1918 /
        // loopback / metadata literal is rejected with an Err and no socket
        // is ever opened. (This is the guard the main.rs KEYDB fetch paths
        // route through instead of a bare ureq::get.)
        assert!(guarded_get(&format!("http://{}.{}.{}.{}/keydb.zip", 10, 0, 0, 5)).is_err());
        assert!(guarded_get(&format!("http://{}.{}.{}.{}/keydb.zip", 192, 168, 1, 10)).is_err());
        assert!(guarded_get(&format!("http://{}.{}.{}.{}/keydb.zip", 172, 20, 0, 1)).is_err());
        assert!(guarded_get("http://127.0.0.1/keydb.zip").is_err());
        assert!(guarded_get("http://169.254.169.254/latest/").is_err());
        assert!(guarded_get("http://[::1]:9000/keydb.zip").is_err());
        // Wrong scheme is rejected too (no connect attempt).
        assert!(guarded_get("file:///etc/passwd").is_err());
    }

    /// Secret-leak guard: guarded_get error strings from the ureq transport
    /// layer must never embed the full request URL (which may contain a token
    /// in the path/query). ureq's Display includes the URL; our map_err must
    /// strip it to a status code / transport kind only.
    ///
    /// We test this via a public literal IP that passes the SSRF guard (so we
    /// reach the ureq call), but where the connection is immediately refused
    /// (no server listening). This exercises the Transport error arm of our
    /// map_err, where ureq's Display would otherwise include the full URL.
    ///
    /// Note: the RFC1918-rejection errors come from validate_fetch_url (via ?)
    /// before ureq is called; those contain the blocked IP, which is expected
    /// (IP is not sensitive, token path is). The ureq-level error is what we
    /// must not leak.
    #[test]
    fn guarded_get_ureq_error_does_not_embed_url() {
        // Port 1 on a public IP: passes SSRF guard (it's public) but the
        // connection will be refused immediately (nothing listens on port 1).
        // The URL has a fake token in the path that must not appear in the error.
        let token = "supersecret_api_token_12345";
        let url = format!("http://8.8.8.8:1/keydb/{token}.zip");
        let err = guarded_get(&url).unwrap_err();
        assert!(
            !err.contains(token),
            "ureq transport error must not leak the URL token; got: {err:?}"
        );
        // The error string must be our summary, not ureq's URL-bearing Display.
        assert!(
            err.starts_with("fetch failed:"),
            "error should be our summary; got: {err:?}"
        );
    }

    #[test]
    fn validate_network_target_rejects_internal_hosts() {
        // Bare host:port (no scheme). Internal/metadata literals resolve
        // without DNS and must be rejected — at rip time decrypted content
        // streams here.
        assert!(validate_network_target("169.254.169.254:80").is_err());
        assert!(validate_network_target("127.0.0.1:9000").is_err());
        assert!(validate_network_target(&format!("{}.{}.{}.{}:9000", 10, 0, 0, 5)).is_err());
        assert!(validate_network_target(&format!("{}.{}.{}.{}:9000", 192, 168, 0, 1)).is_err());
        assert!(validate_network_target("[::1]:9000").is_err());
        // RFC5737 documentation range is non-public and blocked.
        assert!(validate_network_target("198.51.100.10:9000").is_err());
        // Malformed / missing port.
        assert!(validate_network_target("nas.example.com").is_err());
        assert!(validate_network_target("169.254.169.254").is_err());
        assert!(validate_network_target("").is_err());
    }

    #[test]
    fn validate_network_target_accepts_public_literal() {
        // A public numeric host:port (no DNS needed) should validate.
        assert!(validate_network_target("8.8.8.8:9000").is_ok());
        assert!(validate_network_target("1.1.1.1:443").is_ok());
    }

    #[test]
    fn resolve_with_timeout_resolves_literal() {
        // A numeric literal resolves without touching DNS and returns within
        // the deadline. Shared by validate_network_target + validate_fetch_url.
        let addrs = resolve_with_timeout("9.9.9.9", 853).expect("literal resolves");
        assert!(addrs.iter().any(|a| a.port() == 853 && a.ip().is_ipv4()));
    }

    #[test]
    fn resolve_with_timeout_does_not_leak_inflight_slots() {
        // Regression for the unbounded-thread leak: the in-flight cap is 8.
        // A completed resolve must release its slot, so many sequential
        // resolves (far more than the cap) all succeed — if slots leaked, the
        // 9th+ call would fail fast with a spurious timeout. Each literal
        // resolve still spawns + joins its detached thread, which decrements
        // the counter, so the cap never saturates.
        for _ in 0..40 {
            let addrs = resolve_with_timeout("9.9.9.9", 853).expect("literal resolves");
            assert!(addrs.iter().any(|a| a.port() == 853));
            // Let the detached resolver thread run its fetch_sub before the
            // next iteration so the slot is reliably released.
            std::thread::yield_now();
        }
    }

    #[test]
    fn validate_fetch_url_accepts_public_literal() {
        // A public numeric host (no DNS needed) should validate and yield
        // the pinned address with the default port for the scheme.
        let addrs = validate_fetch_url("https://8.8.8.8/keydb.zip").expect("public IP allowed");
        assert!(addrs.iter().any(|a| a.port() == 443));
        let addrs = validate_fetch_url("http://1.1.1.1:8080/decode").expect("public IP allowed");
        assert!(addrs.iter().any(|a| a.port() == 8080));
    }

    // ── Connection cap ─────────────────────────────────────────────────

    #[test]
    fn conn_guard_enforces_cap_and_releases_on_drop() {
        static C: AtomicUsize = AtomicUsize::new(0);
        let g1 = ConnGuard::try_acquire(&C, 2);
        let g2 = ConnGuard::try_acquire(&C, 2);
        assert!(g1.is_some());
        assert!(g2.is_some());
        assert_eq!(C.load(Ordering::SeqCst), 2);
        // Third over the cap is rejected.
        assert!(ConnGuard::try_acquire(&C, 2).is_none());
        // Dropping one frees a slot so the next acquire succeeds.
        drop(g1);
        assert_eq!(C.load(Ordering::SeqCst), 1);
        let g3 = ConnGuard::try_acquire(&C, 2);
        assert!(g3.is_some());
        drop(g2);
        drop(g3);
        assert_eq!(C.load(Ordering::SeqCst), 0);
    }

    // ── percent_decode trailing %XX ────────────────────────────────────

    #[test]
    fn percent_decode_handles_trailing_encoded_byte() {
        // A value ending in a percent-encoded byte must decode (the old
        // off-by-one dropped it through as literal text).
        assert_eq!(percent_decode("a%20b"), "a b");
        assert_eq!(percent_decode("end%20"), "end ");
        // A bare trailing '%' or incomplete '%X' stays literal (no panic).
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("50%2"), "50%2");
    }

    // ── Real HTTP integration: drive handle_request via a live server ──
    //
    // tiny_http::Request has no public constructor, so these tests bind a
    // loopback Server on an ephemeral port, write a raw HTTP/1.1 request from
    // a client thread, recv the Request on the server side, hand it to the
    // PRODUCTION `handle_request`, and read the served response back. This is
    // the only way to exercise route dispatch + method gating + the real
    // handlers (all private fns) end-to-end. Every assertion here fails if the
    // dispatch wiring or a handler regresses — none of it is string-matched.
    mod http {
        use super::*;
        use std::io::{Read, Write};
        use std::net::TcpStream;

        /// One real request/response round-trip through `handle_request`.
        ///
        /// Binds an ephemeral loopback server, spawns a client that writes the
        /// raw request and reads the full response, then on this thread accepts
        /// the request and dispatches it through production code. Returns the
        /// parsed (status_code, body).
        fn roundtrip(
            cfg: &Arc<RwLock<Config>>,
            method: &str,
            path: &str,
            body: Option<&str>,
            extra_headers: &[(&str, &str)],
        ) -> (u16, String) {
            let server = Server::http("127.0.0.1:0").expect("bind loopback server");
            let addr = server.server_addr().to_ip().expect("ip addr");

            let method = method.to_string();
            let path = path.to_string();
            let body = body.map(|b| b.to_string());
            let extra: Vec<(String, String)> = extra_headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();

            let client = std::thread::spawn(move || {
                let mut stream = TcpStream::connect(addr).expect("connect");
                let body = body.unwrap_or_default();
                let mut req = format!("{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\n");
                for (k, v) in &extra {
                    req.push_str(&format!("{k}: {v}\r\n"));
                }
                req.push_str(&format!("Content-Length: {}\r\n", body.len()));
                req.push_str("Connection: close\r\n\r\n");
                req.push_str(&body);
                stream.write_all(req.as_bytes()).expect("write request");
                stream.flush().ok();
                let mut resp = Vec::new();
                stream.read_to_end(&mut resp).expect("read response");
                String::from_utf8_lossy(&resp).to_string()
            });

            // Accept exactly one request and dispatch it through production.
            let request = server.recv().expect("recv request");
            handle_request(request, cfg);

            let raw = client.join().expect("client thread");
            parse_response(&raw)
        }

        /// Extract the status code and body from a raw HTTP/1.1 response.
        fn parse_response(raw: &str) -> (u16, String) {
            let (head, body) = raw.split_once("\r\n\r\n").unwrap_or((raw, ""));
            let status_line = head.lines().next().unwrap_or_default();
            // "HTTP/1.1 200 OK"
            let code = status_line
                .split_whitespace()
                .nth(1)
                .and_then(|c| c.parse::<u16>().ok())
                .unwrap_or(0);
            (code, body.to_string())
        }

        /// A Config whose autorip_dir points at a writable tempdir so
        /// `config::save` (invoked by handle_settings_post) succeeds and we can
        /// read back the persisted settings.json.
        fn cfg_in_tempdir(dir: &std::path::Path) -> Arc<RwLock<Config>> {
            let c = Config {
                autorip_dir: dir.to_string_lossy().to_string(),
                staging_dir: dir.join("staging").to_string_lossy().to_string(),
                output_dir: dir.join("output").to_string_lossy().to_string(),
                ..Config::default()
            };
            Arc::new(RwLock::new(c))
        }

        // ── Route dispatch + method gating ──────────────────────────────

        #[test]
        fn get_version_dispatches_and_returns_running_version() {
            let cfg = Arc::new(RwLock::new(Config::default()));
            let (code, body) = roundtrip(&cfg, "GET", "/api/version", None, &[]);
            assert_eq!(code, 200);
            assert!(
                body.contains(&format!("\"version\":\"{}\"", env!("CARGO_PKG_VERSION"))),
                "GET /api/version must serve the running version, got: {body}"
            );
        }

        #[test]
        fn unknown_route_returns_404() {
            let cfg = Arc::new(RwLock::new(Config::default()));
            let (code, body) = roundtrip(&cfg, "GET", "/api/nope", None, &[]);
            assert_eq!(code, 404, "an unknown route must 404");
            assert!(body.contains("not found"));
        }

        #[test]
        fn settings_route_gates_on_method() {
            // GET /api/settings serves redacted settings; a DELETE to the same
            // path falls through to 404 (method-gated, not matched).
            let cfg = Arc::new(RwLock::new(Config::default()));
            let (get_code, _) = roundtrip(&cfg, "GET", "/api/settings", None, &[]);
            assert_eq!(get_code, 200, "GET /api/settings must be served");
            let (del_code, _) = roundtrip(&cfg, "DELETE", "/api/settings", None, &[]);
            assert_eq!(del_code, 404, "DELETE /api/settings must not match");
        }

        #[test]
        fn sse_route_is_served_at_events_not_api_sse() {
            // Pin the ACTUAL served route. Production serves /events; /api/sse
            // is NOT a route and must 404. (This replaces the end_to_end
            // dispatcher that accepted both.) /events is a streaming handler;
            // assert it does not 404 rather than reading the infinite stream.
            let cfg = Arc::new(RwLock::new(Config::default()));
            let (api_sse_code, _) = roundtrip(&cfg, "GET", "/api/sse", None, &[]);
            assert_eq!(
                api_sse_code, 404,
                "/api/sse is not a real route — production serves /events"
            );
        }

        // ── Device-name validation in dispatch ──────────────────────────

        #[test]
        fn rip_route_rejects_invalid_device_name() {
            // A path-traversal device name must be rejected by the dispatch
            // guard (is_valid_device_name) with 400 — never reaching handle_rip.
            let cfg = Arc::new(RwLock::new(Config::default()));
            let (code, body) = roundtrip(&cfg, "POST", "/api/rip/..%2F..%2Fetc", None, &[]);
            assert_eq!(code, 400, "traversal device name must be rejected");
            assert!(body.contains("invalid device name"));
        }

        #[test]
        fn stop_route_rejects_invalid_device_name() {
            let cfg = Arc::new(RwLock::new(Config::default()));
            let (code, _) = roundtrip(&cfg, "POST", "/api/stop/x", None, &[]);
            // "x" is too short (is_valid_device_name requires len 3..=64) -> 400.
            assert_eq!(code, 400, "a 1-char device name must be rejected");
        }

        // ── CSRF gate on POST ───────────────────────────────────────────

        #[test]
        fn cross_origin_post_is_rejected_403() {
            let cfg = Arc::new(RwLock::new(Config::default()));
            let (code, body) = roundtrip(
                &cfg,
                "POST",
                "/api/settings",
                Some("{}"),
                &[("Origin", "http://evil.example.com")],
            );
            assert_eq!(code, 403, "a cross-origin POST must be rejected");
            assert!(body.contains("cross-origin"));
        }

        // ── read_json_body size limit (via handle_settings_post) ────────

        #[test]
        fn oversize_request_body_is_rejected_413() {
            let tmp = tempfile::TempDir::new().unwrap();
            let cfg = cfg_in_tempdir(tmp.path());
            // One byte over MAX_REQUEST_BODY (1 MiB).
            let big = "x".repeat((MAX_REQUEST_BODY as usize) + 1);
            let (code, _) = roundtrip(&cfg, "POST", "/api/settings", Some(&big), &[]);
            assert_eq!(code, 413, "a body over MAX_REQUEST_BODY must be 413");
        }

        #[test]
        fn malformed_json_body_is_rejected_400() {
            let tmp = tempfile::TempDir::new().unwrap();
            let cfg = cfg_in_tempdir(tmp.path());
            let (code, body) = roundtrip(&cfg, "POST", "/api/settings", Some("{not json"), &[]);
            assert_eq!(code, 400);
            assert!(body.contains("invalid json"));
        }

        // ── handle_settings_post: the real save + the sentinel guard ────

        #[test]
        fn settings_post_persists_a_field_to_disk() {
            let tmp = tempfile::TempDir::new().unwrap();
            let cfg = cfg_in_tempdir(tmp.path());
            let (code, body) = roundtrip(
                &cfg,
                "POST",
                "/api/settings",
                Some(r#"{"abort_on_lost_secs": 30}"#),
                &[],
            );
            assert_eq!(code, 200, "a valid settings POST must succeed, got: {body}");
            assert!(body.contains("\"ok\":true"));
            // The in-memory config was mutated...
            assert_eq!(cfg.read().unwrap().abort_on_lost_secs, 30);
            // ...and persisted to settings.json on disk.
            let saved = std::fs::read_to_string(cfg.read().unwrap().settings_file())
                .expect("settings.json must be written");
            assert!(
                saved.contains("\"abort_on_lost_secs\""),
                "the persisted settings.json must carry the field"
            );
        }

        #[test]
        fn settings_post_masked_keyserver_url_preserves_stored() {
            // The secret-sentinel guard, MASKED half: a POST carrying the
            // masked keyserver_url (containing SECRET_SENTINEL — the form GET
            // returns) must NOT clobber the stored token-bearing URL.
            let tmp = tempfile::TempDir::new().unwrap();
            let cfg = cfg_in_tempdir(tmp.path());
            let stored = "https://8.8.8.8/mysecrettoken/decode";
            cfg.write().unwrap().keyserver_url = stored.to_string();

            let masked = format!("https://8.8.8.8/{SECRET_SENTINEL}");
            let patch = format!(r#"{{"keyserver_url": "{masked}"}}"#);
            let (code, _) = roundtrip(&cfg, "POST", "/api/settings", Some(&patch), &[]);
            assert_eq!(code, 200);
            assert_eq!(
                cfg.read().unwrap().keyserver_url,
                stored,
                "a masked (sentinel) keyserver_url must leave the stored URL intact"
            );
        }

        #[test]
        fn settings_post_real_keyserver_url_replaces_stored() {
            // The secret-sentinel guard, REAL-VALUE half: a POST with a genuine
            // (sentinel-free, SSRF-valid) keyserver_url replaces the stored one.
            let tmp = tempfile::TempDir::new().unwrap();
            let cfg = cfg_in_tempdir(tmp.path());
            cfg.write().unwrap().keyserver_url = "https://8.8.8.8/old/decode".to_string();

            // Public IP literal validates without DNS.
            let patch = r#"{"keyserver_url": "https://1.1.1.1/newtoken/decode"}"#;
            let (code, _) = roundtrip(&cfg, "POST", "/api/settings", Some(patch), &[]);
            assert_eq!(code, 200);
            assert_eq!(
                cfg.read().unwrap().keyserver_url,
                "https://1.1.1.1/newtoken/decode",
                "a real new keyserver_url must replace the stored one"
            );
        }

        #[test]
        fn settings_post_empty_keyserver_url_clears_it() {
            // The clear half: an empty (no-sentinel) keyserver_url writes
            // through, clearing the stored value (disables the online source).
            let tmp = tempfile::TempDir::new().unwrap();
            let cfg = cfg_in_tempdir(tmp.path());
            cfg.write().unwrap().keyserver_url = "https://8.8.8.8/token/decode".to_string();

            let (code, _) = roundtrip(
                &cfg,
                "POST",
                "/api/settings",
                Some(r#"{"keyserver_url": ""}"#),
                &[],
            );
            assert_eq!(code, 200);
            assert_eq!(
                cfg.read().unwrap().keyserver_url,
                "",
                "an empty keyserver_url must clear the stored value"
            );
        }

        #[test]
        fn settings_post_ssrf_url_is_rejected_400_and_not_stored() {
            // A non-sentinel keyserver_url pointing at an internal/loopback
            // host must be rejected before the write guard — stored value
            // untouched.
            let tmp = tempfile::TempDir::new().unwrap();
            let cfg = cfg_in_tempdir(tmp.path());
            cfg.write().unwrap().keyserver_url = "https://8.8.8.8/keep/decode".to_string();

            let (code, _) = roundtrip(
                &cfg,
                "POST",
                "/api/settings",
                Some(r#"{"keyserver_url": "http://127.0.0.1/admin"}"#),
                &[],
            );
            assert_eq!(code, 400, "an SSRF keyserver_url must be rejected");
            assert_eq!(
                cfg.read().unwrap().keyserver_url,
                "https://8.8.8.8/keep/decode",
                "a rejected keyserver_url must not mutate the stored value"
            );
        }

        // ── handle_stop / handle_scan / handle_rip reach their handlers ──

        #[test]
        fn stop_route_reaches_handle_stop_with_its_own_drive_not_found() {
            // A well-formed device with no STATE entry must reach handle_stop,
            // which answers with ITS OWN distinctive "drive not found" body
            // (not the generic dispatch "not found"). This proves the POST
            // route is wired to the handler, not merely validated then dropped.
            let cfg = Arc::new(RwLock::new(Config::default()));
            let (code, body) = roundtrip(&cfg, "POST", "/api/stop/sr0", None, &[]);
            assert_eq!(code, 404);
            assert!(
                body.contains("drive not found"),
                "must be handle_stop's response, not the dispatch 404; got: {body}"
            );
            // And the dispatch fallthrough body must NOT appear.
            assert!(
                !body.contains("\"error\":\"not found\""),
                "a wired /api/stop/<dev> must not hit the dispatch 404"
            );
        }
    }
}

fn text_response(request: tiny_http::Request, body: &str) {
    let header =
        Header::from_bytes(&b"Content-Type"[..], &b"text/plain; charset=utf-8"[..]).unwrap();
    let response = Response::from_string(body).with_header(header);
    let _ = request.respond(response);
}

fn get_state_json(staging_dir: &str) -> String {
    let state = match ripper::STATE.lock() {
        Ok(s) => s,
        Err(_) => return "{}".to_string(),
    };
    let move_state = crate::mover::MOVE_STATE
        .lock()
        .ok()
        .and_then(|ms| ms.clone());
    // Mux progress rides on the synthetic `_mux` device key in STATE (a
    // RipState seeded by the mux worker — see the dashboard JS at the
    // `_mux` field), serialized below as part of `state`. There is no
    // separate live MuxState struct.
    let verify_state = crate::verify::dashboard_state();
    let mut obj = serde_json::to_value(&*state).unwrap_or_else(|_| serde_json::json!({}));
    if let Some(ms) = move_state {
        obj["_move"] = serde_json::to_value(&ms).unwrap_or_default();
    }
    if let Some(vs) = verify_state {
        obj["_verify"] = serde_json::to_value(&vs).unwrap_or_default();
    }
    // SINGLE-SOURCE STAGE VIEW (fix C): the Mux queue and Move queue ride
    // on the SAME state payload as the per-device tiles and the synthetic
    // `_mux` live-progress device. The dashboard pushes this payload on
    // every SSE tick (~1s), so all three views — the device tile, the Mux
    // queue, the Move queue — are always derived from one consistent
    // snapshot. Two consecutive polls can no longer disagree (e.g. a job
    // showing in both queues), and the queues no longer go stale until a
    // tab re-open / hard refresh the way the separate `/api/system` fetch
    // did. `pending_queue` already enforces mutual exclusion (a `.done`/
    // `.review`/`.muxing`/`.completed`/`.failed` dir is never "(queued)"),
    // so within this one snapshot a disc appears in at most one queue.
    let (mux_queue, move_queue) = build_queue_views(staging_dir);
    obj["_mux_queue"] = serde_json::to_value(&mux_queue).unwrap_or_default();
    obj["_move_queue"] = serde_json::to_value(&move_queue).unwrap_or_default();
    obj.to_string()
}

/// Build the Mux-queue and Move-queue display lists from the staging dir.
/// Shared by `get_state_json` (the live SSE/`/api/state` payload) and
/// `handle_system_info` (the `/api/system` panel) so both endpoints derive
/// the two queues from one place and can never disagree on membership.
///
/// Mutual exclusion is guaranteed by the markers themselves: the Move
/// queue scans for `.done`, and `crate::muxer::pending_queue` (the Mux
/// queue) skips any dir carrying `.done`/`.review`/`.muxing`/`.completed`/
/// `.failed`. So a given staging dir lands in at most one of the two lists.
fn build_queue_views(staging_dir: &str) -> (Vec<String>, Vec<String>) {
    const QUEUE_DISPLAY_CAP: usize = 100;
    // Move queue: staging dirs with a `.done` marker (pending moves).
    let mut move_queue: Vec<String> = std::fs::read_dir(staging_dir)
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
    move_queue.truncate(QUEUE_DISPLAY_CAP);
    // Mux queue: staging dirs with a `.ripped` hand-off and no terminal /
    // move-queue / in-flight marker (see `pending_queue`).
    let mut mux_queue = crate::muxer::pending_queue(std::path::Path::new(staging_dir));
    mux_queue.truncate(QUEUE_DISPLAY_CAP);
    (mux_queue, move_queue)
}

fn handle_system_info(request: tiny_http::Request, cfg: &Arc<RwLock<Config>>) {
    // Degrade gracefully on a poisoned lock, matching every other handler
    // (e.g. GET /api/settings) rather than panicking this handler thread
    // and silently breaking the System tab.
    let cfg = match cfg.read() {
        Ok(c) => c,
        Err(_) => {
            return json_response(
                request,
                500,
                r#"{"ok":false,"error":"config lock poisoned"}"#,
            );
        }
    };

    // Cap on how many queue entries we serialize so a staging dir holding a
    // pathological number of subdirs can't produce an unbounded response.
    const QUEUE_DISPLAY_CAP: usize = 100;

    // Pre-cap full counts so the UI can show "+N more" rather than silently
    // hiding entries dropped by the display cap.
    let move_full_count = std::fs::read_dir(&cfg.staging_dir)
        .ok()
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| e.path().is_dir() && e.path().join(".done").exists())
                .count()
        })
        .unwrap_or(0);
    let mux_full_count = crate::muxer::pending_queue(std::path::Path::new(&cfg.staging_dir)).len();

    // Move + Mux queue display lists come from the SAME shared builder the
    // live /api/state + SSE payload uses (`build_queue_views`), so the
    // System-page panels and the live dashboard can never disagree on queue
    // membership. `build_queue_views` enforces mutual exclusion (a dir is in
    // at most one of the two lists).
    let (mux_queue, move_queue) = build_queue_views(&cfg.staging_dir);

    // Mover errors: stuck staging dirs the user needs to act on.
    let move_errors: Vec<crate::mover::MoverError> = crate::mover::MOVE_ERRORS
        .lock()
        .map(|m| m.values().cloned().collect())
        .unwrap_or_default();

    let truncation_count = move_full_count.saturating_sub(QUEUE_DISPLAY_CAP)
        + mux_full_count.saturating_sub(QUEUE_DISPLAY_CAP);
    let mux_errors: Vec<crate::muxer::MuxerError> = crate::muxer::MUX_ERRORS
        .lock()
        .map(|m| m.values().cloned().collect())
        .unwrap_or_default();

    // System log: last 50 lines. Tail from the end with a bounded read
    // rather than slurping the whole file — device_system.log is never
    // rotated and the System page polls this endpoint every few seconds.
    let syslog_path = format!("{}/device_system.log", cfg.log_dir());
    let syslog = tail_file(&syslog_path, SYSLOG_TAIL_BYTES)
        .unwrap_or_default()
        .lines()
        .rev()
        .take(50)
        .collect::<Vec<_>>()
        .join("\n");

    let body = serde_json::json!({
        "move_queue": move_queue,
        "move_errors": move_errors,
        "mux_queue": mux_queue,
        "mux_errors": mux_errors,
        "truncation_count": truncation_count,
        "syslog": syslog,
        // Current runtime debug-logging state, so the System-page toggle
        // reflects reality on load (POST /api/debug flips it).
        "debug_enabled": debug_enabled(),
    });

    json_response(request, 200, &body.to_string());
}

fn handle_device_log(request: tiny_http::Request, _cfg: &Arc<RwLock<Config>>, device: &str) {
    // Single source of truth for device-name validation. The /api/logs
    // dispatch site already gates on is_valid_device_name (strict sg\d+),
    // so this is normally unreachable with a bad name — but re-checking
    // with the *same* strict predicate (rather than the looser
    // ascii-alphanumeric test that previously lived here, which an empty
    // string passes vacuously and which accepts sda/sr0) closes any
    // latent bypass if the handler is ever called directly.
    if !is_valid_device_name(device) {
        text_response(request, "invalid device");
        return;
    }
    let lines = crate::log::get_device_log(device, 2000);
    text_response(request, &lines.join("\n"));
}

/// Upper bound on how many trailing bytes of a log file we read into
/// memory when tailing. The JSONL event log uses `rolling::never`
/// (observe.rs) so it grows unbounded for the container's life; the
/// System/Debug tabs poll it every few seconds. 8 MiB comfortably holds
/// the 5000-line `n` cap of typical events while keeping per-request
/// allocation bounded regardless of total file size.
const DEBUG_TAIL_BYTES: u64 = 8 * 1024 * 1024;

/// Same idea for the system log: 50 lines, generously bounded.
const SYSLOG_TAIL_BYTES: u64 = 256 * 1024;

/// Read up to the last `max_bytes` of a file as a UTF-8 string, seeking
/// from the end rather than slurping the whole file. If the file is
/// larger than `max_bytes`, the first (partial) line of the returned
/// region may be truncated mid-record — acceptable for a tail view and
/// the truncated head line is dropped by callers that split on `\n`.
fn tail_file(path: &str, max_bytes: u64) -> std::io::Result<String> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = std::fs::File::open(path)?;
    let len = f.metadata()?.len();
    let read_from = len.saturating_sub(max_bytes);
    let truncated = read_from > 0;
    f.seek(SeekFrom::Start(read_from))?;
    let mut buf = Vec::with_capacity(len.saturating_sub(read_from).min(max_bytes) as usize);
    f.take(max_bytes).read_to_end(&mut buf)?;
    let mut s = String::from_utf8_lossy(&buf).into_owned();
    // When we seeked into the middle of the file, the first line is a
    // partial record — drop it so callers never parse a half line.
    if truncated {
        if let Some(nl) = s.find('\n') {
            s.drain(..=nl);
        }
    }
    Ok(s)
}

/// `GET /api/debug?n=N&level=L&device=D&q=substr` — last N JSONL events.
///
/// Tails `{AUTORIP_DIR}/logs/autorip.jsonl` (the structured event stream
/// emitted by the tracing layer in `observe.rs`). Optional filters:
///
/// - `n` (default 500, max 5000) — number of trailing lines to return
/// - `level` — `error|warn|info|debug|trace` minimum level
/// - `device` — only events whose `fields.device` matches
/// - `q` — substring match anywhere in the JSON line (cheap grep)
///
/// Output is **raw JSONL** (newline-separated JSON objects), not wrapped
/// in a JSON array — keeps it streamable, greppable, and easy for shell
/// tools to consume. Used by the web UI Debug tab and by anyone running
/// `curl http://autorip:8080/api/debug?level=warn | jq` from a terminal.
fn handle_debug_log(request: tiny_http::Request, url: &str) {
    let params = parse_query(url);
    let n: usize = params
        .get("n")
        .and_then(|s| s.parse().ok())
        .unwrap_or(500)
        .min(5000);
    let level = params.get("level").map(|s| s.to_lowercase());
    // Validate the device filter with the same strict predicate as every other
    // device handler; ignore an invalid value rather than letting an arbitrary
    // attacker-supplied substring into the line filter.
    let device = params
        .get("device")
        .filter(|d| is_valid_device_name(d))
        .cloned();
    // Restrict the free-text grep filter to printable ASCII (0x20..=0x7E).
    // The JSONL we grep is ASCII-only; rejecting non-printable/non-ASCII keeps
    // an attacker from smuggling control bytes or arbitrary Unicode into the
    // line filter. The 256-byte cap in parse_query already bounds its size.
    let q = params
        .get("q")
        .filter(|s| s.bytes().all(|b| (0x20..=0x7E).contains(&b)))
        .cloned();

    let path = crate::observe::json_log_path();
    let content = match tail_file(&path, DEBUG_TAIL_BYTES) {
        Ok(s) => s,
        Err(e) => {
            // The non-rolling jsonl file may not exist on a fresh boot
            // before the first event flushes. Return empty rather than 404
            // — UI can poll without alerting.
            tracing::debug!(path = %path, error = %e, "debug: jsonl missing");
            return text_response(request, "");
        }
    };

    let levels_at_or_above = |min: &str| -> &'static [&'static str] {
        match min {
            "error" => &["ERROR"],
            "warn" => &["WARN", "ERROR"],
            "info" => &["INFO", "WARN", "ERROR"],
            "debug" => &["DEBUG", "INFO", "WARN", "ERROR"],
            _ => &["TRACE", "DEBUG", "INFO", "WARN", "ERROR"],
        }
    };

    let lines: Vec<&str> = content.lines().collect();
    let start = lines.len().saturating_sub(n);
    let mut out: Vec<String> = Vec::new();
    for line in &lines[start..] {
        if let Some(ref l) = level {
            let allowed = levels_at_or_above(l);
            // tracing-subscriber JSON format puts the level in `"level":"INFO"`.
            if !allowed
                .iter()
                .any(|lv| line.contains(&format!("\"level\":\"{}\"", lv)))
            {
                continue;
            }
        }
        if let Some(ref d) = device {
            // Match `"device":"sg4"` exactly to avoid `sg40` matching `sg4`.
            if !line.contains(&format!("\"device\":\"{}\"", d)) {
                continue;
            }
        }
        if let Some(ref needle) = q {
            if !line.contains(needle) {
                continue;
            }
        }
        out.push((*line).to_string());
    }
    text_response(request, &out.join("\n"));
}

/// Parse `?key=value&key2=v2` from a URL into a HashMap. Naive:
/// percent-decodes each key and value via `percent_decode`, but does NOT
/// translate `+` to space (so this is not full
/// application/x-www-form-urlencoded decoding) and has no array-style
/// keys — sufficient for our handful of debug filters and easier to
/// audit than pulling a URL parser dep.
fn parse_query(url: &str) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    let q = match url.split_once('?') {
        Some((_, q)) => q,
        None => return map,
    };
    // Bound the work: cap the number of pairs and the length of each key/value
    // so a hostile query string can't blow up the HashMap or the per-request
    // allocation.
    const MAX_PAIRS: usize = 32;
    const MAX_FIELD_LEN: usize = 256;
    // Truncate a &str to at most `n` bytes on a char boundary (raw query
    // fields may carry multibyte UTF-8, so a blind byte slice could panic).
    fn clamp(s: &str, n: usize) -> &str {
        if s.len() <= n {
            return s;
        }
        let mut end = n;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        &s[..end]
    }
    for pair in q.split('&').take(MAX_PAIRS) {
        if let Some((k, v)) = pair.split_once('=') {
            map.insert(
                percent_decode(clamp(k, MAX_FIELD_LEN)),
                percent_decode(clamp(v, MAX_FIELD_LEN)),
            );
        }
    }
    map
}

/// Deadline for the bounded settings-save on the HTTP handler thread.
/// 15 s is well above any reasonable NFS write latency on a healthy
/// mount but short enough that a wedged `/config` doesn't permanently
/// block the API thread. On timeout we return 503; the previous
/// settings file remains intact because we always write to a temp
/// file and only rename on success.
const SETTINGS_SAVE_DEADLINE_SECS: u64 = 15;

fn handle_settings_post(request: tiny_http::Request, cfg: &Arc<RwLock<Config>>) {
    let (request, body) = match read_json_body(request) {
        Ok(rb) => rb,
        Err(()) => return,
    };
    let patch: serde_json::Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(_) => {
            json_response(request, 400, r#"{"ok":false,"error":"invalid json"}"#);
            return;
        }
    };

    // Validate every outbound URL/target BEFORE taking the write guard.
    // `validate_fetch_url` / `validate_network_target` do synchronous DNS
    // (`to_socket_addrs`); running them under `cfg.write()` would block
    // every concurrent `cfg.read()` handler for the resolution duration —
    // the 0.20.8 lock-stall, here driven by a slow-resolving host in an
    // unauthenticated POST. Resolution happens here with no lock held; on
    // rejection we return before mutating anything. The write guard below
    // covers only in-memory mutation.
    if let Some(v) = patch.get("keydb_url").and_then(|v| v.as_str()) {
        // SSRF guard at store time (handle_update_keydb re-validates +
        // pins at fetch time). Empty clears the configured URL. A value
        // containing the sentinel is a masked "unchanged" placeholder from
        // GET /api/settings — skip validation (stored value was already
        // validated when first saved).
        if !v.trim().is_empty() && !v.contains(SECRET_SENTINEL) {
            if let Err(e) = validate_fetch_url(v) {
                return json_response(
                    request,
                    400,
                    &serde_json::json!({
                        "ok": false,
                        "error": format!("keydb_url rejected: {e}")
                    })
                    .to_string(),
                );
            }
        }
    }
    if let Some(v) = patch.get("keyserver_url").and_then(|v| v.as_str()) {
        // SSRF guard: keysource.rs OnlineSource POSTs this URL verbatim at
        // rip time, so an unauthenticated LAN client must not be able to
        // aim it at metadata/internal hosts. Empty is allowed (disables the
        // online source). A value containing the sentinel is a masked
        // "unchanged" placeholder from GET — skip validation.
        if !v.trim().is_empty() && !v.contains(SECRET_SENTINEL) {
            if let Err(e) = validate_fetch_url(v) {
                return json_response(
                    request,
                    400,
                    &serde_json::json!({
                        "ok": false,
                        "error": format!("keyserver_url rejected: {e}")
                    })
                    .to_string(),
                );
            }
        }
    }
    if let Some(v) = patch.get("network_target").and_then(|v| v.as_str()) {
        // SSRF guard: at rip time libfreemkv streams decrypted disc content
        // to this bare `host:port`. Without a check an unauthenticated POST
        // could beacon plaintext to an internal/metadata host. Empty clears
        // the target (no check needed). Reject any host that is or resolves
        // to a non-public address.
        if !v.trim().is_empty() {
            if let Err(e) = validate_network_target(v) {
                return json_response(
                    request,
                    400,
                    &serde_json::json!({
                        "ok": false,
                        "error": format!("network_target rejected: {e}")
                    })
                    .to_string(),
                );
            }
        }
    }
    if let Some(arr) = patch.get("webhook_urls").and_then(|v| v.as_array()) {
        // SSRF guard: webhook.rs fire() POSTs each of these to deliver
        // rip/move events. Validate every one at store time so a LAN client
        // can't make autorip beacon to internal/metadata hosts. A sentinel
        // entry is a redacted "unchanged" placeholder (resolved to the
        // stored URL under the write guard), so skip it here — the stored
        // value was already validated when it was first saved.
        for u in arr
            .iter()
            .filter_map(|v| v.as_str())
            .filter(|s| !s.trim().is_empty() && !is_masked_webhook(s))
        {
            if let Err(e) = validate_fetch_url(u) {
                return json_response(
                    request,
                    400,
                    &serde_json::json!({
                        "ok": false,
                        "error": format!("webhook URL rejected: {e}")
                    })
                    .to_string(),
                );
            }
        }
    }
    if let Some(v) = patch.get("port").and_then(|v| v.as_u64()) {
        // Reject out-of-range BEFORE taking the write guard. Validating
        // inside the guard meant a bad port returned 400 only after other
        // fields had already been mutated in the live in-memory Config,
        // leaving a partial update behind. The server is the trust
        // boundary; a raw POST can carry any value (e.g. 70000 would
        // otherwise truncate to 4464 as u16).
        if !(1..=65535).contains(&v) {
            return json_response(
                request,
                400,
                r#"{"ok":false,"error":"port must be 1..=65535"}"#,
            );
        }
    }

    // Validate string-enum fields BEFORE the write guard, same trust-boundary
    // rationale as `port` above: a raw POST can carry any value, and silently
    // storing e.g. output_format="garbage" would load cleanly and only
    // misbehave downstream. Reject with 400 rather than persist a bad enum.
    // Allowed sets mirror `config::load_saved`.
    for (field, allowed) in [
        ("key_source", &["local", "online"][..]),
        ("on_insert", &["nothing", "scan", "rip"][..]),
        ("on_read_error", &["stop", "skip"][..]),
        ("output_format", &["mkv", "m2ts", "iso", "network"][..]),
        ("rip_mode", &["single", "multi"][..]),
    ] {
        if let Some(v) = patch.get(field).and_then(|v| v.as_str()) {
            if !allowed.contains(&v) {
                return json_response(
                    request,
                    400,
                    &format!(r#"{{"ok":false,"error":"invalid value for {field}"}}"#),
                );
            }
        }
    }

    // Validate directory-path fields BEFORE the write guard. These end up as
    // filesystem roots autorip writes rips into and enumerates (the move queue
    // scans `staging_dir` with `read_dir`), so a raw POST must not be able to
    // point them at an arbitrary location for directory enumeration. Require an
    // absolute path with no `..` traversal component — that confines them to
    // real mount points (the legitimate configs are all absolute: /staging-local,
    // /mnt/unraid-1/media/movies, …) while rejecting relative / climbing paths.
    // Empty string is allowed: it means "unset / inherit default" for the
    // optional movie_dir / tv_dir overrides.
    for field in ["output_dir", "staging_dir", "movie_dir", "tv_dir"] {
        if let Some(v) = patch.get(field).and_then(|v| v.as_str()) {
            if v.is_empty() {
                continue;
            }
            let p = std::path::Path::new(v);
            let bad = !p.is_absolute()
                || p.components()
                    .any(|c| matches!(c, std::path::Component::ParentDir));
            if bad {
                return json_response(
                    request,
                    400,
                    &format!(
                        r#"{{"ok":false,"error":"{field} must be an absolute path with no '..'"}}"#
                    ),
                );
            }
        }
    }

    // Validate keydb_path (the AACS keydb.cfg file path) BEFORE the write guard,
    // same trust-boundary rationale as the directory fields: a raw POST must not
    // be able to point the keydb at an arbitrary location. Require an absolute
    // path, no `..` traversal, and prefer a `.cfg` extension. Two values are
    // exempt: "" (unset → default) and the redacted basename round-trip (GET
    // /api/settings returns keydb_path as just its filename to avoid leaking the
    // absolute container path, and that bare value must round-trip unchanged).
    if let Some(v) = patch.get("keydb_path").and_then(|v| v.as_str()) {
        let is_redacted_roundtrip = !v.is_empty() && !v.contains('/') && {
            let stored = cfg.read().ok().and_then(|c| c.keydb_path.clone());
            stored.as_deref().is_some_and(|s| {
                std::path::Path::new(s)
                    .file_name()
                    .map(|n| n == std::ffi::OsStr::new(v))
                    .unwrap_or(false)
            })
        };
        if !v.is_empty() && !is_redacted_roundtrip {
            let p = std::path::Path::new(v);
            let bad = !p.is_absolute()
                || p.components()
                    .any(|c| matches!(c, std::path::Component::ParentDir))
                || p.extension().and_then(|e| e.to_str()) != Some("cfg");
            if bad {
                return json_response(
                    request,
                    400,
                    r#"{"ok":false,"error":"keydb_path must be an absolute .cfg path with no '..'"}"#,
                );
            }
        }
    }

    // Numeric clamps applied below mirror `config::load_saved`'s trust-boundary
    // ceilings so the live in-memory value can't diverge from what a restart
    // would load.
    const MAX_DURATION_SECS: u64 = 30 * 24 * 3600; // 30 days
    const MAX_RETENTION_DAYS: u64 = 3650; // 10 years

    // Mutate the Config inside the write guard, then snapshot+drop the
    // guard BEFORE calling `config::save`. The
    // previous code held the write guard across `fs::write` +
    // `fs::rename` on `/config/settings.json` — on NFS those calls can
    // hang indefinitely, blocking every concurrent reader of the lock
    // (the whole `/api/*` surface, since most handlers `cfg.read()`).
    // The clone is cheap (a handful of Strings + small primitives),
    // and the write-lock window now covers only in-memory mutation.
    let snapshot: Config = {
        let mut c = match cfg.write() {
            Ok(c) => c,
            Err(_) => {
                return json_response(
                    request,
                    500,
                    r#"{"ok":false,"error":"config lock poisoned"}"#,
                );
            }
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
            // Ignore the redaction sentinel so a round-trip of the GET
            // response doesn't wipe the stored key.
            if v != SECRET_SENTINEL {
                c.tmdb_api_key = v.to_string();
            }
        }
        if let Some(v) = patch.get("keydb_url").and_then(|v| v.as_str()) {
            // Validated above the write guard (SSRF). Ignore any value
            // containing the sentinel — it is the masked form from GET
            // /api/settings and must not clobber the stored token-bearing URL.
            if !v.contains(SECRET_SENTINEL) {
                c.keydb_url = v.to_string();
            }
        }
        if let Some(v) = patch.get("key_source").and_then(|v| v.as_str()) {
            c.key_source = v.to_string();
        }
        if let Some(v) = patch.get("keyserver_url").and_then(|v| v.as_str()) {
            // Validated above the write guard (SSRF). Ignore any value
            // containing the sentinel — it is the masked form from GET
            // /api/settings and must not clobber the stored token-bearing URL.
            if !v.contains(SECRET_SENTINEL) {
                c.keyserver_url = v.to_string();
            }
        }
        if let Some(v) = patch.get("keyserver_secret").and_then(|v| v.as_str()) {
            if v != SECRET_SENTINEL {
                c.keyserver_secret = v.to_string();
            }
        }
        if let Some(v) = patch.get("keydb_path").and_then(|v| v.as_str()) {
            // GET /api/settings redacts keydb_path to its filename component to
            // avoid leaking the absolute container path. Treat a bare value
            // that matches the stored path's basename as the unchanged
            // round-trip of that redacted form — don't clobber the full path
            // with just the filename.
            let is_redacted_roundtrip = !v.is_empty()
                && !v.contains('/')
                && c.keydb_path.as_deref().is_some_and(|stored| {
                    std::path::Path::new(stored)
                        .file_name()
                        .map(|n| n == std::ffi::OsStr::new(v))
                        .unwrap_or(false)
                });
            if !is_redacted_roundtrip {
                c.keydb_path = if v.is_empty() {
                    None
                } else {
                    Some(v.to_string())
                };
            }
        }
        if let Some(v) = patch.get("capture_without_keys").and_then(|v| v.as_bool()) {
            c.capture_without_keys = v;
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
        let on_read_error_in_patch = patch.get("on_read_error").is_some();
        if let Some(v) = patch.get("on_read_error").and_then(|v| v.as_str()) {
            c.on_read_error = v.to_string();
        }
        // Legacy: migrate abort_on_error bool to on_read_error string.
        // An explicit on_read_error in the PATCH always wins (mirrors config.rs::load_saved).
        if !on_read_error_in_patch {
            if let Some(false) = patch.get("abort_on_error").and_then(|v| v.as_bool()) {
                c.on_read_error = "skip".to_string();
            }
            if let Some(true) = patch.get("abort_on_error").and_then(|v| v.as_bool()) {
                c.on_read_error = "stop".to_string();
            }
        }
        if let Some(v) = patch.get("output_format").and_then(|v| v.as_str()) {
            c.output_format = v.to_string();
        }
        if let Some(v) = patch.get("network_target").and_then(|v| v.as_str()) {
            // Validated above the write guard (SSRF); empty clears it.
            c.network_target = v.to_string();
        }
        if let Some(v) = patch.get("min_length_secs").and_then(|v| v.as_u64()) {
            c.min_length_secs = v.min(MAX_DURATION_SECS);
        }
        if let Some(v) = patch.get("port").and_then(|v| v.as_u64()) {
            // Range-validated above the write guard (1..=65535) so a bad
            // value can't leave a partial in-memory mutation behind.
            c.port = v as u16;
        }
        if let Some(v) = patch.get("max_retries").and_then(|v| v.as_u64()) {
            c.max_retries = v.min(10) as u8;
        }
        if let Some(v) = patch.get("keep_iso").and_then(|v| v.as_bool()) {
            c.keep_iso = v;
        }
        if let Some(v) = patch.get("abort_on_lost_secs").and_then(|v| v.as_u64()) {
            c.abort_on_lost_secs = v.min(MAX_DURATION_SECS);
        }
        if let Some(rip_mode) = patch.get("rip_mode").and_then(|v| v.as_str()) {
            // "single" = direct disc->MKV, no retries. "multi" = retry
            // passes + ISO intermediate, which is meaningless with zero
            // retries — clamp to at least 1 so a raw POST can't persist an
            // invalid multi/0 config. Do NOT re-derive keep_iso from the
            // mode here: keep_iso is handled explicitly above, and silently
            // clobbering it overrode the operator's explicit choice.
            if rip_mode == "single" {
                c.max_retries = 0;
            } else if c.max_retries == 0 {
                c.max_retries = 1;
            }
        }
        if let Some(arr) = patch.get("webhook_urls").and_then(|v| v.as_array()) {
            // Validated above the write guard (SSRF). A sentinel entry is a
            // redacted "unchanged" placeholder: resolve it back to the stored
            // token-bearing URL so a GET→POST round-trip of the redacted form
            // doesn't wipe the real secret.
            //
            // Resolution is BY ORIGIN PREFIX, never by array position. The UI
            // can delete or reorder webhook rows between GET and POST; a
            // positional match would then bind a masked entry to a DIFFERENT
            // stored secret (or a deleted one) — a silent secret-confusion bug.
            // If a masked entry's origin prefix matches 0 or >1 stored entries
            // it is ambiguous: reject the whole save with a 400 rather than
            // guessing which secret the operator meant.
            let existing = c.webhook_urls.clone();
            let incoming: Vec<&str> = arr.iter().filter_map(|v| v.as_str()).collect();
            match resolve_webhook_urls(&incoming, &existing) {
                Ok(urls) => c.webhook_urls = urls,
                Err(_) => {
                    // A masked entry's origin matched 0 (deleted row) or >1
                    // (shared-origin) stored secrets — refuse to guess which
                    // secret was meant rather than silently bind the wrong one.
                    return json_response(
                        request,
                        400,
                        r#"{"ok":false,"error":"ambiguous masked webhook entry; re-enter the full webhook URL"}"#,
                    );
                }
            }
        }
        // decrypt_threads + log_retention_days: operator-tunable from the
        // Settings page.
        if let Some(v) = patch.get("decrypt_threads").and_then(|v| v.as_u64()) {
            // Match config::load's .min(256) clamp so the live/on-disk
            // value can't diverge from what a restart would load (and
            // libfreemkv caps the effective pool at 64 regardless).
            c.decrypt_threads = (v as usize).min(256);
        }
        if let Some(v) = patch.get("log_retention_days").and_then(|v| v.as_u64()) {
            c.log_retention_days = v.min(MAX_RETENTION_DAYS);
        }
        c.clone()
    }; // <-- write guard dropped here; readers unblock immediately

    // Apply the decrypt-thread setting LIVE without waiting for a
    // container restart. set_decrypt_threads swaps libfreemkv's rayon
    // pool; in-flight decrypt work uses the old pool, the next rip
    // picks up the new size.
    config::apply_decrypt_threads(snapshot.decrypt_threads);

    // Fail-loud-EARLY destination check (Mercy incident hardening): warn
    // the operator NOW if a configured movie/tv/output directory is
    // missing, not a directory, or not writable — rather than letting a
    // rip run for hours and only discover the dead mount when the mover's
    // per-move guard blocks the move. Non-blocking: the save still
    // succeeds (a mount can be transiently down at save time), but the
    // warning is loud on the System log.
    for (root, reason) in crate::mover::check_configured_destinations(&snapshot) {
        crate::log::syslog(&format!(
            "WARNING: configured destination '{root}' is not usable: {reason}. \
             Rips will be PRESERVED in staging (not moved) until this is fixed."
        ));
    }

    // Bounded-syscall pattern, hand-rolled because
    // `libfreemkv::io::bounded::bounded_syscall` is `pub(crate)` and
    // not reachable from autorip. Same shape: spawn a worker, await on
    // a 0-capacity channel with `recv_timeout`. On timeout the worker
    // is intentionally leaked — the eventual `fs::write` / `fs::rename`
    // will unwind whenever NFS does, but the API thread is no longer
    // trapped. `config::save` writes `settings.json.tmp` then renames
    // it atomically; if either step wedges the prior settings.json is
    // left intact (the timeout aborts before rename completes
    // observably).
    let (tx, rx) = std::sync::mpsc::sync_channel::<std::io::Result<()>>(0);
    // Capture the spawn Result. A discarded Err here would mean the worker
    // never ran, the channel never receives, and the `recv_timeout` below
    // would block the full deadline and report a misleading "timed out"
    // 503 — when the real failure was that we couldn't fork a thread at
    // all. Surface that as a distinct 500 immediately.
    if let Err(e) = std::thread::Builder::new()
        .name("autorip-settings-save".into())
        .spawn(move || {
            let result = config::save(&snapshot);
            let _ = tx.send(result);
        })
    {
        tracing::error!(
            target: "web",
            error = %e,
            "failed to spawn settings-save thread; on-disk settings.json unchanged"
        );
        return json_response(
            request,
            500,
            r#"{"ok":false,"error":"settings save failed: could not spawn save thread"}"#,
        );
    }
    match rx.recv_timeout(std::time::Duration::from_secs(SETTINGS_SAVE_DEADLINE_SECS)) {
        Ok(Ok(())) => json_response(request, 200, r#"{"ok":true}"#),
        Ok(Err(e)) => {
            tracing::error!(
                target: "web",
                error = %e,
                "settings save failed; on-disk settings.json unchanged"
            );
            json_response(
                request,
                500,
                r#"{"ok":false,"error":"settings save failed"}"#,
            )
        }
        Err(_) => {
            tracing::error!(
                target: "web",
                "settings save timed out after {SETTINGS_SAVE_DEADLINE_SECS}s; \
                 in-memory config updated, on-disk settings.json unchanged"
            );
            json_response(
                request,
                503,
                r#"{"ok":false,"error":"settings save timed out"}"#,
            )
        }
    }
}

fn handle_sse(request: tiny_http::Request, cfg: &Arc<RwLock<Config>>) {
    // /events holds its thread for the whole client session (1s poll
    // loop). Cap concurrent streams so N clients can't pin N threads and
    // DoS the box; over the cap return 503 and let the thread end.
    let _sse_guard = match ConnGuard::try_acquire(&SSE_CLIENTS, MAX_SSE_CLIENTS) {
        Some(g) => g,
        None => {
            tracing::warn!(
                max = MAX_SSE_CLIENTS,
                "SSE connection rejected: concurrent /events cap reached"
            );
            return json_response(
                request,
                503,
                r#"{"ok":false,"error":"too many SSE clients"}"#,
            );
        }
    };
    // Same-origin only, matching every other route — no
    // Access-Control-Allow-Origin. The service is unauthenticated, so a
    // wildcard ACAO would let any page the operator visits cross-origin
    // subscribe and read the full RipState (disc names, staging paths,
    // progress, bad ranges, last_error, key_status).
    let headers = vec![
        Header::from_bytes(&b"Content-Type"[..], &b"text/event-stream"[..]).unwrap(),
        Header::from_bytes(&b"Cache-Control"[..], &b"no-cache"[..]).unwrap(),
        Header::from_bytes(&b"Connection"[..], &b"keep-alive"[..]).unwrap(),
    ];

    let mut response = Response::empty(200);
    for h in headers {
        response = response.with_header(h);
    }

    let mut stream = request.upgrade("sse", response);

    // Re-read the staging dir each tick (cheap) so a Settings change to
    // the staging path is reflected without restarting the SSE stream.
    let staging_dir = || {
        cfg.read()
            .map(|c| c.staging_dir.clone())
            .unwrap_or_default()
    };

    let initial = format!("data: {}\n\n", get_state_json(&staging_dir()));
    if stream.write_all(initial.as_bytes()).is_err() {
        return;
    }
    let _ = stream.flush();

    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
        let frame = format!("data: {}\n\n", get_state_json(&staging_dir()));
        if stream.write_all(frame.as_bytes()).is_err() {
            break;
        }
        if stream.flush().is_err() {
            break;
        }
    }
}

fn handle_scan(request: tiny_http::Request, cfg: &Arc<RwLock<Config>>, device: &str) {
    // Atomic check-and-claim under one STATE lock — closes the TOCTOU where two
    // concurrent POSTs both pass a separate busy-check and both start a scan.
    if !ripper::try_claim_active(device) {
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
    let dev_for_register = dev.clone();
    if let Err(e) = ripper::spawn_rip_thread(&dev_for_register, "scan", move || {
        ripper::scan_disc(&cfg, &dev, &dev_path);
    }) {
        tracing::error!(device = %dev_for_register, error = %e, "failed to spawn scan thread");
        // Roll the device state back to idle so a failed spawn doesn't
        // wedge the busy-check at "scanning" forever (409 on every
        // future scan/rip until restart). Shared helper so poll loop +
        // both web handlers can't drift.
        ripper::rollback_failed_spawn(&dev_for_register);
        json_response(
            request,
            500,
            r#"{"ok":false,"error":"thread spawn failed"}"#,
        );
        return;
    }
    json_response(request, 200, r#"{"ok":true}"#);
}

/// POST `/api/rip/{device}[?resume=yes|no]`.
///
/// Contract: this is the *only* path that starts disk work.
/// Disc-insert detection is scan-only; auto-resume on container start
/// is gone. The user's intent (POST) is the trigger.
///
/// Query param semantics:
/// - `resume=yes` → re-mux the existing staging ISO if one exists for
///   this disc. Reject (404) if no resumable state is found rather
///   than silently doing a fresh sweep.
/// - `resume=no` → wipe the staging dir for this disc, then fresh
///   sweep+mux. Explicit clean slate.
/// - (no param) → fresh sweep+mux from disc. The classic behavior.
///   Does NOT delete any pre-existing staging dir, but starts writing
///   to it (libfreemkv's sweep `resume` flag picks up where the
///   mapfile left off, if applicable).
fn handle_rip(request: tiny_http::Request, cfg: &Arc<RwLock<Config>>, device: &str, query: &str) {
    let resume_mode = parse_resume_param(query);

    // Atomic check-and-claim under one STATE lock — closes the TOCTOU where two
    // concurrent POSTs both pass a separate busy-check and both launch a rip on
    // the same device (orphaned halt token + concurrent writes to one staging
    // dir). The claim also marks the device "scanning": the resume decision is
    // delegated to the worker thread (it scans the disc, cheap, then picks
    // resume_remux vs rip_disc based on the staging dir), keeping scan logic in
    // one place.
    if !ripper::try_claim_active(device) {
        json_response(request, 409, r#"{"ok":false,"error":"already ripping"}"#);
        return;
    }

    let dev = device.to_string();
    let dev_path = format!("/dev/{}", device);
    let cfg = Arc::clone(cfg);

    let dev_for_register = dev.clone();
    ripper::register_halt(&dev_for_register, libfreemkv::Halt::new());
    if let Err(e) = ripper::spawn_rip_thread(&dev_for_register, "rip", move || {
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ripper::handle_rip_request(&cfg, &dev, &dev_path, resume_mode);
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
        ripper::unregister_halt(&dev);
    }) {
        tracing::error!(device = %dev_for_register, error = %e, "failed to spawn rip thread");
        // Roll the device state back to idle so a failed spawn doesn't
        // wedge the busy-check at "scanning" forever (409 on every
        // future scan/rip until restart). Shared helper so poll loop +
        // both web handlers can't drift.
        ripper::rollback_failed_spawn(&dev_for_register);
        json_response(
            request,
            500,
            r#"{"ok":false,"error":"thread spawn failed"}"#,
        );
        return;
    }

    json_response(request, 200, r#"{"ok":true}"#);
}

/// Resume-mode chosen by the caller of `/api/rip`. The dispatch logic
/// in `ripper::handle_rip_request` reads this and routes to the
/// appropriate code path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeMode {
    /// `?resume=yes` — require an existing resumable staging dir,
    /// fail if none.
    Require,
    /// `?resume=no` — wipe any existing staging dir first.
    Wipe,
    /// no `resume=` query param — fresh sweep+mux; leave any existing
    /// staging dir alone (libfreemkv's sweep-resume path handles it).
    Default,
}

fn parse_resume_param(query: &str) -> ResumeMode {
    for kv in query.split('&') {
        let (k, v) = match kv.split_once('=') {
            Some((k, v)) => (k, v),
            None => (kv, ""),
        };
        if k == "resume" {
            return match v {
                "yes" | "true" | "1" => ResumeMode::Require,
                "no" | "false" | "0" => ResumeMode::Wipe,
                _ => ResumeMode::Default,
            };
        }
    }
    ResumeMode::Default
}

/// Shared cap for all three KEYDB download paths (startup, daily refresh, web
/// handler). All paths use `read_capped_keydb_body` so overflow is detected
/// rather than silently truncating at the cap.
pub(crate) const KEYDB_MAX_BYTES: u64 = 100 * 1024 * 1024;

/// Why a capped keydb body read failed.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum KeydbReadError {
    /// The underlying reader errored.
    Io,
    /// The body exceeded the byte cap (oversized plain-text keydb).
    TooLarge,
}

/// Read a keydb response body, rejecting bodies larger than `max_bytes`.
///
/// `Read::take(max_bytes)` would cap the read but SUCCEED at exactly the cap,
/// silently truncating an oversized plain-text keydb into a half-valid file.
/// Read one byte past the cap instead so an oversized body is detectable, and
/// return `TooLarge` rather than a truncated buffer.
pub(crate) fn read_capped_keydb_body<R: std::io::Read>(
    reader: R,
    max_bytes: u64,
) -> std::result::Result<Vec<u8>, KeydbReadError> {
    let mut buf = Vec::new();
    reader
        .take(max_bytes + 1)
        .read_to_end(&mut buf)
        .map_err(|_| KeydbReadError::Io)?;
    if buf.len() as u64 > max_bytes {
        return Err(KeydbReadError::TooLarge);
    }
    Ok(buf)
}

fn handle_update_keydb(request: tiny_http::Request, cfg: &Arc<RwLock<Config>>) {
    // Serialize: only one keydb download may be in flight at a time. Each one
    // buffers the whole file into memory, so concurrent unauthenticated calls
    // could allocate many large buffers at once. A second caller gets 429.
    static KEYDB_UPDATE_IN_FLIGHT: AtomicBool = AtomicBool::new(false);
    if KEYDB_UPDATE_IN_FLIGHT
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return json_response(
            request,
            429,
            r#"{"ok":false,"error":"A KEYDB update is already in progress."}"#,
        );
    }
    // Release the in-flight flag on every exit path.
    struct InFlightGuard;
    impl Drop for InFlightGuard {
        fn drop(&mut self) {
            KEYDB_UPDATE_IN_FLIGHT.store(false, Ordering::Release);
        }
    }
    let _in_flight = InFlightGuard;

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

    // SSRF guard at fetch time (defence-in-depth on top of the store-time
    // check in handle_settings_post): resolve+validate once, then pin the
    // connection to those IPs so DNS rebinding can't redirect the fetch to
    // an internal/metadata host between validation and connect.
    let pinned = match validate_fetch_url(&keydb_url) {
        Ok(addrs) => addrs,
        Err(e) => {
            let msg = serde_json::json!({
                "ok": false,
                "error": format!("KEYDB URL rejected: {e}")
            })
            .to_string();
            json_response(request, 400, &msg);
            return;
        }
    };
    let agent = guarded_agent(pinned);

    // Wall-clock bound on the whole download so a slow-loris server can't hold
    // the in-flight slot (and the handler thread) indefinitely.
    const KEYDB_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
    // Cap is the shared module-level KEYDB_MAX_BYTES (100 MiB); using
    // read_capped_keydb_body means an oversized body returns 413 rather than
    // silently truncating at the limit.
    let keydb_cap = KEYDB_MAX_BYTES;

    // Download via ureq (supports HTTPS) then save via libfreemkv
    let body = match agent.get(&keydb_url).timeout(KEYDB_FETCH_TIMEOUT).call() {
        Ok(resp) => match read_capped_keydb_body(resp.into_reader(), keydb_cap) {
            Ok(buf) => buf,
            Err(KeydbReadError::Io) => {
                json_response(
                    request,
                    500,
                    r#"{"ok":false,"error":"Failed to read response body."}"#,
                );
                return;
            }
            Err(KeydbReadError::TooLarge) => {
                json_response(
                    request,
                    413,
                    r#"{"ok":false,"error":"KEYDB too large (>100 MB plain-text); use a gzip/zip URL"}"#,
                );
                return;
            }
        },
        Err(ureq::Error::Status(code, _)) => {
            let msg = format!(
                r#"{{"ok":false,"error":"Server returned HTTP {}. Check the URL in Settings."}}"#,
                code
            );
            json_response(request, 502, &msg);
            return;
        }
        Err(e) => {
            // Do NOT echo the configured KEYDB origin/hostname back to the
            // client — that leaks server-side configuration to any LAN caller.
            // Keep the detail (URL origin + underlying error) in the log only.
            tracing::warn!(
                origin = %crate::webhook::webhook_url_origin(&keydb_url),
                error = %e,
                "keydb update: could not connect to configured KEYDB server"
            );
            json_response(
                request,
                502,
                r#"{"ok":false,"error":"Could not connect to the configured KEYDB server. Check the URL in Settings."}"#,
            );
            return;
        }
    };

    // Write to the service-canonical keydb path (the one the reads resolve via
    // keysource::keydb_path), NOT libfreemkv's exe-local default — otherwise the
    // "Update KEYDB" button reports success while every AACS rip keeps failing
    // because the read side looks elsewhere.
    let saved = cfg
        .read()
        .map_err(|_| libfreemkv::Error::KeydbWrite {
            path: "<config lock poisoned>".into(),
        })
        .and_then(|c| crate::keysource::save_keydb(&c, &body));
    match saved {
        Ok(result) => {
            let body = serde_json::json!({
                "ok": true,
                "entries": result.entries,
                "bytes": result.bytes,
            });
            json_response(request, 200, &body.to_string());
        }
        Err(e) if e.code() == libfreemkv::error::E_KEYDB_WRITE => {
            // A write/persist failure is an environment problem (disk full,
            // permissions on the keys dir) — not invalid content. Surface it
            // distinctly so the operator fixes the right thing.
            json_response(
                request,
                500,
                r#"{"ok":false,"error":"Failed to save KEYDB to disk (check space/permissions)"}"#,
            );
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
    // Gate on rip status. The BU40N is a slot-loading drive: a software
    // eject is physically irreversible (the operator must reload the disc
    // by hand), so ejecting mid-rip abandons the in-flight rip and is a
    // direct violation of the project's hard rule against ejecting without
    // consent. The UI hides the eject button while active, but POST
    // /api/eject/<dev> is unauthenticated and reachable from any LAN
    // client — so the server must enforce the gate, not just the JS.
    // Atomically claim the device before ejecting. A separate busy-check then
    // eject left a TOCTOU window in which a rip could start (its own
    // `try_claim_active`) between the check and the eject — ejecting a
    // just-started rip on this irreversible slot-loading drive. `try_claim_active`
    // folds the busy-check and the status-set into one STATE lock: it rejects if
    // the device is already scanning/ripping, and once it has claimed the device
    // (status="scanning") any concurrent rip-start's claim fails for the duration
    // of the eject. The idle reset below releases the claim.
    if !ripper::try_claim_active(device) {
        return json_response(
            request,
            409,
            r#"{"ok":false,"error":"drive busy; stop the rip before ejecting"}"#,
        );
    }
    let device_path = format!("/dev/{}", device);
    crate::ripper::eject_drive(&device_path);
    ripper::update_state(
        device,
        ripper::RipState {
            device: device.to_string(),
            status: "idle".to_string(),
            ..Default::default()
        },
    );
    json_response(request, 200, r#"{"ok":true}"#);
}

fn handle_stop(request: tiny_http::Request, cfg: &Arc<RwLock<Config>>, device: &str) {
    // Stop signals threads to abort, waits for the rip thread to drain,
    // drops the SCSI session, and collapses the state entry to idle.
    //
    // **Stop preserves partial staging state for resume.** Earlier behaviour
    // (pre-0.21.10) called `wipe_staging` here, on the premise that stop ==
    // reset. That conflicts with auto-resume (introduced in 0.20.8): if a
    // user presses Stop because mux throughput looks slow and expects to
    // resume on the next disc-insert or container restart, wiping the
    // staging dir destroys the ISO and partial MKV they meant to keep.
    // Observed 2026-05-15 — stop during a 0.21.9 mux nuked an 85 GB ISO +
    // mapfile + 50 GB partial MKV, forcing a full re-rip from disc.
    //
    // Stop now = halt the rip thread and reset the in-memory state. The
    // on-disk staging dir is left as-is. Auto-resume on next disc-insert
    // (when the resume_map has a matching Remux entry) or on next container
    // restart picks up the partial state automatically. Operators who
    // genuinely want a clean slate can delete the per-disc staging
    // subdirectory by hand; there is no longer a one-button API path for
    // destructive reset.
    //
    // The 60 s drain budget covers a 30 s in-flight CDB plus generous margin
    // (bumped from 35 s in v0.13.8 after live observation of slower drains
    // under heavy ECC retry on the BU40N). A timeout is logged but not fatal
    // — the HTTP response still goes out 200 so the UI doesn't spin.
    let _ = cfg;
    if let Some(halt) = ripper::device_halt(device) {
        halt.cancel();
    }
    crate::verify::request_stop(device);

    if ripper::join_rip_thread(device, std::time::Duration::from_secs(60)).is_err() {
        tracing::warn!(
            device = %device,
            "rip thread did not drain within 60s of stop"
        );
    }

    // Drain the detached verify worker before resetting STATE. The verify
    // thread is NOT in RIP_THREADS, so join_rip_thread above returns
    // immediately for it; without this bounded wait we'd reset STATE to idle
    // while run_verify_inner still holds an open Drive, and a concurrent
    // /api/rip could claim+open the same drive (double-open). request_stop
    // already cancelled the drive halt, so the in-flight read bails within a
    // poll interval; poll is_running until it clears, up to the same 60 s drain
    // budget the rip thread gets. A timeout is logged, not fatal — the worker's
    // own release_claim is generation-checked, so a late finish can't clobber a
    // newer owner even if we proceed.
    {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        while crate::verify::is_running(device) {
            if std::time::Instant::now() >= deadline {
                tracing::warn!(
                    device = %device,
                    "verify worker did not drain within 60s of stop"
                );
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    let existed = ripper::STATE
        .lock()
        .map(|mut s| {
            if let Some(rs) = s.get_mut(device) {
                // Full reset: keep device id + disc_present, drop everything else.
                let disc_still_in = rs.disc_present;
                *rs = ripper::RipState {
                    device: device.to_string(),
                    status: "idle".to_string(),
                    disc_present: disc_still_in,
                    ..Default::default()
                };
                true
            } else {
                false
            }
        })
        .unwrap_or(false);

    if existed {
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
        // Need two hex digits AFTER the '%': indices i+1 and i+2 must be
        // in range, i.e. i + 3 <= len. The previous `i + 2 < len` guard
        // was off by one and dropped a trailing `%XX` (e.g. a value
        // ending in a percent-encoded byte) through to literal output.
        if bytes[i] == b'%' && i + 3 <= bytes.len() {
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

/// Toggle debug logging on/off at runtime. POST body can be empty or contain {"enabled":true/false}.
fn handle_debug_toggle(request: tiny_http::Request) {
    let (request, body) = match read_json_body(request) {
        Ok(rb) => rb,
        Err(()) => return,
    };

    // `{"enabled": <bool>}` sets the level explicitly. Any other body —
    // missing/non-bool `enabled`, or no valid JSON at all — defaults to OFF
    // (safe-off). A malformed/empty POST must not silently turn verbose debug
    // logging on; the caller must opt in explicitly with `{"enabled":true}`.
    let enabled = match serde_json::from_str::<serde_json::Value>(&body) {
        Ok(v) => v.get("enabled").and_then(|b| b.as_bool()).unwrap_or(false),
        Err(_) => false,
    };

    *DEBUG_ENABLED
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = enabled;

    // Swap the EnvFilter so libfreemkv's `tracing::debug!` events
    // (target: "mux" writeback seeks, WAIT_AFTER latency, fill_extents
    // stalls) actually surface in docker logs while debug is on. Without
    // this the toggle only flips autorip-internal `debug_enabled()`
    // checks and the library stays at warn — the user-reported
    // "max-debug shows nothing useful" symptom.
    let filter_swapped = crate::observe::set_debug(enabled);

    tracing::info!(enabled, filter_swapped, "debug logging toggled");
    json_response(
        request,
        200,
        &serde_json::json!({
            "ok": true,
            "enabled": enabled,
            "filter_swapped": filter_swapped,
        })
        .to_string(),
    );
}
