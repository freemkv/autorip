use crate::config::{self, Config, WebhookEntry};
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
// The freemkv brand favicon, shared verbatim with the marketing site
// (freemkv.org's public/favicon.svg). Served at /favicon.svg so the dashboard
// tab shows the same icon as the website.
const FAVICON_SVG: &str = r##"<svg width="256" height="256" viewBox="0 0 256 256" xmlns="http://www.w3.org/2000/svg">
<g transform="translate(128, 128)">
<circle cx="0" cy="0" r="120" fill="#0D9488" />
<circle cx="0" cy="0" r="95" fill="#0F766E" />
<circle cx="0" cy="0" r="70" fill="#0D9488" />
<circle cx="0" cy="0" r="28" fill="#F0FDFA" />
<circle cx="0" cy="0" r="15" fill="#0D9488" />
<path d="M0,-120 A120,120 0 0,1 103.9,60 L77.9,45 A90,90 0 0,0 0,-90 Z" fill="#14B8A6" opacity="0.6"/>
<path d="M-15,-10 L-2,-10 L-2,10 L-15,10 Z" fill="#F0FDFA" opacity="0.9" transform="translate(55, 0)"/>
<path d="M0,-9 L16,0 L0,9 Z" fill="#F0FDFA" opacity="0.9" transform="translate(58, 0)"/>
</g>
</svg>
"##;

const DASHBOARD_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="icon" href="/favicon.svg" type="image/svg+xml">
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
.brand { font-size:1.1rem; color:var(--text3); font-weight:400; letter-spacing:3px; text-transform:uppercase; }
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
/* ── Mobile / narrow viewports ─────────────────────────────────────────
   Additive: these rules only take effect below the breakpoints, so the
   desktop layout above is untouched. Keep the Now-Playing card a COMPACT
   horizontal row (small poster + info) rather than a full-width stacked
   poster, tighten the sticky header so the wordmark stops crowding the
   nav, wrap the action buttons, and enlarge tap targets for touch. */
@media(max-width:600px){
  .c{padding:10px 10px 20px}
  .headerbar{padding:10px 12px;gap:6px 10px}
  .brand{font-size:.95rem;letter-spacing:2px}
  .nav{font-size:.9rem;padding:7px 0}
  .card{padding:13px;margin-bottom:12px}
  .np{gap:14px;padding:14px;min-height:0}
  .poster,.ph{width:84px;height:126px;min-height:0;max-height:none}
  .ph svg{width:28px;height:28px}
  .mt{font-size:1.2rem}
  .mo{-webkit-line-clamp:4}
  .actions{flex-wrap:wrap}
  .btn{padding:7px 12px}
  .log{font-size:.72rem;padding:10px}
  table{font-size:.75rem}
  th,td{padding:6px 8px}
  .dtab{padding:6px 12px}
}
@media(max-width:400px){
  .brand{display:none}
  .mt{font-size:1.12rem}
  .poster,.ph{width:72px;height:108px}
}
</style>
</head>
<body>
<div class="c">
<div class="headerbar">
  <span class="brand">AUTORIP</span>
  <button class="nav active" data-tab="ripper">Ripper</button>
  <button class="nav" data-tab="system">System</button>
  <button class="nav" data-tab="settings">Settings</button>
  <button class="btn" style="margin-left:auto" onclick="toggleTheme()" id="thm"></button>
</div>

<!-- Ripper page -->
<div id="ripper" class="section active">
  <div id="dtabs"></div>
  <div id="muxbanner"></div>
  <div id="np"></div>
  <div id="actions"></div>
  <div id="steps" style="margin-bottom:16px"></div>
  <div id="err"></div>
  <details style="margin-top:16px"><summary style="font-size:.7rem;color:var(--text3);text-transform:uppercase;font-weight:600;letter-spacing:1px;cursor:pointer;user-select:none">Log</summary>
  <div id="log" class="log" style="flex:1;max-height:none;margin-top:8px"></div></details>
  <details id="debugBox" open style="margin-top:12px;display:none"><summary style="font-size:.7rem;color:var(--accent);text-transform:uppercase;font-weight:600;letter-spacing:1px;cursor:pointer;user-select:none">Debug Log (live) — the patch walk + timings</summary>
  <div id="debuglog" class="log" style="flex:1;max-height:none;margin-top:8px"></div></details>
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
/* Every device action button goes through this, and none of them may be
   fire-and-forget. The drive-card buttons used to call fetch() bare, with no
   .then and no .catch: the server's answer was DISCARDED. Two of them are
   rendered exactly in the window where the server now answers 409 — Eject
   renders on discIn && !active, and "Accept & deliver" renders on
   lossAborted && !active — and both of those windows can overlap a worker that
   is still unwinding, which the claim refuses. So the operator clicked Eject
   and the disc stayed in the drive; or clicked "Accept & deliver", watched the
   button grey out (it set this.disabled=true first, which READS as success),
   and no `.accept-loss` marker was ever written. A 409 rendered as a success.
   Report the failure, and put the button back the way it was. */
function apiPost(u,btn,label){
  if(btn)btn.disabled=true;
  return fetch(u,{method:'POST'}).then(function(r){
    if(r.ok)return null;
    return r.text().then(function(t){
      let msg='';
      try{const j=JSON.parse(t);if(j&&j.error)msg=j.error}catch(e){}
      throw new Error(msg||('HTTP '+r.status));
    });
  }).catch(function(e){
    alert((label||'Request')+' failed: '+e.message);
  }).then(function(){
    /* Re-enable even on success: the poll re-renders the card from server
       state a moment later, and a button left disabled after a failure is
       exactly the "it looked like it worked" bug. */
    if(btn)btn.disabled=false;
  });
}

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
/* ---- Step-by-step progress ---- */
const ACTIVE_STATES=['ripping','scanning','detecting'];
let _lastStatus={};
let _activeTab=null;

function renderBar(s,p){
  /* Two modes, one renderer:
     - Pass 1 (sequential sweep, s.pass<=1): a genuine left-to-right progress
       fill. x-axis = work done. Green grows to the read head. Only damage the
       sweep has ALREADY passed over shows red; the unread region ahead of the
       head stays blank (NonTried is unknown, not bad — see the overlay clip
       below).
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
  /* Same height in EVERY pass (pass 1 sweep + pass N defrag map) so the bar
     doesn't resize between phases. 20px reads as a disc map, not a hairline. */
  const barH=20;
  /* "You are here" caret ABOVE the bar — consistent in EVERY pass.
     pass 1 (sweep): at last_sector, the real sequential read head.
     pass N (patch): at the active section = the highest-LBA bad range still
     present (autorip patches in reverse, so it steps left as ranges recover).
     Derived from published state — in patch, last_sector is work-done (not a
     disc LBA), so the active-section position is used instead. */
  let caretPct=null;
  if(total>0&&s&&s.status==='ripping'){
    if(positional&&ranges.length){
      /* The handler-chain engine works the LARGEST bad range first
         (largest-first ordering), so the live position sits on the largest
         remaining range, not the highest-LBA one. Track that — otherwise the
         caret parks on a small high-LBA block while recovery is really chewing
         a big block elsewhere. When the largest range shrinks below another,
         the arrow jumps to the new largest. */
      let act=ranges[0];
      ranges.forEach(r=>{ if(r.count>act.count) act=r; });
      /* Align to the red block's RENDERED right edge — same offset + min-width
         0.5% clamp the overlay uses below — so the arrow sits exactly on the
         end of the red even for tiny ranges (where the true lba+count would
         fall short of the clamped-wider block). */
      let offPct=(act.lba*2048)/total*100;
      if(offPct<0)offPct=0; if(offPct>100)offPct=100;
      let wPct=Math.max((act.count*2048)/total*100,0.5);
      if(offPct+wPct>100)wPct=100-offPct;
      caretPct=offPct+wPct;
    }else if(!positional&&s.last_sector>0){
      caretPct=(s.last_sector*2048)/total*100;
    }
  }
  let caret='';
  if(caretPct!=null){
    if(caretPct<0)caretPct=0; if(caretPct>100)caretPct=100;
    caret='<div style="position:relative;height:14px">'
      +'<div style="position:absolute;left:'+caretPct+'%;bottom:0;transform:translateX(-50%);'
      +'font-size:.8rem;color:var(--blue);line-height:1;animation:ph 1.2s infinite">'
      +'▼</div></div>';
  }
  let html='<div style="background:var(--chip);border-radius:4px;height:'+barH+'px;overflow:hidden;position:relative">';
  if(positional){
    /* Positional map: the entire bar is green (the whole disc, good), then
       red bad ranges are punched in at their real offsets. If total is
       unknown (0) we can't place anything positionally — fall back to a
       neutral green fill so we never divide by zero. */
    html+='<div style="position:absolute;left:0;top:0;width:100%;height:100%;background:var(--green)"></div>';
  }else{
    /* Sequential sweep: green grows with the swept position. Use the
       FRACTIONAL position (last_sector advances every 250 ms) rather than the
       integer pass_progress_pct, otherwise the fill jumps a whole 1% at a time
       (~50 s per step on a UHD) and reads as "nothing, boom chunk". */
    let fillPct=(total>0&&s&&s.last_sector>0)?(s.last_sector*2048)/total*100:p;
    if(fillPct<0)fillPct=0; if(fillPct>100)fillPct=100;
    html+='<div style="background:var(--green);height:100%;width:'+fillPct+'%;transition:width 1s"></div>';
  }
  /* Red bad-range overlay. Drawn at each range's real LBA position. min-width
     0.5% keeps single-sector ranges visible on a 72GB UHD; clamp left+width so
     a range near the tail never overflows the bar.

     Pass 1 (sweep): the region AHEAD of the read head is UNREAD — NonTried, not
     bad. We don't know if it's good or bad until it's read, so it stays BLANK
     (same reason the "maybe" bucket shows no red pill: nothing is a verdict
     until it's been read). The live bad_ranges during a sweep is dominated by
     one giant NonTried range covering the whole un-swept tail; painting it red
     makes a pristine disc look 100% damaged. So CLIP the red to the swept
     portion [0, last_sector]: only damage the sweep has actually passed over
     shows red; the unread remainder is neutral track.
     Pass N (positional): every bad_range IS determined-bad, so draw it in full. */
  if(total>0&&ranges.length){
    const sweptPct=positional?100
      :((s&&s.last_sector>0)?(s.last_sector*2048)/total*100:0);
    ranges.forEach(r=>{
      let offPct=(r.lba*2048)/total*100;
      if(offPct<0)offPct=0; if(offPct>100)offPct=100;
      let wPct=Math.max((r.count*2048)/total*100,0.5);
      if(offPct+wPct>100)wPct=100-offPct;
      if(!positional){
        /* Blank until read: skip ranges entirely ahead of the head, trim the
           unread tail off one that straddles it. */
        if(offPct>=sweptPct)return;
        if(offPct+wPct>sweptPct)wPct=sweptPct-offPct;
        if(wPct<=0)return;
      }
      html+='<div style="position:absolute;left:'+offPct+'%;top:0;width:'+wPct+'%;height:100%;background:var(--red);opacity:0.9;transition:left 1s,width 1s"></div>';
    });
  }
  html+='</div>';
  return caret+html;
}
function passLabelFor(s){
   /* Resolve the current pass into a human-readable label for the Ripping
      step. During multipass we show pass number + phase; otherwise "Ripping". */
   if(s.pass>0&&s.total_passes>0){
     /* This is the RIP tab: mux is a 100% separate process and view, so it
        is NOT a rip pass and is excluded from the count. The backend's
        total_passes still includes the trailing mux pass (max_retries + 2);
        subtract it here so the operator sees "pass 2/6" (sweep + 5 retries),
        never "2/7". */
     const ripTotal=Math.max(s.total_passes-1,1); // drop the mux pass
     if(s.pass>=s.total_passes){
       /* Mux phase \u2014 from the rip tab's view, recovery is finished;
          muxing lives in its own view. */
       return 'recovery complete';
     }
     const phase=s.pass===1?'copying':'retrying';
     return 'pass '+s.pass+'/'+ripTotal+' \u00b7 '+phase;
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
      const spdStr=col(speed||'',85,'left');
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
      /* No second "total" bar — mux is a separate view, and a bytes-recovered
         bar just sits at ~99% through the whole patch (the sweep already
         recovered nearly everything; the slow grind is the last <1%). The
         live patch signal is TEXTUAL instead: how many disc sections are
         still bad and how many sectors remain. */
      const fmtBytes = (b)=> b>=1073741824 ? (b/1073741824).toFixed(2)+' GB'
                          : b>=1048576    ? (b/1048576).toFixed(1)+' MB'
                          : b>=1024       ? (b/1024).toFixed(1)+' KB'
                          : b+' B';
      let sectorsLine='';
      {
        /* Same line in EVERY pass (consistency): bytes_maybe is the bad/not-yet-
           good set (NonTrimmed+NonScraped), so this reads as "damage left to
           recover" in both the sweep and the patch passes. The "N sections ·"
           prefix only appears once bad ranges exist. */
        const nSec=(typeof s.num_bad_ranges==='number'&&s.num_bad_ranges>0)
                    ?s.num_bad_ranges:((s.bad_ranges&&s.bad_ranges.length)||0);
        const remBytes=(s.bytes_maybe||0)+(s.bytes_lost||0);
        const remSect=Math.round(remBytes/2048);
        if(remSect>0){
          const secPrefix = nSec>0 ? (nSec+' '+(nSec===1?'section':'sections')+' · ') : '';
          sectorsLine='<div style="font-size:.75rem;color:var(--text2);margin-top:7px;font-variant-numeric:tabular-nums">'
            +secPrefix+remSect.toLocaleString()+' sectors ('+fmtBytes(remBytes)+') remaining</div>';
        }
      }
      let badLine='';
      /* TWO pills, ever \u2014 Good and Maybe. Never a third.
         GOOD  = whole-disc bytes successfully read off the disc (Finished).
         MAYBE = whole-disc bytes not-yet-good: pending, NonTrimmed, and
                 currently-unreadable/undecryptable all folded together.
                 NOTHING is "lost"/"no chance" mid-rip \u2014 a later pass (or a
                 freshly power-cycled drive) still recovers it, so there is no
                 terminal bucket here. "Bad" is a VERDICT, decided once after
                 the final pass (main-feature lost time vs abort_on_lost_secs),
                 not a live pill.
         The Maybe pill's BYTES are whole-disc, but its TIME is the MAIN-FEATURE
         lost time (main_lost_ms) \u2014 that is what the abort gate judges. So
         "Maybe 990 MB \u00b7 0:00" = 990 MB pending but zero movie time \u21d2 will pass;
         "Maybe 12 KB \u00b7 ~1 ms" = a few sectors of movie \u21d2 fails a 0 threshold.
         Time is rendered at ms precision (fmtMs): 6 sectors is 1 ms, never 0. */
      const bg=s.bytes_good||0, bm=(s.bytes_maybe||0)+(s.bytes_lost||0);
      if(bg>0 || bm>0){
        /* FIXED width (not min-width) + border-box so the pills never grow or
           shrink as the byte/time values change — the row stays rock-steady. */
        const pill = (label, color, body, wPx)=>
          '<span style="display:inline-block;box-sizing:border-box;padding:2px 8px;border-radius:10px;background:'+color
          +';color:var(--pill-fg);font-size:.65rem;font-weight:600;margin-right:6px;'
          +'width:'+wPx+'px;text-align:center;white-space:nowrap;overflow:hidden;font-variant-numeric:tabular-nums">'
          +label+' '+body+'</span>';
        let pills='';
        if(bg>0){
          pills+=pill('Good','var(--green,#3aaa55)', fmtBytes(bg), 150);
        }
        if(bm>0){
          /* Time = MAIN-FEATURE movie time at risk (main_at_risk_ms: pending +
             lost \u2229 feature), NOT the terminal Unreadable-only main_lost_ms which
             is structurally 0 until the final pass. So 0:00 honestly means "no
             movie impact" even mid-rip, and a real ms figure means the movie is
             affected. Melts toward 0 as retries recover pending sectors. */
          const atRiskMs = (s.main_at_risk_ms!=null && s.main_at_risk_ms>=0) ? s.main_at_risk_ms : 0;
          const t = atRiskMs>0 ? '~'+fmtMs(atRiskMs) : '0:00';
          pills+=pill('Maybe','var(--yellow,#f0c000)', fmtBytes(bm)+' \u00b7 '+t, 200);
        }
        if(pills) badLine='<div style="font-size:.7rem;margin-top:14px">'+pills+'</div>';
      }
      detail='<div style="margin-top:6px">'
        +renderBar(s,passPct)
        +'<div style="font-size:.75rem;color:var(--text2);margin-top:7px">'+passLine+'</div>'
        +sectorsLine
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
  renderMuxBanner(data);
  if(document.getElementById('system').classList.contains('active')){renderMuxes();renderMoves();}
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

  renderCurrent();
}

/* Ripper-page banner: muxing/moving of PREVIOUS rips runs in the background
   on the System tab. When the drive is idle (No disc / done card) new users
   have no idea that work is still happening off-screen — so surface a hint
   between the header and the disc card whenever a mux or move is in flight or
   queued. It clears itself the moment both queues drain. */
function renderMuxBanner(data){
  const el=document.getElementById('muxbanner');
  if(!el)return;
  data=data||window._stateData||{};
  const mx=data._mux;
  const muxActive=!!(mx&&mx.status==='ripping'&&mx.disc_name);
  const muxQ=!!(data._mux_queue&&data._mux_queue.length);
  /* `_move` is an ARRAY of per-artifact bars (1.6.7+); tolerate the legacy
     single-object shape. An active move is any bar carrying a name. The old
     `mv.name` check silently missed the array form, so the banner never lit
     up while a move was running — mux showed, move didn't. */
  const mv=data._move;
  const moveActive=Array.isArray(mv)?mv.some(m=>m&&m.name):!!(mv&&mv.name);
  const moveQ=!!(data._move_queue&&data._move_queue.length);
  const muxing=muxActive||muxQ, moving=moveActive||moveQ;
  if(!muxing&&!moving){el.innerHTML='';return;}
  let what;
  if(muxing&&moving)what='Muxing &amp; moving of previous rips';
  else if(moving)what='Moving of previous rips';
  else what='Muxing of previous rips';
  el.innerHTML='<div onclick="goSystemTab()" '
    +'style="margin:0 0 16px;padding:10px 14px;background:var(--chip);border:1px solid var(--border);'
    +'border-radius:8px;font-size:.85rem;color:var(--text2);cursor:pointer;display:flex;align-items:center;gap:8px">'
    +'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--green);animation:p 1.5s infinite;flex-shrink:0"></span>'
    +'<span>'+what+' still in progress — see the <b>System</b> tab.</span></div>';
}
/* Programmatically activate the System tab (reuses the nav click handler, so
   loadSystem() fires). Used by the Ripper-page mux/move banner. */
function goSystemTab(){var b=document.querySelector('.nav[data-tab="system"]');if(b)b.click();}
function renderCurrent(){
  const data=window._stateData;
  if(!data)return;
  const dev=_activeTab;
  const s=data[dev];
  if(!s)return;

  /* Derived state */
  const active=ACTIVE_STATES.includes(s.status);
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
    /* Elapsed counter goes BEFORE the Stop button: the action row is
       right-anchored, so the counter's growth (1m → 1h 02m 34s) extends
       LEFTWARD into empty space and never shoves the Stop button. tabular-nums
       keeps digits a fixed pixel width (no per-second jitter); text-align:right
       + a min-width wide enough for the "1h 02m 34s" form keeps it stable. */
    btns='<span id="rip-elapsed-'+dev+'" data-started="'+(s.started_epoch_secs||0)+'" style="margin-right:10px;font-size:.78rem;color:var(--text2);align-self:center;font-variant-numeric:tabular-nums;min-width:95px;text-align:right;display:inline-block"></span>';
    btns+='<button class="btn btn-stop" onclick="if(confirm(\'Stop?\')){apiPost(\'/api/stop/'+dev+'\',this,\'Stop\')}">Stop</button>';
  }else if(scanned){
    /* Keys resolved at scan time. If they're missing (and the operator
       hasn't opted into capture-without-keys), don't offer Rip at all —
       it would just error. Offer "Scan again" so a freshly-loaded KEYDB
       or a corrected key source can be re-checked without a page reload. */
    const notReady=(s.key_status||'').indexOf('Missing')===0;
    if(notReady){
      btns='<button class="btn" onclick="apiPost(\'/api/scan/'+dev+'\',this,\'Scan\')">Scan again</button>';
    }else if(s.resumable){
      /* A resumable partial exists. Design: the PRIMARY action (Resume —
         continue where it left off) is the filled accent button and comes
         first; "Start over" is the DESTRUCTIVE alternative (wipes the partial
         and re-sweeps from scratch), so it is de-emphasized as a red OUTLINE
         (not a green fill that competed with the primary) and confirmed. For
         "remux" Resume just re-muxes the staged ISO. */
      const rl=s.resumable==='remux'?'Resume (re-mux)':'Resume';
      btns='<button class="btn" style="background:var(--accent);color:#fff;border-color:var(--accent)" onclick="apiPost(\'/api/rip/'+dev+'?resume=yes\',this,\'Resume\')">'+rl+'</button>';
      btns+='<button class="btn" style="background:transparent;color:var(--red);border-color:var(--red)" onclick="if(confirm(\'Start over from scratch? This discards the resumable partial for this disc and re-rips the whole disc.\')){apiPost(\'/api/rip/'+dev+'?resume=no\',this,\'Start over\')}">Start over</button>';
    }else{
      btns='<button class="btn" style="background:var(--green);color:#fff;border-color:var(--green)" onclick="apiPost(\'/api/rip/'+dev+'?resume=no\',this,\'Rip\')">Rip</button>';
    }
  }else if(discIn){
    btns='<button class="btn" onclick="apiPost(\'/api/scan/'+dev+'\',this,\'Scan\')">Scan</button>';
  }
  /* Loss-aborted off-ramp: the rip aborted because main-movie loss exceeded the
     threshold, but the COMPLETE ISO is staged on disk. Offer exactly TWO clear
     choices and REPLACE the generic start-over (so the operator isn't offered a
     destructive fresh rip here):
       • "Run one more pass" — Resume (resume=yes): another recovery pass over
         the bad ranges (Pass N from the mapfile, recovering only the bad core).
       • "Accept & deliver"  — accept the recorded loss and deliver as-is
         (re-mux with the abort gate bypassed; one-shot, confirmed).
     Detected by the loss_aborted flag OR a loss-abort last_error; shown ONCE.
     (Previously two separate blocks each appended an "Accept damage & deliver",
     so it rendered twice with no Resume; --yellow is a dark brown, so black
     text on it was unreadable — Resume is accent/white, Accept is amber-outlined.) */
  const lossAborted=s.loss_aborted||(s.last_error||'').indexOf('lost in main movie')>=0||(s.last_error||'').indexOf('lost at mux')>=0;
  if(lossAborted&&!active){
    btns='<button class="btn" style="background:var(--accent);color:#fff;border-color:var(--accent)" onclick="apiPost(\'/api/rip/'+dev+'?resume=yes\',this,\'Run one more pass\')">Run one more pass</button>';
    btns+='<button class="btn" style="background:transparent;color:var(--yellow);border-color:var(--yellow);font-weight:500" onclick="if(confirm(\'Accept the recorded main-movie damage and deliver this rip as-is? The unreadable section will be missing, but the rest is intact.\')){apiPost(\'/api/accept-loss/\'+dev,this,\'Accept &amp; deliver\')}">Accept &amp; deliver</button>';
  }
  if(discIn&&!active)btns+='<button class="btn btn-eject" onclick="apiPost(\'/api/eject/'+dev+'\',this,\'Eject\')">Eject</button>';

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
  const speedStr=fmtSpeed(s.speed_mbs);
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
/* Render one autorip.jsonl line as a compact human line for the debug box:
   "HH:MM:SS LEVEL message  k=v k=v" (the patch walk + timings). Falls back
   to the raw line if it isn't JSON. */
function fmtDebugLine(line){
  try{
    const o=JSON.parse(line);
    const t=(o.timestamp||'').replace('T',' ').replace('Z','').replace(/\.[0-9]+/,'');
    const f=o.fields||{};
    const msg=f.message||'';
    const extra=Object.keys(f).filter(k=>k!=='message'&&k!=='build').map(k=>k+'='+f[k]).join(' ');
    return t+' '+(o.level||'')+' '+msg+(extra?'  '+extra:'');
  }catch(e){return line;}
}
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
  /* Debug Log box: only shown + polled when debug logging is ON. Reuses the
     existing /api/debug jsonl tailer, filtered to this device, newest-first. */
  const db=document.getElementById('debugBox');
  if(window._debugOn){
    if(db)db.style.display='';
    fetch('/api/debug?device='+encodeURIComponent(dev)+'&n=500').then(r=>r.text()).then(text=>{
      const el=document.getElementById('debuglog');
      const lines=text.split('\n').filter(l=>l).map(fmtDebugLine).reverse().join('\n');
      if(el&&el._last!==lines){el.textContent=lines;el._last=lines;}
    }).catch(()=>{});
  }else if(db){db.style.display='none';}
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

/* Speed string from MB/s. Drops to KB/s, then B/s for sub-KB rates, so a slow
   patch reads e.g. "512 B/s" ("it's doing something") instead of "0 KB/s", and
   "0 B/s" when work is genuinely frozen (grinding one sector's ECC) — never a
   blank gap. */
function fmtSpeed(mbs){
  mbs=+mbs||0;
  if(mbs>=1) return mbs.toFixed(1)+' MB/s';
  if(mbs*1024>=1) return (mbs*1024).toFixed(0)+' KB/s';
  return Math.round(mbs*1048576)+' B/s';
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
    .then(r=>r.json().then(j=>({ok:r.ok,j:j})))
    .then(({ok,j})=>{if(!ok||(j&&j.ok===false)){alert('Resolve failed: '+((j&&j.error)||'server error'));}loadReview();})
    .catch(()=>{alert('Resolve failed: could not reach the server');loadReview();});
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
    /* Header: re-check (refresh) + clear-all, matching the Move queue. A
       cleared error the worker still considers blocked reappears on its next
       tick UNLESS dismissed (loss-aborts stay cleared). */
    html+='<div style="display:flex;align-items:center;gap:10px;margin-bottom:4px">'
      +'<span style="font-size:.75rem;color:var(--text3);text-transform:uppercase;letter-spacing:.4px">Needs action</span>'
      +'<span style="flex:1"></span>'
      +'<a href="#" onclick="event.preventDefault();loadSystem()" style="font-size:.75rem;color:var(--text2);text-decoration:none">↻ Refresh</a>'
      +'<a href="#" onclick="event.preventDefault();clearAllMuxErr()" style="font-size:.75rem;color:var(--text2);text-decoration:none">Clear all</a>'
      +'</div>';
    window._muxErrors.forEach(e=>{
      var p=e.path||'';
      html+='<div style="padding:6px 0;font-size:.8rem">'
        +'<div style="display:flex;align-items:center;gap:8px;margin-bottom:2px">'
        +'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--red);flex-shrink:0"></span>'
        +'<span style="font-weight:500;color:var(--red);flex:1;min-width:0;word-break:break-all">'+esc(p)+'</span>'
        +'<span onclick="clearMuxErr('+JSON.stringify(p)+')" title="Clear this error" '
          +'style="flex-shrink:0;cursor:pointer;color:var(--text3);font-size:1rem;line-height:1;padding:0 2px">&times;</span>'
        +'</div>'
        +'<div style="margin-left:16px;color:var(--text2)">'+esc(e.reason||'')+'</div>'
        +(e.hint?'<div style="margin-left:16px;color:var(--text3);font-size:.75rem;margin-top:2px">'+esc(e.hint)+'</div>':'')
        +'</div>';
    });
    html+='</div>';
  }
  upd('muxes',html);
}

/* Clear a single stuck mux error (the ✕), then re-pull. */
function clearMuxErr(path){
  fetch('/api/mux-errors/clear?path='+encodeURIComponent(path),{method:'POST'})
    .then(()=>loadSystem()).catch(()=>loadSystem());
}
function clearAllMuxErr(){
  fetch('/api/mux-errors/clear-all',{method:'POST'})
    .then(()=>loadSystem()).catch(()=>loadSystem());
}
/* ---- Move queue with live progress ---- */
function renderMoves(){
  const el=document.getElementById('moves');
  if(!el)return;
  const data=window._stateData||{};
  /* `_move` is an ARRAY of per-artifact bars (the movie file and, with
     keep_iso, its companion ISO \u2014 one bar each). Tolerate the legacy single
     object shape for safety across a rolling deploy. */
  const moves=Array.isArray(data._move)?data._move:(data._move&&data._move.name?[data._move]:[]);
  let html='';
  let hasContent=false;
  /* Active move: one progress bar per artifact, labelled "Title (iso)" /
     "Title (mkv)" so the two legs of a keep_iso move read distinctly. */
  moves.forEach(mv=>{
    if(!mv||!mv.name)return;
    hasContent=true;
    const pct=mv.progress_pct||0;
    const spdStr=mv.speed_mbs>=1?mv.speed_mbs.toFixed(1)+' MB/s':mv.speed_mbs>0?(mv.speed_mbs*1024).toFixed(0)+' KB/s':'';
    const etaStr=mv.eta?mv.eta+' remaining':'';
    const label=[pct+'%',spdStr,etaStr].filter(x=>x).join(' \u00b7 ');
    const title=esc(mv.name)+(mv.artifact?' ('+esc(mv.artifact)+')':'');
    html+='<div style="padding:6px 0"><div style="display:flex;align-items:center;gap:8px;margin-bottom:4px"><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--green);animation:p 1.5s infinite;flex-shrink:0"></span><span style="font-size:.85rem;font-weight:500">'+title+'</span></div>';
    html+='<div style="display:flex;align-items:center;gap:8px">';
    if(pct>0)html+='<div style="flex:1;background:var(--chip);border-radius:3px;height:3px;overflow:hidden"><div style="background:var(--green);height:100%;width:'+pct+'%;transition:width 1s"></div></div>';
    html+='<span style="font-size:.75rem;color:var(--text2)">'+label+'</span></div></div>';
  });
  /* Pending queue items — from the live state payload (_move_queue),
     refreshed every SSE tick (falls back to the /api/system _moveQueue only
     if absent). The server already excludes the actively-moving dir from this
     list (it's shown as the bars above), so no client-side de-dup is needed. */
  const moveQ=(data._move_queue!=null)?data._move_queue:window._moveQueue;
  if(moveQ){
    moveQ.forEach(m=>{
      hasContent=true;
      html+='<div style="padding:4px 0;font-size:.8rem"><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--yellow);margin-right:8px;vertical-align:middle"></span>'+esc(m)+'</div>';
    });
  }
  if(!hasContent)html='<div style="color:var(--text3);font-size:.8rem">No pending moves</div>';
  /* Stuck-move errors that need user action (orphaned staging dirs etc.) */
  if(window._moveErrors&&window._moveErrors.length){
    html+='<div style="margin-top:8px;padding-top:8px;border-top:1px solid var(--chip)">';
    /* Header: re-check (refresh) + clear-all. A cleared error the mover still
       considers blocked reappears on its next tick, so refresh confirms which
       are actually solved. */
    html+='<div style="display:flex;align-items:center;gap:10px;margin-bottom:4px">'
      +'<span style="font-size:.75rem;color:var(--text3);text-transform:uppercase;letter-spacing:.4px">Needs action</span>'
      +'<span style="flex:1"></span>'
      +'<a href="#" onclick="event.preventDefault();loadSystem()" style="font-size:.75rem;color:var(--text2);text-decoration:none">↻ Refresh</a>'
      +'<a href="#" onclick="event.preventDefault();clearAllMoveErr()" style="font-size:.75rem;color:var(--text2);text-decoration:none">Clear all</a>'
      +'</div>';
    window._moveErrors.forEach(e=>{
      var p=e.path||'';
      html+='<div style="padding:6px 0;font-size:.8rem">'
        +'<div style="display:flex;align-items:center;gap:8px;margin-bottom:2px">'
        +'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--red);flex-shrink:0"></span>'
        +'<span style="font-weight:500;color:var(--red);flex:1;min-width:0;word-break:break-all">'+esc(p)+'</span>'
        +'<span onclick="clearMoveErr('+JSON.stringify(p)+')" title="Clear this error" '
          +'style="flex-shrink:0;cursor:pointer;color:var(--text3);font-size:1rem;line-height:1;padding:0 2px">&times;</span>'
        +'</div>'
        +'<div style="margin-left:16px;color:var(--text2)">'+esc(e.reason||'')+'</div>'
        +(e.hint?'<div style="margin-left:16px;color:var(--text3);font-size:.75rem;margin-top:2px">'+esc(e.hint)+'</div>':'')
        +'</div>';
    });
    html+='</div>';
  }
  upd('moves',html);
}

/* Clear a single stuck move error (the ✕), then re-pull so a still-blocked
   one reappears and a solved one stays gone. */
function clearMoveErr(path){
  fetch('/api/move-errors/clear?path='+encodeURIComponent(path),{method:'POST'})
    .then(()=>loadSystem()).catch(()=>loadSystem());
}
function clearAllMoveErr(){
  fetch('/api/move-errors/clear-all',{method:'POST'})
    .then(()=>loadSystem()).catch(()=>loadSystem());
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
    /* Global flag so the device-page Debug Log box shows/polls only when on */
    window._debugOn=!!data.debug_enabled;
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
      {key:'abort_on_lost_secs',label:'Max Acceptable Main Movie Loss',type:'number',hint:'Governs the RIP phase only: seconds of UNREADABLE main-movie data tolerated after all retry passes. If unreadable-sector loss in the MAIN FEATURE still exceeds this once retries are exhausted, the rip aborts (leaving a resumable staging dir). 0 = require a perfect rip — abort on ANY unreadable byte, not just ≥1s. Scoped to the main movie only (a scratched menu/extra outside the title never aborts). Applies to title-selected output (MKV / M2TS / Network stream) — IGNORED for an ISO rip, which is kept whole as-is (for a byte-perfect full-disc image use the freemkv CLI). The mux itself never aborts: any undecryptable/demux-time loss is concealed and tallied, never quarantined. Multi-pass only.',indent:true,showIf:{key:'rip_mode',value:'multi'},hideIf:{key:'output_format',value:'iso'}},
    ]},
    {title:'Recovery',fields:[
      {key:'rip_mode',label:'Rip Mode',type:'radio',options:[{value:'single',label:'Single Pass'},{value:'multi',label:'Multi Pass'}],hint:'Single Pass: stream disc → MKV directly. Fastest, best for healthy discs. Multi Pass: rip an ISO, retry bad sectors with progressively smaller blocks, then mux to MKV. Use for discs with read errors.'},
      /* Single-pass error policy: only meaningful when there's no retry safety net. */
      {key:'on_read_error',label:'On Read Error',type:'radio',options:[{value:'stop',label:'Stop'},{value:'skip',label:'Skip (zero-fill)'}],hint:'Drive read error policy for single-pass rips. Stop aborts on the first bad sector. Skip zero-fills it and keeps streaming — useful when the disc is mostly fine and you accept minor loss for speed.',indent:true,showIf:{key:'rip_mode',value:'single'}},
      /* Multi-pass knobs: retries + accept-loss threshold. on_read_error doesn't apply
         in multi-pass — sweep always skips by design, retries always retry, and the
         post-retry abort decision is governed by abort_on_lost_secs (time-based). */
      {key:'max_retries',label:'Retry Passes',type:'number',hint:'How many retry passes to run on bad sectors. Each pass uses smaller blocks (60→30→15→7→1 sectors) and alternates direction. Default 5 covers most recoverable damage.',indent:true,showIf:{key:'rip_mode',value:'multi'}},
      {key:'keep_iso',label:'Keep Intermediate ISO',type:'bool',hint:'Keep the intermediate disc ISO after muxing. Off by default to reclaim disk. Filed beside the muxed title unless you set an ISO Folder (under Output).',indent:true,showIf:{key:'rip_mode',value:'multi'}},
    ]},
    {title:'Output',fields:[
      {key:'staging_dir',label:'Staging Directory',type:'text',hint:'Where rips are written before being moved to the final destination. Use a fast local disk for performance; the finished MKV is moved to the output directory on completion.'},
      {key:'output_dir',label:'Output Directory',type:'text',hint:'Where all ripped files go by default'},
      {key:'movie_dir',label:'Movies',type:'text',hint:'',indent:true,placeholder:'Same as output directory'},
      {key:'tv_dir',label:'TV Series',type:'text',hint:'',indent:true,placeholder:'Same as output directory'},
      {key:'iso_dir',label:'ISO Folder',type:'text',hint:'Where kept ISOs are stored (applies when Keep Intermediate ISO is on, or Output Format is ISO). Relative (e.g. isos) sits under the Output Directory; an absolute path (e.g. /mnt/archive/isos) targets another disk. Blank = beside the muxed title.',indent:true,placeholder:'Beside the muxed title'},
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
      const showAttr=(f.showIf?' data-show-key="'+f.showIf.key+'" data-show-value="'+f.showIf.value+'"':'')+(f.hideIf?' data-hide-key="'+f.hideIf.key+'" data-hide-value="'+f.hideIf.value+'"':'');
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
      /* Each webhook is now {url, post_rip, post_mux, post_move}; tolerate a
         legacy bare string (older payload) by coercing it to an object that
         fires on every stage. A pre-1.6.8 object with no post_mux defaults it
         to true (post_mux!==false below), matching the config loader. */
      const hooks=(s.webhook_urls||[])
        .map(h=>typeof h==='string'?{url:h,post_rip:true,post_mux:true,post_move:true}:h)
        .filter(h=>h&&h.url);
      html+='<div class="card"><h2>Webhooks</h2>';
      html+='<div id="webhook-list">';
      hooks.forEach((h,i)=>{ html+=webhookRow(i,h.url,h.post_rip!==false,h.post_mux!==false,h.post_move!==false); });
      html+='</div>';
      html+='<button class="btn" onclick="addWebhook()" style="font-size:.75rem;margin-top:4px">+ Add Webhook</button>';
      html+='<div style="font-size:12px;color:var(--text3);margin-top:8px;line-height:1.4">POST JSON to each endpoint. Choose per hook which stage fires it — Rip (disc read done, drive free), Mux (.mkv produced), Move (in library) — in any combination. Works with Discord, Jellyfin, n8n, or any HTTP endpoint.</div>';
      html+='</div>';
    }
  });
  document.getElementById('settings-form').innerHTML=html;
  toggleConditional();
}
function toggleConditional(){
  // A field may carry BOTH a showIf and a hideIf (e.g. abort_on_lost_secs:
  // show only in multi-pass AND not for ISO output). Compute visibility per
  // element from both: hidden if the showIf condition isn't met OR the hideIf
  // condition IS met. Single-condition fields keep their prior behaviour.
  document.querySelectorAll('[data-show-key],[data-hide-key]').forEach(el=>{
    let visible=true;
    if(el.dataset.showKey){
      const r=document.querySelector('input[data-key="'+el.dataset.showKey+'"]:checked');
      if(!(r&&r.value===el.dataset.showValue)) visible=false;
    }
    if(el.dataset.hideKey){
      const r=document.querySelector('input[data-key="'+el.dataset.hideKey+'"]:checked');
      if(r&&r.value===el.dataset.hideValue) visible=false;
    }
    el.style.display=visible?'':'none';
  });
}

/* One webhook row: URL input + a "Rip", "Mux" and "Move" checkbox (which
   pipeline stage fires this hook) + a remove button. Rip = disc read done /
   drive free; Mux = .mkv produced; Move = landed in the library.
   `postRip`/`postMux`/`postMove` seed the checkboxes; new hooks default all
   three to true. The checkbox data-attributes are read back per-row in
   saveSettings(). */
function webhookRow(i,url,postRip,postMux,postMove){
  const cb=(attr,on,label)=>'<label style="display:inline-flex;align-items:center;gap:4px;font-size:12px;color:var(--text2);cursor:pointer;white-space:nowrap"><input type="checkbox" '+attr+' '+(on?'checked':'')+' style="width:14px;height:14px;margin:0;accent-color:var(--accent)">'+label+'</label>';
  return '<div class="webhook-row" style="display:flex;gap:8px;margin-bottom:6px;align-items:center;flex-wrap:wrap">'
    +'<input type="text" data-webhook="'+i+'" value="'+esc(url||'')+'" placeholder="https://discord.com/api/webhooks/..." style="flex:1;min-width:180px;padding:8px 10px;border:1px solid var(--border);border-radius:6px;background:var(--log-bg);color:var(--text);font-size:13px;font-family:inherit">'
    +cb('data-webhook-rip','undefined'==typeof postRip?true:postRip,'Rip')
    +cb('data-webhook-mux','undefined'==typeof postMux?true:postMux,'Mux')
    +cb('data-webhook-move','undefined'==typeof postMove?true:postMove,'Move')
    +'<button class="btn" onclick="this.parentElement.remove()" style="padding:5px 8px;font-size:.75rem">X</button>'
    +'</div>';
}

function addWebhook(){
  const list=document.getElementById('webhook-list');
  const i=list.children.length;
  const tmp=document.createElement('div');
  tmp.innerHTML=webhookRow(i,'',true,true,true);
  const div=tmp.firstChild;
  list.appendChild(div);
  div.querySelector('input[type="text"]').focus();
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
  /* Collect webhooks as {url, post_rip, post_mux, post_move}. Read each flag
     from the row's own checkboxes so a URL only fires on the stages the
     operator chose. */
  const hooks=[];
  document.querySelectorAll('#webhook-list .webhook-row').forEach(row=>{
    const urlEl=row.querySelector('input[data-webhook]');
    const v=(urlEl&&urlEl.value||'').trim();
    if(!v)return;
    const rip=row.querySelector('input[data-webhook-rip]');
    const mux=row.querySelector('input[data-webhook-mux]');
    const mov=row.querySelector('input[data-webhook-move]');
    hooks.push({url:v,post_rip:rip?rip.checked:true,post_mux:mux?mux.checked:true,post_move:mov?mov.checked:true});
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
    .then(async r=>{
      if(!r.ok){
        let msg='HTTP '+r.status;
        try{const j=await r.json();if(j&&j.error)msg=j.error;}catch(_){}
        throw new Error(msg);
      }
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
        //
        // The cap a request is measured against depends on whether it carries
        // a BODY. tiny_http yields a request once its headers have parsed and
        // the handler thread reads the body itself, holding its admission
        // token for the whole read — and tiny_http 0.12 sets no socket read
        // timeout and exposes no way to set one, so a peer that sends headers
        // and then stalls parks a token indefinitely. Sixty-four of those took
        // every slot, and the FIRST casualty is the container healthcheck
        // (`GET /api/state`), whose three failed retries restart the daemon —
        // possibly mid-rip. Body-carrying requests are therefore held to a
        // lower cap, leaving slots that only a bodyless request can use, so a
        // stall storm can no longer starve the healthcheck.
        let cap = if carries_body(&request) {
            MAX_INFLIGHT_BODY_HANDLERS
        } else {
            MAX_INFLIGHT_HANDLERS
        };
        let guard = match ConnGuard::try_acquire(&INFLIGHT_HANDLERS, cap) {
            Some(g) => g,
            None => {
                tracing::warn!(max = cap, "request rejected: in-flight handler cap reached");
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
    } else if is_get && url == "/favicon.svg" {
        serve_favicon(request);
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
            &format!("{{\"version\":\"{}\"}}", crate::VERSION_LABEL),
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
    } else if is_post && url == "/api/move-errors/clear-all" {
        crate::mover::clear_all_move_errors();
        json_response(request, 200, r#"{"ok":true}"#);
    } else if is_post && url.starts_with("/api/move-errors/clear?") {
        // Clear ONE move error by path. The path carries slashes/spaces, so it
        // arrives percent-encoded in the `path=` query param.
        let query = url.split_once('?').map(|x| x.1).unwrap_or("");
        let target = query
            .split('&')
            .find_map(|kv| kv.strip_prefix("path="))
            .map(percent_decode)
            .unwrap_or_default();
        if target.is_empty() {
            return json_response(request, 400, r#"{"ok":false,"error":"missing path"}"#);
        }
        crate::mover::clear_move_error(&target);
        json_response(request, 200, r#"{"ok":true}"#);
    } else if is_post && url == "/api/mux-errors/clear-all" {
        crate::muxer::clear_all_mux_errors();
        json_response(request, 200, r#"{"ok":true}"#);
    } else if is_post && url.starts_with("/api/mux-errors/clear?") {
        // Clear ONE mux error by path (percent-encoded `path=` query param).
        let query = url.split_once('?').map(|x| x.1).unwrap_or("");
        let target = query
            .split('&')
            .find_map(|kv| kv.strip_prefix("path="))
            .map(percent_decode)
            .unwrap_or_default();
        if target.is_empty() {
            return json_response(request, 400, r#"{"ok":false,"error":"missing path"}"#);
        }
        crate::muxer::clear_mux_error(&target);
        json_response(request, 200, r#"{"ok":true}"#);
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
    } else if is_post && url.starts_with("/api/accept-loss/") {
        let device = percent_decode(url.trim_start_matches("/api/accept-loss/"));
        if !is_valid_device_name(&device) {
            return json_response(request, 400, r#"{"error":"invalid device name"}"#);
        }
        handle_accept_loss(request, cfg, &device);
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
    let media_type = normalize_media_type(v["media_type"].as_str().unwrap_or("movie"));
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
        if let Some(prev) = *last
            && now.duration_since(prev) < TMDB_MIN_INTERVAL
        {
            return json_response(request, 429, r#"{"error":"rate limited"}"#);
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
    let html = DASHBOARD_HTML.replace("{VERSION}", crate::VERSION_LABEL);
    // The dashboard is a single self-contained page (HTML + inline CSS/JS), so
    // it IS the app shell. Serve it non-cacheable: without this, browsers cache
    // it heuristically and keep running the OLD UI + old client-side validation
    // after a deploy ("caching isn't invalidated on release"). no-store forces a
    // fresh fetch on every load, so a new autorip version takes effect at once.
    let response = Response::from_string(html).with_header(header).with_header(
        Header::from_bytes(
            &b"Cache-Control"[..],
            &b"no-store, no-cache, must-revalidate"[..],
        )
        .unwrap(),
    );
    let _ = request.respond(response);
}

fn serve_favicon(request: tiny_http::Request) {
    let header = Header::from_bytes(&b"Content-Type"[..], &b"image/svg+xml"[..]).unwrap();
    // Unlike the app shell, the icon is immutable brand art — let the browser
    // cache it so the tab icon doesn't refetch on every poll/load.
    let response = Response::from_string(FAVICON_SVG)
        .with_header(header)
        .with_header(
            Header::from_bytes(&b"Cache-Control"[..], &b"public, max-age=86400"[..]).unwrap(),
        );
    let _ = request.respond(response);
}

fn json_response(request: tiny_http::Request, status: u16, body: &str) {
    let header = Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
    // This is a local control app, not a website: NOTHING is cacheable. The API
    // responses (state/version/etc.) are polled live, so a cached body would show
    // stale rip state. Match the HTML shell's no-store.
    let response = Response::from_string(body)
        .with_status_code(StatusCode(status))
        .with_header(header)
        .with_header(
            Header::from_bytes(
                &b"Cache-Control"[..],
                &b"no-store, no-cache, must-revalidate"[..],
            )
            .unwrap(),
        );
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

/// One webhook as it arrives on POST /api/settings: a (possibly masked) URL
/// plus the two per-event flags the UI collected from its checkboxes.
/// [`resolve_webhook_entries`] turns a slice of these into stored
/// [`WebhookEntry`]s, unmasking the URL while carrying the flags through.
struct IncomingWebhook {
    /// May be a real URL (newly entered) or a masked placeholder to resolve.
    url: String,
    post_rip: bool,
    post_mux: bool,
    post_move: bool,
}

/// Resolve an incoming `webhook_urls` array against the currently-stored
/// entries, replacing each redacted URL placeholder with its real
/// (token-bearing) value while preserving the per-entry `post_rip`/`post_mux`/`post_move`
/// flags the client sent. Only the URL is ever masked, so the flags always
/// come straight from `incoming`.
///
/// URL matching is BY STABLE `#idx` (falling back to origin prefix), never by
/// array position: the UI can delete or reorder rows between GET and POST, so
/// a positional match would bind a masked entry to a different stored secret.
/// A masked URL whose `#idx` (or, for older clients, origin) resolves to
/// exactly one stored entry takes that entry's real URL. A non-masked URL is
/// taken verbatim (a newly-entered secret). `Err(url)` is returned when a
/// masked URL is ambiguous — it matches 0 stored entries (the row it referred
/// to was deleted) or >1 (two stored hooks share an origin) — so the caller
/// can reject the save instead of guessing. Entries with a blank/whitespace
/// URL are dropped.
fn resolve_webhook_entries(
    incoming: &[IncomingWebhook],
    existing: &[WebhookEntry],
) -> Result<Vec<WebhookEntry>, String> {
    let mut resolved: Vec<WebhookEntry> = Vec::with_capacity(incoming.len());
    for hook in incoming {
        let s = hook.url.as_str();
        // Resolve the URL only; the flags are always taken from `incoming`.
        let url = if is_masked_webhook(s) {
            // Preferred path: the masked form carries a stable `#<idx>`
            // identifier (see mask_webhook_url_indexed). Resolve by that index
            // so two same-origin webhooks round-trip unambiguously. The index
            // must both be in range AND still mask to exactly this placeholder
            // (so a reordered/deleted row can't silently bind the wrong secret).
            if let Some((origin_mask, idx_str)) = s.rsplit_once('#')
                && let Ok(idx) = idx_str.parse::<usize>()
            {
                match existing.get(idx) {
                    Some(stored) if mask_webhook_url(&stored.url) == origin_mask => {
                        stored.url.clone()
                    }
                    // Index stale (row deleted/reordered) — reject rather
                    // than guess.
                    _ => return Err(s.to_string()),
                }
            } else {
                // Fallback: no embedded index (older client). Match by origin;
                // only unambiguous when exactly one stored URL shares the origin.
                let matches: Vec<&WebhookEntry> = existing
                    .iter()
                    .filter(|stored| mask_webhook_url(&stored.url) == s)
                    .collect();
                match matches.as_slice() {
                    [one] => one.url.clone(),
                    _ => return Err(s.to_string()),
                }
            }
        } else {
            s.to_string()
        };
        if url.trim().is_empty() {
            continue;
        }
        resolved.push(WebhookEntry {
            url,
            post_rip: hook.post_rip,
            post_mux: hook.post_mux,
            post_move: hook.post_move,
        });
    }
    Ok(resolved)
}

/// Serialize Config for GET /api/settings with credential fields redacted.
/// No route is authenticated and the server binds 0.0.0.0, so returning
/// `keyserver_secret` / `tmdb_api_key` in cleartext would hand any
/// LAN/host client the operator's bearer token and API key.
fn settings_json_redacted(c: &Config) -> String {
    let mut v = serde_json::to_value(c).unwrap_or_else(|_| serde_json::json!({}));
    for field in ["keyserver_secret", "tmdb_api_key"] {
        if let Some(s) = v.get(field).and_then(|x| x.as_str())
            && !s.is_empty()
        {
            v[field] = serde_json::json!(SECRET_SENTINEL);
        }
    }
    // keyserver_url and keydb_url may carry auth tokens in the path/query
    // (e.g. https://keyserver.example.com/token/decode). Mask path/query
    // with the origin-preserving helper so the operator can see the host
    // but not the embedded secret. A masked value round-trips on POST.
    for field in ["keyserver_url", "keydb_url"] {
        if let Some(s) = v.get(field).and_then(|x| x.as_str())
            && !s.is_empty()
        {
            v[field] = serde_json::json!(mask_webhook_url(s));
        }
    }
    // webhook_urls embed bearer tokens (Discord/Slack/Jellyfin webhook
    // secrets live in the path/query). Mask the token but keep the origin
    // visible so the operator can identify each hook. A masked entry
    // round-trips on POST: any entry containing the sentinel is "unchanged".
    // keydb_path is an absolute container path; leaking it to any LAN/host
    // client exposes the internal filesystem layout. Return only the filename
    // component (enough for the operator to confirm which file is in use).
    if let Some(s) = v.get("keydb_path").and_then(|x| x.as_str())
        && !s.is_empty()
    {
        let name = std::path::Path::new(s)
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        v["keydb_path"] = serde_json::json!(name);
    }
    // Each entry serializes as {url, post_rip, post_mux, post_move}. Mask the token in
    // `url` (keeping the origin + stable `#idx` for round-trip resolution) and
    // pass the boolean flags through untouched.
    if let Some(arr) = v.get_mut("webhook_urls").and_then(|x| x.as_array_mut()) {
        for (idx, entry) in arr.iter_mut().enumerate() {
            if let Some(u) = entry.get("url").and_then(|x| x.as_str())
                && !u.is_empty()
            {
                let masked = mask_webhook_url_indexed(u, idx);
                entry["url"] = serde_json::json!(masked);
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
/// The stored `media_type` for a title override, reduced to the vocabulary
/// the router actually understands.
///
/// `media_type` is the one field of `handle_title_override` that was neither
/// clamped nor allow-listed, on a route reachable unauthenticated from the
/// LAN — the sibling fields all go through `clamp_chars`. It is also not
/// free-form data: `mover::routing_media_type` recognises `"tv"` and treats
/// everything else as a movie, and TMDB itself only ever yields these two. An
/// allow-list is therefore both the tighter bound AND the honest description
/// of the field — it stores what the router will act on, instead of persisting
/// and re-broadcasting an arbitrary caller-supplied string to every dashboard.
fn normalize_media_type(raw: &str) -> String {
    match raw.trim().to_ascii_lowercase().as_str() {
        "tv" => "tv".to_string(),
        _ => "movie".to_string(),
    }
}

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
/// The three failure strings [`resolve_with_timeout`] and
/// [`validate_fetch_url`] emit for "we could not find out", as opposed to
/// "this URL is not allowed". They are constants because
/// [`is_transient_resolve_error`] classifies on them: a caller that has to
/// tell a DNS blip from a config error would otherwise be matching literals
/// typed twice, and the day one side is reworded the classification silently
/// inverts.
pub(crate) const RESOLVE_TIMEOUT_MSG: &str = "DNS resolution timed out";
pub(crate) const RESOLVE_FAILED_PREFIX: &str = "could not resolve host: ";
pub(crate) const RESOLVE_NO_ADDRS_MSG: &str = "host did not resolve to any address";

/// True when a [`validate_fetch_url`] / [`resolve_with_timeout`] error means
/// the host could not be LOOKED UP right now — a DNS timeout, a resolver
/// failure, or an empty answer — rather than a permanent verdict on the URL
/// (bad scheme, no host, blocked address).
///
/// The distinction matters wherever a failed validation is folded into a
/// judgement about the remote SERVICE: a resolver blip is not evidence that
/// the service answered. See `keysource::probe_online_reachability`, where
/// getting this wrong finalised a rippable disc as permanently keyless.
pub(crate) fn is_transient_resolve_error(msg: &str) -> bool {
    msg == RESOLVE_TIMEOUT_MSG
        || msg == RESOLVE_NO_ADDRS_MSG
        || msg.starts_with(RESOLVE_FAILED_PREFIX)
}

pub(crate) fn resolve_with_timeout(host: &str, port: u16) -> Result<Vec<SocketAddr>, String> {
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

    // RAII admission token: its Drop releases the slot on EVERY exit path.
    // We move it into the resolver closure so the slot is held until the
    // (possibly detached) worker actually returns — but if `thread::spawn`
    // itself unwinds (the OS refusing a new thread is exactly the
    // resource-exhaustion case this throttle bounds), the guard is dropped
    // on the spawning thread instead, so the counter never leaks.
    let guard = match ConnGuard::try_acquire(&INFLIGHT, MAX_INFLIGHT) {
        Some(g) => g,
        None => return Err(RESOLVE_TIMEOUT_MSG.to_string()),
    };

    let host = host.to_string();
    // Bounded channel of capacity 1: the resolver thread's single send never
    // blocks forever (the buffer always has room for its one message), so the
    // thread always exits cleanly once resolution completes — even if the
    // receiver has already timed out and gone away.
    let (tx, rx) = mpsc::sync_channel::<Result<Vec<SocketAddr>, std::io::Error>>(1);
    std::thread::spawn(move || {
        let _g = guard;
        let res = (host.as_str(), port)
            .to_socket_addrs()
            .map(|it| it.collect::<Vec<SocketAddr>>());
        // Receiver may be gone after the timeout — ignore the send error.
        let _ = tx.send(res);
    });
    match rx.recv_timeout(DNS_TIMEOUT) {
        Ok(Ok(addrs)) => Ok(addrs),
        Ok(Err(e)) => Err(format!("{RESOLVE_FAILED_PREFIX}{e}")),
        Err(_) => Err(RESOLVE_TIMEOUT_MSG.to_string()),
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
        return Err(RESOLVE_NO_ADDRS_MSG.to_string());
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
        return Err(RESOLVE_NO_ADDRS_MSG.to_string());
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
/// ureq's `ResolvedSocketAddrs` is a fixed 16-slot array whose `push` writes
/// straight into it, so handing it a 17th address is an out-of-bounds panic —
/// on a host that merely publishes a lot of A records. Keep the first 16; all
/// of them were vetted by [`validate_fetch_url`].
const MAX_PINNED_ADDRS: usize = 16;

/// The addresses a resolve may actually hand back, capped.
///
/// Separated from the `Resolver` impl so the cap is TESTABLE: ureq's
/// `ResolvedSocketAddrs` and `NextTimeout` are built from types that are not
/// nameable outside the crate, so nothing in the suite could construct a
/// resolve call — and every socket test pins exactly one address, so deleting
/// the cap left the whole suite green.
///
/// The cap is not a nicety. `ResolvedSocketAddrs` is a fixed 16-slot array
/// whose `push` writes `self.arr[self.len]` with no bounds check, so a
/// seventeenth address is an out-of-bounds panic inside a resolver that runs on
/// every request. `validate_fetch_url` returns whatever DNS gave it, with no
/// count limit of its own.
fn pinned_addrs(addrs: &[SocketAddr]) -> Vec<SocketAddr> {
    addrs.iter().copied().take(MAX_PINNED_ADDRS).collect()
}

/// The pinned-address resolver behind [`guarded_agent`].
///
/// ureq 3 replaced v2's resolver closure with this trait, and the agent must be
/// built through `Agent::with_parts` to take one. `Agent::new_with_config`
/// compiles identically and then silently uses the DEFAULT resolver — which
/// would re-resolve the hostname over live DNS and reopen the exact rebinding
/// TOCTOU this agent exists to close, with no visible symptom. Pinned by
/// `guarded_agent_connects_to_the_pinned_address_not_dns`.
#[derive(Debug)]
struct PinnedResolver(Vec<SocketAddr>);

impl ureq::unversioned::resolver::Resolver for PinnedResolver {
    fn resolve(
        &self,
        _uri: &ureq::http::Uri,
        _config: &ureq::config::Config,
        _timeout: ureq::unversioned::transport::NextTimeout,
    ) -> Result<ureq::unversioned::resolver::ResolvedSocketAddrs, ureq::Error> {
        let addrs = pinned_addrs(&self.0);
        if addrs.is_empty() {
            return Err(ureq::Error::HostNotFound);
        }
        let mut out = self.empty();
        for addr in addrs {
            out.push(addr);
        }
        Ok(out)
    }
}

/// A short, URL-FREE description of a ureq failure.
///
/// ureq's own `Display` embeds the full request URL, and these summaries reach
/// syslog, `autorip.jsonl` and the unauthenticated `/api/system` + `/api/debug`
/// endpoints. The URLs involved carry secrets: a TMDB api_key in the query
/// string, a Discord/Slack/Jellyfin token in the webhook path, a token-bearing
/// keydb_url. So the error is never formatted — each variant maps to a fixed
/// label instead.
///
/// ureq 3 split v2's single `Transport(t)` (which had `.kind()`) across many
/// variants, and the enum is `non_exhaustive`, so the catch-all is both
/// required and the safe default: an unrecognised variant degrades to a bare
/// label rather than to something that might interpolate a URL.
pub(crate) fn ureq_error_kind(e: &ureq::Error) -> String {
    match e {
        ureq::Error::StatusCode(code) => format!("HTTP {code}"),
        ureq::Error::Io(io) => match io.raw_os_error() {
            // An OS-generated error (has an errno): its Display is the
            // syscall's own message — "Connection reset by peer (os error 54)",
            // "Broken pipe (os error 32)" — which is derived purely from the
            // errno and cannot embed the URL. Surface it: this turns the
            // useless "io: uncategorized error" (what `io.kind()` alone prints
            // for the ErrorKind::Uncategorized that a reset/refused socket maps
            // to) into an actionable line an operator can act on.
            Some(_) => format!("io: {io}"),
            // No errno — a ureq/std-synthesized io error whose payload we do
            // NOT trust to be URL-free. Fall back to the ErrorKind's fixed
            // description, which is a constant string and never the URL.
            None => format!("io: {}", io.kind()),
        },
        ureq::Error::Timeout(_) => "timeout".to_string(),
        ureq::Error::HostNotFound => "host not found".to_string(),
        ureq::Error::ConnectionFailed => "connection failed".to_string(),
        ureq::Error::TooManyRedirects => "too many redirects".to_string(),
        ureq::Error::Tls(_) => "tls error".to_string(),
        ureq::Error::BodyExceedsLimit(_) => "body exceeds limit".to_string(),
        _ => "transport error".to_string(),
    }
}

/// No progress for this long on an open connection means the peer is dead,
/// whatever it promised in its headers. This is the knob ureq 2's
/// `timeout_read` used to provide and the 2→3 migration dropped: it is
/// ROLLING, re-armed on every read that returns bytes, so it kills a stalled
/// transfer without putting a ceiling on a slow-but-progressing one.
pub(crate) const STALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

/// Build a DNS-pinned, redirect-blocking ureq agent with caller-chosen
/// timeouts. The ONE place the pinned agent is constructed, so no call site
/// can quietly drop the resolver.
///
/// ureq sets NO default connect/read timeout. Without one a peer that
/// accepts the connection but never responds would block the caller's
/// thread (and hold its socket) forever, so every caller must pass bounds.
/// The key-service reachability probe wants to give up much sooner than a
/// keydb download; the caller picks.
///
/// `response` is NOT just a header timeout, despite ureq naming it
/// `timeout_recv_response`. In ureq 3 the body read also checks its preceding
/// timeout, and that deadline is absolute — `headers_complete + response` — so
/// `response` is the ceiling on the WHOLE transfer. Measured against a real
/// socket: with `response = 2s` a server that trickles one byte every 500 ms
/// is killed at 2.0 s, four bytes in. Size it for the largest body this caller
/// should ever accept, not for how long a header may take.
///
/// `idle` is the rolling stall detector ([`STALL_TIMEOUT`]) — the one that
/// catches a dead peer quickly regardless of how generous `response` is.
pub(crate) fn guarded_agent_with_timeouts(
    pinned: Vec<SocketAddr>,
    connect: std::time::Duration,
    response: std::time::Duration,
    idle: std::time::Duration,
) -> ureq::Agent {
    let config = ureq::config::Config::builder()
        .max_redirects(0)
        .timeout_connect(Some(connect))
        .timeout_recv_response(Some(response))
        .timeout_recv_body(Some(idle))
        .build();
    // `with_parts`, never `new_with_config` — see [`PinnedResolver`].
    ureq::Agent::with_parts(
        config,
        ureq::unversioned::transport::DefaultConnector::new(),
        PinnedResolver(pinned),
    )
}

/// Agent for webhook delivery — a plain outbound POST with the standard
/// resolver, deliberately NOT SSRF-guarded.
///
/// Unlike `keydb_url` / `keyserver_url` / `network_target`, a webhook is a
/// blind fire-and-forget notification: autorip POSTs a rip/move event and
/// never reads the response body back to any caller, so there is no
/// disc-key or plaintext-exfiltration channel to protect — and the
/// operator who sets a webhook is on the same LAN as any host it targets.
/// Aiming a webhook at a LAN service (Home Assistant, a NAS, an internal
/// automation endpoint) is the *intended* use, which the pinned-resolver
/// guard on the other URL classes actively prevents. So this agent uses
/// the DEFAULT resolver (`new_with_config`) — no private-address block, no
/// DNS pinning — while keeping the two properties that are about
/// robustness rather than SSRF: bounded timeouts (a dead receiver must not
/// wedge the per-delivery thread) and `max_redirects(0)` (a 3xx is not a
/// delivery — see `webhook::deliver`).
pub(crate) fn webhook_agent() -> ureq::Agent {
    let config = ureq::config::Config::builder()
        .max_redirects(0)
        .timeout_connect(Some(std::time::Duration::from_secs(5)))
        .timeout_recv_response(Some(std::time::Duration::from_secs(30)))
        .timeout_recv_body(Some(STALL_TIMEOUT))
        .build();
    ureq::Agent::new_with_config(config)
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
pub fn guarded_get(url: &str) -> Result<ureq::http::Response<ureq::Body>, String> {
    guarded_get_within(url, KEYDB_TRANSFER_BUDGET)
}

/// End-to-end ceiling on the unauthenticated `/api` KEYDB update.
///
/// Deliberately tighter than [`KEYDB_TRANSFER_BUDGET`]: this path is reachable
/// without authentication, holds an in-flight handler slot and the
/// process-wide update flag that 429s every other attempt, so a hostile peer
/// must not be able to hold it for minutes.
pub(crate) const KEYDB_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// How long a KEYDB body may take IN TOTAL, once headers are in.
///
/// [`guarded_agent`]'s 30 s is right for a webhook POST and far too short
/// here: `read_capped_keydb_body` accepts up to [`KEYDB_MAX_BYTES`], and 30 s
/// is the ceiling on the whole transfer (see
/// [`guarded_agent_with_timeouts`]), so a keydb that takes longer than half a
/// minute to arrive fails on a link that is merely slow rather than broken —
/// and the daily refresh thread then retries on the same too-short budget
/// once every 24 hours.
///
/// A dead peer is still caught in [`STALL_TIMEOUT`] seconds, because the idle
/// timeout is rolling and independent of this ceiling. This number only buys
/// patience for a transfer that is actually progressing.
///
/// Sized to the REAL artifact, not to [`KEYDB_MAX_BYTES`]: that 100 MiB is a
/// defensive ceiling on what will be read, and a published keydb is a
/// single-digit-MB compressed export. Two minutes covers ~10 MB at well under
/// 1 Mbit/s. Deriving the budget from the DoS cap instead would put a
/// five-minute stall in front of the operator at first boot — `main.rs`
/// fetches the keydb BEFORE the web server thread starts, so a slow keydb
/// host would hold back the very Settings page they would use to fix the URL.
pub(crate) const KEYDB_TRANSFER_BUDGET: std::time::Duration = std::time::Duration::from_secs(120);

/// [`guarded_get`] with an explicit total-transfer budget.
pub(crate) fn guarded_get_within(
    url: &str,
    budget: std::time::Duration,
) -> Result<ureq::http::Response<ureq::Body>, String> {
    let pinned = validate_fetch_url(url)?;
    guarded_agent_with_timeouts(
        pinned,
        std::time::Duration::from_secs(5),
        budget,
        STALL_TIMEOUT,
    )
    .get(url)
    .call()
    // Do NOT embed `e` directly. This was written against ureq 2, whose
    // Display carried the request URL — a token-bearing keydb_url would have
    // reached the system log and thence the unauthenticated /api/system.
    // ureq 3's Display is URL-free on every variant reachable here (`io:
    // {kind}`, `timeout: …`, `connection failed`, `host not found`), so this
    // is no longer load-bearing for THAT leak — but `BadUri` does print the
    // offending URI, and it is one refactor away from this path. Keep the
    // masking and state the real reason. See [`ureq_error_kind`].
    .map_err(|e| format!("fetch failed: {}", ureq_error_kind(&e)))
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

/// The cap for a request that carries a BODY, which the handler thread must
/// read off the socket while holding its admission token.
///
/// Lower than [`MAX_INFLIGHT_HANDLERS`] on purpose, and the gap is the whole
/// point: those remaining slots can only ever be taken by a bodyless request,
/// so no number of stalled POSTs can keep `GET /api/state` — the container
/// healthcheck — from being answered. Without the gap, 64 half-sent POSTs held
/// every slot until their sockets died, the healthcheck 503'd three times, and
/// the daemon was restarted, possibly mid-rip.
///
/// This bounds the DAMAGE, not the stall: the honest fix is a socket read
/// timeout, and tiny_http 0.12 neither sets one nor exposes the stream to set
/// it on. That needs a server change, which is not a thing to do quietly.
const MAX_INFLIGHT_BODY_HANDLERS: usize = 48;

/// The gap between the two caps is what the healthcheck survives on, so it is
/// checked at COMPILE time rather than in a test — equalising them cannot even
/// build. (A test asserting it would be an assertion over two constants, which
/// clippy rejects for exactly this reason: the compiler is the right place.)
const _: () = assert!(MAX_INFLIGHT_BODY_HANDLERS < MAX_INFLIGHT_HANDLERS);

/// Whether a request will make its handler read a body off the socket.
///
/// Read from the headers tiny_http has ALREADY parsed by the time the request
/// is yielded, so this costs nothing and cannot itself block. A chunked or
/// unknown-length body counts too: the length is what the reader waits for.
fn carries_body(request: &tiny_http::Request) -> bool {
    match request.body_length() {
        Some(0) => false,
        Some(_) => true,
        // No Content-Length. For a method that never has a body this is just
        // an ordinary GET; for anything else, assume the reader will wait.
        None => !matches!(
            request.method(),
            tiny_http::Method::Get | tiny_http::Method::Head | tiny_http::Method::Options
        ),
    }
}
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
        let (mux, mv, _, _) = build_queue_views(&staging);
        assert_eq!(mux.len(), 1, "fresh .ripped must be in the Mux queue");
        assert!(mv.is_empty(), "not yet in the Move queue");
        assert!(!both_contain(&mux, &mv));

        // Step 2: mux in flight — `.muxing` added. Out of the Mux queue
        // (shown as the live `_mux` device), still not in Move.
        crate::ripper::staging::write_muxing_marker(&disc);
        let (mux, mv, _, _) = build_queue_views(&staging);
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
        let (mux, mv, _, _) = build_queue_views(&staging);
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
        let (mux, mv, _, _) = build_queue_views(&staging);
        assert!(mux.is_empty());
        assert_eq!(mv.len(), 1);
        assert!(!both_contain(&mux, &mv));
    }

    /// Regression for the double-render bug: the staging dir currently being
    /// moved keeps its `.done` marker throughout the copy, so it appears in the
    /// Move-queue scan — but it is ALSO shown as its live per-artifact progress
    /// bars. `build_queue_views` must exclude the dir named in
    /// `ACTIVE_MOVE_DIR` so it is listed exactly once (as bars, not a queue
    /// row). The old client-side de-dup matched on a punctuation-stripped title
    /// and silently failed for any title containing `:` `/` `*`, etc.; this
    /// exclusion is by exact on-disk basename and so is punctuation-proof.
    #[test]
    fn build_queue_views_excludes_the_actively_moving_dir() {
        use std::fs;
        // Serialize against every test that touches the global move statics.
        let _g = crate::mover::TEST_STATE_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());

        let tmp = tempfile::TempDir::new().unwrap();
        let staging = tmp.path().to_string_lossy().to_string();
        // Two pending moves. On disk the title's colon has been sanitized away
        // (`X-Men: Apocalypse` → `X-Men_Apocalypse`), which is exactly what
        // used to defeat the client-side title match.
        let active = tmp.path().join("X-Men_Apocalypse");
        let other = tmp.path().join("Interstellar");
        fs::create_dir_all(&active).unwrap();
        fs::create_dir_all(&other).unwrap();
        fs::write(active.join(".done"), b"{}").unwrap();
        fs::write(other.join(".done"), b"{}").unwrap();

        // Nothing moving yet: both dirs are queued.
        *crate::mover::ACTIVE_MOVE_DIR
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = None;
        let (_, mv, _, _) = build_queue_views(&staging);
        assert_eq!(mv.len(), 2, "with nothing moving, both .done dirs queue");

        // Mark X-Men as the actively-moving dir (by its on-disk basename).
        *crate::mover::ACTIVE_MOVE_DIR
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some("X-Men_Apocalypse".to_string());
        let (_, mv, _, full) = build_queue_views(&staging);
        assert_eq!(
            mv,
            vec!["Interstellar (moving)".to_string()],
            "the actively-moving dir must be excluded from the queue (shown as bars instead)"
        );
        assert_eq!(
            full, 1,
            "the uncapped count must also exclude the active dir"
        );

        // Clear so no other test observes a stale active dir.
        *crate::mover::ACTIVE_MOVE_DIR
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = None;
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
        let (mux, mv, _, _) = build_queue_views(&staging);
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
        let (mux, mv, _, _) = build_queue_views(&staging);
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
        let (mux, mv, _, _) = build_queue_views(&staging);
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
        let (mux, mv, _, _) = build_queue_views(&staging);
        assert!(
            mux.is_empty(),
            "a .done dir must NOT still be (queued) in the Mux queue"
        );
        assert_eq!(mv.len(), 1, ".done → Move queue");
        assert!(!in_both_queues(&mux, &mv), "BUG #3: never in both queues");
        assert_eq!(device_status(device), Some("done".into()));

        // --- Stage 4: `.completed` lands (terminal). Still Move-only.
        crate::ripper::staging::write_completed_marker(&disc);
        let (mux, mv, _, _) = build_queue_views(&staging);
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
        let (mux, _, _, _) = build_queue_views(&staging);
        assert_eq!(mux.len(), 1, "fresh .ripped is queued for mux");

        // Low-confidence mux success: `.review` instead of `.done`, then
        // `.completed`.
        fs::write(disc.join(".review"), b"{}").unwrap();
        let (mux, mv, _, _) = build_queue_views(&staging);
        assert!(mux.is_empty(), "a .review dir must leave the Mux queue");
        assert!(
            !in_both_queues(&mux, &mv),
            "never in both queues on the review path"
        );

        crate::ripper::staging::write_completed_marker(&disc);
        let (mux, mv, _, _) = build_queue_views(&staging);
        assert!(mux.is_empty());
        assert!(!in_both_queues(&mux, &mv));
    }

    /// FAILURE path: a terminal mux failure writes `.failed` (no `.done`/
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
        let (mux, _, _, _) = build_queue_views(&staging);
        assert_eq!(mux.len(), 1);

        // A terminal mux failure quarantines: `.failed`, tile=error.
        crate::ripper::staging::write_failed_marker(
            &disc,
            "mux finalize failed (unseekable output)",
        );
        crate::ripper::update_state(
            device,
            crate::ripper::RipState {
                device: device.to_string(),
                status: "error".to_string(),
                disc_name: "Lossy Disc".to_string(),
                last_error: "mux finalize failed (unseekable output)".to_string(),
                ..Default::default()
            },
        );
        let (mux, mv, _, _) = build_queue_views(&staging);
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

        let (mux, mv, _, _) = build_queue_views(&staging);
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

    #[test]
    fn queue_view_cache_reuses_within_ttl_and_refreshes_after() {
        // The /events SSE loop calls get_state_json once per second per
        // connected client; build_queue_views_cached exists so those
        // concurrent per-client calls share ONE staging-dir scan instead of
        // each re-walking it. Pin: (1) a second call against the SAME
        // staging dir within the TTL reuses the first scan's result even
        // though the directory changed underneath it, (2) a call against a
        // DIFFERENT staging dir is never served the wrong dir's cache, and
        // (3) once the TTL elapses the next call re-scans and picks up the
        // change.
        use std::fs;
        let tmp_a = tempfile::TempDir::new().unwrap();
        let staging_a = tmp_a.path().to_string_lossy().to_string();
        let disc1 = tmp_a.path().join("First");
        fs::create_dir_all(&disc1).unwrap();
        crate::muxer::write_marker(&disc1, &ripped_marker_for("First", "sg0")).unwrap();

        let (mux1, _, _, _) = build_queue_views_cached(&staging_a);
        assert!(
            mux1.iter().any(|s| s.contains("First")),
            "initial scan must see the pre-existing disc"
        );

        // A different staging dir, scanned right after, must reflect ITS
        // OWN contents (empty), not staging_a's cached entry.
        let tmp_b = tempfile::TempDir::new().unwrap();
        let staging_b = tmp_b.path().to_string_lossy().to_string();
        let (mux_b, _, _, _) = build_queue_views_cached(&staging_b);
        assert!(
            mux_b.is_empty(),
            "a different staging dir must not be served staging_a's cached queue"
        );

        // Add a second disc to staging_a's directory, then immediately
        // re-query staging_a within the TTL window: the cache must still
        // return the STALE (pre-addition) view.
        let disc2 = tmp_a.path().join("Second");
        fs::create_dir_all(&disc2).unwrap();
        crate::muxer::write_marker(&disc2, &ripped_marker_for("Second", "sg1")).unwrap();
        let (mux2, _, _, _) = build_queue_views_cached(&staging_a);
        assert!(
            !mux2.iter().any(|s| s.contains("Second")),
            "a call within the TTL must reuse the cached (stale) scan, not re-walk the dir"
        );

        // After the TTL elapses, the next call must re-scan and see the new disc.
        std::thread::sleep(QUEUE_VIEW_CACHE_TTL + std::time::Duration::from_millis(150));
        let (mux3, _, _, _) = build_queue_views_cached(&staging_a);
        assert!(
            mux3.iter().any(|s| s.contains("Second")),
            "after the TTL expires, the next call must re-scan and see the new disc"
        );
    }

    /// `/api/state` (and therefore `--healthcheck`, and therefore the
    /// Dockerfile HEALTHCHECK) must stay responsive while a staging-dir
    /// refresh is in flight. A slow `read_dir` must never be able to park
    /// every other caller behind it.
    ///
    /// Timing margin: the in-flight scan is held for 1500 ms; the reader is
    /// asserted to return in under 250 ms — a 6x margin over a code path
    /// that, when it does not block, is a HashMap lookup plus two Vec
    /// clones (microseconds). The test FAILS rather than hangs: a blocked
    /// reader returns after the 1500 ms scan and trips the bound.
    #[test]
    fn queue_view_cache_reader_not_blocked_by_in_flight_scan() {
        use std::time::{Duration, Instant};
        const SCAN_MS: u64 = 2000;
        // The reader serves the cached (stale) view — about a millisecond of
        // work — so any measurable delay means it BLOCKED behind the in-flight
        // scan (~SCAN_MS). Bound at HALF the scan: comfortably above a loaded CI
        // runner's scheduling jitter for a cache hit, yet a genuine block
        // (~SCAN_MS) still trips it by a 2× margin. The old fixed 250 ms bound
        // flaked on the macOS leg under parallel-test load.
        const READER_BOUND_MS: u128 = (SCAN_MS / 2) as u128;

        let tmp = tempfile::TempDir::new().unwrap();
        // Unique fixture path (tempdir) so the process-global probe/cache
        // keyed by staging_dir cannot collide with another test.
        let staging = tmp.path().to_string_lossy().to_string();
        let disc = tmp.path().join("Primed");
        std::fs::create_dir_all(&disc).unwrap();
        crate::muxer::write_marker(&disc, &ripped_marker_for("Primed", "sg0")).unwrap();

        // Prime the cache with a fast scan (the steady state on the rig:
        // /api/state has been polled once a second for the container's life).
        let (primed, _, _, _) = build_queue_views_cached(&staging);
        assert!(
            primed.iter().any(|s| s.contains("Primed")),
            "priming scan must see the pre-existing disc"
        );

        // Now make this dir's scan pathologically slow, and let the cached
        // entry age past the TTL so the next caller triggers a refresh.
        queue_scan_probe::arm(&staging, SCAN_MS);
        std::thread::sleep(QUEUE_VIEW_CACHE_TTL + Duration::from_millis(50));

        let s2 = staging.clone();
        let refresher = std::thread::spawn(move || build_queue_views_cached(&s2));

        // Bounded wait until the slow scan is genuinely in flight.
        let spin = Instant::now();
        while queue_scan_probe::scans(&staging) < 1 && spin.elapsed() < Duration::from_millis(500) {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(
            queue_scan_probe::scans(&staging),
            1,
            "the slow refresh scan never started; test setup is wrong"
        );

        // The healthcheck-equivalent read, concurrent with that scan.
        let t0 = Instant::now();
        let (mux, _, _, _) = build_queue_views_cached(&staging);
        let elapsed = t0.elapsed();
        let _ = refresher.join();

        assert!(
            mux.iter().any(|s| s.contains("Primed")),
            "a reader served during a refresh must still get a usable (stale) queue view"
        );
        assert!(
            elapsed.as_millis() < READER_BOUND_MS,
            "/api/state reader blocked {elapsed:?} behind an in-flight staging scan \
             (bound {READER_BOUND_MS}ms, scan {SCAN_MS}ms) — a stalled scan can stall \
             the Docker healthcheck"
        );
    }

    /// The counterpart guard: fixing the blocking above must NOT turn the
    /// cache into a thundering herd. N concurrent cold callers must produce
    /// ONE scan of the staging dir, not N.
    #[test]
    fn queue_view_cache_single_flights_concurrent_callers() {
        const SCAN_MS: u64 = 300;
        const CALLERS: usize = 8;

        let tmp = tempfile::TempDir::new().unwrap();
        let staging = tmp.path().to_string_lossy().to_string();
        let disc = tmp.path().join("Solo");
        std::fs::create_dir_all(&disc).unwrap();
        crate::muxer::write_marker(&disc, &ripped_marker_for("Solo", "sg0")).unwrap();

        // Armed BEFORE the first call: this dir has never been scanned, so
        // every caller below is a cold miss racing every other one.
        queue_scan_probe::arm(&staging, SCAN_MS);

        let handles: Vec<_> = (0..CALLERS)
            .map(|_| {
                let s = staging.clone();
                std::thread::spawn(move || build_queue_views_cached(&s))
            })
            .collect();
        for h in handles {
            let (mux, _, _, _) = h.join().expect("caller thread panicked");
            assert!(
                mux.iter().any(|s| s.contains("Solo")),
                "every concurrent caller must get the real queue view"
            );
        }
        assert_eq!(
            queue_scan_probe::scans(&staging),
            1,
            "{CALLERS} concurrent callers caused {} staging scans; single-flight is broken \
             (thundering herd on the staging dir)",
            queue_scan_probe::scans(&staging)
        );

        // And a further caller inside the TTL window still re-uses it.
        let _ = build_queue_views_cached(&staging);
        assert_eq!(
            queue_scan_probe::scans(&staging),
            1,
            "a caller within the TTL must be served from cache"
        );
    }

    /// Run `build_queue_views_cached(dir)` on a scratch thread and return its
    /// result, or `None` if it had not returned within `bound`.
    ///
    /// Every timing assertion below goes through this: the call under test is
    /// the thing that might block forever, so a regression must surface as a
    /// FAILED assertion, not as a suite that hangs until CI's job timeout.
    /// The scratch thread is deliberately never joined on the timeout path —
    /// a wedged scan is exactly what we are simulating, and the process exits
    /// fine with it parked.
    fn cached_within(
        dir: &str,
        bound: std::time::Duration,
    ) -> Option<(Vec<String>, Vec<String>, usize, usize)> {
        let (tx, rx) = std::sync::mpsc::channel();
        let d = dir.to_string();
        std::thread::spawn(move || {
            let _ = tx.send(build_queue_views_cached(&d));
        });
        rx.recv_timeout(bound).ok()
    }

    /// Spin until this dir has taken `n` scans, or `bound` elapses.
    fn await_scans(dir: &str, n: usize, bound: std::time::Duration) -> bool {
        let t0 = std::time::Instant::now();
        while queue_scan_probe::scans(dir) < n {
            if t0.elapsed() >= bound {
                return false;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        true
    }

    /// A refresher that HANGS — a wedged `read_dir` on an unresponsive mount,
    /// no panic needed — must not latch the single-flight marker forever.
    ///
    /// The `refreshing` marker was a plain bool cleared only by the refresher
    /// itself, so a refresher that never returns left it set for the process
    /// lifetime: every later caller took the serve-stale branch and the queue
    /// views froze permanently. `/api/state` kept 200-ing with a snapshot from
    /// the moment the mount wedged, so nothing surfaced the freeze either.
    ///
    /// Timing margin: the wedged scan is held for 60 s; this dir's marker
    /// deadline is cut to 300 ms; the takeover is asserted to land within
    /// 4 s — a >13x margin over the deadline, and 1/200th of the wedge, so
    /// a pass can only mean a real takeover. Bounded by `cached_within`, so
    /// a regression that blocks the caller FAILS instead of hanging.
    #[test]
    fn queue_view_cache_recovers_from_a_refresher_that_never_returns() {
        use std::time::Duration;
        const WEDGE_MS: u64 = 60_000;
        const DEADLINE_MS: u64 = 300;
        const TAKEOVER_BOUND: Duration = Duration::from_secs(4);

        let tmp = tempfile::TempDir::new().unwrap();
        // Unique to this test: the cache, the probe and STATE are all
        // process-global, so a shared fixture name would be a real race.
        let staging = tmp.path().to_string_lossy().to_string();
        let first = tmp.path().join("WedgeFirst");
        std::fs::create_dir_all(&first).unwrap();
        crate::muxer::write_marker(&first, &ripped_marker_for("WedgeFirst", "sg0")).unwrap();

        // Steady state: the cache is warm, as it is on the rig after the
        // first second of /api/state polling.
        let (primed, _, _, _) =
            cached_within(&staging, Duration::from_secs(5)).expect("priming scan must return");
        assert!(
            primed.iter().any(|s| s.contains("WedgeFirst")),
            "priming scan must see the pre-existing disc"
        );

        queue_scan_probe::set_refresh_deadline(&staging, DEADLINE_MS);
        // The mount wedges. Age the entry past the TTL so the next caller
        // owns the refresh, then let that caller disappear into `read_dir`.
        queue_scan_probe::arm(&staging, WEDGE_MS);
        std::thread::sleep(QUEUE_VIEW_CACHE_TTL + Duration::from_millis(50));
        let wedged = staging.clone();
        std::thread::spawn(move || build_queue_views_cached(&wedged));
        assert!(
            await_scans(&staging, 1, Duration::from_secs(2)),
            "the wedged refresh never started; test setup is wrong"
        );

        // Reality moves on underneath the wedged refresher.
        let second = tmp.path().join("WedgeSecond");
        std::fs::create_dir_all(&second).unwrap();
        crate::muxer::write_marker(&second, &ripped_marker_for("WedgeSecond", "sg1")).unwrap();
        // ...and the mount comes back for anyone who tries again. The
        // original refresher is still parked in its 60 s `read_dir`.
        queue_scan_probe::arm(&staging, 0);

        // Poll as /api/state does. Once the marker's deadline passes, some
        // caller must take the refresh over and publish the new disc.
        let deadline = std::time::Instant::now() + TAKEOVER_BOUND;
        let mut saw_second = false;
        while std::time::Instant::now() < deadline {
            let (mux, _, _, _) = cached_within(&staging, TAKEOVER_BOUND)
                .expect("a caller blocked past the takeover bound behind a wedged refresh");
            if mux.iter().any(|s| s.contains("WedgeSecond")) {
                saw_second = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        assert!(
            saw_second,
            "the single-flight marker latched on a refresher that never returned: \
             the queue views are frozen at the moment the mount wedged and will \
             stay frozen for the process lifetime"
        );
    }

    /// The cold-path counterpart: when there is NOTHING to serve and the
    /// in-flight first scan is wedged, callers must neither park forever nor
    /// pile up.
    ///
    /// The old cold valve abandoned single-flight — after 5 s each waiter went
    /// and scanned for itself — so on a wedged staging mount one HTTP worker
    /// thread (and its admission token) was consumed every 5 s until
    /// `/api/state` started 503-ing and the container HEALTHCHECK restarted
    /// the daemon mid-rip.
    ///
    /// Timing margin: the wedged scan is held for 60 s and the cold wait is
    /// cut to 200 ms; callers are asserted to return within 3 s — 15x the cold
    /// wait, 1/20th of the wedge. `cached_within` bounds every call, so both
    /// failure modes (park forever / pile up behind a 60 s scan) FAIL here
    /// rather than hang.
    #[test]
    fn queue_view_cache_cold_callers_neither_park_nor_pile_up_on_a_wedged_scan() {
        use std::time::Duration;
        const WEDGE_MS: u64 = 60_000;
        const COLD_WAIT_MS: u64 = 200;
        const CALLER_BOUND: Duration = Duration::from_secs(3);
        const CALLERS: usize = 6;

        let tmp = tempfile::TempDir::new().unwrap();
        let staging = tmp.path().to_string_lossy().to_string();
        let disc = tmp.path().join("ColdWedge");
        std::fs::create_dir_all(&disc).unwrap();
        crate::muxer::write_marker(&disc, &ripped_marker_for("ColdWedge", "sg0")).unwrap();

        // Armed before the first ever call: this key is genuinely cold, so
        // there is no snapshot to fall back on.
        queue_scan_probe::arm(&staging, WEDGE_MS);
        queue_scan_probe::set_cold_wait(&staging, COLD_WAIT_MS);
        let wedged = staging.clone();
        std::thread::spawn(move || build_queue_views_cached(&wedged));
        assert!(
            await_scans(&staging, 1, Duration::from_secs(2)),
            "the wedged cold scan never started; test setup is wrong"
        );

        // Every subsequent caller must come back — degraded is fine, wedged
        // is not. This is the /api/state + --healthcheck path.
        for i in 0..CALLERS {
            assert!(
                cached_within(&staging, CALLER_BOUND).is_some(),
                "cold caller {i} was still parked after {CALLER_BOUND:?} behind a wedged \
                 first scan — /api/state (and the Docker HEALTHCHECK) stalls with it"
            );
        }

        // ...and none of them may launch a scan of its own: that is the
        // accumulation that eats an HTTP worker per giving-up caller.
        assert_eq!(
            queue_scan_probe::scans(&staging),
            1,
            "{CALLERS} cold callers launched {} scans of a wedged staging dir; the cold \
             valve abandoned single-flight, so each one burns an HTTP worker thread and \
             its admission token until /api/state 503s",
            queue_scan_probe::scans(&staging),
        );
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

    /// `handle_settings_post` must finish mutating and DROP the config write
    /// guard before it saves: `config::save` does `fs::write` + `fs::rename`
    /// on `/config/settings.json`, and on NFS those can hang indefinitely,
    /// blocking every concurrent `cfg.read()` — which is nearly the whole
    /// `/api/*` surface (the 0.20.8 lock stall).
    ///
    /// This is an ORDERING property inside one private function, so it is
    /// pinned against that function's own source, the way the resume-path
    /// invariants in `ripper/resume.rs` are. The test it replaces lived in
    /// `tests/watchdog.rs` and re-implemented the pattern inline — it never
    /// called the handler, so reinstating the bug left it green.
    ///
    /// The behavioural half (the handler really does persist through
    /// `config::save`) is covered by `http::settings_post_persists_a_field_to_disk`.
    #[test]
    fn settings_post_saves_outside_the_config_write_guard() {
        let src = crate::util::source_lf(include_str!("web.rs"));
        // Leading newline so this does not match the literal on this line.
        let start = src
            .find("\nfn handle_settings_post(")
            .expect("web.rs must define handle_settings_post");
        let body = &src[start..];

        // The mutation window: a `snapshot` bound from a block that takes
        // `cfg.write()`, closing at the block's `};`.
        let snap = body
            .find("let snapshot: Config = {")
            .expect("handle_settings_post must snapshot the config out of the guard");
        assert!(
            body[snap..].starts_with("let snapshot: Config = {")
                && body[snap..]
                    .find("cfg.write()")
                    .is_some_and(|w| w < body[snap..].find("\n    };").unwrap_or(usize::MAX)),
            "the write guard must be taken INSIDE the snapshot block"
        );
        let guard_end = snap
            + body[snap..]
                .find("\n    };")
                .expect("the snapshot block must close at function-body indentation");

        let save = guard_end
            + body[guard_end..]
                .find("config::save(")
                .expect("handle_settings_post must call config::save");

        // Nothing may re-take the write guard between the snapshot block and
        // the save — that is the whole ordering.
        assert!(
            !body[guard_end..save].contains("cfg.write()"),
            "the config write guard must not be held across config::save; \
             re-taking it before the save reintroduces the 0.20.8 lock stall"
        );
        // And the save must be handed to the bounded-syscall worker, which
        // owns the snapshot — a guard cannot travel with it.
        assert!(
            body[guard_end..save].contains("std::thread::Builder::new()"),
            "config::save must run on the bounded save worker, not inline on \
             the handler thread"
        );
    }

    // NOTE: the keyserver_url sentinel round-trip is now tested
    // executing-style by `http::settings_post_masked_keyserver_url_preserves_stored`
    // (it drives the real handle_settings_post via a live server + config::save,
    // not an inline re-implementation of the guard).

    /// A stored webhook entry that fires on every stage — the common case in
    /// these tests, which predate per-stage flags and care only about URL
    /// masking/resolution.
    fn we(url: &str) -> WebhookEntry {
        WebhookEntry {
            url: url.to_string(),
            post_rip: true,
            post_mux: true,
            post_move: true,
        }
    }

    /// An incoming (POST-side) webhook with all flags set — mirrors what the
    /// UI sends for a fire-on-every-stage hook.
    fn inc(url: &str) -> IncomingWebhook {
        IncomingWebhook {
            url: url.to_string(),
            post_rip: true,
            post_mux: true,
            post_move: true,
        }
    }

    /// Just the resolved URLs, for asserting URL resolution independently of
    /// the flags (which these tests carry through unchanged).
    fn urls(entries: &[WebhookEntry]) -> Vec<String> {
        entries.iter().map(|e| e.url.clone()).collect()
    }

    #[test]
    fn settings_get_masks_webhook_token_keeps_origin() {
        // Webhook URLs embed bearer tokens (Discord/Slack/Jellyfin) in the
        // path, so a GET must mask the token — but keep the origin visible so
        // the operator can tell which hook is which.
        let c = Config {
            webhook_urls: vec![
                we("https://discord.com/api/webhooks/123/secrettoken"),
                we(""),
                we("https://hooks.slack.com/services/AAA/BBB/cccsecret"),
            ],
            ..Config::default()
        };
        let json: serde_json::Value = serde_json::from_str(&settings_json_redacted(&c)).unwrap();
        let arr = json["webhook_urls"].as_array().unwrap();
        // Each entry serializes as an object; only the `url` is masked and it
        // carries a stable per-entry index (#<pos>) so two same-origin hooks
        // round-trip unambiguously (#8). The flags pass through untouched.
        assert_eq!(arr[0]["url"], "https://discord.com/********#0");
        assert_eq!(arr[0]["post_rip"], true);
        assert_eq!(arr[0]["post_mux"], true);
        assert_eq!(arr[0]["post_move"], true);
        // Empty entry stays empty (no sentinel) so the UI shows a blank row.
        assert_eq!(arr[1]["url"], "");
        assert_eq!(arr[2]["url"], "https://hooks.slack.com/********#2");
        // The masked form must NOT leak the token.
        assert!(!arr[0]["url"].as_str().unwrap().contains("secrettoken"));
        assert!(!arr[2]["url"].as_str().unwrap().contains("cccsecret"));
    }

    #[test]
    fn settings_get_redacts_keydb_path_to_filename() {
        // keydb_path is an absolute container path (mount layout, username);
        // GET /api/settings must strip it down to the bare filename so a LAN
        // client can confirm which file is active without learning the
        // container's filesystem layout.
        let c = Config {
            keydb_path: Some("/data/keys/subdir/KEYDB.cfg".into()),
            ..Config::default()
        };
        let json: serde_json::Value = serde_json::from_str(&settings_json_redacted(&c)).unwrap();
        assert_eq!(json["keydb_path"], "KEYDB.cfg");
        assert!(
            !json["keydb_path"].as_str().unwrap().contains('/'),
            "redacted keydb_path must not leak any directory component"
        );
        // No keydb_path set (None) — field passes through untouched (null),
        // no panic, nothing to redact.
        let json_none: serde_json::Value =
            serde_json::from_str(&settings_json_redacted(&Config::default())).unwrap();
        assert!(json_none["keydb_path"].is_null());
        // An explicitly empty string is left alone (not redacted into "" ->
        // something else), matching the other secret fields' "empty stays
        // empty" convention.
        let c_empty = Config {
            keydb_path: Some(String::new()),
            ..Config::default()
        };
        let json_empty: serde_json::Value =
            serde_json::from_str(&settings_json_redacted(&c_empty)).unwrap();
        assert_eq!(json_empty["keydb_path"], "");
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
    fn is_masked_webhook_recognizes_only_real_placeholders() {
        // Bare sentinel (mask_webhook_url's own output, e.g. no identifiable
        // scheme).
        assert!(is_masked_webhook(SECRET_SENTINEL));
        // Indexed placeholder form produced by mask_webhook_url_indexed.
        assert!(is_masked_webhook(&format!(
            "https://discord.com/{SECRET_SENTINEL}#1"
        )));
        // Index 0 is still a valid, non-empty all-digit index.
        assert!(is_masked_webhook(&format!(
            "https://discord.com/{SECRET_SENTINEL}#0"
        )));
        // Empty index after '#' must NOT be treated as masked.
        assert!(!is_masked_webhook(&format!(
            "https://discord.com/{SECRET_SENTINEL}#"
        )));
        // Non-digit index must NOT be treated as masked.
        assert!(!is_masked_webhook(&format!(
            "https://discord.com/{SECRET_SENTINEL}#abc"
        )));
        // A hostile, never-masked URL that merely ends in "#<digits>" must NOT
        // be misclassified as a redacted placeholder — this is the exact SSRF
        // bypass an &&->|| mutation in is_masked_webhook would open up: a
        // plain metadata URL with a `#1` fragment must still be validated.
        assert!(!is_masked_webhook("http://169.254.169.254/x#1"));
        // No '#' at all, and no sentinel — not masked.
        assert!(!is_masked_webhook("http://example.com/hook/realtoken"));
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

    /// One question, one predicate. A genuine URL that merely EMBEDS the
    /// sentinel is not masked (`is_masked_webhook` says so, and the SSRF
    /// filter therefore validates it as real) — so the resolver must take it
    /// verbatim. While the resolver used `contains` instead, that URL was
    /// validated as genuine and then rejected 400 as an "ambiguous masked
    /// webhook entry", an error about a masking that never happened, and the
    /// whole settings save died with no way to enter the URL at all.
    #[test]
    fn a_url_that_only_embeds_the_sentinel_is_saved_verbatim() {
        // Not masked by the strict predicate — the filter validates it.
        let embedded = format!("https://example.com/hook/{SECRET_SENTINEL}/tail");
        assert!(
            !is_masked_webhook(&embedded),
            "fixture must be a NON-masked URL for this test to mean anything"
        );

        let existing = vec![we("https://discord.com/api/webhooks/1/aaa")];
        let resolved = resolve_webhook_entries(&[inc(&embedded)], &existing)
            .expect("a genuine URL must not be rejected as an ambiguous placeholder");
        assert_eq!(
            urls(&resolved),
            vec![embedded],
            "a non-masked entry is taken verbatim"
        );
    }

    #[test]
    fn webhook_post_sentinel_preserves_stored_url() {
        // A GET→POST round-trip of the redacted form must NOT wipe the
        // token-bearing stored URL. A masked placeholder resolves back to its
        // stored secret by origin; a real entry replaces; an empty entry drops.
        let existing = vec![
            we("https://discord.com/api/webhooks/1/aaa"),
            we("https://hooks.slack.com/services/x/y/zzz"),
        ];
        let incoming = [
            inc("https://discord.com/********"), // masked → keep discord secret
            inc("https://example.com/new-hook"), // changed → replace
        ];
        let resolved = resolve_webhook_entries(&incoming, &existing).unwrap();
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].url, "https://discord.com/api/webhooks/1/aaa");
        assert_eq!(resolved[1].url, "https://example.com/new-hook");
    }

    #[test]
    fn webhook_post_masked_resolves_by_origin_not_position() {
        // HIGH regression: stored = [discord=secretA, slack=secretB]. The UI
        // reorders the masked rows to [slack-masked, discord-masked]. Resolving
        // BY POSITION would bind slack's row to discord's secret and vice
        // versa — a silent secret-confusion bug. By origin, each masked entry
        // must resolve to ITS OWN stored secret regardless of order.
        let existing = vec![
            we("https://discord.com/api/webhooks/1/secretA"),
            we("https://hooks.slack.com/services/x/y/secretB"),
        ];
        // Reordered: slack first, discord second (each still masked).
        let reordered = [
            inc("https://hooks.slack.com/********"),
            inc("https://discord.com/********"),
        ];
        let resolved = resolve_webhook_entries(&reordered, &existing).unwrap();
        assert_eq!(
            urls(&resolved),
            vec![
                "https://hooks.slack.com/services/x/y/secretB".to_string(),
                "https://discord.com/api/webhooks/1/secretA".to_string(),
            ],
            "each masked entry must carry its own origin's secret, not the other's"
        );

        // Deleting the discord row and keeping only the (masked) slack row must
        // still resolve slack correctly — never to discord's secret.
        let only_slack = [inc("https://hooks.slack.com/********")];
        let resolved = resolve_webhook_entries(&only_slack, &existing).unwrap();
        assert_eq!(
            urls(&resolved),
            vec!["https://hooks.slack.com/services/x/y/secretB".to_string()]
        );
    }

    #[test]
    fn webhook_post_masked_unresolvable_origin_is_rejected() {
        // A masked entry whose origin matches NO stored URL (the referenced row
        // was deleted) is ambiguous — reject rather than guess. Likewise when
        // two stored hooks share an origin (>1 match).
        let existing = vec![we("https://discord.com/api/webhooks/1/aaa")];
        // Masked slack origin has no stored counterpart → Err.
        let orphan = [inc("https://hooks.slack.com/********")];
        assert!(resolve_webhook_entries(&orphan, &existing).is_err());

        // Two stored discord hooks share an origin → a masked discord entry is
        // ambiguous (>1 match) → Err.
        let two_discord = vec![
            we("https://discord.com/api/webhooks/1/aaa"),
            we("https://discord.com/api/webhooks/2/bbb"),
        ];
        let masked = [inc("https://discord.com/********")];
        assert!(resolve_webhook_entries(&masked, &two_discord).is_err());
    }

    #[test]
    fn webhook_two_same_origin_round_trip_by_index() {
        // Regression (#8): two webhooks that share an origin used to mask to the
        // SAME placeholder, so a GET→POST round-trip was ambiguous (>1 origin
        // match) and the save was permanently rejected. With a stable per-entry
        // index embedded in the mask, each resolves to its OWN stored secret.
        let existing = vec![
            we("https://discord.com/api/webhooks/1/secretA"),
            we("https://discord.com/api/webhooks/2/secretB"),
        ];
        // Exactly what GET /api/settings now emits.
        let masked0 = mask_webhook_url_indexed(&existing[0].url, 0);
        let masked1 = mask_webhook_url_indexed(&existing[1].url, 1);
        assert_ne!(masked0, masked1, "same-origin masks must differ by index");

        let incoming = [inc(&masked0), inc(&masked1)];
        let resolved = resolve_webhook_entries(&incoming, &existing).unwrap();
        assert_eq!(
            urls(&resolved),
            vec![
                "https://discord.com/api/webhooks/1/secretA".to_string(),
                "https://discord.com/api/webhooks/2/secretB".to_string(),
            ],
            "each indexed mask must resolve to its own stored secret"
        );

        // A stale index whose origin mask no longer matches must be rejected,
        // not silently bound to the wrong secret.
        let stale = [inc(&mask_webhook_url_indexed("https://discord.com/x", 5))];
        assert!(resolve_webhook_entries(&stale, &existing).is_err());
    }

    #[test]
    fn resolve_webhook_entries_carries_flags_through_masking() {
        // The per-event flags come from the INCOMING request, never from the
        // stored entry — resolving a masked URL back to its stored secret must
        // NOT also restore the stored entry's old flags. Here the stored hook
        // fired on both; the client re-saves it (masked URL) as move-only, and
        // that new intent must win while the secret URL is preserved.
        let existing = vec![WebhookEntry {
            url: "https://discord.com/api/webhooks/1/secretA".into(),
            post_rip: true,
            post_mux: true,
            post_move: true,
        }];
        let masked = mask_webhook_url_indexed(&existing[0].url, 0);
        let incoming = [IncomingWebhook {
            url: masked,
            post_rip: false,
            post_mux: false,
            post_move: true,
        }];
        let resolved = resolve_webhook_entries(&incoming, &existing).unwrap();
        assert_eq!(
            resolved,
            vec![WebhookEntry {
                url: "https://discord.com/api/webhooks/1/secretA".into(),
                post_rip: false,
                post_mux: false,
                post_move: true,
            }],
            "URL resolves to the stored secret but the flags follow the new request"
        );
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
    fn blocks_multicast_ipv4_and_ipv6() {
        use std::net::{Ipv4Addr, Ipv6Addr};
        // Pure multicast, not caught by any of the loopback/private/
        // link-local/broadcast/documentation/unspecified/CGN/0.x/240+
        // branches — this is only reachable via is_multicast(). An
        // `||`->`&&` mutant immediately before is_multicast() in the IPv4
        // chain (folding it into `is_unspecified() && is_multicast()`,
        // which can never be true) would let this through.
        assert!(is_blocked_ip(&IpAddr::V4(Ipv4Addr::new(230, 1, 2, 3))));
        // IPv6 multicast, not unspecified — same shape of gap on the v6 side.
        assert!(is_blocked_ip(&IpAddr::V6(Ipv6Addr::new(
            0xff02, 0, 0, 0, 0, 0, 0, 1
        ))));
    }

    #[test]
    fn cgn_check_does_not_over_block_unrelated_public_space() {
        use std::net::Ipv4Addr;
        // Carrier-grade NAT 100.64.0.0/10: octet[0]==100 AND top two bits of
        // octet[1] == 01 (0x40..0x7f). 100.64.0.1 is inside the range and
        // must be blocked; 100.128.0.1 has octet[1]=128 (0x80, top bits 10)
        // so it is OUTSIDE the /10 and must be allowed. A `&&`->`||` mutant
        // in the CGN check blocks both (and much unrelated public space with
        // octet[0]!=100 too).
        assert!(is_blocked_ip(&IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1))));
        assert!(!is_blocked_ip(&IpAddr::V4(Ipv4Addr::new(100, 128, 0, 1))));
        // A public address whose second octet has the CGN-like bit pattern
        // (01xxxxxx) but whose first octet is NOT 100 must NOT be blocked —
        // pins the `&&` (not `||`) between the two octet checks.
        assert!(!is_blocked_ip(&IpAddr::V4(Ipv4Addr::new(1, 65, 2, 3))));
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

    /// A KEYDB body that is SLOW but PROGRESSING must be allowed to finish.
    ///
    /// ureq 2's `timeout_read` was a per-read bound, so a slow transfer never
    /// tripped it. ureq 3's `timeout_recv_response` — what the 2→3 migration
    /// replaced it with — is an ABSOLUTE deadline anchored at header
    /// completion that also caps the body, so `guarded_agent`'s 30 s became a
    /// hard 30 s ceiling on the whole download. Measured against a real
    /// socket: with a 2 s bound, a server trickling a byte every 500 ms dies
    /// at 2.0 s having delivered four bytes.
    ///
    /// NOTE what this does NOT prove: delete `timeout_recv_body` entirely and
    /// this test still passes, because with no body timeout at all the slow
    /// body also arrives. Removal is caught by its sibling
    /// (`a_stalled_body_is_cut_off_by_the_idle_bound_not_the_total_budget`),
    /// which was proven red at 30 s and green at 1 s. This one guards the
    /// complementary property, and the two are only meaningful together.
    ///
    /// What this guards is the NEW idle knob: `timeout_recv_body` is ROLLING,
    /// so a body that keeps arriving must survive even when the transfer takes
    /// many times the idle bound. Wire it up as a total instead — the easy
    /// mistake — and this test fails. It is a guard on the fix, not a
    /// reproduction of the original defect: the budget change itself
    /// (`guarded_get`'s 30 s → `KEYDB_TRANSFER_BUDGET`) has no automated proof
    /// here, because `guarded_get` resolves and rejects loopback before it
    /// connects, so no local listener can stand in for a keydb mirror.
    #[test]
    fn a_slow_but_progressing_keydb_body_is_not_killed_by_the_header_deadline() {
        use std::io::{Read as _, Write as _};
        use std::net::TcpListener;

        let listener =
            TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).expect("bind stub listener");
        let pinned = listener.local_addr().expect("stub listener address");

        let server = std::thread::spawn(move || {
            let (mut sock, _peer) = listener.accept().expect("accept failed");
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") {
                match sock.read(&mut byte) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => head.push(byte[0]),
                }
            }
            let _ = sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 40\r\n\r\n");
            let _ = sock.flush();
            // Forty bytes, 100 ms apart: ~4 s of body, with no single gap
            // anywhere near the idle bound. The TOTAL is what matters — see
            // the timeout comment below.
            for _ in 0..40 {
                if sock.write_all(b"k").is_err() {
                    return;
                }
                let _ = sock.flush();
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        });

        // The two numbers this test lives or dies by:
        //
        //   per-gap margin: 100 ms between writes against a 1 s idle bound —
        //     10x, so a loaded CI box cannot fail this by scheduling alone.
        //   total overrun:  ~4 s of body against that same 1 s bound — 4x, so
        //     a ROLLING bound passes and any TOTAL interpretation fails.
        //
        // Both are required, and the second was missing. An earlier revision
        // widened the idle bound to 5 s while shortening the writes to 100 ms,
        // which left a 0.8 s body inside every deadline in play: the doc above
        // still claimed "wire it up as a total instead and this test fails",
        // and it no longer did. Nor did shrinking the 30 s response ceiling to
        // 2 s. The guard had quietly become an assertion that a fast download
        // finishes.
        let agent = guarded_agent_with_timeouts(
            vec![pinned],
            std::time::Duration::from_secs(5),
            std::time::Duration::from_secs(30),
            std::time::Duration::from_secs(1),
        );
        let resp = agent
            .get("http://keydb-mirror.test/keydb.zip")
            .call()
            .expect("headers must arrive");
        let mut body = Vec::new();
        let read = resp.into_body().into_reader().read_to_end(&mut body);
        let _ = server.join();

        assert!(
            read.is_ok(),
            "a steadily-progressing body was aborted: {:?}",
            read.err()
        );
        assert_eq!(body, vec![b'k'; 40], "the whole body must arrive");
    }

    /// The other half: a peer that sends headers and then NOTHING must be cut
    /// off by the rolling idle bound, not held until the (much larger) total
    /// budget expires. This is the protection ureq 2's `timeout_read` gave and
    /// the migration dropped; without `timeout_recv_body` a dead peer would be
    /// held for the whole KEYDB budget.
    #[test]
    fn a_stalled_body_is_cut_off_by_the_idle_bound_not_the_total_budget() {
        use std::io::{Read as _, Write as _};
        use std::net::TcpListener;

        let listener =
            TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).expect("bind stub listener");
        let pinned = listener.local_addr().expect("stub listener address");

        let server = std::thread::spawn(move || {
            let (mut sock, _peer) = listener.accept().expect("accept failed");
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") {
                match sock.read(&mut byte) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => head.push(byte[0]),
                }
            }
            let _ = sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1048576\r\n\r\n");
            let _ = sock.flush();
            // Promise a megabyte and send none of it — but hold the socket by
            // BLOCKING ON A READ, not by sleeping. The read returns the moment
            // the client gives up and drops the connection, so this thread
            // ends with the test instead of outliving it. A `sleep(30s)` here
            // detached a thread holding an accepted socket for ~28 s after the
            // test returned, overlapping every other test in the binary.
            let mut sink = [0u8; 1];
            let _ = sock.read(&mut sink);
        });

        let idle = std::time::Duration::from_secs(1);
        let agent = guarded_agent_with_timeouts(
            vec![pinned],
            std::time::Duration::from_secs(5),
            // A total budget far larger than the idle bound, so only the idle
            // bound can be what ends this.
            std::time::Duration::from_secs(120),
            idle,
        );
        let started = std::time::Instant::now();
        let resp = agent
            .get("http://keydb-mirror.test/keydb.zip")
            .call()
            .expect("headers must arrive");
        let mut body = Vec::new();
        let read = resp.into_body().into_reader().read_to_end(&mut body);
        let elapsed = started.elapsed();

        assert!(read.is_err(), "a stalled body must not read as success");
        assert!(
            elapsed < std::time::Duration::from_secs(20),
            "a stalled peer was held for {elapsed:?} — the idle bound did not fire, \
             so the total budget is the only thing ending this"
        );
        // Joinable because the stub ends on client disconnect; `drop` on a
        // JoinHandle only detaches, it does not stop the thread.
        let _ = server.join();
    }

    /// The tests above prove which addresses `validate_fetch_url` REJECTS.
    /// None of them makes a connection, so every one still passes if
    /// `guarded_agent_with_timeouts` ignores the pinned addresses and resolves
    /// through live DNS instead — which is exactly how the rebinding TOCTOU
    /// gets back in, with no visible symptom. This is the test that notices.
    ///
    /// Pin the agent at a loopback listener this test owns, then ask for a
    /// host that cannot resolve (`.test`, reserved by RFC 6761). Only a
    /// consulted resolver can turn that name into a connection. Touches no
    /// network, and drives `guarded_agent_with_timeouts` directly, since
    /// `guarded_get`'s guard blocks loopback by design.
    #[test]
    fn guarded_agent_connects_to_the_pinned_address_not_dns() {
        use std::io::{Read as _, Write as _};
        use std::net::TcpListener;
        use std::sync::mpsc;

        let listener =
            TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).expect("bind stub listener");
        let pinned = listener.local_addr().expect("stub listener address");
        let (tx, rx) = mpsc::channel();

        let server = std::thread::spawn(move || {
            let (mut sock, _peer) = listener.accept().expect("stub listener accept failed");
            let _ = tx.send(());
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") {
                match sock.read(&mut byte) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => head.push(byte[0]),
                }
            }
            let _ = sock
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nhi");
            let _ = sock.flush();
            head
        });

        let sent = guarded_agent_with_timeouts(
            vec![pinned],
            std::time::Duration::from_secs(5),
            std::time::Duration::from_secs(30),
            STALL_TIMEOUT,
        )
        .get("http://keydb-mirror.test/keydb.zip")
        .call();

        rx.recv_timeout(std::time::Duration::from_secs(10)).expect(
            "guarded_agent never connected to the pinned address — the custom \
             resolver is not being consulted, so a DNS rebind between \
             validate_fetch_url and the fetch can still redirect the request",
        );
        let resp = sent.expect("the pinned round-trip must complete");
        assert_eq!(resp.status(), 200, "the stub server's reply must come back");
        let head = server.join().expect("stub server panicked");
        let head = String::from_utf8_lossy(&head);
        assert!(
            head.contains("keydb-mirror.test"),
            "the pinned agent must still address the original host; got: {head}"
        );
    }

    /// Secret-leak guard: guarded_get error strings from the ureq transport
    /// layer must never embed the full request URL (which may contain a token
    /// in the path/query). ureq 2's Display included the URL, which is what
    /// this guards; ureq 3's does not for the variants reachable here, but
    /// `BadUri` still prints its URI — so the map_err must keep stripping to a
    /// status code / transport kind only.
    ///
    /// We test this via a public literal IP that passes the SSRF guard (so we
    /// reach the ureq call), but where the connection is immediately refused
    /// (no server listening). This exercises the Transport error arm of our
    /// map_err, which is what keeps the full URL out of the message.
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

    /// `is_transient_resolve_error` must say NO to every permanent verdict
    /// `validate_fetch_url` can reach without a network round-trip. Its
    /// consumer (`keysource::probe_online_reachability`) turns a `true` here
    /// into "the key service is down", which parks a disc — so a scheme
    /// error or a blocked address must never be mistaken for a DNS blip.
    /// Driven through the REAL validator, not against hand-written strings.
    #[test]
    fn a_rejected_url_is_never_classified_as_a_failed_lookup() {
        for url in [
            "",
            "ftp://example.com/keys",
            "http://",
            // Literal, so no DNS is involved — the guard rejects the address.
            "http://127.0.0.1:8080/keys",
            "http://169.254.169.254/latest/meta-data",
        ] {
            let err = validate_fetch_url(url)
                .expect_err("this URL must be rejected outright, not accepted");
            assert!(
                !is_transient_resolve_error(&err),
                "{url:?} is a permanent verdict on the URL, but its error \
                 {err:?} classifies as a failed lookup"
            );
        }
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
            // Let the detached resolver thread finish (dropping its ConnGuard)
            // before the next iteration so the slot is reliably released.
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
    fn conn_guard_releases_slot_when_holder_unwinds() {
        // Regression (resolve_with_timeout INFLIGHT leak): the DNS throttle
        // now owns its slot via a ConnGuard moved into the resolver closure,
        // so the slot is released even when the code holding it unwinds
        // instead of returning normally — which is exactly what happens when
        // `thread::spawn` panics (OS refuses a new thread) before the worker
        // body that used to own the decrement could ever run. Model that here
        // by panicking while a guard is in scope and confirming Drop still
        // ran the fetch_sub.
        static C: AtomicUsize = AtomicUsize::new(0);
        let g0 = ConnGuard::try_acquire(&C, 4).expect("first slot");
        assert_eq!(C.load(Ordering::SeqCst), 1);
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = ConnGuard::try_acquire(&C, 4).expect("second slot");
            assert_eq!(C.load(Ordering::SeqCst), 2);
            panic!("simulate thread::spawn unwinding with the guard held");
        }));
        assert!(r.is_err(), "the inner closure must have panicked");
        // The unwound guard's Drop must have released its slot; only g0 remains.
        assert_eq!(
            C.load(Ordering::SeqCst),
            1,
            "guard slot leaked across an unwind"
        );
        drop(g0);
        assert_eq!(C.load(Ordering::SeqCst), 0);
    }

    // ── A stalled POST must not starve the healthcheck ────────────────────
    //
    // tiny_http yields a request once its HEADERS parse; the handler thread
    // reads the body itself while holding its admission token, and tiny_http
    // 0.12 sets no socket read timeout and exposes no stream to set one on. So
    // a peer that sends headers and then stalls parks a token indefinitely.
    // The first casualty was `GET /api/state` — the container healthcheck —
    // whose three failed retries restart the daemon, possibly mid-rip.

    /// The reservation, exercised against the real counter: fill it to the
    /// body cap, then show a bodyless request still gets in and a
    /// body-carrying one does not.
    #[test]
    fn a_bodyless_request_is_admitted_while_the_body_cap_is_full() {
        static C: AtomicUsize = AtomicUsize::new(0);
        let held: Vec<_> = (0..MAX_INFLIGHT_BODY_HANDLERS)
            .map(|_| {
                ConnGuard::try_acquire(&C, MAX_INFLIGHT_BODY_HANDLERS).expect("under the body cap")
            })
            .collect();
        assert!(
            ConnGuard::try_acquire(&C, MAX_INFLIGHT_BODY_HANDLERS).is_none(),
            "the body cap must actually stop admitting"
        );
        let spare = ConnGuard::try_acquire(&C, MAX_INFLIGHT_HANDLERS);
        assert!(
            spare.is_some(),
            "a bodyless request must still be admitted — this is the slot the \
             healthcheck lives in"
        );
        drop(spare);
        drop(held);
        assert_eq!(C.load(Ordering::SeqCst), 0, "every slot released");
    }

    // ── Two guards nothing exercised ──────────────────────────────────────

    /// The pin caps at what ureq's fixed 16-slot array can hold. A seventeenth
    /// address is an out-of-bounds panic inside a resolver that runs on every
    /// request, and `validate_fetch_url` applies no count limit of its own.
    #[test]
    fn the_pinned_address_list_cannot_overrun_ureqs_fixed_array() {
        let many: Vec<SocketAddr> = (0..MAX_PINNED_ADDRS + 1)
            .map(|i| SocketAddr::from(([203, 0, 113, 1], 1000 + i as u16)))
            .collect();
        assert_eq!(
            pinned_addrs(&many).len(),
            MAX_PINNED_ADDRS,
            "more addresses than ureq's array holds must be truncated"
        );
        // An ordinary answer is passed through untouched.
        let few: Vec<SocketAddr> = many.iter().copied().take(3).collect();
        assert_eq!(pinned_addrs(&few), few);
    }

    /// An EMPTY pin must not resolve to anything: the resolver turns that into
    /// `HostNotFound` rather than letting the agent fall back to live DNS,
    /// which would reopen the rebinding hole the pin exists to close.
    #[test]
    fn an_empty_pin_yields_no_addresses() {
        assert!(pinned_addrs(&[]).is_empty());
    }

    /// `ureq_error_kind` is the single chokepoint keeping token-bearing URLs
    /// out of four log sites that reach the unauthenticated `/api/debug` and
    /// `/api/system`. The only test that reached it made ONE connection-refused
    /// call, which lands in the explicit `Io` arm — so mutating the CATCH-ALL
    /// to `e.to_string()` stayed green, and the catch-all is exactly where
    /// `BadUri` (which prints the URI it rejected) arrives, the enum being
    /// non_exhaustive.
    #[test]
    fn no_ureq_error_kind_output_can_carry_a_url() {
        let cases = vec![
            ureq::Error::StatusCode(404),
            ureq::Error::Io(std::io::Error::from(std::io::ErrorKind::ConnectionRefused)),
            ureq::Error::HostNotFound,
            ureq::Error::ConnectionFailed,
            ureq::Error::TooManyRedirects,
            // The variant the catch-all exists for: its Display embeds the URI.
            ureq::Error::BadUri("https://keydb.example/t/SECRETTOKEN/keydb.zip".into()),
        ];
        for e in &cases {
            let kind = ureq_error_kind(e);
            assert!(
                !kind.contains("://") && !kind.contains("SECRETTOKEN"),
                "a URL reached the summary for {e:?}: {kind}"
            );
        }
        assert_eq!(ureq_error_kind(&ureq::Error::StatusCode(404)), "HTTP 404");
    }

    /// An OS-generated transport error (a real errno, e.g. ECONNRESET from a
    /// receiver that RSTs the socket instead of answering) must surface its
    /// descriptive syscall message, not collapse to the useless
    /// "io: uncategorized error" that `io.kind()` alone prints when the kind is
    /// `Uncategorized`. Regression for a webhook that logged only
    /// "io: uncategorized error" with nothing an operator could act on.
    #[test]
    fn ureq_error_kind_surfaces_os_error_detail() {
        // ECONNRESET (54 on macOS/BSD, 104 on Linux) — its io::ErrorKind is
        // ConnectionReset here, but an errno the OS doesn't map to a named
        // ErrorKind arrives as Uncategorized, whose kind-Display is the
        // unhelpful string this change fixes. Build via from_raw_os_error so
        // raw_os_error() is Some and the descriptive Display is used.
        let econnreset = if cfg!(target_os = "linux") { 104 } else { 54 };
        let e = ureq::Error::Io(std::io::Error::from_raw_os_error(econnreset));
        let summary = ureq_error_kind(&e);
        assert!(
            summary.contains(&format!("os error {econnreset}")),
            "the errno detail must be surfaced, got: {summary}"
        );
        // Still URL-free — the whole point of routing through this function.
        assert!(!summary.contains("://"));

        // An io error WITHOUT an errno (constructed from a bare ErrorKind, as
        // ureq/std synthesize) falls back to the fixed kind description.
        let synthetic =
            ureq::Error::Io(std::io::Error::from(std::io::ErrorKind::ConnectionRefused));
        assert_eq!(ureq_error_kind(&synthetic), "io: connection refused");
    }

    /// The fourth ureq log site. Round 1 routed three failures through
    /// `ureq_error_kind` and missed this one, which masks the configured KEYDB
    /// origin from the CLIENT and then logged the raw error anyway — and
    /// `keydb_url` is token-bearing, with this line reaching `autorip.jsonl`
    /// and thence the unauthenticated `GET /api/debug`.
    ///
    /// A source-pin, like `resolve_with_timeout_uses_raii_guard_...` above,
    /// because the leak is in a `tracing` field inside a handler that wants a
    /// live `tiny_http::Request` and a real connection failure to reach. What
    /// it pins is exactly the shape of the defect: `%e` on the ureq error.
    #[test]
    fn the_keydb_update_handler_masks_its_ureq_error() {
        let src = crate::util::source_lf(include_str!("web.rs"));
        // Anchored on the DEFINITION, not the name: this test mentions the
        // name too, and it is the earlier occurrence in the file. Both ends
        // are `expect`ed rather than defaulted — an anchor that stopped
        // matching would otherwise silently widen the slice to the rest of
        // the module and start reporting other handlers' log lines.
        let start = src
            .find("\nfn handle_update_keydb(request: tiny_http::Request")
            .expect("handle_update_keydb definition present");
        let end = start
            + src[start..]
                .find("\n    // Write to the service-canonical keydb path")
                .expect("the handler's post-fetch section still starts here");
        let body = &src[start..end];
        assert!(
            body.contains("ureq_error_kind(&e)"),
            "the keydb-update handler must summarise its ureq failure through \
             ureq_error_kind, which is URL-free"
        );
        assert!(
            !body.contains("error = %e"),
            "the keydb-update handler must not format its ureq error by Display"
        );
    }

    /// Catches the mutation that restores `get_state_json`'s
    /// `Err(_) => return "{}"` bail-out on a poisoned STATE.
    ///
    /// A source-pin, like the handler pins above, because the only way to
    /// exercise the behaviour is to poison the process-global `ripper::STATE`,
    /// and a `Mutex` stays poisoned for the life of the process — it would
    /// panic every other test in this binary that locks STATE with `unwrap`.
    ///
    /// What it pins is the whole point of the defect: this was the ONE STATE
    /// consumer that abandoned on poison instead of recovering the guard like
    /// its ten siblings. STATE is poisoned by the first panic taken while its
    /// guard is held, so from that moment `GET /api/state` answered `{}` with
    /// HTTP 200 forever: a blank dashboard, and — because
    /// `main.rs::run_healthcheck` only checks for an `HTTP/1.1 200` status
    /// line — a permanently green Docker HEALTHCHECK that never restarts the
    /// container. The map's contents are still perfectly readable; serving
    /// them is both correct and the house convention.
    #[test]
    fn get_state_json_recovers_a_poisoned_state_lock() {
        let src = crate::util::source_lf(include_str!("web.rs"));
        // Anchored on the DEFINITION (leading newline) so this test's own
        // mention of the name cannot match, and both ends are `expect`ed so a
        // stale anchor fails loudly instead of silently widening the slice.
        let start = src
            .find("\nfn get_state_json(staging_dir: &str) -> String {")
            .expect("web.rs must define get_state_json");
        let end = start
            + src[start..]
                .find("\n    let move_state =")
                .expect("get_state_json still binds move_state after the STATE lock");
        // Comment lines are stripped: this function's own comment quotes the
        // defective arm verbatim (that is the house style — the comment names
        // the defect), and a naive substring search would match the very
        // explanation of the fix.
        let body: String = src[start..end]
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            body.contains("STATE.lock().unwrap_or_else(|e| e.into_inner())"),
            "get_state_json must recover a poisoned STATE guard, like every \
             other STATE consumer in the crate"
        );
        assert!(
            !body.contains("Err(_) =>"),
            "get_state_json must not have an abandon-on-poison arm: it turns \
             one panic into a permanently blank dashboard served with 200, \
             which run_healthcheck reads as healthy forever"
        );
    }

    #[test]
    fn resolve_with_timeout_uses_raii_guard_not_closure_side_fetch_sub() {
        // Source-pin for the INFLIGHT-leak fix: the decrement must NOT live as
        // a bare `INFLIGHT.fetch_sub(...)` inside the resolver closure (the old
        // shape that leaked when thread::spawn panicked). It must flow through
        // a ConnGuard whose Drop releases the slot on every path.
        let src = crate::util::source_lf(include_str!("web.rs"));
        let start = src
            .find("pub(crate) fn resolve_with_timeout")
            .expect("resolve_with_timeout present");
        let body = &src[start..start + 1500];
        assert!(
            body.contains("ConnGuard::try_acquire(&INFLIGHT, MAX_INFLIGHT)"),
            "resolve_with_timeout must acquire its slot via ConnGuard"
        );
        assert!(
            !body.contains("INFLIGHT.fetch_sub"),
            "resolve_with_timeout must not decrement INFLIGHT by hand; the \
             ConnGuard Drop owns the release"
        );
    }

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

    /// `media_type` is the one title-override field that was neither clamped
    /// nor allow-listed, on an unauthenticated-LAN route whose value is
    /// persisted into the `.done` marker and re-broadcast to every dashboard.
    /// The router only ever acts on `"tv"`; anything else is a movie. Store
    /// that vocabulary, not an arbitrary caller string.
    #[test]
    fn media_type_is_reduced_to_the_routers_vocabulary() {
        assert_eq!(normalize_media_type("tv"), "tv");
        assert_eq!(normalize_media_type("movie"), "movie");
        // Case/whitespace noise still resolves to the real values.
        assert_eq!(normalize_media_type(" TV "), "tv");
        // Anything else routes as a movie today, so it is stored as one
        // instead of being persisted verbatim.
        assert_eq!(normalize_media_type("anime"), "movie");
        assert_eq!(normalize_media_type(""), "movie");
        assert_eq!(
            normalize_media_type(&"A".repeat(10_000)),
            "movie",
            "an unbounded caller-supplied string must never reach STATE, the \
             .done marker, or the dashboard broadcast"
        );
    }

    /// Only ASCII hex digits are percent-escape payloads.
    ///
    /// `u8::from_str_radix` accepts a leading sign, so `%+3` parsed as 3 and a
    /// device segment or settings value carrying a literal `%+3` decoded to a
    /// control byte instead of staying as typed. RFC 3986 admits `HEXDIG`
    /// only; anything else is literal text.
    #[test]
    fn percent_decode_rejects_non_hex_escape_payloads() {
        assert_eq!(
            percent_decode("a%+3b"),
            "a%+3b",
            "'+' is not a hex digit — `%+3` must stay literal, not decode to 0x03"
        );
        assert_eq!(
            percent_decode("%-1"),
            "%-1",
            "a leading '-' must not be accepted as a sign either"
        );
        // Whitespace is the other thing from_str_radix-adjacent parsers trip
        // on; and the valid cases must keep working, upper and lower case.
        assert_eq!(percent_decode("%2f"), "/");
        assert_eq!(percent_decode("%2F"), "/");
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

        /// Classify ONE real `tiny_http::Request` — built by sending `head`
        /// verbatim over a loopback socket — with the production
        /// [`carries_body`] predicate.
        ///
        /// Verbatim, because the arm that matters is the one with NO
        /// `Content-Length` header at all, and `roundtrip` always sends one.
        ///
        /// Watchdog: `recv_timeout(5s)` rather than `recv()`, so a request
        /// the server never sees FAILS this test instead of parking the
        /// suite forever. The client writes a complete request head before
        /// the accept, so the real margin is a loopback round-trip.
        fn carries_body_of(head: &str) -> bool {
            let server = Server::http("127.0.0.1:0").expect("bind loopback server");
            let addr = server.server_addr().to_ip().expect("ip addr");
            let head = head.to_string();
            let client = std::thread::spawn(move || {
                let mut stream = TcpStream::connect(addr).expect("connect");
                stream.write_all(head.as_bytes()).expect("write request");
                stream.flush().ok();
                let mut resp = Vec::new();
                let _ = stream.read_to_end(&mut resp);
            });
            let request = server
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("server error")
                .expect("the request must arrive within 5s");
            let carries = carries_body(&request);
            let _ = request.respond(tiny_http::Response::empty(204));
            client.join().expect("client thread");
            carries
        }

        /// The accept loop reserves a smaller admission cap for requests that
        /// carry a body, because those hold a handler thread across an
        /// unbounded socket read. Which cap a request gets is decided ONLY by
        /// [`carries_body`], and no test called it: the sibling cap test
        /// exercises `ConnGuard` arithmetic at two literal caps, so inverting
        /// `Some(0) => false`, dropping `Get` from the bodyless methods, or
        /// negating the `None` arm all stayed green while every healthcheck
        /// GET started consuming a body slot. Drive it with real requests.
        #[test]
        fn carries_body_classifies_real_requests() {
            // No Content-Length, bodyless method — the healthcheck's own
            // shape. This is the slot that must stay available when the body
            // cap is full.
            assert!(
                !carries_body_of("GET /api/state HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"),
                "a GET with no Content-Length carries no body"
            );
            // Explicit zero length: a body header, but nothing to read.
            assert!(
                !carries_body_of(
                    "POST /api/settings HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\
                     Connection: close\r\n\r\n"
                ),
                "Content-Length: 0 is not a body"
            );
            // A real body.
            assert!(
                carries_body_of(
                    "POST /api/settings HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\
                     Connection: close\r\n\r\nhello"
                ),
                "a non-zero Content-Length carries a body"
            );
            // No Content-Length on a method that may carry one: assume the
            // reader will wait, and charge it the body cap.
            assert!(
                carries_body_of(
                    "POST /api/settings HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"
                ),
                "a POST with no Content-Length must be charged the body cap"
            );
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
                body.contains(&format!("\"version\":\"{}\"", crate::VERSION_LABEL)),
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

        /// A shape-VALID but nonexistent device name must not create state.
        ///
        /// `/api/scan`, `/api/rip` and `/api/eject` are unauthenticated and
        /// reachable from any LAN host, and `is_valid_device_name` only checks
        /// the shape (3..=64 ASCII alphanumeric) — never that the drive exists.
        /// `try_claim_active` used to insert a permanent STATE entry for any
        /// such name, and `spawn_rip_thread` a permanent JoinHandle, while the
        /// hot-unplug sweep only prunes devices it actually enumerated. Looping
        /// `POST /api/scan/<random>` therefore grew both maps without bound
        /// until the process was OOM-killed.
        ///
        /// This asserts the END-TO-END property through the real HTTP handler,
        /// not just the helper: the guard lives in `try_claim_active_checked`,
        /// but it is inert unless the handlers actually pass `known = false`.
        #[test]
        fn unauthenticated_scan_of_a_nonexistent_device_does_not_grow_state() {
            // Assert per-device, never on the total STATE size. STATE is a
            // process-global and the suite runs in parallel, so a sibling test
            // registering its own device between a snapshot and a later
            // comparison would fail this for a reason that has nothing to do
            // with the guard. That is exactly what happened: an earlier version
            // compared `state_len_for_test()` before and after, passed on a
            // laptop, and failed on a 96-core CI box where far more tests run
            // concurrently. Counting only OUR devices is both stricter about
            // the property under test and immune to the neighbours.
            let cfg = Arc::new(RwLock::new(Config::default()));
            let devices: Vec<String> = (0..25).map(|i| format!("zznotadrive{i:03}")).collect();
            for dev in &devices {
                let (code, _) = roundtrip(&cfg, "POST", &format!("/api/scan/{dev}"), None, &[]);
                assert_ne!(
                    code, 200,
                    "a nonexistent device must not be accepted for scanning"
                );
                assert!(
                    !crate::ripper::device_known(dev),
                    "no STATE entry may be created for unknown device {dev}"
                );
            }
            // Re-check every one after the whole loop: a handler that deferred
            // the insert (spawning a worker that registers later) would pass
            // the per-iteration check above and still leak all 25.
            let leaked: Vec<&String> = devices
                .iter()
                .filter(|d| crate::ripper::device_known(d))
                .collect();
            assert!(
                leaked.is_empty(),
                "fabricated devices left in STATE: {leaked:?}"
            );
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
        fn exact_cap_request_body_is_accepted_not_413() {
            // A body of EXACTLY MAX_REQUEST_BODY bytes is in-spec and must be
            // accepted, distinguishing read_body_capped's `>` from a `>=`
            // mutant (the existing oversize test uses cap+1, which trips
            // both operators identically). Pad a valid JSON settings patch
            // with leading whitespace (which serde_json skips) out to
            // exactly the cap.
            let tmp = tempfile::TempDir::new().unwrap();
            let cfg = cfg_in_tempdir(tmp.path());
            let payload = r#"{"abort_on_lost_secs": 31}"#;
            let padding = " ".repeat((MAX_REQUEST_BODY as usize) - payload.len());
            let body = format!("{padding}{payload}");
            assert_eq!(body.len() as u64, MAX_REQUEST_BODY);
            let (code, resp) = roundtrip(&cfg, "POST", "/api/settings", Some(&body), &[]);
            assert_ne!(
                code, 413,
                "a body of exactly MAX_REQUEST_BODY bytes must not be rejected as too large"
            );
            assert_eq!(
                code, 200,
                "the exact-cap body is valid JSON and must succeed, got: {resp}"
            );
            assert_eq!(cfg.read().unwrap().abort_on_lost_secs, 31);
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

        /// A present-but-invalid `on_read_error` must not block the legacy
        /// `abort_on_error` migration.
        ///
        /// The legacy fallback was gated on the KEY EXISTING
        /// (`patch.get(..).is_some()`) while the assignment required a
        /// STRING. A body carrying `"on_read_error": null` (a templating bug,
        /// an older client) alongside `abort_on_error` therefore took neither
        /// branch: the new field was never applied, the migration was skipped
        /// as "explicitly overridden", and the PATCH still answered 200. The
        /// operator sees the save succeed while the read-error policy silently
        /// keeps its old value — config drift that only shows up as rips
        /// behaving under a policy nobody selected.
        #[test]
        fn settings_post_null_on_read_error_still_applies_the_legacy_migration() {
            let tmp = tempfile::TempDir::new().unwrap();
            let cfg = cfg_in_tempdir(tmp.path());
            cfg.write().unwrap().on_read_error = "skip".to_string();

            let (code, body) = roundtrip(
                &cfg,
                "POST",
                "/api/settings",
                Some(r#"{"on_read_error": null, "abort_on_error": true}"#),
                &[],
            );

            assert_eq!(code, 200, "the PATCH reports success: {body}");
            assert_eq!(
                cfg.read().unwrap().on_read_error,
                "stop",
                "a null on_read_error carries no policy, so the legacy \
                 abort_on_error=true must still migrate to \"stop\" — \
                 answering 200 while applying neither is a save that looks \
                 like it worked and did nothing"
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

        #[test]
        fn settings_post_unresolvable_masked_webhook_leaves_output_dir_unmutated() {
            // Red/green regression for the write-guard early-return defect:
            // `resolve_webhook_urls` runs INSIDE the `cfg.write()` guard, after
            // ~20 other fields (including output_dir) have already been
            // written onto the live in-memory Config. When resolution fails
            // (an unresolvable masked webhook entry), the handler returns 400
            // from inside the guard, but the earlier mutations are never
            // undone — the live Config silently diverges from settings.json
            // while the operator is told the save was rejected.
            let tmp = tempfile::TempDir::new().unwrap();
            let cfg = cfg_in_tempdir(tmp.path());
            let original_output_dir = cfg.read().unwrap().output_dir.clone();

            // A valid output_dir change ordered BEFORE an unresolvable masked
            // webhook entry in the patch (mirrors field-mutation order inside
            // the guard: output_dir first, webhook_urls near the end).
            let patch = serde_json::json!({
                "output_dir": "/mnt/zz-settings-guard-fixture-4711/output",
                "webhook_urls": ["https://hooks.slack.com/********"],
            })
            .to_string();
            let (code, body) = roundtrip(&cfg, "POST", "/api/settings", Some(&patch), &[]);

            assert_eq!(
                code, 400,
                "an unresolvable masked webhook_urls entry must be rejected, got: {body}"
            );
            assert_eq!(
                cfg.read().unwrap().output_dir,
                original_output_dir,
                "output_dir on the live Config must be UNCHANGED when the save is \
                 rejected — a partial in-memory mutation must never survive a 400"
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

        // ── handle_accept_loss: rejected claim must not arm the override ──

        #[test]
        fn accept_loss_rejected_while_busy_does_not_arm_marker() {
            // A rip already in flight on this device means handle_accept_loss's
            // OWN try_claim_active loses, and the request must be rejected 409
            // WITHOUT writing `.accept-loss` (or clearing `.failed`/restart
            // markers) into the staging dir. Before the fix this order was
            // reversed: the marker was written first and handle_rip's claim
            // (called after) was what actually produced the 409, leaving the
            // override armed on disk for a rip that never consumed it — the
            // next resume on this device would then mux a rip whose loss was
            // never actually accepted.
            let tmp = tempfile::TempDir::new().unwrap();
            let cfg = cfg_in_tempdir(tmp.path());
            let device = "sgacceptloss1";
            let disc_name = "DamagedDisc";

            let staging_dir = {
                let c = cfg.read().unwrap();
                c.staging_device_dir(&crate::util::sanitize_path_compact(disc_name))
            };
            let dir = std::path::Path::new(&staging_dir);
            std::fs::create_dir_all(dir).unwrap();
            // Pre-existing terminal markers a real damaged/failed rip would
            // have left behind — these must survive a rejected accept too.
            std::fs::write(dir.join(ripper::staging::FAILED_MARKER), b"loss").unwrap();

            // Mark the device busy (as if a rip were already running) with a
            // known current disc name, exactly like a real in-flight rip.
            ripper::update_state(
                device,
                ripper::RipState {
                    device: device.to_string(),
                    status: "ripping".to_string(),
                    disc_name: disc_name.to_string(),
                    ..Default::default()
                },
            );

            let (code, _) = roundtrip(
                &cfg,
                "POST",
                &format!("/api/accept-loss/{device}"),
                None,
                &[],
            );
            assert_eq!(
                code, 409,
                "accept-loss on a busy device must be rejected, not silently dropped"
            );
            assert!(
                !dir.join(ripper::staging::ACCEPT_LOSS_MARKER).exists(),
                "a rejected accept-loss must NOT arm the one-shot override on disk"
            );
            assert!(
                dir.join(ripper::staging::FAILED_MARKER).exists(),
                "a rejected accept-loss must leave the existing .failed marker intact"
            );
        }
    }
}

fn text_response(request: tiny_http::Request, body: &str) {
    let header =
        Header::from_bytes(&b"Content-Type"[..], &b"text/plain; charset=utf-8"[..]).unwrap();
    let response = Response::from_string(body).with_header(header).with_header(
        Header::from_bytes(
            &b"Cache-Control"[..],
            &b"no-store, no-cache, must-revalidate"[..],
        )
        .unwrap(),
    );
    let _ = request.respond(response);
}

/// Cached result of [`build_queue_views`], shared across every concurrent
/// `/events` (SSE) client and `/api/state` poller.
///
/// `/events` holds one thread per client for the life of the connection
/// (up to `MAX_SSE_CLIENTS`) and each thread independently rebuilds the
/// Mux/Move queue view every second — a fresh `read_dir` over the whole
/// staging directory plus a handful of `Path::exists()`/marker reads per
/// subdirectory (see [`build_queue_views`] / `crate::muxer::pending_queue`).
/// With a large staging backlog and several dashboard tabs open, that is
/// the same filesystem the ripper and mover are actively writing to,
/// scanned redundantly by every open tab in lockstep. The queue view only
/// changes when a rip/mux/move transitions state, so a sub-second-stale
/// shared snapshot is invisible in a UI that itself only polls once a
/// second — trading a small, bounded staleness window for turning N
/// concurrent full-directory scans per second into at most one.
struct QueueViewSnapshot {
    computed_at: std::time::Instant,
    mux_queue: Vec<String>,
    move_queue: Vec<String>,
    mux_full: usize,
    move_full: usize,
}

impl QueueViewSnapshot {
    fn views(&self) -> (Vec<String>, Vec<String>, usize, usize) {
        (
            self.mux_queue.clone(),
            self.move_queue.clone(),
            self.mux_full,
            self.move_full,
        )
    }
}

struct QueueViewCache {
    /// `None` only between the moment a key's FIRST scan starts and the
    /// moment it finishes — there is simply nothing to serve yet.
    snapshot: Option<QueueViewSnapshot>,
    /// When the scan that currently owns this key STARTED, if one does.
    ///
    /// Single-flight marker, deliberately a timestamp rather than a bool: a
    /// bool can only be cleared by the refresher that set it, so a refresher
    /// that never returns latches it forever. Trusted for
    /// `QUEUE_VIEW_REFRESH_DEADLINE`, after which the owner is presumed dead
    /// and the next caller may take the key over.
    refresh_started: Option<std::time::Instant>,
    /// How many threads are inside `scan_queue_views` for this key right now.
    /// Maintained by `RefreshGuard`, so it is decremented on panic too, and
    /// capped at `QUEUE_VIEW_MAX_REFRESHERS` so repeated takeovers of a key
    /// that stays wedged cannot pile threads up without bound.
    refreshers: usize,
}

/// RAII owner of a key's single-flight marker.
///
/// Drop — not the happy path — is what releases the marker, so a scan that
/// panics inside `read_dir` (or anywhere else in `build_queue_views`) hands
/// the key straight back instead of stranding it until the deadline.
struct RefreshGuard {
    key: String,
    claimed_at: std::time::Instant,
}

impl Drop for RefreshGuard {
    fn drop(&mut self) {
        {
            let mut map = QUEUE_VIEW_CACHE.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(entry) = map.get_mut(&self.key) {
                entry.refreshers = entry.refreshers.saturating_sub(1);
                // Only clear the marker if it is still OURS. A refresher that
                // was presumed dead and taken over must not clear the marker
                // of the live refresher that replaced it.
                if entry.refresh_started == Some(self.claimed_at) {
                    entry.refresh_started = None;
                }
            }
        }
        QUEUE_VIEW_REFRESHED.notify_all();
    }
}

/// Keyed by staging_dir rather than a single slot: the staging path can
/// change at runtime (a Settings edit), and — just as importantly — this
/// keeps two DIFFERENT staging dirs from evicting each other's cached scan
/// (a real scenario if the operator ever repoints staging, and exactly the
/// shape our own parallel tests exercise with distinct tempdirs).
static QUEUE_VIEW_CACHE: Lazy<std::sync::Mutex<std::collections::HashMap<String, QueueViewCache>>> =
    Lazy::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

/// Signalled when an in-flight scan completes. Only the cold-start case
/// (no snapshot to serve at all) ever waits on it; a caller that has ANY
/// snapshot, however stale, is served immediately instead.
static QUEUE_VIEW_REFRESHED: Lazy<std::sync::Condvar> = Lazy::new(std::sync::Condvar::new);

/// Safety valve for the cold-start wait: a caller with NOTHING to serve gives
/// up after this long and returns an empty queue view rather than parking on
/// the condvar for as long as the in-flight scan takes. It deliberately does
/// NOT scan for itself — that abandons single-flight, and on a wedged staging
/// mount it consumes one HTTP worker thread (plus its admission token) per
/// give-up, which is how `/api/state` starts 503-ing and the container
/// HEALTHCHECK restarts the daemon mid-rip. Recovery from a dead scanner is
/// `QUEUE_VIEW_REFRESH_DEADLINE`'s job instead. Never hit in normal operation.
const QUEUE_VIEW_COLD_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

/// How long the per-key single-flight marker is TRUSTED. A refresher that has
/// held it longer than this is presumed dead — panicked before the drop guard
/// could run, or wedged inside `read_dir` on an unresponsive mount — and the
/// next caller that needs fresh data is allowed to take the marker over.
///
/// Without this, `refreshing` is a latch: a refresher that never returns makes
/// every later caller take the serve-stale branch forever, so the queue views
/// freeze for the process lifetime.
const QUEUE_VIEW_REFRESH_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

/// Hard ceiling on threads inside `scan_queue_views` for ONE key at a time.
/// The deadline above permits a takeover; this bounds how many takeovers can
/// pile up when the takeover ALSO wedges (a mount that is down stays down).
/// Two: the original plus one retry. When the mount recovers both publish, the
/// count drops back to zero, and normal single-flight resumes.
const QUEUE_VIEW_MAX_REFRESHERS: usize = 2;

/// How long a cached queue view is served before the next caller triggers a
/// fresh scan. Kept comfortably under the ~1s SSE tick so no client ever
/// observes staleness worse than what the poll cadence already implies.
const QUEUE_VIEW_CACHE_TTL: std::time::Duration = std::time::Duration::from_millis(750);

/// Test-only seam around the staging-dir scan a cache miss performs.
///
/// Keyed by staging dir so tests that arm it are isolated from every other
/// test in this process (the cache itself, `STATE`, and the log dir are all
/// process-global here, so a shared fixture name would be a real race).
/// Lets a test (a) make one specific dir's scan artificially slow and
/// (b) count how many scans a dir actually received.
#[cfg(test)]
pub(crate) mod queue_scan_probe {
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Default, Clone, Copy)]
    pub struct Probe {
        pub delay_ms: u64,
        /// Incremented on scan ENTRY (before the delay), so a waiting test
        /// can tell that the slow scan is genuinely in flight.
        pub scans: usize,
        /// Per-dir override of `QUEUE_VIEW_REFRESH_DEADLINE` (0 = use the
        /// production value). Keyed by dir like everything else here so a
        /// timing test cannot perturb another test's staging dir.
        pub refresh_deadline_ms: u64,
        /// Per-dir override of `QUEUE_VIEW_COLD_WAIT` (0 = production value).
        pub cold_wait_ms: u64,
    }

    static PROBES: Mutex<Option<HashMap<String, Probe>>> = Mutex::new(None);

    fn with<R>(f: impl FnOnce(&mut HashMap<String, Probe>) -> R) -> R {
        let mut g = PROBES.lock().unwrap_or_else(|e| e.into_inner());
        f(g.get_or_insert_with(HashMap::new))
    }

    /// Arm the probe for `dir`; every subsequent scan of it sleeps `delay_ms`.
    pub fn arm(dir: &str, delay_ms: u64) {
        with(|m| {
            m.entry(dir.to_string()).or_default().delay_ms = delay_ms;
        });
    }

    /// Number of scans this dir has received since it was first armed.
    pub fn scans(dir: &str) -> usize {
        with(|m| m.get(dir).map(|p| p.scans).unwrap_or(0))
    }

    /// Shorten (or lengthen) how long this dir's in-flight refresh marker is
    /// trusted before it is presumed dead. Lets a test observe the
    /// wedged-refresher takeover in milliseconds instead of the production
    /// deadline.
    pub fn set_refresh_deadline(dir: &str, ms: u64) {
        with(|m| {
            m.entry(dir.to_string()).or_default().refresh_deadline_ms = ms;
        });
    }

    /// Shorten how long a COLD caller (nothing at all to serve) parks before
    /// giving up on the in-flight scan.
    pub fn set_cold_wait(dir: &str, ms: u64) {
        with(|m| {
            m.entry(dir.to_string()).or_default().cold_wait_ms = ms;
        });
    }

    /// `(refresh_deadline_ms, cold_wait_ms)`; 0 means "use production".
    pub fn overrides(dir: &str) -> (u64, u64) {
        with(|m| {
            m.get(dir)
                .map(|p| (p.refresh_deadline_ms, p.cold_wait_ms))
                .unwrap_or((0, 0))
        })
    }

    /// Called from the production scan path. Never holds the probe lock
    /// across the sleep.
    pub fn enter(dir: &str) {
        let delay = with(|m| match m.get_mut(dir) {
            Some(p) => {
                p.scans += 1;
                p.delay_ms
            }
            None => 0,
        });
        if delay > 0 {
            std::thread::sleep(std::time::Duration::from_millis(delay));
        }
    }
}

/// How long an in-flight refresh marker for `dir` is trusted.
#[cfg(not(test))]
fn queue_view_refresh_deadline(_dir: &str) -> std::time::Duration {
    QUEUE_VIEW_REFRESH_DEADLINE
}

#[cfg(test)]
fn queue_view_refresh_deadline(dir: &str) -> std::time::Duration {
    match queue_scan_probe::overrides(dir).0 {
        0 => QUEUE_VIEW_REFRESH_DEADLINE,
        ms => std::time::Duration::from_millis(ms),
    }
}

/// How long a cold caller parks for someone else's first scan of `dir`.
#[cfg(not(test))]
fn queue_view_cold_wait(_dir: &str) -> std::time::Duration {
    QUEUE_VIEW_COLD_WAIT
}

#[cfg(test)]
fn queue_view_cold_wait(dir: &str) -> std::time::Duration {
    match queue_scan_probe::overrides(dir).1 {
        0 => QUEUE_VIEW_COLD_WAIT,
        ms => std::time::Duration::from_millis(ms),
    }
}

/// The scan a cache miss performs, behind a test-only instrumentation seam.
fn scan_queue_views(staging_dir: &str) -> (Vec<String>, Vec<String>, usize, usize) {
    #[cfg(test)]
    queue_scan_probe::enter(staging_dir);
    build_queue_views(staging_dir)
}

/// [`build_queue_views`], but shared across concurrent callers within
/// `QUEUE_VIEW_CACHE_TTL` instead of re-scanning the staging directory once
/// per caller. Used by [`get_state_json`] (the per-second SSE/`/api/state`
/// payload); `handle_system_info`'s on-demand `/api/system` panel calls the
/// uncached `build_queue_views` directly so a manual refresh always sees the
/// latest disk state.
///
/// The cache mutex is NEVER held across the scan. `build_queue_views` does
/// `read_dir` plus a stat per entry, and this function backs `/api/state` —
/// which `--healthcheck` probes and the Dockerfile HEALTHCHECK runs — so a
/// staging dir that is slow to enumerate must not be able to park every
/// other caller (and get the container restarted mid-rip). Single-flight is
/// preserved without the lock: a per-key marker means exactly one caller
/// scans, while everyone else is served the previous snapshot
/// (stale-while-revalidate) and never blocks. Only a genuinely cold key —
/// no snapshot at all — makes callers wait, and they wait on a condvar with
/// the map lock released, not on the scan's mutex.
///
/// Three separate bounds keep a refresher that never comes back from wedging
/// the whole daemon, and they are deliberately distinct:
///
/// * `QUEUE_VIEW_REFRESH_DEADLINE` bounds the MARKER. A single-flight bool can
///   only be cleared by the thread that set it, so a thread stuck in `read_dir`
///   latches it and every later caller serves stale forever. A timestamp lets a
///   later caller decide the owner is dead and take the key over, which is what
///   unfreezes the views.
/// * `QUEUE_VIEW_MAX_REFRESHERS` bounds the takeovers that deadline permits, so
///   a mount that stays down cannot accumulate one wedged thread per deadline.
/// * `QUEUE_VIEW_COLD_WAIT` bounds an individual COLD caller, which returns an
///   empty view rather than scanning: the give-up path must not start work, or
///   the caller bound becomes an accumulation rate.
fn build_queue_views_cached(staging_dir: &str) -> (Vec<String>, Vec<String>, usize, usize) {
    // Phase 1 — decide, under the lock, whether THIS caller scans. The lock
    // is never held across `scan_queue_views` (read_dir + a stat per entry):
    // a staging dir that is slow to enumerate must not be able to park
    // `/api/state`, which is what `--healthcheck` — and so the Dockerfile
    // HEALTHCHECK — probes.
    enum Decision {
        /// Serve this (possibly stale) snapshot; do not touch the disk.
        Serve(Vec<String>, Vec<String>, usize, usize),
        /// This caller owns the refresh.
        Scan,
        /// Cold key with a scan already in flight — wait for its result.
        Wait,
    }

    let deadline = queue_view_refresh_deadline(staging_dir);
    let cold_wait = queue_view_cold_wait(staging_dir);
    let mut map = QUEUE_VIEW_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    let waited_from = std::time::Instant::now();
    let claimed_at = loop {
        let decision = match map.get_mut(staging_dir) {
            // Never seen: claim the slot and scan.
            None => Decision::Scan,
            Some(entry) => {
                // "A refresh is in flight" is a marker YOUNGER than the
                // deadline. Past it the owner is presumed dead — panicked
                // past its guard, or wedged in `read_dir` on a mount that
                // stopped answering — and its claim stops justifying either
                // serving stale data or making a cold caller wait.
                let live_refresh = entry
                    .refresh_started
                    .is_some_and(|t| t.elapsed() < deadline);
                // Serve a snapshot if it is fresh, OR if it is stale but a
                // live refresh is already in flight: queueing behind someone
                // else's I/O is exactly the stall we are avoiding. A
                // sub-second-stale queue view is invisible in a UI that
                // polls once a second; an unresponsive /api/state is not.
                let serve = entry
                    .snapshot
                    .as_ref()
                    .filter(|s| s.computed_at.elapsed() < QUEUE_VIEW_CACHE_TTL || live_refresh)
                    .map(|s| s.views());
                // The takeover a dead marker permits is itself capped: if
                // this key already has the maximum number of threads inside
                // `read_dir`, the mount is not merely slow, and adding
                // another thread to it only burns another HTTP worker.
                let may_scan = !live_refresh && entry.refreshers < QUEUE_VIEW_MAX_REFRESHERS;
                match serve {
                    Some((mux, mv, mux_full, move_full)) => {
                        Decision::Serve(mux, mv, mux_full, move_full)
                    }
                    // Stale (or cold) with no live refresher: take the key
                    // over, unless we are already at the refresher cap.
                    None if may_scan => Decision::Scan,
                    // Capped out but we have SOMETHING: never block a warm
                    // caller — hand back the stale view.
                    None => match entry.snapshot.as_ref().map(|s| s.views()) {
                        Some((mux, mv, mux_full, move_full)) => {
                            Decision::Serve(mux, mv, mux_full, move_full)
                        }
                        None => Decision::Wait,
                    },
                }
            }
        };
        match decision {
            Decision::Serve(mux, mv, mux_full, move_full) => {
                return (mux, mv, mux_full, move_full);
            }
            Decision::Scan => {
                let claimed_at = std::time::Instant::now();
                let entry = map
                    .entry(staging_dir.to_string())
                    .or_insert_with(|| QueueViewCache {
                        snapshot: None,
                        refresh_started: None,
                        refreshers: 0,
                    });
                entry.refresh_started = Some(claimed_at);
                entry.refreshers += 1;
                break claimed_at;
            }
            // Cold key, live scan in flight: wait for its result instead of
            // launching a duplicate one (single-flight). The wait RELEASES
            // the map lock, so every other staging dir and every warm reader
            // keeps running while we sleep here.
            Decision::Wait => {
                if waited_from.elapsed() >= cold_wait {
                    // Nothing to serve and the owner is still working. Give
                    // up on THIS call rather than start a competing scan:
                    // a caller that scans anyway is a caller (and an HTTP
                    // worker, and its admission token) consumed every
                    // `cold_wait` for as long as the mount stays wedged,
                    // which is how /api/state ends up 503-ing and the
                    // container HEALTHCHECK restarts the daemon mid-rip.
                    // Empty is what a cold key looks like before its first
                    // scan lands anyway; the owner (or a post-deadline
                    // takeover) publishes the real view shortly.
                    tracing::warn!(
                        staging_dir = %staging_dir,
                        waited_ms = waited_from.elapsed().as_millis() as u64,
                        "queue view still cold after waiting for an in-flight staging scan; \
                         serving an empty queue view for this request"
                    );
                    return (Vec::new(), Vec::new(), 0, 0);
                }
                let (m, _) = QUEUE_VIEW_REFRESHED
                    .wait_timeout(map, std::time::Duration::from_millis(50))
                    .unwrap_or_else(|e| e.into_inner());
                map = m;
            }
        }
    };
    drop(map);
    // Marker released on EVERY exit from here on, panic included.
    let _refresh_guard = RefreshGuard {
        key: staging_dir.to_string(),
        claimed_at,
    };

    // Phase 2 — scan with NO lock held.
    let (mux_queue, move_queue, mux_full, move_full) = scan_queue_views(staging_dir);

    // Phase 3 — publish. The single-flight marker is released by
    // `_refresh_guard` as this function returns.
    let mut map = QUEUE_VIEW_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    // Opportunistic prune so a staging path that keeps changing (or a test
    // suite hammering many distinct tempdirs) can't grow this map forever.
    // Keep anything with a scan in flight, and this key (updated below).
    map.retain(|k, v| {
        k == staging_dir
            || v.refreshers > 0
            || v.snapshot
                .as_ref()
                .is_some_and(|s| s.computed_at.elapsed() < QUEUE_VIEW_CACHE_TTL)
    });
    let entry = map
        .entry(staging_dir.to_string())
        .or_insert_with(|| QueueViewCache {
            snapshot: None,
            refresh_started: None,
            refreshers: 0,
        });
    // A presumed-dead refresher that comes back to life must not overwrite the
    // fresher snapshot its replacement already published. Anything computed
    // after we started is at least as current as what we hold.
    let superseded = entry
        .snapshot
        .as_ref()
        .is_some_and(|s| s.computed_at > claimed_at);
    if !superseded {
        entry.snapshot = Some(QueueViewSnapshot {
            computed_at: std::time::Instant::now(),
            mux_queue: mux_queue.clone(),
            move_queue: move_queue.clone(),
            mux_full,
            move_full,
        });
    }
    drop(map);
    QUEUE_VIEW_REFRESHED.notify_all();
    (mux_queue, move_queue, mux_full, move_full)
}

fn get_state_json(staging_dir: &str) -> String {
    // Recover-and-proceed on poison, like every other STATE consumer
    // (`is_busy`, `update_state`, `try_claim_active_checked`, ...). This was
    // the ONE site that bailed out with `Err(_) => "{}"`, and the consequence
    // was permanent and silent: STATE is poisoned by the first panic taken
    // while its guard is held, and a `Mutex` stays poisoned for the life of
    // the process. So every later `GET /api/state` returned `{}` with a 200 —
    // a blank dashboard forever, AND a permanently green Docker HEALTHCHECK,
    // because `main.rs::run_healthcheck` only looks for an `HTTP/1.1 200`
    // status line and so never restarts the container. A failure that looks
    // like success is exactly the class this project refuses to ship: the
    // poisoned map's contents are still perfectly readable, so serve them.
    let state = ripper::STATE.lock().unwrap_or_else(|e| e.into_inner());
    // `_move` is now an ARRAY of per-artifact bars (movie file + companion ISO
    // get one each), so clone the whole Vec; empty means nothing is moving.
    let move_state = crate::mover::MOVE_STATE
        .lock()
        .ok()
        .map(|ms| ms.clone())
        .unwrap_or_default();
    // Mux progress rides on the synthetic `_mux` device key in STATE (a
    // RipState seeded by the mux worker — see the dashboard JS at the
    // `_mux` field), serialized below as part of `state`. There is no
    // separate live MuxState struct.
    let mut obj = serde_json::to_value(&*state).unwrap_or_else(|_| serde_json::json!({}));
    if !move_state.is_empty() {
        obj["_move"] = serde_json::to_value(&move_state).unwrap_or_default();
    }
    // Release the STATE lock before the staging-dir scan below. `build_queue_views`
    // does filesystem I/O (read_dir + per-dir stat); holding STATE across it would
    // serialize the ripper's once-per-tick progress writes against this
    // once-per-second scan. `obj` already holds everything we needed from `state`.
    drop(state);
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
    let (mux_queue, move_queue, _, _) = build_queue_views_cached(staging_dir);
    obj["_mux_queue"] = serde_json::to_value(&mux_queue).unwrap_or_default();
    obj["_move_queue"] = serde_json::to_value(&move_queue).unwrap_or_default();
    obj.to_string()
}

/// Cap on how many queue entries we serialize so a staging dir holding a
/// pathological number of subdirs can't produce an unbounded response. Shared
/// by `build_queue_views` (the actual truncation) and `handle_system_info`
/// (the "+N more" math) so the displayed list and its overflow count can never
/// drift apart.
const QUEUE_DISPLAY_CAP: usize = 100;

/// Build the Mux-queue and Move-queue display lists from the staging dir.
/// Shared by `get_state_json` (the live SSE/`/api/state` payload) and
/// `handle_system_info` (the `/api/system` panel) so both endpoints derive
/// the two queues from one place and can never disagree on membership.
///
/// Returns `(mux_queue, move_queue, mux_full_count, move_full_count)`: the
/// first two are capped at `QUEUE_DISPLAY_CAP` for display, the last two are
/// the uncapped totals from the SAME scan so callers can compute a "+N more"
/// overflow count that always matches the displayed lists (one snapshot, no
/// TOCTOU between count and list).
///
/// Mutual exclusion is guaranteed by the markers themselves: the Move
/// queue scans for `.done`, and `crate::muxer::pending_queue` (the Mux
/// queue) skips any dir carrying `.done`/`.review`/`.muxing`/`.completed`/
/// `.failed`. So a given staging dir lands in at most one of the two lists.
///
/// The Move queue additionally excludes the staging dir currently being moved
/// (`crate::mover::ACTIVE_MOVE_DIR`): that dir keeps its `.done` throughout the
/// copy and is already shown as its live per-artifact progress bars (`_move`),
/// so listing it here as a "(moving)" row too is the double-render bug. The
/// exclusion is by exact on-disk basename, so it holds regardless of any title
/// punctuation the filesystem sanitizer drops.
fn build_queue_views(staging_dir: &str) -> (Vec<String>, Vec<String>, usize, usize) {
    let active_move_dir = crate::mover::ACTIVE_MOVE_DIR
        .lock()
        .ok()
        .and_then(|d| d.clone());
    // Move queue: staging dirs with a `.done` marker (pending moves), minus the
    // one actively being moved (shown as live bars, not a queue row).
    let mut move_queue: Vec<String> = std::fs::read_dir(staging_dir)
        .ok()
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| e.path().is_dir() && e.path().join(".done").exists())
                .filter(|e| {
                    active_move_dir.as_deref() != Some(e.file_name().to_string_lossy().as_ref())
                })
                .map(|e| {
                    let name = e.file_name().to_string_lossy().replace('_', " ");
                    format!("{} (moving)", name)
                })
                .collect()
        })
        .unwrap_or_default();
    // Mux queue: staging dirs with a `.ripped` hand-off and no terminal /
    // move-queue / in-flight marker (see `pending_queue`).
    let mut mux_queue = crate::muxer::pending_queue(std::path::Path::new(staging_dir));
    // Uncapped totals captured before truncation so "+N more" math shares this
    // one snapshot with the displayed lists.
    let move_full_count = move_queue.len();
    let mux_full_count = mux_queue.len();
    move_queue.truncate(QUEUE_DISPLAY_CAP);
    mux_queue.truncate(QUEUE_DISPLAY_CAP);
    (mux_queue, move_queue, mux_full_count, move_full_count)
}

fn handle_system_info(request: tiny_http::Request, cfg: &Arc<RwLock<Config>>) {
    // Degrade gracefully on a poisoned lock, matching every other handler
    // (e.g. GET /api/settings) rather than panicking this handler thread
    // and silently breaking the System tab.
    //
    // Copy the two paths we need out of the config and DROP the read guard
    // before any I/O. Everything below does filesystem work — a staging-dir
    // scan and a log tail — and holding the RwLock across it would block any
    // concurrent `cfg.write()`, i.e. the Settings-save path an operator would
    // use to repoint the very staging dir that is being slow.
    let (staging_dir, syslog_path) = match cfg.read() {
        Ok(c) => (
            c.staging_dir.clone(),
            format!("{}/device_system.log", c.log_dir()),
        ),
        Err(_) => {
            return json_response(
                request,
                500,
                r#"{"ok":false,"error":"config lock poisoned"}"#,
            );
        }
    };

    // Move + Mux queue display lists come from the SAME shared builder the
    // live /api/state + SSE payload uses (`build_queue_views`), so the
    // System-page panels and the live dashboard can never disagree on queue
    // membership. `build_queue_views` enforces mutual exclusion (a dir is in
    // at most one of the two lists) and returns the uncapped totals from the
    // same scan, so the "+N more" overflow math below shares one snapshot with
    // the displayed lists (no count-vs-list TOCTOU).
    let (mux_queue, move_queue, mux_full_count, move_full_count) = build_queue_views(&staging_dir);

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
    if truncated && let Some(nl) = s.find('\n') {
        s.drain(..=nl);
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
        if let Some(ref needle) = q
            && !line.contains(needle)
        {
            continue;
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

#[cfg(test)]
mod parse_query_tests {
    use super::*;

    // parse_query's internal `clamp` truncates a query field to
    // MAX_FIELD_LEN (256) bytes on a char boundary. These pin the three
    // shapes an off-by-one (`-=`->`+=`, or the no-op `/=`) mutant in that
    // loop would break: (1) a value that truncates mid multi-byte char and
    // must back up to the last full char, (2) a value exactly at the cap
    // (no truncation), (3) a value over the cap that already lands on a
    // boundary at the cutoff (loop body never runs). An `end += 1` mutant
    // would spin forever on case (1); this test completing at all is part
    // of what pins it, but we also assert the exact returned bytes so a
    // silent off-by-one can't hide.
    #[test]
    fn clamps_query_value_at_char_boundary_when_cutoff_lands_mid_char() {
        // 255 ASCII bytes, then a 2-byte 'é' straddling the 256-byte cutoff
        // (occupies bytes 255..257), then more filler. Byte offset 256 sits
        // inside 'é', so clamp must back up to 255 and drop 'é' entirely.
        let value = format!("{}é{}", "a".repeat(255), "b".repeat(10));
        let url = format!("/x?q={value}");
        let map = parse_query(&url);
        assert_eq!(
            map.get("q").map(String::as_str),
            Some("a".repeat(255).as_str()),
            "must truncate to the last full character before the cutoff, not split 'é'"
        );
    }

    #[test]
    fn query_value_exactly_at_cap_is_not_truncated() {
        // Exactly 256 bytes (128 two-byte 'é' chars) — s.len() <= n, the
        // `<=` early-return branch, no truncation at all.
        let value = "é".repeat(128);
        assert_eq!(value.len(), 256);
        let url = format!("/x?q={value}");
        let map = parse_query(&url);
        assert_eq!(map.get("q").map(String::as_str), Some(value.as_str()));
    }

    #[test]
    fn query_value_over_cap_already_on_boundary_truncates_cleanly() {
        // 260 plain ASCII bytes: over the cap, but byte 256 is already a
        // char boundary, so the backward-scan loop body never executes.
        let value = "a".repeat(260);
        let url = format!("/x?q={value}");
        let map = parse_query(&url);
        assert_eq!(
            map.get("q").map(String::as_str),
            Some("a".repeat(256).as_str())
        );
    }
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
        if !v.trim().is_empty()
            && !v.contains(SECRET_SENTINEL)
            && let Err(e) = validate_fetch_url(v)
        {
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
    if let Some(v) = patch.get("keyserver_url").and_then(|v| v.as_str()) {
        // SSRF guard: keysource.rs OnlineSource POSTs this URL verbatim at
        // rip time, so an unauthenticated LAN client must not be able to
        // aim it at metadata/internal hosts. Empty is allowed (disables the
        // online source). A value containing the sentinel is a masked
        // "unchanged" placeholder from GET — skip validation.
        if !v.trim().is_empty()
            && !v.contains(SECRET_SENTINEL)
            && let Err(e) = validate_fetch_url(v)
        {
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
    if let Some(v) = patch.get("network_target").and_then(|v| v.as_str()) {
        // SSRF guard: at rip time libfreemkv streams decrypted disc content
        // to this bare `host:port`. Without a check an unauthenticated POST
        // could beacon plaintext to an internal/metadata host. Empty clears
        // the target (no check needed). Reject any host that is or resolves
        // to a non-public address.
        if !v.trim().is_empty()
            && let Err(e) = validate_network_target(v)
        {
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
    // NOTE: webhook_urls are intentionally NOT SSRF-validated here (unlike
    // keydb_url / keyserver_url / network_target above). A webhook is a
    // blind fire-and-forget notification with no response channel, and
    // pointing one at a LAN service (Home Assistant, a NAS) is the intended
    // use — the private-address guard only got in the way of that. Delivery
    // uses the un-pinned `web::webhook_agent`; see its doc comment for the
    // full rationale.
    //
    // Resolve masked webhook_urls placeholders BEFORE the write guard, same
    // trust-boundary rationale as `port` below: resolve_webhook_urls used to
    // run INSIDE cfg.write(), so an ambiguous/orphaned masked entry returned
    // 400 only after ~20 earlier fields (including output_dir) had already
    // been mutated onto the live in-memory Config — a partial update behind
    // a rejected save. resolve_webhook_urls does no I/O (pure string
    // matching against the existing stored URLs), so it is cheap to run
    // under a short-lived `cfg.read()` here; the result is threaded into the
    // write guard below instead of being recomputed there.
    //
    // Race note: the `cfg.read()` here and the `cfg.write()` later are two
    // separate, non-overlapping lock acquisitions (never held together, so
    // no lock-ordering/deadlock risk). If a second settings POST races in
    // between and changes webhook_urls, this request's resolution was
    // computed against a now-stale `existing` snapshot. The write guard does
    // not re-validate — it applies the already-resolved value — so the
    // worst case is last-write-wins on a resolution computed one snapshot
    // earlier, the same TOCTOU window `keydb_path`'s redacted-round-trip
    // check below already accepts (it also reads `cfg` outside the write
    // guard). It cannot bind a masked entry to a WRONG secret: resolution
    // still only succeeds when the origin (or index) unambiguously matches
    // one entry in whichever snapshot was read.
    let webhook_urls_resolved: Option<Vec<WebhookEntry>> = if let Some(arr) =
        patch.get("webhook_urls").and_then(|v| v.as_array())
    {
        // Each element is the modern object `{url, post_rip, post_mux, post_move}`; a
        // bare string (legacy client) is accepted too and treated as
        // fire-on-both. A missing flag defaults to true (fire), matching the
        // config loader's backward-compat rule.
        let incoming: Vec<IncomingWebhook> = arr
            .iter()
            .filter_map(|v| {
                if let Some(s) = v.as_str() {
                    Some(IncomingWebhook {
                        url: s.to_string(),
                        post_rip: true,
                        post_mux: true,
                        post_move: true,
                    })
                } else if let Some(obj) = v.as_object() {
                    let url = obj.get("url").and_then(|u| u.as_str())?.to_string();
                    let flag = |k: &str| obj.get(k).and_then(|b| b.as_bool()).unwrap_or(true);
                    Some(IncomingWebhook {
                        url,
                        post_rip: flag("post_rip"),
                        post_mux: flag("post_mux"),
                        post_move: flag("post_move"),
                    })
                } else {
                    None
                }
            })
            .collect();
        let existing = match cfg.read() {
            Ok(c) => c.webhook_urls.clone(),
            Err(_) => {
                return json_response(
                    request,
                    500,
                    r#"{"ok":false,"error":"config lock poisoned"}"#,
                );
            }
        };
        match resolve_webhook_entries(&incoming, &existing) {
            Ok(urls) => Some(urls),
            Err(_) => {
                // A masked entry's origin matched 0 (deleted row) or >1
                // (shared-origin) stored secrets — refuse to guess which
                // secret was meant rather than silently bind the wrong
                // one. Returned BEFORE the write guard so no other field
                // in this patch is mutated onto the live Config either.
                return json_response(
                    request,
                    400,
                    r#"{"ok":false,"error":"ambiguous masked webhook entry; re-enter the full webhook URL"}"#,
                );
            }
        }
    } else {
        None
    };
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
        if let Some(v) = patch.get(field).and_then(|v| v.as_str())
            && !allowed.contains(&v)
        {
            return json_response(
                request,
                400,
                &format!(r#"{{"ok":false,"error":"invalid value for {field}"}}"#),
            );
        }
    }

    // Validate directory-path fields BEFORE the write guard. These end up as
    // filesystem roots autorip writes rips into and enumerates (the move queue
    // scans `staging_dir` with `read_dir`), so a raw POST must not be able to
    // point them at an arbitrary location for directory enumeration. Require an
    // absolute path with no `..` traversal component — that confines them to
    // real mount points (the legitimate configs are all absolute: /staging-local,
    // /mnt/media/movies, …) while rejecting relative / climbing paths.
    // Empty string is allowed: it means "unset / inherit default" for the
    // optional movie_dir / tv_dir overrides.
    let has_parent_dir = |p: &std::path::Path| {
        p.components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
    };
    // output_dir / staging_dir are MOUNT ROOTS: they must be absolute (a real
    // mount point) and may not climb with `..`.
    for field in ["output_dir", "staging_dir"] {
        if let Some(v) = patch.get(field).and_then(|v| v.as_str()) {
            if v.is_empty() {
                continue;
            }
            let p = std::path::Path::new(v);
            if !p.is_absolute() || has_parent_dir(p) {
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
    // movie_dir / tv_dir are SUB-DIRECTORY names placed UNDER output_dir (the
    // defaults are the relative "movies" / "tv"). Relative is the norm; only
    // reject `..` so they can't escape the output root. An absolute override is
    // also permitted.
    for field in ["movie_dir", "tv_dir", "iso_dir"] {
        if let Some(v) = patch.get(field).and_then(|v| v.as_str()) {
            if v.is_empty() {
                continue;
            }
            if has_parent_dir(std::path::Path::new(v)) {
                return json_response(
                    request,
                    400,
                    &format!(r#"{{"ok":false,"error":"{field} must not contain '..'"}}"#),
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
        if let Some(v) = patch.get("iso_dir").and_then(|v| v.as_str()) {
            c.iso_dir = v.to_string();
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
        if let Some(v) = patch.get("keyserver_secret").and_then(|v| v.as_str())
            && v != SECRET_SENTINEL
        {
            c.keyserver_secret = v.to_string();
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
        // Presence is not the question the fallback below is asking — VALIDITY
        // is. Gating on `.is_some()` while the assignment requires `.as_str()`
        // meant a present-but-non-string value (`null`, a number, an object)
        // applied neither the new field nor the legacy migration, and still
        // answered 200: a settings save that looks applied and is not.
        let on_read_error_in_patch = patch
            .get("on_read_error")
            .and_then(|v| v.as_str())
            .is_some();
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
        if let Some(urls) = webhook_urls_resolved {
            // Resolved (SSRF-validated + masked-placeholder-resolved) above
            // the write guard — see the rationale there. Applying it here is
            // infallible: any ambiguity already returned 400 before any
            // field, including this one, could be mutated.
            c.webhook_urls = urls;
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
    // Check-and-claim. It takes TWO facts, and only one of them is a STATE
    // fact: `try_claim_active_checked` reads the device's thread liveness
    // FIRST and OUTSIDE the STATE lock, then folds the status-check and the
    // status-set into a single STATE lock — which is what closes the TOCTOU
    // where two concurrent POSTs both pass a separate busy-check and both
    // start a scan. The two registries are deliberately never held at the same
    // time; that is the whole no-lock-inversion argument, and calling this
    // "atomic under one STATE lock" (as this comment used to) misdescribes the
    // very ordering that argument rests on. The claim hands back the
    // generation identifying it, which is what a failed spawn rolls back.
    let Some(claim_gen) = ripper::try_claim_active_checked(device, false) else {
        json_response(request, 409, r#"{"ok":false,"error":"busy"}"#);
        return;
    };

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
        // Catch the panic, exactly as the rip spawn site and the poll loop do.
        // Without this a panic in `scan_disc` unwinds past every state write and
        // leaves the claim standing: `status="scanning"` with no thread, so
        // `is_busy` answers true forever and scan/rip/eject/accept-loss all 409
        // for the rest of the container's life. The claim is set BEFORE the
        // spawn, so whoever takes it owns clearing it on every exit path — and
        // an unwind is an exit path.
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ripper::scan_disc(&cfg, &dev, &dev_path);
        }))
        .is_err()
        {
            crate::log::device_log(&dev, "Scan thread panicked");
            ripper::update_state(
                &dev,
                ripper::RipState {
                    device: dev.clone(),
                    status: "error".to_string(),
                    disc_present: true,
                    last_error: "Internal error (panic)".to_string(),
                    ..Default::default()
                },
            );
        }
    }) {
        tracing::error!(device = %dev_for_register, error = %e, "failed to spawn scan thread");
        // Roll the device state back to idle so a failed spawn doesn't
        // wedge the busy-check at "scanning" forever (409 on every
        // future scan/rip until restart). Shared helper so poll loop +
        // both web handlers can't drift.
        ripper::rollback_failed_spawn(&dev_for_register, claim_gen);
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

    // Check-and-claim — see `handle_scan` for the ordering. The liveness half
    // is read outside the STATE lock; the status-check and status-set are one
    // STATE lock, which closes the TOCTOU where two concurrent POSTs both pass
    // a separate busy-check and both launch a rip on the same device (orphaned
    // halt token + concurrent writes to one staging dir).
    // The claim also marks the device "scanning": the resume decision is
    // delegated to the worker thread (it scans the disc, cheap, then picks
    // resume_remux vs rip_disc based on the staging dir), keeping scan logic in
    // one place.
    let Some(claim_gen) = ripper::try_claim_active_checked(device, false) else {
        json_response(request, 409, r#"{"ok":false,"error":"already ripping"}"#);
        return;
    };
    let _ = spawn_rip_after_claim(request, cfg, device, resume_mode, claim_gen);
}

/// Spawn the rip worker thread for `device`, assuming the caller has ALREADY
/// won the claim via `ripper::try_claim_active`. Shared by [`handle_rip`]
/// (claims for itself) and [`handle_accept_loss`] (must claim BEFORE writing
/// any staging markers, so a losing claim leaves the on-disk override
/// unarmed — see the comment there).
/// Returns `true` if the worker was spawned. The response has already been
/// sent either way; the boolean exists so a caller that armed on-disk state
/// BEFORE calling (only [`handle_accept_loss`], which writes `.accept-loss`)
/// can disarm it when no worker will ever consume it.
#[must_use]
fn spawn_rip_after_claim(
    request: tiny_http::Request,
    cfg: &Arc<RwLock<Config>>,
    device: &str,
    resume_mode: ResumeMode,
    claim_gen: u64,
) -> bool {
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
        ripper::rollback_failed_spawn(&dev_for_register, claim_gen);
        json_response(
            request,
            500,
            r#"{"ok":false,"error":"thread spawn failed"}"#,
        );
        return false;
    }

    json_response(request, 200, r#"{"ok":true}"#);
    true
}

/// POST `/api/accept-loss/{device}` — operator override.
///
/// Accept the recorded over-threshold main-movie loss and deliver the existing
/// rip instead of re-ripping. Writes the one-shot `.accept-loss` marker into the
/// disc's staging dir, clears the terminal/abort markers so the dir is resumable
/// again, then re-muxes the EXISTING ISO via the resume path (no re-sweep) — where
/// `resume_remux` honors the marker and bypasses the abort gate. Fixes the
/// exhaust → `.failed` → wasteful full-re-rip loop.
fn handle_accept_loss(request: tiny_http::Request, cfg: &Arc<RwLock<Config>>, device: &str) {
    // Resolve the staging dir through the one naming rule
    // (`ripper::staging_basename_for_device`), not from the title alone: with a
    // boxset in the drive the operator's Accept must arm the marker on THIS
    // disc's dir, not on the sibling disc that happens to share its title.
    let staging = {
        let c = match cfg.read() {
            Ok(c) => c,
            Err(e) => e.into_inner(),
        };
        match ripper::staging_basename_for_device(&c, device) {
            Some(base) => c.staging_device_dir(&base),
            None => {
                drop(c);
                json_response(
                    request,
                    404,
                    r#"{"ok":false,"error":"no disc state for device"}"#,
                );
                return;
            }
        }
    };
    let dir = std::path::Path::new(&staging);
    if !dir.exists() {
        json_response(
            request,
            404,
            r#"{"ok":false,"error":"no staging dir to accept"}"#,
        );
        return;
    }
    // Claim the device BEFORE touching any on-disk marker. A rejected
    // (409) accept must leave the staging dir exactly as it was: if we
    // wrote `.accept-loss` first and only THEN discovered the device was
    // already ripping (handle_rip's own try_claim_active), the override
    // stays armed on disk with no rip in flight to consume it. The NEXT
    // legitimate rip/resume on this device would then silently pick up
    // the stale override and mux a rip whose loss was never actually
    // accepted for that run — a damaged rip filed as finished with no
    // operator confirmation for that abort.
    let Some(claim_gen) = ripper::try_claim_active_checked(device, false) else {
        json_response(request, 409, r#"{"ok":false,"error":"already ripping"}"#);
        return;
    };
    // Arm the one-shot override and clear the terminal/abort markers so the dir
    // resumes (re-mux) instead of being refused as failed.
    ripper::staging::write_accept_loss_marker(dir);
    let _ = std::fs::remove_file(dir.join(ripper::staging::FAILED_MARKER));
    ripper::staging::clear_aborted_loss_marker(dir);
    ripper::staging::clear_restart_count(dir);
    crate::log::device_log(
        device,
        "Accept-damage requested — re-muxing the existing ISO with the loss override.",
    );
    // Delegate to the already-claimed spawn path (resume_remux consumes
    // `.accept-loss`); do NOT go through handle_rip, which would try to
    // claim a second time and always lose against the claim just above.
    if !spawn_rip_after_claim(request, cfg, device, ResumeMode::Require, claim_gen) {
        // The OS refused the thread, so NOTHING will consume the override we
        // just armed. Leaving `.accept-loss` on disk after a 500 is the same
        // stale-override hazard the claim-before-write ordering above exists to
        // prevent, just reached by the other door: the next legitimate
        // rip/resume of this disc would silently pick it up and deliver a rip
        // whose loss was never accepted FOR THAT RUN. Disarm, and say so.
        ripper::staging::clear_accept_loss_marker(dir);
        crate::log::device_log(
            device,
            "Accept-damage override disarmed: the rip thread could not be spawned, so no run will consume it.",
        );
    }
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

#[cfg(test)]
mod stop_report_tests {
    use super::stop_report;

    /// Catches the mutation that makes a timed-out Stop report the clean-stop
    /// answer (status "idle" + `{"ok":true}`) — a failure rendered as a
    /// success, which is the highest-value defect class in this crate.
    ///
    /// `handle_stop` used to reset the row to "idle" and answer `{"ok":true}`
    /// whether or not the rip thread drained. When it had NOT drained the
    /// worker was still running, still holding the drive and the staging dir,
    /// and still making the claim's liveness half refuse every later
    /// scan/rip/eject on that device — so the operator saw an idle, quiet card
    /// and a 409 on the next click, with no explanation anywhere but a WARN in
    /// the server log.
    #[test]
    fn a_stop_that_did_not_drain_is_not_reported_as_a_clean_stop() {
        let clean = stop_report(true);
        assert_eq!(
            clean.status, "idle",
            "a drained stop leaves the device idle"
        );
        assert!(clean.last_error.is_empty(), "no error on the clean path");
        assert!(
            clean.body.contains(r#""ok":true"#),
            "a drained stop answers ok:true; got {}",
            clean.body
        );

        let timed_out = stop_report(false);
        assert_ne!(
            timed_out.status, "idle",
            "a stop whose worker is still running must NOT publish idle — the \
             device is still held and every route will refuse it"
        );
        assert!(
            !timed_out.last_error.is_empty(),
            "the reason the device is still busy must reach the state row the \
             dashboard renders, not just the server log"
        );
        assert!(
            timed_out.body.contains(r#""ok":false"#),
            "a stop that stopped nothing must not answer ok:true; got {}",
            timed_out.body
        );
    }
}

#[cfg(test)]
mod worker_panic_tests {
    /// Catches the mutation that removes the `catch_unwind` from ANY worker
    /// spawn site.
    ///
    /// The claim is taken before the spawn (`status="scanning"`), so the worker
    /// owns clearing it on every exit path — and an unwind is an exit path. The
    /// scan spawn site had no `catch_unwind` while the rip site and the poll
    /// loop both did: a panic anywhere in `scan_disc` therefore left the device
    /// claimed with no thread behind the claim, `is_busy` true forever, and
    /// every route on that device answering 409 until the container restarted.
    /// `forget_removed_device` will not evict a busy row and the poll loop
    /// skips busy devices, so nothing self-heals it.
    #[test]
    fn every_worker_spawn_site_catches_its_panic() {
        let src = crate::util::source_lf(include_str!("web.rs"));
        // Match the production call shape only. (This file's test modules are
        // interleaved with production code, so a bare name match would also
        // count this test's own string literals.)
        const SITE: &str = "if let Err(e) = ripper::spawn_rip_thread(";
        let mut sites = 0;
        for (idx, _) in src.match_indices(SITE) {
            sites += 1;
            // The closure body follows the call; a spawn site's panic handling
            // must appear before the next spawn site (or the end of the file).
            let rest = &src[idx..];
            let window_end = rest[1..].find(SITE).map(|i| i + 1).unwrap_or(rest.len());
            assert!(
                rest[..window_end].contains("catch_unwind"),
                "a worker spawn site with no catch_unwind leaves the device's \
                 claim set forever if the worker panics; site {sites}"
            );
        }
        assert!(sites >= 2, "expected both web spawn sites; found {sites}");
    }
}

#[cfg(test)]
mod accept_loss_spawn_failure_tests {
    /// Catches the mutation that drops the disarm branch from
    /// `handle_accept_loss` when `spawn_rip_after_claim` fails.
    ///
    /// The handler arms `.accept-loss` on disk BEFORE it spawns, deliberately:
    /// a rejected (409) accept must leave the staging dir untouched, so the
    /// claim is taken first. But if the OS then refuses the thread, the handler
    /// answers 500 and NOTHING will ever consume the marker it just wrote. The
    /// next legitimate rip or resume of that disc picks the stale override up
    /// and delivers a rip whose loss was never accepted for that run — a
    /// damaged rip filed as finished with no operator confirmation, which is
    /// exactly the hazard the claim-before-write ordering exists to prevent,
    /// reached through the other door.
    ///
    /// Proven structurally: making a real `thread::Builder::spawn` fail inside
    /// the test binary means exhausting the process's thread limit.
    #[test]
    fn a_failed_spawn_disarms_the_accept_loss_override() {
        let src = crate::util::source_lf(include_str!("web.rs"));
        let start = src
            .find("fn handle_accept_loss(")
            .expect("handle_accept_loss must exist");
        let rest = &src[start..];
        let end = rest.find("\n}\n").expect("function must end");
        let body: String = rest[..end]
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            body.contains("if !spawn_rip_after_claim("),
            "handle_accept_loss must CHECK whether the spawn succeeded — a \
             fire-and-forget call cannot disarm the override it armed"
        );
        assert!(
            body.contains("clear_accept_loss_marker("),
            "a spawn failure must disarm `.accept-loss`, or the override sits \
             on disk with no run to consume it and the NEXT rip of this disc \
             silently inherits the operator's consent"
        );
    }
}

#[cfg(test)]
mod dashboard_button_tests {
    /// Catches the mutation that puts a bare `fetch(...)` back into any
    /// device-action button's `onclick`.
    ///
    /// The drive-card buttons were fire-and-forget: `fetch()` with no `.then`
    /// and no `.catch`, so the server's answer was discarded entirely. Eject
    /// renders on `discIn && !active` and "Accept & deliver" on
    /// `lossAborted && !active` — both of those overlap the window in which the
    /// claim refuses with 409 because a worker is still unwinding. The operator
    /// clicked Eject and the disc stayed in the drive with no message; clicked
    /// "Accept & deliver" and watched it grey out (it set `this.disabled=true`
    /// FIRST, which reads as success) while no `.accept-loss` marker was ever
    /// written and the override was never armed. A 409 rendered as success.
    ///
    /// Comment lines are stripped before matching so this pin cannot be
    /// satisfied by its own explanation.
    #[test]
    fn no_device_action_button_discards_the_servers_answer() {
        let src = crate::util::source_lf(include_str!("web.rs"));
        let code: Vec<&str> = src
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .filter(|l| !l.trim_start().starts_with('*'))
            .collect();
        let mut checked = 0;
        for line in code {
            let is_device_action = [
                "/api/scan/",
                "/api/rip/",
                "/api/eject/",
                "/api/stop/",
                "/api/accept-loss/",
            ]
            .iter()
            .any(|ep| line.contains(ep));
            if !line.contains("onclick=") || !is_device_action {
                continue;
            }
            checked += 1;
            assert!(
                !line.contains("fetch("),
                "a device-action button must not call fetch() directly — it \
                 discards the status code, so a 409 renders as a success. Use \
                 apiPost(url, this, label). Offending line:\n{line}"
            );
            assert!(
                line.contains("apiPost("),
                "every device-action button must go through apiPost, which \
                 surfaces the failure and re-enables the button. Offending \
                 line:\n{line}"
            );
        }
        assert!(
            checked >= 8,
            "expected to inspect the whole drive-card button set; only found \
             {checked} — the matcher has drifted away from the buttons"
        );
    }
}

#[cfg(test)]
mod resume_param_tests {
    use super::{ResumeMode, parse_resume_param};

    /// `?resume=no` selects `Wipe`, which DELETES an existing staging dir
    /// before starting a fresh sweep. That makes this the only query-parameter
    /// decision in the crate that can destroy an in-progress or not-yet-moved
    /// rip — the operator's media — and it is reachable unauthenticated from
    /// any host on the LAN.
    ///
    /// It had no test at all. A mutation run flipped the `k == "resume"`
    /// comparison and deleted each value arm with the whole suite still green.
    #[test]
    fn resume_param_maps_only_the_documented_values() {
        // The destructive one. Every spelling of it.
        for q in ["resume=no", "resume=false", "resume=0"] {
            assert_eq!(
                parse_resume_param(q),
                ResumeMode::Wipe,
                "{q} must select Wipe"
            );
        }
        for q in ["resume=yes", "resume=true", "resume=1"] {
            assert_eq!(
                parse_resume_param(q),
                ResumeMode::Require,
                "{q} must select Require"
            );
        }

        // Anything else is Default — never the destructive mode. An
        // unrecognised value must not be read as "no".
        for q in [
            "resume=maybe",
            "resume=",
            "resume",
            "foo=bar",
            "",
            "RESUME=no", // the key match is case-sensitive
            "resume=NO", // ...and so is the value
        ] {
            assert_eq!(
                parse_resume_param(q),
                ResumeMode::Default,
                "{q:?} must fall through to Default, never Wipe"
            );
        }
    }

    /// The scan must match on the KEY, not stumble into the first value that
    /// happens to look like one. With `==` flipped to `!=` the first
    /// non-`resume` pair would decide the mode, so a URL carrying
    /// `?title=no&resume=yes` would wipe the staging dir the operator just
    /// asked to keep.
    #[test]
    fn resume_param_reads_the_resume_key_not_a_neighbouring_one() {
        assert_eq!(
            parse_resume_param("title=no&resume=yes"),
            ResumeMode::Require
        );
        assert_eq!(parse_resume_param("a=1&b=2&resume=no"), ResumeMode::Wipe);
        // A key that merely contains "resume" is not the resume key.
        assert_eq!(parse_resume_param("presume=no"), ResumeMode::Default);
        assert_eq!(parse_resume_param("resumed=no"), ResumeMode::Default);
        // First match wins and stops the scan.
        assert_eq!(
            parse_resume_param("resume=yes&resume=no"),
            ResumeMode::Require
        );
    }
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
    // NOT the plain `guarded_agent`: its 30 s `response` is the ceiling on the
    // whole body (see `guarded_agent_with_timeouts`), which would silently
    // override the KEYDB_FETCH_TIMEOUT budget below — the two would disagree
    // and the smaller, unstated one would win. Built with that budget as the
    // response ceiling so the constant means what it says.
    //
    // This LAN-facing path deliberately keeps the tighter 60 s ceiling rather
    // than `KEYDB_TRANSFER_BUDGET`: it is reachable unauthenticated, holds an
    // in-flight handler slot and the process-wide update flag that 429s every
    // other attempt, so it must not be holdable for five minutes.
    let agent = guarded_agent_with_timeouts(
        pinned,
        std::time::Duration::from_secs(5),
        KEYDB_FETCH_TIMEOUT,
        STALL_TIMEOUT,
    );

    // Wall-clock bound on the whole download so a slow-loris server can't hold
    // the in-flight slot (and the handler thread) indefinitely. Anchored at
    // request start, where the response ceiling above is anchored at header
    // completion, so this stays the true end-to-end bound.
    // Cap is the shared module-level KEYDB_MAX_BYTES (100 MiB); using
    // read_capped_keydb_body means an oversized body returns 413 rather than
    // silently truncating at the limit.
    let keydb_cap = KEYDB_MAX_BYTES;

    // Download via ureq (supports HTTPS) then save via libfreemkv
    let body = match agent
        .get(&keydb_url)
        .config()
        .timeout_global(Some(KEYDB_FETCH_TIMEOUT))
        .build()
        .call()
    {
        Ok(resp) => match read_capped_keydb_body(resp.into_body().into_reader(), keydb_cap) {
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
        Err(ureq::Error::StatusCode(code)) => {
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
            //
            // The error goes through `ureq_error_kind`, like the three sibling
            // sites: `keydb_url` is token-bearing, this log line reaches
            // `autorip.jsonl` and thence the unauthenticated `GET /api/debug`,
            // and `ureq::Error`'s own Display is not guaranteed URL-free
            // (`BadUri` prints the URI it rejected). The deliberate `origin`
            // field above is the whole disclosure this line intends.
            tracing::warn!(
                origin = %crate::webhook::webhook_url_origin(&keydb_url),
                error_kind = %ureq_error_kind(&e),
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
    // Claim the device before ejecting. A separate busy-check then eject left a
    // TOCTOU window in which a rip could start (its own `try_claim_active`)
    // between the check and the eject — ejecting a just-started rip on this
    // irreversible slot-loading drive. The claim closes it: the liveness half
    // is read outside the STATE lock, and the busy-check and the status-set are
    // folded into ONE STATE lock, so it rejects a device that is already
    // scanning/ripping (or whose worker thread is still alive) and, once it has
    // claimed the device (status="scanning"), any concurrent rip-start's claim
    // fails for the duration of the eject. The idle reset below releases the
    // claim. NB: the two registries are never held simultaneously — this is not
    // "one lock over both facts", and the no-inversion argument depends on that.
    if ripper::try_claim_active_checked(device, false).is_none() {
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

    // Cancel the per-device halt and drain the rip thread (the core
    // stop→drain contract; see ripper::stop_and_drain).
    //
    // A drain that TIMES OUT is not a stop. The worker is still running: it
    // still owns the drive, the staging dir and (until it returns) the
    // device's registration, so every later scan/rip/eject/accept-loss on this
    // device is refused by the claim's liveness half — for as long as the
    // worker takes, which for one wedged in `Drive::open`/`eject()` is the
    // container's lifetime. There is no safe way to take the device back: the
    // thread cannot be killed, and re-admitting a rip while it runs is the
    // duplicate-writer bug this whole subsystem is built to prevent.
    //
    // What CAN be fixed is the reporting. This used to reset the row to "idle"
    // and answer `{"ok":true}` either way — a failure rendered as a success:
    // the card went quiet and idle while a worker was still mid-write, and the
    // operator's next click came back 409 with nothing anywhere to explain it.
    // Say what actually happened instead, in the log, in the device log, and
    // in the state row the UI renders.
    let drained = ripper::stop_and_drain(device, std::time::Duration::from_secs(60));
    if !drained {
        tracing::error!(
            device = %device,
            "rip thread did not drain within 60s of stop — the worker is still \
             running and this device stays held until it exits (a worker wedged \
             in a blocking drive ioctl needs a container restart)"
        );
        crate::log::device_log(
            device,
            "Stop: the rip thread did not exit within 60s. It is still running, so \
             this drive stays busy until it does — scan/rip/eject will be refused. \
             If it never exits, restart the container.",
        );
    }

    // Recover-and-proceed on poison (house convention): a poisoned STATE must
    // not turn a Stop into a silent 404.
    let mut state = ripper::STATE.lock().unwrap_or_else(|e| e.into_inner());
    let report = stop_report(drained);
    let existed = state
        .get_mut(device)
        .map(|rs| {
            // Full reset: keep device id + disc_present, drop everything else.
            let disc_still_in = rs.disc_present;
            *rs = ripper::RipState {
                device: device.to_string(),
                status: report.status.to_string(),
                disc_present: disc_still_in,
                last_error: report.last_error.to_string(),
                ..Default::default()
            };
            true
        })
        .unwrap_or(false);
    drop(state);

    if !existed {
        json_response(request, 404, r#"{"ok":false,"error":"drive not found"}"#);
        return;
    }
    ripper::set_stop_cooldown(device);
    json_response(request, 200, report.body);
}

/// What a Stop reports, as a function of whether the rip thread ACTUALLY
/// drained. Split out of [`handle_stop`] so the "a stop that did not stop
/// anything must not render as success" rule is testable without waiting out
/// the real 60 s drain budget.
struct StopReport {
    /// `RipState::status` to publish.
    status: &'static str,
    /// `RipState::last_error` to publish (empty on the clean path).
    last_error: &'static str,
    /// The JSON response body. Always HTTP 200: the Stop WAS delivered (the
    /// `Halt` is cancelled either way) and the UI must not spin — but `ok` is
    /// false when the worker is still running, so a client that checks the
    /// field cannot read a timed-out drain as a completed stop.
    body: &'static str,
}

fn stop_report(drained: bool) -> StopReport {
    if drained {
        StopReport {
            status: "idle",
            last_error: "",
            body: r#"{"ok":true}"#,
        }
    } else {
        StopReport {
            // NOT idle: the device is still held by a live worker. "error" is
            // the status the dashboard renders together with `last_error`, so
            // the reason lands on the card instead of only in the log.
            status: "error",
            last_error: "Stop timed out: the rip thread is still running; the drive stays busy until it exits",
            body: r#"{"ok":false,"error":"stop timed out: the rip thread is still running"}"#,
        }
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
        // Both payload bytes must be ASCII hex digits. `from_str_radix` alone
        // is too lenient: it accepts a leading sign, so `%+3` parsed as 3 and
        // decoded to a control byte instead of staying the literal text the
        // client sent. RFC 3986 percent-escapes are `HEXDIG` only.
        if bytes[i] == b'%'
            && i + 3 <= bytes.len()
            && bytes[i + 1].is_ascii_hexdigit()
            && bytes[i + 2].is_ascii_hexdigit()
            && let Ok(byte) = u8::from_str_radix(&String::from_utf8_lossy(&bytes[i + 1..i + 3]), 16)
        {
            result.push(byte);
            i += 3;
            continue;
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
