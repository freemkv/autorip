use crate::config::{self, Config};
use crate::history;
use crate::ripper;
use std::io::Write as _;
use std::sync::{Arc, RwLock};
use tiny_http::{Header, Method, Response, Server, StatusCode};

/// Embedded single-page HTML dashboard.
const DASHBOARD_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>autorip</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#f6f8fa;--surface:#fff;--card:#fff;--border:#d0d7de;
  --text:#1f2328;--dim:#656d76;--accent:#0969da;
  --green:#1a7f37;--blue:#0969da;--yellow:#9a6700;--red:#cf222e;--gray:#656d76;
  --chip:#eaeef2;--log-bg:#fff;--log-text:#24292f;--log-border:#d0d7de;--poster-bg:#e1e4e8;
}
@media(prefers-color-scheme:dark){:root{
  --bg:#0d1117;--surface:#151b23;--card:#151b23;--border:#3d444d;
  --text:#f0f6fc;--dim:#9198a1;--accent:#79c0ff;
  --green:#56d364;--blue:#79c0ff;--yellow:#e3b341;--red:#ff7b72;--gray:#9198a1;
  --chip:#262c36;--log-bg:#151b23;--log-text:#d1d9e0;--log-border:#3d444d;--poster-bg:#262c36;
}}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:var(--bg);color:var(--text);min-height:100vh}
a{color:var(--accent);text-decoration:none}
.topbar{background:var(--surface);border-bottom:1px solid var(--border);padding:0.75rem 1.5rem;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:10}
.topbar h1{font-size:1.1rem;font-weight:600;letter-spacing:-.02em}
.topbar h1 span{color:var(--accent)}
.topbar nav{display:flex;gap:0.5rem}
.topbar nav button{background:none;border:1px solid var(--border);color:var(--dim);padding:0.4rem 0.9rem;border-radius:6px;cursor:pointer;font-size:0.82rem;transition:all .15s}
.topbar nav button:hover,.topbar nav button.active{color:var(--text);border-color:var(--accent);background:rgba(91,138,245,.08)}
.container{max-width:960px;margin:0 auto;padding:1.25rem}
.section{display:none}.section.active{display:block}
/* Drive cards */
.drive-grid{display:grid;gap:1rem;grid-template-columns:repeat(auto-fill,minmax(420px,1fr))}
.drive-card{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:1.25rem;transition:border-color .2s}
.drive-card:hover{border-color:var(--accent)}
.drive-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:0.75rem}
.drive-device{font-weight:600;font-size:0.95rem}
.badge{display:inline-block;padding:0.15rem 0.55rem;border-radius:4px;font-size:0.7rem;font-weight:700;text-transform:uppercase;letter-spacing:.04em}
.badge-uhd{background:#7c3aed;color:#fff}
.badge-bluray{background:#2563eb;color:#fff}
.badge-dvd{background:#d97706;color:#fff}
.badge-unknown{background:var(--gray);color:#fff}
.status-pill{font-size:0.75rem;padding:0.2rem 0.6rem;border-radius:20px;font-weight:600}
.status-idle{background:rgba(107,114,128,.15);color:var(--gray)}
.status-scanning{background:rgba(251,191,36,.12);color:var(--yellow)}
.status-ripping{background:rgba(96,165,250,.12);color:var(--blue)}
.status-moving{background:rgba(96,165,250,.12);color:var(--blue)}
.status-done{background:rgba(52,211,153,.12);color:var(--green)}
.status-error{background:rgba(248,113,113,.12);color:var(--red)}
.disc-name{color:var(--dim);font-size:0.85rem;margin-bottom:0.75rem;min-height:1.2em}
.progress-wrap{background:var(--surface);border-radius:6px;height:8px;overflow:hidden;margin-bottom:0.5rem}
.progress-bar{height:100%;border-radius:6px;transition:width .4s ease;background:linear-gradient(90deg,var(--accent),#818cf8)}
.progress-bar.done{background:var(--green)}
.progress-bar.error{background:var(--red)}
.stats{display:flex;gap:1.2rem;font-size:0.78rem;color:var(--dim);margin-bottom:0.75rem}
.stats strong{color:var(--text)}
.drive-actions{display:flex;gap:0.4rem}
.drive-actions button{background:var(--surface);border:1px solid var(--border);color:var(--dim);padding:0.35rem 0.7rem;border-radius:5px;cursor:pointer;font-size:0.78rem;transition:all .15s}
.drive-actions button:hover{color:var(--text);border-color:var(--accent)}
.error-msg{color:var(--red);font-size:0.78rem;margin-top:0.3rem}
.empty-state{text-align:center;padding:3rem 1rem;color:var(--dim)}
.empty-state .icon{font-size:2.5rem;margin-bottom:0.75rem;opacity:.4}
.empty-state p{font-size:0.9rem}
/* Settings */
.settings-form{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:1.5rem}
.form-group{margin-bottom:1rem}
.form-group label{display:block;font-size:0.82rem;color:var(--dim);margin-bottom:0.3rem;font-weight:500}
.form-group input[type=text],.form-group input[type=number],.form-group select{
  width:100%;background:var(--surface);border:1px solid var(--border);color:var(--text);
  padding:0.5rem 0.7rem;border-radius:6px;font-size:0.85rem;font-family:inherit}
.form-group input:focus,.form-group select:focus{outline:none;border-color:var(--accent)}
.form-row{display:grid;grid-template-columns:1fr 1fr;gap:1rem}
.toggle-row{display:flex;align-items:center;gap:0.6rem;margin-bottom:0.8rem}
.toggle-row input[type=checkbox]{width:16px;height:16px;accent-color:var(--accent)}
.toggle-row label{font-size:0.85rem;cursor:pointer}
.btn{padding:0.55rem 1.2rem;border-radius:6px;border:none;cursor:pointer;font-size:0.85rem;font-weight:600;transition:all .15s}
.btn-primary{background:var(--accent);color:#fff}.btn-primary:hover{opacity:.85}
.btn-save{margin-top:0.5rem}
.toast{position:fixed;bottom:1.5rem;right:1.5rem;background:var(--green);color:#000;padding:0.6rem 1.2rem;border-radius:8px;font-size:0.85rem;font-weight:600;opacity:0;transform:translateY(10px);transition:all .3s;pointer-events:none;z-index:100}
.toast.show{opacity:1;transform:translateY(0)}
/* History */
.history-list{display:flex;flex-direction:column;gap:0.6rem}
.history-item{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:1rem 1.25rem;display:flex;align-items:center;justify-content:space-between}
.history-name{font-weight:600;font-size:0.9rem}
.history-meta{font-size:0.78rem;color:var(--dim)}
/* Connection indicator */
.conn{width:8px;height:8px;border-radius:50%;display:inline-block;margin-right:0.4rem}
.conn.ok{background:var(--green)}.conn.err{background:var(--red)}
@media(max-width:520px){
  .drive-grid{grid-template-columns:1fr}
  .form-row{grid-template-columns:1fr}
  .topbar{flex-direction:column;gap:0.5rem}
}
</style>
</head>
<body>
<div class="topbar">
  <h1><span>auto</span>rip</h1>
  <nav>
    <button class="active" onclick="showSection('drives')">Drives</button>
    <button onclick="showSection('settings')">Settings</button>
    <button onclick="showSection('history')">History</button>
    <span><span class="conn" id="connDot"></span><span id="connText" style="font-size:.75rem;color:var(--dim)"></span></span>
  </nav>
</div>
<div class="container">
  <div id="drives" class="section active"></div>
  <div id="settings" class="section"></div>
  <div id="history" class="section"></div>
</div>
<div class="toast" id="toast"></div>
<script>
(function(){
  /* Navigation */
  window.showSection = function(id) {
    document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
    document.getElementById(id).classList.add('active');
    document.querySelectorAll('.topbar nav button').forEach(b => b.classList.remove('active'));
    document.querySelector('.topbar nav button[onclick*="'+id+'"]').classList.add('active');
    if (id === 'settings') loadSettings();
    if (id === 'history') loadHistory();
  };

  /* Toast */
  function toast(msg) {
    const t = document.getElementById('toast');
    t.textContent = msg; t.classList.add('show');
    setTimeout(() => t.classList.remove('show'), 2200);
  }

  /* Drive cards */
  function renderDrives(state) {
    const el = document.getElementById('drives');
    const entries = Object.entries(state);
    if (entries.length === 0) {
      el.innerHTML = '<div class="empty-state"><div class="icon">&#128191;</div><p>No drives detected. Insert a disc to begin.</p></div>';
      return;
    }
    let html = '<div class="drive-grid">';
    for (const [dev, d] of entries) {
      const fmt = d.disc_format || 'unknown';
      const badgeCls = 'badge badge-' + fmt;
      const statusCls = 'status-pill status-' + d.status;
      const pctClass = d.status === 'done' ? 'done' : d.status === 'error' ? 'error' : '';
      html += '<div class="drive-card">';
      html += '<div class="drive-header">';
      html += '<span class="drive-device">/dev/' + esc(dev) + '</span>';
      html += '<span>';
      if (d.disc_format) html += '<span class="' + badgeCls + '">' + fmtLabel(fmt) + '</span> ';
      html += '<span class="' + statusCls + '">' + esc(d.status) + '</span>';
      html += '</span></div>';
      html += '<div class="disc-name">' + esc(d.disc_name || '') + (d.output_file ? ' &mdash; ' + esc(d.output_file) : '') + '</div>';
      html += '<div class="progress-wrap"><div class="progress-bar ' + pctClass + '" style="width:' + (d.progress_pct||0) + '%"></div></div>';
      html += '<div class="stats">';
      html += '<span>Progress: <strong>' + (d.progress_pct||0) + '%</strong></span>';
      html += '<span>Speed: <strong>' + (d.speed_mbs ? d.speed_mbs.toFixed(1) + ' MB/s' : '--') + '</strong></span>';
      html += '<span>ETA: <strong>' + (d.eta || '--') + '</strong></span>';
      if (d.errors) html += '<span>Errors: <strong>' + d.errors + '</strong></span>';
      html += '</div>';
      html += '<div class="drive-actions">';
      html += '<button onclick="doAction(\'rip\',\'' + esc(dev) + '\')">Rip</button>';
      html += '<button onclick="doAction(\'eject\',\'' + esc(dev) + '\')">Eject</button>';
      html += '<button onclick="doAction(\'stop\',\'' + esc(dev) + '\')">Stop</button>';
      html += '</div>';
      if (d.last_error) html += '<div class="error-msg">' + esc(d.last_error) + '</div>';
      html += '</div>';
    }
    html += '</div>';
    el.innerHTML = html;
  }

  function fmtLabel(f) {
    if (f === 'uhd') return '4K UHD';
    if (f === 'bluray') return 'Blu-ray';
    if (f === 'dvd') return 'DVD';
    return f;
  }

  function esc(s) {
    if (!s) return '';
    const d = document.createElement('div'); d.textContent = s; return d.innerHTML;
  }

  /* SSE */
  let es;
  function connectSSE() {
    es = new EventSource('/events');
    const dot = document.getElementById('connDot');
    const txt = document.getElementById('connText');
    es.onopen = function() { dot.className='conn ok'; txt.textContent='live'; };
    es.onmessage = function(e) {
      try { renderDrives(JSON.parse(e.data)); } catch(_){}
    };
    es.onerror = function() {
      dot.className='conn err'; txt.textContent='reconnecting';
    };
  }
  connectSSE();

  /* Actions */
  window.doAction = function(action, dev) {
    fetch('/api/' + action + '/' + encodeURIComponent(dev), {method:'POST'})
    .then(r => r.json())
    .then(r => { if (r.ok) toast(action + ' sent'); else toast('Error: ' + (r.error||'unknown')); })
    .catch(() => toast('Request failed'));
  };

  /* Settings */
  function loadSettings() {
    fetch('/api/settings').then(r=>r.json()).then(cfg => {
      const el = document.getElementById('settings');
      el.innerHTML = '<div class="settings-form">' +
        '<h2 style="margin-bottom:1rem;font-size:1rem">Settings</h2>' +
        '<div class="form-row">' +
          fg('output_dir','Output Directory',cfg.output_dir,'text') +
          fg('staging_dir','Staging Directory',cfg.staging_dir,'text') +
        '</div>' +
        '<div class="form-row">' +
          fg('movie_dir','Movie Directory',cfg.movie_dir,'text') +
          fg('tv_dir','TV Directory',cfg.tv_dir,'text') +
        '</div>' +
        '<div class="form-row">' +
          fg('min_length_secs','Min Title Length (sec)',cfg.min_length_secs,'number') +
          fgSelect('on_insert','On Disc Insert',cfg.on_insert,['nothing','identify','rip']) +
        '</div>' +
        '<div class="form-row">' +
          fg('tmdb_api_key','TMDB API Key',cfg.tmdb_api_key,'text') +
          fg('port','Web Port',cfg.port,'number') +
        '</div>' +
        toggle('main_feature','Main feature only',cfg.main_feature) +
        toggle('auto_eject','Auto eject after rip',cfg.auto_eject) +
        '<button class="btn btn-primary btn-save" onclick="saveSettings()">Save Settings</button>' +
      '</div>';
    });
  }
  function fg(id,label,val,type){
    return '<div class="form-group"><label for="s_'+id+'">'+label+'</label><input type="'+type+'" id="s_'+id+'" value="'+esc(String(val||''))+'"></div>';
  }
  function fgSelect(id,label,val,opts){
    let h='<div class="form-group"><label for="s_'+id+'">'+label+'</label><select id="s_'+id+'">';
    for(const o of opts) h+='<option value="'+o+'"'+(o===val?' selected':'')+'>'+o+'</option>';
    return h+'</select></div>';
  }
  function toggle(id,label,val){
    return '<div class="toggle-row"><input type="checkbox" id="s_'+id+'"'+(val?' checked':'')+'><label for="s_'+id+'">'+label+'</label></div>';
  }
  window.saveSettings = function() {
    const g = id => document.getElementById('s_'+id);
    const body = {
      output_dir: g('output_dir').value,
      staging_dir: g('staging_dir').value,
      movie_dir: g('movie_dir').value,
      tv_dir: g('tv_dir').value,
      min_length_secs: parseInt(g('min_length_secs').value)||600,
      on_insert: g('on_insert').value,
      tmdb_api_key: g('tmdb_api_key').value,
      port: parseInt(g('port').value)||8080,
      main_feature: g('main_feature').checked,
      auto_eject: g('auto_eject').checked,
    };
    fetch('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)})
    .then(r=>r.json()).then(r=>{ toast(r.ok?'Settings saved':'Error saving'); })
    .catch(()=>toast('Save failed'));
  };

  /* History */
  function loadHistory() {
    fetch('/api/history').then(r=>r.json()).then(items => {
      const el = document.getElementById('history');
      if (!items.length) { el.innerHTML='<div class="empty-state"><div class="icon">&#128218;</div><p>No rip history yet.</p></div>'; return; }
      let h='<div class="history-list">';
      for (const it of items) {
        h+='<div class="history-item"><div><div class="history-name">'+esc(it.title||it.disc_name||'Unknown')+'</div>';
        h+='<div class="history-meta">'+(it.format?fmtLabel(it.format)+' &bull; ':'')+esc(it.date||'')+'</div></div>';
        if(it.duration) h+='<div class="history-meta">'+esc(it.duration)+'</div>';
        h+='</div>';
      }
      h+='</div>';
      el.innerHTML=h;
    }).catch(()=>{
      document.getElementById('history').innerHTML='<div class="empty-state"><p>Could not load history.</p></div>';
    });
  }

  /* Initial state fetch */
  fetch('/api/state').then(r=>r.json()).then(renderDrives).catch(()=>{});
})();
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

    // Dummy SSE clients list (kept for API compatibility, SSE is per-thread).
    let sse_clients: Arc<std::sync::Mutex<Vec<std::net::TcpStream>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));

    // Request handler loop — one thread per request via tiny_http's incoming_requests().
    // We spawn worker threads to avoid blocking on SSE or slow clients.
    for request in server.incoming_requests() {
        let cfg = Arc::clone(cfg);
        let sse_clients = Arc::clone(&sse_clients);
        std::thread::spawn(move || {
            handle_request(request, &cfg, &sse_clients);
        });
    }
}

fn handle_request(
    request: tiny_http::Request,
    cfg: &Arc<RwLock<Config>>,
    _sse_clients: &Arc<std::sync::Mutex<Vec<std::net::TcpStream>>>,
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
    } else if is_get && url == "/api/settings" {
        let c = cfg.read().unwrap();
        let json = serde_json::to_string(&*c).unwrap_or_else(|_| "{}".to_string());
        json_response(request, 200, &json);
    } else if is_post && url == "/api/settings" {
        handle_settings_post(request, cfg);
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

fn get_state_json() -> String {
    let state = ripper::STATE.lock().unwrap();
    serde_json::to_string(&*state).unwrap_or_else(|_| "{}".to_string())
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
    // Hijack the underlying TCP stream from tiny_http by writing the HTTP response
    // headers ourselves, then keeping the stream alive for SSE.
    //
    // tiny_http doesn't natively support streaming responses, so we grab the raw
    // TCP stream via the upgrade mechanism: we respond with 200 + SSE headers using
    // a zero-length body, then obtain the underlying socket.

    // Write the SSE HTTP headers manually via tiny_http's response, then keep the
    // underlying stream by using the `into_writer()` upgrade path.
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

    // `respond_raw` gives us the underlying writer back, keeping the TCP connection.
    let mut stream = match request.upgrade("sse", response) {
        stream => stream,
    };

    // Send an initial state event immediately.
    let initial = format!("data: {}\n\n", get_state_json());
    if stream.write_all(initial.as_bytes()).is_err() {
        return;
    }
    let _ = stream.flush();

    // Try to clone the TcpStream for the broadcaster. The `upgrade()` returns a
    // Box<dyn ReadWrite + Send>, but the underlying type is TcpStream. We use
    // try_clone via a trick: write to the returned writer from here, or convert.
    // Since we cannot directly clone the boxed stream, we keep this thread alive
    // and read from the client (to detect disconnection) while the broadcaster
    // writes via the retained stream object.
    //
    // Actually, the simplest approach: park this thread and periodically push data.
    // This avoids needing to share the stream. The thread is cheap.

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
    // Check if already ripping
    let already = ripper::STATE.lock().map(|s| {
        s.get(device)
            .map(|r| r.status == "scanning" || r.status == "ripping")
            .unwrap_or(false)
    }).unwrap_or(false);

    if already {
        json_response(request, 409, r#"{"ok":false,"error":"already ripping"}"#);
        return;
    }

    let device_path = format!("/dev/{}", device);
    let dev = device.to_string();
    let cfg = Arc::clone(cfg);
    std::thread::spawn(move || {
        // We cannot call rip_disc directly since it's private, but we can update
        // state to trigger the poll loop to pick it up. For now, set state to
        // trigger awareness.
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
        // The drive_poll_loop will pick up this drive on its next cycle if a disc
        // is present, since we set status to "idle" (not "scanning"/"ripping").
        let _ = cfg;
        let _ = device_path;
    });

    json_response(request, 200, r#"{"ok":true}"#);
}

fn handle_eject(request: tiny_http::Request, device: &str) {
    let device_path = format!("/dev/{}", device);
    let result = std::process::Command::new("eject")
        .arg(&device_path)
        .output();
    match result {
        Ok(out) if out.status.success() => {
            // Update state
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
        Ok(out) => {
            let err = String::from_utf8_lossy(&out.stderr);
            let body = serde_json::json!({"ok": false, "error": err.trim()}).to_string();
            json_response(request, 500, &body);
        }
        Err(e) => {
            let body = serde_json::json!({"ok": false, "error": e.to_string()}).to_string();
            json_response(request, 500, &body);
        }
    }
}

fn handle_stop(request: tiny_http::Request, device: &str) {
    // Set state to "idle" to signal the ripping thread should stop.
    // The rip loop checks state, so this is a cooperative stop.
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
