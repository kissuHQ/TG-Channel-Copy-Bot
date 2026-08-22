// ── Helpers ────────────────────────────────────────────
function toast(msg, isErr = false) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.className = 'show' + (isErr ? ' err' : '');
  clearTimeout(t._to);
  t._to = setTimeout(() => t.className = '', 3200);
}

async function api(path, body = null) {
  const opts = body !== null
    ? { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }
    : {};
  const res = await fetch(path, opts);
  return res.json();
}

// ── Change-driven dashboard updates ────────────────────
let _serverUptime = 0;
let _uptimeReceivedAt = 0;

function formatUptime(totalSeconds) {
  const seconds = Math.max(0, Math.floor(totalSeconds));
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = seconds % 60;
  return h ? `${h}h ${m}m ${s}s` : `${m}m ${s}s`;
}

function renderStatus(data) {
  // Header badge
  const badge = document.getElementById('global-status');
  const dot   = badge.querySelector('.dot');
  const lbl   = document.getElementById('status-label');
  const on    = data.connected;
  badge.className = 'badge ' + (on ? 'on' : 'off');
  dot.className   = 'dot '  + (on ? 'on' : 'off');
  lbl.textContent = data.paused && data.running ? 'Paused' : (on ? 'Connected' : 'Offline');

  // Stat cards
  const stats = data.stats || {};
  const copied = Object.entries(stats).filter(([k]) => k !== 'failed').reduce((a,[,v]) => a+v, 0);
  document.getElementById('c-copied').textContent  = copied;
  document.getElementById('c-failed').textContent  = stats.failed || 0;
  document.getElementById('c-pct').textContent     = data.pct + '%';
  _serverUptime = data.uptime_seconds || 0;
  _uptimeReceivedAt = Date.now();
  document.getElementById('c-uptime').textContent = data.uptime || formatUptime(_serverUptime);

  // Progress section
  const progText = data.paused ? '⏸️ Paused'
                 : data.running ? `🔄 Syncing... ${data.current} / ${data.total}`
                               : '🔴 Stopped';
  document.getElementById('sync-status-text').textContent = progText;
  document.getElementById('progress-bar').style.width = data.pct + '%';
  document.getElementById('prog-cur').textContent = data.current;
  document.getElementById('prog-tot').textContent = data.total;
  document.getElementById('prog-pct').textContent = data.pct + '%';
  document.getElementById('last-id').textContent  = data.last_id;

  // Live transfer card
  const xfer = data.transfer;
  const xferCard = document.getElementById('transfer-card');
  if (xfer && data.running) {
    xferCard.style.display = 'block';
    document.getElementById('xfer-phase').textContent = xfer.phase;
    document.getElementById('xfer-file').textContent  = xfer.file;
    document.getElementById('xfer-pct').textContent   = xfer.pct + '%';
    document.getElementById('xfer-bar').style.width   = xfer.pct + '%';
    document.getElementById('xfer-size').textContent  = `${xfer.cur_mb} / ${xfer.tot_mb} MB`;
    document.getElementById('xfer-speed').textContent = '⚡ ' + xfer.speed;
  } else {
    xferCard.style.display = 'none';
  }

  // Media stats
  document.getElementById('s-photo').textContent  = stats.photo  || 0;
  document.getElementById('s-video').textContent  = stats.video  || 0;
  document.getElementById('s-doc').textContent    = stats.doc    || 0;
  document.getElementById('s-text').textContent   = stats.text   || 0;
  document.getElementById('s-other').textContent  = stats.other  || 0;
  document.getElementById('s-failed').textContent = stats.failed || 0;

  // Channels
  document.getElementById('src-name').textContent = data.source || 'Not set';
  document.getElementById('tgt-name').textContent = data.target || 'Not set';
  const auto = !!data.auto_forward;
  document.getElementById('auto-label').textContent = auto ? 'Enabled' : 'Disabled';
  const autoBtn = document.getElementById('auto-toggle');
  autoBtn.textContent = auto ? 'Disable' : 'Enable';
  autoBtn.className = auto ? 'btn-primary' : 'btn-warn';
  const autoStats = data.auto_stats || {};
  document.getElementById('auto-sent').textContent = autoStats.sent || 0;
  document.getElementById('auto-failed').textContent = autoStats.failed || 0;
  document.getElementById('queue-size').textContent = data.queue_size || 0;
  renderTasks(data.tasks || []);
  if (data.pairs) renderPairs(data.pairs);

  document.getElementById('last-updated').textContent = new Date().toLocaleTimeString();
}

function renderPairs(pairs) {
  const list = document.getElementById('pair-list');
  const select = document.getElementById('task-pair');
  if (!pairs || !pairs.length) {
    list.innerHTML = '<div class="hint">Add a pair to create tracked tasks.</div>';
    select.innerHTML = '<option value="">No pairs</option>';
    return;
  }
  list.innerHTML = pairs.map(p => `<div class="task-item">
    <strong>${escapeHtml(p.name)}</strong>
    <div class="task-route">${escapeHtml(p.source_title)} → ${escapeHtml(p.target_title)}</div>
    <button class="mini-danger" onclick="deletePair('${escapeHtml(p.id)}')">Delete</button>
  </div>`).join('');
  select.innerHTML = pairs.map(p => `<option value="${escapeHtml(p.id)}">${escapeHtml(p.name)}</option>`).join('');
}

function renderTasks(tasks) {
  const box = document.getElementById('task-list');
  if (!tasks.length) {
    box.innerHTML = '<div class="hint">No tasks yet.</div>';
    return;
  }
  box.innerHTML = tasks.slice(-12).reverse().map(task => {
    const statusClass = task.status === 'complete' ? 'done' :
                        task.status === 'failed' ? 'failed' :
                        task.status === 'running' ? 'running' : 'queued';
    return `<div class="task-item">
      <div><strong>${escapeHtml(task.id)}</strong> <span class="task-status ${statusClass}">${escapeHtml(task.status)}</span></div>
      <div class="task-route">${escapeHtml(task.source || 'Source')} → ${escapeHtml(task.target || 'Target')}</div>
      <div class="task-mode">${escapeHtml(task.mode || 'sync')} · direct ${task.stats?.direct || 0} · fallback ${task.stats?.fallback || 0}</div>
      ${task.status === 'queued' || task.status === 'running' ? `<button class="mini-danger" onclick="controlTask('${escapeHtml(task.id)}','cancel')">Cancel</button>` : ''}
      ${task.status === 'running' || task.status === 'paused' ? `<button class="mini-btn" onclick="controlTask('${escapeHtml(task.id)}','${task.status === 'paused' ? 'resume' : 'pause'}')">${task.status === 'paused' ? 'Resume' : 'Pause'}</button>` : ''}
    </div>`;
  }).join('');
}

async function controlTask(id, action) {
  const url = '/api/tasks/' + encodeURIComponent(id);
  const data = action === 'cancel'
    ? await fetch(url, {method:'DELETE'}).then(r => r.json())
    : await fetch(url, {method:'PATCH', headers:{'Content-Type':'application/json'}, body:JSON.stringify({paused: action === 'pause'})}).then(r => r.json());
  toast(data.ok ? '✅ Task updated' : '❌ ' + data.error, !data.ok);
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, c => ({
    '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#039;'
  }[c]));
}

function _logClass(line) {
  if (line.includes('📥') || line.includes('Downloading')) return 'dl';
  if (line.includes('📤') || line.includes('Uploading'))   return 'ul';
  if (line.includes('✅'))                                  return 'ok';
  if (line.includes('❌') || line.includes('Failed'))       return 'err';
  if (line.includes('⏭️') || line.includes('Skipped'))      return 'warn';
  if (line.includes('⚠️') || line.includes('WARNING'))      return 'warn';
  if (line.includes('🏁') || line.includes('complete'))     return 'done';
  if (line.includes('🚀') || line.includes('shuru'))        return 'start';
  if (line.includes('⏸️') || line.includes('Paused') ||
      line.includes('FloodWait') || line.includes('Flood')) return 'warn';
  return '';
}

function renderLogs(lines) {
  const box = document.getElementById('log-box');
  box.innerHTML = '';
  lines.forEach(line => {
    const div = document.createElement('div');
    div.className = 'log-line ' + _logClass(line);
    div.textContent = line;
    box.appendChild(div);
  });
  box.scrollTop = box.scrollHeight;
}

function clearLog() {
  document.getElementById('log-box').innerHTML = '';
}

function applyDashboard(payload) {
  if (payload.status) renderStatus(payload.status);
  if (payload.logs) renderLogs(payload.logs);
}

async function loadDashboard() {
  try {
    const data = await api('/api/bootstrap');
    applyDashboard(data);
    const pairs = await api('/api/pairs');
    if (pairs.ok) renderPairs(pairs.pairs);
  } catch {
    toast('Dashboard connection failed', true);
  }
}

async function addPair() {
  const payload = {
    name: document.getElementById('pair-name').value.trim(),
    source: document.getElementById('pair-source').value.trim(),
    target: document.getElementById('pair-target').value.trim(),
    include_keywords: document.getElementById('pair-include').value,
    exclude_keywords: document.getElementById('pair-exclude').value,
    caption_prefix: document.getElementById('pair-prefix').value,
    caption_suffix: document.getElementById('pair-suffix').value,
    remove_links: document.getElementById('pair-links').checked,
    remove_source_name: document.getElementById('pair-source-name').checked,
    allowed_types: [...document.querySelectorAll('.pair-type:checked')].map(x => x.value)
  };
  const data = await api('/api/pairs', payload);
  if (!data.ok) return toast('❌ ' + data.error, true);
  toast('✅ Pair added');
  renderPairs((await api('/api/pairs')).pairs);
}

async function deletePair(id) {
  if (!confirm('Delete this pair?')) return;
  const data = await fetch('/api/pairs/' + encodeURIComponent(id), {method:'DELETE'}).then(r => r.json());
  toast(data.ok ? '✅ Pair deleted' : '❌ ' + data.error, !data.ok);
  if (data.ok) renderPairs((await api('/api/pairs')).pairs);
}

async function createTask() {
  const pair_id = document.getElementById('task-pair').value;
  const mode = document.getElementById('task-mode').value;
  const value = parseInt(document.getElementById('task-limit').value || '0');
  if (!pair_id) return toast('Pehle pair add karo', true);
  const data = await api('/api/tasks', {pair_id, mode, limit: mode === 'last' ? value : 0, min_id: mode === 'from_id' ? value : 0});
  toast(data.ok ? `✅ Task ${data.task.id} queued` : '❌ ' + data.error, !data.ok);
}

function connectDashboard() {
  const events = new EventSource('/api/events');
  events.addEventListener('dashboard', event => {
    try { applyDashboard(JSON.parse(event.data)); } catch { /* reconnect handles transport errors */ }
  });
  events.onerror = () => {
    // EventSource reconnects automatically; no polling fallback is needed.
    document.getElementById('status-label').textContent = 'Reconnecting...';
  };
}

// ── Channel set ────────────────────────────────────────
async function setChannel(type) {
  const inputId = type === 'source' ? 'src-input' : 'tgt-input';
  const nameId  = type === 'source' ? 'src-name'  : 'tgt-name';
  const val = document.getElementById(inputId).value.trim();
  if (!val) { toast('Channel ID ya username daalo', true); return; }

  toast('Setting...');
  const url  = type === 'source' ? '/api/setsource' : '/api/settarget';
  const data = await api(url, { channel: val });
  if (data.ok) {
    toast('✅ ' + data.title + ' set!');
    document.getElementById(nameId).textContent = data.title;
    document.getElementById(inputId).value = '';
  } else {
    toast('❌ ' + data.error, true);
  }
}

// ── Sync actions ───────────────────────────────────────
async function syncAction(action) {
  const data = await api('/api/' + action, {});
  if (data.ok) {
    const labels = { sync:'▶ Sync shuru!', pause:'⏸ Paused', resume:'▶ Resumed', stop:'⏹ Stopped' };
    toast(labels[action] || '✅ Done');
  } else {
    toast('❌ ' + data.error, true);
  }
}

async function syncFrom() {
  const mid = document.getElementById('syncfrom-input').value.trim();
  if (!mid) { toast('Message ID daalo', true); return; }
  const data = await api('/api/syncfrom', { min_id: parseInt(mid) });
  toast(data.ok ? `✅ Sync from ID ${mid} shuru` : '❌ ' + data.error, !data.ok);
}

async function syncLast() {
  const n = document.getElementById('synclast-input').value.trim();
  if (!n) { toast('Number daalo', true); return; }
  const data = await api('/api/synclast', { n: parseInt(n) });
  toast(data.ok ? `✅ Last ${n} messages sync shuru` : '❌ ' + data.error, !data.ok);
}

async function resetBot() {
  if (!confirm('Sab config reset ho jayega. Sure?')) return;
  const data = await api('/api/reset', {});
  toast(data.ok ? '✅ Reset done' : '❌ ' + data.error, !data.ok);
}

async function toggleAutoForward() {
  const enabled = !document.getElementById('auto-label').textContent.includes('Enabled');
  const data = await api('/api/autoforward', { enabled });
  toast(data.ok
    ? (enabled ? '✅ Auto-forward enabled' : '✅ Auto-forward disabled')
    : '❌ ' + data.error, !data.ok);
}

// ── Init ───────────────────────────────────────────────
loadDashboard();
connectDashboard();
setInterval(() => {
  // Only the local clock moves; no request is made.
  const elapsed = (Date.now() - _uptimeReceivedAt) / 1000;
  document.getElementById('c-uptime').textContent =
    formatUptime(_serverUptime + elapsed);
}, 1000);
