const page = window.PAGE || 'dashboard';
let appData = { status: {}, pairs: [], tasks: [] };

function esc(value) {
  return String(value ?? '').replace(/[&<>"']/g, char => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[char]));
}
function toast(message, error = false) {
  const node = document.getElementById('toast');
  if (!node) return;
  node.textContent = message; node.className = 'app-toast show' + (error ? ' error' : '');
  clearTimeout(node._timer); node._timer = setTimeout(() => node.className = 'app-toast', 3200);
}
async function request(path, options = {}) {
  try {
    const fetchOptions = {...options, headers:{...(options.headers || {})}};
    if (fetchOptions.body && !(fetchOptions.body instanceof FormData) &&
        !fetchOptions.headers['Content-Type']) {
      fetchOptions.headers['Content-Type'] = 'application/json';
    }
    const response = await fetch(path, fetchOptions);
    const text = await response.text();
    let result;
    try { result = text ? JSON.parse(text) : {}; } catch (_) {
      result = {ok:false, error:'The server returned an invalid response'};
    }
    if (!response.ok) {
      return {...result, ok:false, error:result.error || `Request failed (${response.status})`};
    }
    return result;
  } catch (_) {
    return {ok:false, error:'Network error. Please try again.'};
  }
}
function statusClass(status) { return ['running','queued','paused','complete','failed'].includes(status) ? status : 'neutral'; }
function statusPill(status) { return `<span class="status-pill ${statusClass(status)}">${esc(status || 'unknown')}</span>`; }
function statsTotal(task) {
  const stats = task.stats || {};
  return ['text','photo','video','doc','other'].reduce((sum, key) => sum + Number(stats[key] || 0), 0);
}
function keywordText(value) { return Array.isArray(value) ? value.join(', ') : String(value || ''); }
function renderConnection(status) {
  const online = !!status.connected;
  document.querySelectorAll('#side-status,#top-status').forEach(node => node.textContent = online ? 'Connected' : 'Offline');
  document.querySelectorAll('#side-status-dot').forEach(node => node.classList.toggle('online', online));
  document.querySelectorAll('.live-indicator').forEach(node => node.classList.toggle('online', online));
}
function applyData(payload) {
  if (payload.status) {
    appData.status = payload.status;
    appData.tasks = payload.status.tasks || appData.tasks;
    appData.pairs = payload.status.pairs || appData.pairs;
    renderConnection(payload.status);
    if (page === 'dashboard') renderDashboard();
    if (page === 'tasks') renderTaskPage();
    if (page === 'task-detail') renderTaskDetail();
  }
  if (payload.pairs) { appData.pairs = payload.pairs; if (page === 'pairs' || page === 'tasks') renderPageLists(); }
}
async function loadAppData() {
  try {
    const [bootstrap, pairs, tasks] = await Promise.all([
      request('/api/bootstrap'), request('/api/pairs'), request('/api/tasks')
    ]);
    if (!bootstrap.ok || !pairs.ok || !tasks.ok) {
      throw new Error(bootstrap.error || pairs.error || tasks.error || 'Could not load workspace data');
    }
    appData.status = bootstrap.status || {};
    appData.tasks = tasks.tasks || appData.status.tasks || [];
    appData.pairs = pairs.pairs || appData.status.pairs || [];
    applyData({status:appData.status, pairs:appData.pairs});
    renderPageLists();
    if (page === 'settings') await loadSettings();
    if (page === 'task-detail') renderTaskDetail();
  } catch (error) { toast('Could not load the workspace', true); }
}
function connectLive() {
  const events = new EventSource('/api/events');
  events.addEventListener('dashboard', event => { try { applyData(JSON.parse(event.data)); } catch (_) {} });
  events.onerror = () => document.querySelectorAll('#top-status').forEach(node => node.textContent = 'Reconnecting');
}
function renderDashboard() {
  const s = appData.status || {}, stats = s.stats || {}, tasks = appData.tasks || [];
  const copied = ['text','photo','video','doc','other'].reduce((sum,key) => sum + Number(stats[key] || 0), 0);
  const set = (id, value) => { const node = document.getElementById(id); if (node) node.textContent = value; };
  set('dash-copied', copied); set('dash-failed', stats.failed || 0); set('dash-progress', `${s.pct || 0}%`);
  set('dash-progress-note', s.running ? 'Task in progress' : 'No active task'); set('dash-pairs', appData.pairs.length);
  set('dash-progress-count', `${s.current || 0} / ${s.total || 0}`); set('dash-last-sync', `Last synced ID ${s.last_id || '—'}`);
  set('dash-transfer', s.transfer ? `${s.transfer.phase}: ${s.transfer.file}` : (s.running ? 'Syncing messages' : 'Waiting for a task'));
  const bar = document.getElementById('dash-progress-bar'); if (bar) bar.style.width = `${s.pct || 0}%`;
  const run = document.getElementById('dash-run-status'); if (run) { const activePaused = !!s.running && !!s.paused; run.className = `status-pill ${activePaused ? 'paused' : s.running ? 'running' : 'neutral'}`; run.textContent = activePaused ? 'Paused' : s.running ? 'Running' : 'Stopped'; }
  const list = document.getElementById('dash-task-list');
  if (list) list.innerHTML = tasks.length ? tasks.slice(-4).reverse().map(task => `<a class="compact-item" href="/tasks/${encodeURIComponent(task.id)}"><div class="compact-item-top"><strong>${esc(task.id)}</strong>${statusPill(task.status)}</div><small>${esc(task.source || 'Source')} → ${esc(task.target || 'Target')} · ${statsTotal(task)} copied</small></a>`).join('') : '<div class="empty-state">No tasks yet. Create your first sync.</div>';
  const health = document.getElementById('dash-health-list');
  if (health) health.innerHTML = appData.pairs.length ? appData.pairs.slice(0,4).map(pair => { const h = (s.health || {})[pair.id] || {}; const ok = h.source_accessible && h.target_writable; return `<div class="compact-item"><div class="compact-item-top"><strong>${esc(pair.name)}</strong>${statusPill(ok ? 'complete' : 'failed')}</div><small>${ok ? 'Source and target are ready' : 'Check source access or target permissions'}</small></div>`; }).join('') : '<div class="empty-state">Add a channel pair to see health.</div>';
  const storage = document.getElementById('dash-storage'); const store = s.storage || {};
  if (storage) storage.innerHTML = `<strong>${store.used_mb || 0} MB</strong> <span>of ${store.limit_mb || '—'} MB used</span><div class="progress-track"><span style="width:${store.limit_bytes ? Math.min(100, store.used_bytes / store.limit_bytes * 100) : 0}%"></span></div>`;
}
function renderPageLists() { if (page === 'dashboard') renderDashboard(); if (page === 'tasks') renderTaskPage(); if (page === 'pairs') renderPairPage(); }
function renderTaskPage() {
  const list = document.getElementById('page-task-list'); if (!list) return;
  const filter = document.getElementById('task-filter')?.value || 'all';
  const tasks = appData.tasks.filter(task => filter === 'all' || task.status === filter).slice().reverse();
  list.innerHTML = tasks.length ? tasks.map(task => `<article class="task-card"><div class="task-card-top"><div><label><input class="task-check" type="checkbox" value="${esc(task.id)}"> <span class="muted">Select</span></label><h3>${esc(task.id)}</h3><div class="task-card-route">${esc(task.source || 'Source')} → ${esc(task.target || 'Target')}</div></div>${statusPill(task.status)}</div><div class="task-card-meta">${esc(task.mode || 'sync')} · ${esc(task.priority || 'normal')} priority · ${statsTotal(task)} copied · ${task.stats?.failed || 0} failed</div>${task.status === 'paused' ? `<div class="notice warning" style="margin-top:10px">${esc(task.pause_reason || 'A temporary limit paused this task')}</div>` : ''}<div class="task-card-actions"><a class="button small secondary" href="/tasks/${encodeURIComponent(task.id)}">Open task</a>${task.status === 'paused' ? `<button class="button small primary" onclick="continueTask('${esc(task.id)}')">Continue</button>` : ''}${task.status === 'running' ? `<button class="button small secondary" onclick="pauseTask('${esc(task.id)}')">Pause</button>` : ''}</div></article>`).join('') : '<div class="empty-state tasks-empty">No tasks match this filter.</div>';
}
async function createTaskFromPage() {
  const pairId = document.getElementById('task-pair')?.value, mode = document.getElementById('task-mode')?.value, value = Number(document.getElementById('task-limit')?.value || 0);
  if (!pairId) return toast('Choose a channel pair first', true);
  const payload = {pair_ids:[pairId], mode, priority:document.getElementById('task-priority').value, limit:mode === 'last' ? value : 0, min_id:mode === 'from_id' ? value : 0};
  let result = await request('/api/tasks', {method:'POST', body:JSON.stringify(payload)});
  if (!result.ok && result.code === 'duplicate' &&
      confirm('A similar task is already queued or running. Queue another copy anyway?')) {
    result = await request('/api/tasks', {method:'POST', body:JSON.stringify({...payload, allow_duplicate:true})});
  }
  toast(result.ok ? 'Task added to the queue' : result.error, !result.ok); if (result.ok) { await loadAppData(); }
}
async function runDryPage() {
  const pairId = document.getElementById('task-pair')?.value, mode = document.getElementById('task-mode')?.value, value = Number(document.getElementById('task-limit')?.value || 0);
  if (!pairId) return toast('Choose a channel pair first', true);
  const result = await request('/api/tasks/dry-run', {method:'POST', body:JSON.stringify({pair_ids:[pairId], mode, value})});
  const node = document.getElementById('task-dry-run'); if (!node) return;
  node.classList.remove('hidden'); node.innerHTML = result.ok ? result.reports.map(r => `${esc(r.pair)}: ${r.allowed_messages} messages allowed, about ${Math.ceil(r.approximate_seconds / 60)} min`).join('<br>') : esc(result.error);
}
async function controlTask(id, action) {
  if (action === 'cancel' && !confirm('Cancel this task?')) return;
  const result = action === 'cancel' ? await request(`/api/tasks/${encodeURIComponent(id)}`, {method:'DELETE'}) : await request(`/api/tasks/${encodeURIComponent(id)}`, {method:'PATCH', body:JSON.stringify(action === 'continue' ? {continue:true} : {paused:action === 'pause'})});
  toast(result.ok ? (action === 'continue' ? 'Task continued from saved progress' : 'Task updated') : (result.error || result.message), !result.ok); if (result.ok) loadAppData();
}
function continueTask(id) { return controlTask(id, 'continue'); } function pauseTask(id) { return controlTask(id, 'pause'); }
async function bulkTaskPage(action) { const ids = [...document.querySelectorAll('.task-check:checked')].map(n => n.value); if (!ids.length) return toast('Select at least one task', true); const result = await request('/api/tasks/bulk', {method:'POST', body:JSON.stringify({task_ids:ids, action})}); toast(result.ok ? 'Tasks updated' : result.error, !result.ok); if (result.ok) loadAppData(); }
async function renderTaskDetail() {
  const task = appData.tasks.find(item => item.id === window.TASK_ID), title = document.getElementById('detail-title');
  if (!task) { if (title) title.textContent = 'Task not found'; return; }
  const set = (id,value) => { const node = document.getElementById(id); if (node) node.textContent = value; };
  set('detail-title', `Task ${task.id}`); set('detail-route', `${task.source || 'Source'} → ${task.target || 'Target'}`); set('detail-status-title', task.status === 'paused' ? 'Waiting for your go-ahead' : `Task ${task.status}`); set('detail-status', task.status); set('detail-progress', `${task.current || 0} / ${task.total || 0}`); set('detail-mode', task.mode); set('detail-priority', task.priority); set('detail-failed', task.stats?.failed || 0); set('detail-created', task.created_at || '—');
  const status = document.getElementById('detail-status'); if (status) status.className = `status-pill ${statusClass(task.status)}`;
  const bar = document.getElementById('detail-progress-bar'); if (bar) bar.style.width = `${task.total ? Math.min(100, task.current / task.total * 100) : 0}%`;
  const reason = document.getElementById('detail-reason'); if (reason) { reason.classList.toggle('hidden', task.status !== 'paused'); reason.textContent = task.pause_reason || 'A temporary limit paused this task.'; }
  const actions = document.getElementById('detail-actions'); if (actions) actions.innerHTML = task.status === 'paused' ? `<button class="button primary" onclick="continueTask('${esc(task.id)}')">Continue task</button>` : task.status === 'running' ? `<button class="button secondary" onclick="pauseTask('${esc(task.id)}')">Pause task</button>` : '';
  const pair = appData.pairs.find(item => item.id === task.pair_id);
  renderTaskProfile(pair, task.task_settings && Object.keys(task.task_settings).length ? task.task_settings : pair);
}
function renderTaskProfile(pair, settings) {
  const form = document.getElementById('detail-form'); if (!form) return; if (!pair) { form.innerHTML = '<div class="empty-state">The pair profile is no longer available.</div>'; return; }
  form.innerHTML = `<div class="notice">These overrides belong only to this task. The reusable pair defaults stay unchanged.</div><div class="form-grid two"><label>Include keywords<input id="detail-include" value="${esc(keywordText(settings.include_keywords))}"></label><label>Exclude keywords<input id="detail-exclude" value="${esc(keywordText(settings.exclude_keywords))}"></label><label>Caption prefix<input id="detail-prefix" value="${esc(settings.caption_prefix || '')}"></label><label>Caption suffix<input id="detail-suffix" value="${esc(settings.caption_suffix || '')}"></label></div><div class="check-list"><label><input id="detail-caption" type="checkbox" ${settings.caption_enabled ? 'checked' : ''}> Custom captions</label><label><input id="detail-thumbnail" type="checkbox" ${settings.thumbnail_enabled ? 'checked' : ''}> Video thumbnails</label><label><input id="detail-links" type="checkbox" ${settings.remove_links ? 'checked' : ''}> Remove links</label><label><input id="detail-source-name" type="checkbox" ${settings.remove_source_name ? 'checked' : ''}> Remove source name</label></div><label>Caption template<textarea id="detail-template" rows="4">${esc(settings.caption_template || '')}</textarea><div class="form-grid two"><label>Thumbnail image<input id="detail-thumbnail-file" type="file" accept="image/*"></label><div class="form-actions"><button class="button secondary" type="button" onclick="uploadTaskThumbnail()">Upload task thumbnail</button></div></div><div class="form-actions"><button class="button primary" onclick="saveTaskProfile()">Save task settings</button><a class="button secondary" href="/pairs#pair-editor">Edit pair defaults</a></div>`;
}
async function uploadTaskThumbnail() { const file = document.getElementById('detail-thumbnail-file')?.files[0]; if (!file) return toast('Choose a thumbnail image first', true); const body = new FormData(); body.append('thumbnail', file); const result = await request(`/api/tasks/${encodeURIComponent(window.TASK_ID)}/thumbnail`, {method:'POST', body}); toast(result.ok ? 'Task thumbnail uploaded' : result.error, !result.ok); }
async function saveTaskProfile() {
  const payload = {include_keywords:document.getElementById('detail-include').value, exclude_keywords:document.getElementById('detail-exclude').value, caption_prefix:document.getElementById('detail-prefix').value, caption_suffix:document.getElementById('detail-suffix').value, caption_enabled:document.getElementById('detail-caption').checked, thumbnail_enabled:document.getElementById('detail-thumbnail').checked, remove_links:document.getElementById('detail-links').checked, remove_source_name:document.getElementById('detail-source-name').checked, caption_template:document.getElementById('detail-template').value};
  const taskId = window.TASK_ID;
  const result = await request(`/api/tasks/${encodeURIComponent(taskId)}`, {method:'PATCH', body:JSON.stringify({settings:payload})}); toast(result.ok ? 'Task-only settings saved' : result.error, !result.ok); if (result.ok) loadAppData();
}
function renderPairPage() {
  const list = document.getElementById('page-pair-list'); if (!list) return;
  document.getElementById('pair-count').textContent = `${appData.pairs.length} pair${appData.pairs.length === 1 ? '' : 's'}`;
  list.innerHTML = appData.pairs.length ? appData.pairs.map(pair => `<article class="pair-card"><div class="pair-card-top"><h3>${esc(pair.name)}</h3><span class="status-pill neutral">${pair.auto_forward ? 'Auto-forward' : 'Manual'}</span></div><div class="pair-route">${esc(pair.source_title || pair.source)} → ${esc(pair.target_title || pair.target)}</div><div class="pair-meta">${pair.caption_enabled ? 'Custom captions' : 'Original captions'} · ${pair.thumbnail_enabled ? 'Thumbnails on' : 'Thumbnails off'} · ${pair.rate_delay || 3}s delay</div><div class="pair-card-actions"><button class="button small secondary" onclick="editPairPage('${esc(pair.id)}')">Edit pair</button><button class="button small secondary" onclick="copyPairAgain('${esc(pair.id)}')">Copy again</button><button class="button small secondary" onclick="deletePairPage('${esc(pair.id)}')">Delete</button></div></article>`).join('') : '<div class="empty-state">No pairs yet. Create your first route.</div>';
}
async function uploadPairThumbnail() { const pairId = document.getElementById('pair-id').value, file = document.getElementById('pair-thumbnail-file')?.files[0]; if (!pairId) return toast('Save the pair before uploading a thumbnail', true); if (!file) return toast('Choose a thumbnail image first', true); const body = new FormData(); body.append('thumbnail', file); const result = await request(`/api/pairs/${encodeURIComponent(pairId)}/thumbnail`, {method:'POST', body}); toast(result.ok ? 'Thumbnail uploaded' : result.error, !result.ok); }
function pairPayload() { return {name:document.getElementById('pair-name').value.trim(), source:document.getElementById('pair-source').value.trim(), target:document.getElementById('pair-target').value.trim(), include_keywords:document.getElementById('pair-include').value, exclude_keywords:document.getElementById('pair-exclude').value, caption_prefix:document.getElementById('pair-prefix').value, caption_suffix:document.getElementById('pair-suffix').value, rate_profile:document.getElementById('pair-profile').value, rate_delay:Number(document.getElementById('pair-rate').value || 3), max_messages:Number(document.getElementById('pair-max').value || 5000), daily_message_limit:Number(document.getElementById('pair-daily-msg').value || 5000), daily_media_mb:Number(document.getElementById('pair-daily-mb').value || 2048), auto_forward:document.getElementById('pair-auto').checked, allowed_types:[...document.querySelectorAll('.pair-type:checked')].map(input => input.value), protected_behavior:document.getElementById('pair-protected').value, caption_enabled:document.getElementById('pair-caption-enabled').checked, caption_types:[...document.querySelectorAll('.caption-type:checked')].map(input => input.value), thumbnail_enabled:document.getElementById('pair-thumbnail-enabled').checked, remove_links:document.getElementById('pair-links').checked, remove_source_name:document.getElementById('pair-source-name').checked, caption_template:document.getElementById('pair-caption-template').value, schedule_start:document.getElementById('pair-schedule-start').value, schedule_end:document.getElementById('pair-schedule-end').value, quiet_start:document.getElementById('pair-quiet-start').value, quiet_end:document.getElementById('pair-quiet-end').value, max_posts_per_hour:Number(document.getElementById('pair-hourly').value || 0), caption_parse_mode:document.getElementById('pair-caption-mode').value}; }
async function savePairFromPage() { const payload = pairPayload(); if (!payload.name || !payload.source || !payload.target) return toast('Pair name, source, and target are required', true); if (!payload.allowed_types.length) return toast('Select at least one message type', true); const id = document.getElementById('pair-id').value, result = await request(id ? `/api/pairs/${encodeURIComponent(id)}` : '/api/pairs', {method:id ? 'PATCH' : 'POST', body:JSON.stringify(payload)}); toast(result.ok ? 'Pair saved' : result.error, !result.ok); if (result.ok) { resetPairForm(); loadAppData(); } }
function editPairPage(id) { const pair = appData.pairs.find(item => item.id === id); if (!pair) return; document.getElementById('pair-id').value = id; document.getElementById('pair-form-title').textContent = 'Edit pair'; Object.entries({'pair-name':'name','pair-source':'source','pair-target':'target','pair-include':'include_keywords','pair-exclude':'exclude_keywords','pair-prefix':'caption_prefix','pair-suffix':'caption_suffix','pair-profile':'rate_profile','pair-rate':'rate_delay','pair-max':'max_messages','pair-daily-msg':'daily_message_limit','pair-daily-mb':'daily_media_mb','pair-protected':'protected_behavior','pair-caption-template':'caption_template','pair-schedule-start':'schedule_start','pair-schedule-end':'schedule_end','pair-quiet-start':'quiet_start','pair-quiet-end':'quiet_end','pair-hourly':'max_posts_per_hour','pair-caption-mode':'caption_parse_mode'}).forEach(([id,key]) => { const el = document.getElementById(id); if (el) el.value = Array.isArray(pair[key]) ? pair[key].join(', ') : (pair[key] ?? ''); }); ['caption-enabled','thumbnail-enabled','links','source-name','auto'].forEach(key => { const el = document.getElementById(`pair-${key}`); if (el) el.checked = !!pair[key === 'caption-enabled' ? 'caption_enabled' : key === 'thumbnail-enabled' ? 'thumbnail_enabled' : key === 'source-name' ? 'remove_source_name' : key === 'auto' ? 'auto_forward' : 'remove_links']; }); document.querySelectorAll('.pair-type').forEach(input => input.checked = (pair.allowed_types || []).includes(input.value)); document.querySelectorAll('.caption-type').forEach(input => input.checked = (pair.caption_types || []).includes(input.value)); document.getElementById('pair-cancel').classList.remove('hidden'); location.hash = 'pair-editor'; }
function resetPairForm() { document.getElementById('pair-id').value = ''; document.getElementById('pair-form-title').textContent = 'Create a pair'; document.querySelectorAll('#pair-editor input:not([type=hidden]),#pair-editor textarea').forEach(el => { if (el.type === 'checkbox') el.checked = false; else el.value = ''; }); document.querySelectorAll('.pair-type,.caption-type').forEach(input => input.checked = true); document.getElementById('pair-rate').value = 3; document.getElementById('pair-max').value = 5000; document.getElementById('pair-daily-msg').value = 5000; document.getElementById('pair-daily-mb').value = 2048; document.getElementById('pair-profile').value = 'balanced'; document.getElementById('pair-protected').value = 'download'; document.getElementById('pair-cancel').classList.add('hidden'); }
async function copyPairAgain(id) { const result = await request(`/api/pairs/${encodeURIComponent(id)}/dedupe`, {method:'POST', body:'{}'}); toast(result.ok ? `${result.removed} duplicate identities cleared` : result.error, !result.ok); }
async function deletePairPage(id) { if (!confirm('Delete this channel pair?')) return; const result = await request(`/api/pairs/${encodeURIComponent(id)}`, {method:'DELETE'}); toast(result.ok ? 'Pair deleted' : result.error, !result.ok); if (result.ok) loadAppData(); }
async function loadSettings() { const result = await request('/api/settings'); if (!result.ok) return; ['complete','failed','flood'].forEach(key => { const input = document.getElementById(`setting-${key}`); if (input) input.checked = !!result.notification_settings[ key === 'complete' ? 'task_complete' : key === 'failed' ? 'task_failed' : 'flood_wait' ]; }); const auto = document.getElementById('setting-auto'); if (auto) auto.checked = !!result.auto_forward; const set = (id,value) => { const node = document.getElementById(id); if (node) node.textContent = value; }; set('setting-max', result.max_task_messages); set('setting-storage', `${result.storage_limit_mb} MB`); set('settings-source', appData.status.source || 'Not set'); set('settings-target', appData.status.target || 'Not set'); }
async function saveGlobalSettings() { const result = await request('/api/settings', {method:'PATCH', body:JSON.stringify({task_complete:document.getElementById('setting-complete').checked, task_failed:document.getElementById('setting-failed').checked, flood_wait:document.getElementById('setting-flood').checked, auto_forward:document.getElementById('setting-auto').checked})}); toast(result.ok ? 'Global settings saved' : result.error, !result.ok); if (result.ok) loadSettings(); }
async function setChannelPage(type) { const id = type === 'source' ? 'settings-src-input' : 'settings-tgt-input', value = document.getElementById(id).value.trim(); if (!value) return toast('Enter a channel first', true); const result = await request(`/api/set${type}`, {method:'POST', body:JSON.stringify({channel:value})}); toast(result.ok ? `${type} saved` : result.error, !result.ok); if (result.ok) { document.getElementById(id).value = ''; loadAppData(); } }
async function cleanStorage() { if (!confirm('Remove temporary downloaded files?')) return; const result = await request('/api/storage/cleanup', {method:'POST', body:'{}'}); toast(result.ok ? `${result.removed || 0} temporary file(s) cleaned` : result.error, !result.ok); if (result.ok) loadAppData(); }
function updateTaskLimitField() { const mode = document.getElementById('task-mode')?.value, label = document.querySelector('#task-limit-wrap'), input = label?.querySelector('input'); if (!label || !input) return; if (mode === 'full') { label.firstChild.textContent = 'Optional limit'; input.placeholder = 'Uses pair limit'; input.disabled = true; input.value = ''; } else if (mode === 'last') { label.firstChild.textContent = 'Last N messages'; input.placeholder = 'e.g. 100'; input.disabled = false; } else { label.firstChild.textContent = 'Start from message ID'; input.placeholder = 'e.g. 12345'; input.disabled = false; } }
function closeMobileNav() { document.body.classList.remove('nav-open'); document.querySelector('.sidebar')?.classList.remove('open'); document.getElementById('menu-toggle')?.setAttribute('aria-expanded', 'false'); }
document.addEventListener('DOMContentLoaded', () => { const sidebar = document.querySelector('.sidebar'), toggle = document.getElementById('menu-toggle'), backdrop = document.getElementById('mobile-nav-backdrop'); toggle?.addEventListener('click', () => { const open = !sidebar.classList.contains('open'); sidebar.classList.toggle('open', open); document.body.classList.toggle('nav-open', open); toggle.setAttribute('aria-expanded', String(open)); }); backdrop?.addEventListener('click', closeMobileNav); document.querySelectorAll('.nav-link,.help-link').forEach(link => link.addEventListener('click', closeMobileNav)); document.addEventListener('keydown', event => { if (event.key === 'Escape') closeMobileNav(); }); document.getElementById('task-mode')?.addEventListener('change', updateTaskLimitField); updateTaskLimitField(); loadAppData(); connectLive(); });