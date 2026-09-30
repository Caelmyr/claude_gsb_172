/* 日志搜索 Logs */
Components.init('logs');
const C = Components;

let currentJob = '';

async function render() {
  if (!currentJob) return;
  const params = new URLSearchParams();
  const search = document.getElementById('search').value.trim();
  const stage = document.getElementById('stage').value;
  const level = document.getElementById('level').value;
  if (search) params.set('search', search);
  if (stage) params.set('stage', stage);
  if (level) params.set('level', level);
  params.set('limit', '1000');

  let d;
  try { d = await API.get('/api/jobs/' + currentJob + '/logs?' + params.toString()); } catch (e) { return; }
  const records = d.records || [];
  document.getElementById('log-count').textContent =
    `匹配 ${records.length} / ${d.total} 条 · 扫描 ${d.scanned} 条 lines`;

  document.getElementById('logs').innerHTML = records.length
    ? records.map(r => `<div class="log-line lvl-${C.esc(r.level)}">
        <span class="ts">${C.fmtTime(r.ts_ms)}</span>
        <span class="lvl">${C.esc(r.level)}</span>
        <span class="stage">${C.esc(r.stage)}</span>
        <span class="msg">${C.esc(r.message)}</span>
        ${r.worker_id ? `<span class="stage">${C.esc(r.worker_id)}</span>` : ''}
      </div>`).join('')
    : C.empty('无匹配日志 No matching logs');
}

C.jobPicker('job-picker', (id) => { currentJob = id; render(); });
document.getElementById('refresh').addEventListener('click', render);
document.getElementById('search').addEventListener('input', debounce(render, 400));
document.getElementById('stage').addEventListener('change', render);
document.getElementById('level').addEventListener('change', render);

function debounce(fn, ms) {
  let t; return () => { clearTimeout(t); t = setTimeout(fn, ms); };
}

C.poll(render, 3000).start();
