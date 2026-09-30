/* 故障恢复 Fault */
Components.init('fault');
const C = Components;

let currentJob = '';

const KIND_LABEL = {
  task_failed: '任务失败 Task failed',
  worker_dead: 'Worker 失联 Worker dead',
  reassigned: '任务改派 Reassigned',
  speculation: '推测执行 Speculation',
};

async function render() {
  if (!currentJob) return;
  let d;
  try { d = await API.get('/api/jobs/' + currentJob + '/faults'); } catch (e) { return; }
  const faults = d.faults || [];

  const byKind = {};
  faults.forEach(f => byKind[f.kind] = (byKind[f.kind] || 0) + 1);
  document.getElementById('stats').innerHTML = [
    { label: '故障总数 Total faults', value: faults.length, cls: faults.length ? 'bad' : 'good' },
    { label: '任务失败 Task failed', value: byKind.task_failed || 0 },
    { label: 'Worker 失联 Dead', value: byKind.worker_dead || 0 },
    { label: '重试/改派 Reassigned', value: (byKind.reassigned || 0) },
    { label: '推测执行 Speculation', value: byKind.speculation || 0 },
  ].map(s => `<div class="stat"><div class="label">${s.label}</div><div class="value ${s.cls || ''}">${C.fmtNum(s.value)}</div></div>`).join('');

  document.getElementById('faults').innerHTML = faults.length ? C.table([
    { key: 'created_ms', label: '时间 Time', render: r => C.fmtTime(r.created_ms) },
    { key: 'kind', label: '类型 Kind', render: r => `<span class="badge ${r.kind === 'worker_dead' ? 'bad' : 'warn'}">${C.esc(KIND_LABEL[r.kind] || r.kind)}</span>` },
    { key: 'task_id', label: '任务 Task', render: r => r.task_id ? `<span class="mono">${C.esc(r.task_id)}</span>` : '-' },
    { key: 'attempt', label: '尝试 Att', render: r => r.attempt, num: true },
    { key: 'message', label: '详情 Detail', render: r => C.esc(r.message) },
    { key: 'worker_id', label: 'Worker', render: r => `<span class="mono small">${C.esc(r.worker_id || '-')}</span>` },
  ], faults) : C.empty('无故障事件 No faults — 本次作业无重试 All tasks succeeded on first attempt');
}

C.jobPicker('job-picker', (id) => { currentJob = id; render(); });
C.poll(render, 2500).start();
