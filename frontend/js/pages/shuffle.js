/* Shuffle 与排序 Shuffle & Sort */
Components.init('shuffle');
const C = Components;

let currentJob = '';

async function render() {
  if (!currentJob) return;
  let m;
  try { m = await API.get('/api/jobs/' + currentJob + '/shuffle'); } catch (e) { return; }
  const parts = m.partitions || [];

  document.getElementById('stats').innerHTML = [
    { label: '分区数 Partitions', value: m.num_partitions },
    { label: '已完成 Done', value: m.partitions_done, cls: m.partitions_done ? 'good' : '' },
    { label: 'Shuffle 总量 Total bytes', value: C.fmtBytes(m.total_bytes) },
    { label: '进度 Progress', value: (m.progress_pct || 0) + '%', cls: (m.progress_pct >= 100) ? 'good' : '' },
  ].map(s => `<div class="stat"><div class="label">${s.label}</div><div class="value ${s.cls || ''}">${C.esc(s.value)}</div></div>`).join('');

  document.getElementById('matrix').innerHTML = parts.length ? C.table([
    { key: 'partition_name', label: '分区 Partition', render: r => `<span class="mono">${C.esc(r.partition_name)}</span>` },
    { key: 'num_sources', label: '源数 Sources', render: r => r.num_sources, num: true },
    { key: 'total_bytes', label: '数据量 Bytes', render: r => C.fmtBytes(r.total_bytes), num: true },
    { key: 'status', label: '状态 Status', render: r => C.stateBadge(r.status, true) },
    { key: 'reduce_task_id', label: 'Reduce 任务 Task', render: r => `<span class="mono">${C.esc(r.reduce_task_id)}</span>` },
    { key: 'sources', label: '来源 Sources (map → bytes)', render: r => sourceList(r) },
  ], parts) : C.empty();
}

function sourceList(r) {
  const srcs = r.sources || [];
  if (!srcs.length) return '<span class="muted">-</span>';
  return '<div class="small mono" style="max-height:72px;overflow-y:auto">' +
    srcs.map(s => `<div>${C.esc(s.map_task_id)} @ ${C.esc(s.worker_id || '')} → ${C.fmtBytes(s.bytes)}</div>`).join('') +
    '</div>';
}

C.jobPicker('job-picker', (id) => { currentJob = id; render(); });
C.poll(render, 2000).start();
