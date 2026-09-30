/* 节点管理 Nodes */
Components.init('nodes');
const C = Components;

async function load() {
  let d;
  try { d = await API.get('/api/workers'); } catch (e) { return; }
  const workers = d.workers || [];

  document.getElementById('stats').innerHTML = [
    { label: 'Worker 总数 Total', value: d.total },
    { label: '存活 Alive', value: d.alive, cls: 'good' },
    { label: '失联 Dead', value: d.dead, cls: d.dead ? 'bad' : '' },
    { label: '累计完成任务 Completed', value: workers.reduce((s, w) => s + (w.total_tasks_completed || 0), 0) },
  ].map(s => `<div class="stat"><div class="label">${s.label}</div><div class="value ${s.cls || ''}">${C.fmtNum(s.value)}</div></div>`).join('');

  document.getElementById('workers').innerHTML = workers.length ? C.table([
    { key: 'name', label: '节点 Worker', render: r => `<b>${C.esc(r.name)}</b><div class="small muted mono">${C.esc(r.worker_id)}</div>` },
    { key: 'address', label: '地址 Address', render: r => `<span class="mono">${C.esc(r.host)}:${r.port}</span>` },
    { key: 'status', label: '状态 Status', render: r => C.stateBadge(r.status, true) },
    { key: 'heartbeat', label: '最后心跳 Heartbeat', render: r => `<span class="small">${C.fmtTime(r.last_heartbeat_ms)}</span><div class="small muted">${(Date.now()-r.last_heartbeat_ms)/1000 < 8 ? '刚刚 now' : '超时 stale'}</div>` },
    { key: 'cpu', label: 'CPU', render: r => C.meter(r.cpu_percent) },
    { key: 'mem', label: '内存 Mem', render: r => C.meter(r.mem_percent) },
    { key: 'load1', label: '负载 load', render: r => C.fmtNum(Number(r.load1).toFixed(2)), num: true },
    { key: 'running_tasks', label: '运行中 Running', render: r => r.running_tasks, num: true },
    { key: 'completed', label: '完成/失败 Done/Failed', render: r => `<span class="good">${r.total_tasks_completed||0}</span> / <span class="bad">${r.total_tasks_failed||0}</span>`, num: true },
  ], workers) : C.empty();
}

C.poll(load, 2000).start();
load();
