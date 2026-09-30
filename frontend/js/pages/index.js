/* 总览 Overview */
Components.init('home');
const C = Components;

async function load() {
  let ov;
  try { ov = await API.get('/api/overview'); } catch (e) { return; }
  const w = ov.workers;

  document.getElementById('stats').innerHTML = [
    { label: '作业总数 Jobs', value: ov.jobs_total },
    { label: '运行中 Active', value: ov.jobs_active, cls: ov.jobs_active ? 'good' : '' },
    { label: '成功 Succeeded', value: ov.jobs_succeeded, cls: 'good' },
    { label: '失败 Failed', value: ov.jobs_failed, cls: ov.jobs_failed ? 'bad' : '' },
    { label: 'Worker 存活 Alive', value: w.alive, cls: 'good' },
    { label: 'Worker 失联 Dead', value: w.dead, cls: w.dead ? 'bad' : '' },
  ].map(s => `<div class="stat"><div class="label">${s.label}</div><div class="value ${s.cls || ''}">${C.fmtNum(s.value)}</div></div>`).join('');

  const jobs = ov.recent_jobs || [];
  document.getElementById('recent-jobs').innerHTML = jobs.length
    ? C.table(
        [
          { key: 'name', label: '作业 Job' },
          { key: 'status', label: '状态 Status', render: r => C.stateBadge(r.status, true) },
          { key: 'stage', label: '阶段进度 Progress', render: r => stageBars(r) },
          { key: 'created_ms', label: '提交时间 Submitted', render: r => C.fmtTime(r.created_ms) },
        ],
        jobs, { onClick: null })
    : C.empty();

  document.getElementById('worker-summary').innerHTML = (w.workers && w.workers.length)
    ? C.table(
        [
          { key: 'name', label: '节点 Worker' },
          { key: 'status', label: '状态 Status', render: r => C.stateBadge(r.status, true) },
          { key: 'load1', label: '负载 load', render: r => Number(r.load1).toFixed(2), num: true },
          { key: 'running_tasks', label: '运行任务 Running', render: r => r.running_tasks, num: true },
          { key: 'total_tasks_completed', label: '已完成 Done', render: r => r.total_tasks_completed, num: true },
        ], w.workers)
    : C.empty();
}

function stageBars(job) {
  const p = job.stage_progress || {};
  const order = [['map', 'Map'], ['shuffle', 'Shuffle'], ['reduce', 'Reduce']];
  const parts = order.map(([k, label]) => {
    const st = p[k] || {};
    return `<div style="margin-bottom:5px">${C.progress(st.pct || 0, label)}</div>`;
  });
  return `<div style="min-width:180px">${parts.join('')}</div>`;
}

C.poll(load, 2500).start();
load();
