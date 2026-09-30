/* 性能指标 Metrics */
Components.init('metrics');
const C = Components;
const Charts = window.Charts;

let currentJob = '';
let workers = [];

async function loadJobMetrics() {
  if (!currentJob) return;
  let m;
  try { m = await API.get('/api/jobs/' + currentJob + '/metrics'); } catch (e) { return; }

  document.getElementById('stats').innerHTML = [
    { label: '处理记录 Processed', value: C.fmtNum(m.total_records_processed) },
    { label: '输出记录 Emitted', value: C.fmtNum(m.total_records_emitted) },
    { label: '平均吞吐 Avg throughput', value: (m.avg_throughput_rps || 0) + ' r/s' },
    { label: '峰值吞吐 Peak throughput', value: (m.peak_throughput_rps || 0) + ' r/s', cls: 'good' },
    { label: '平均延迟 Avg latency', value: C.fmtDur(m.avg_latency_ms) },
    { label: '任务数 Tasks', value: m.task_count },
  ].map(s => `<div class="stat"><div class="label">${s.label}</div><div class="value ${s.cls || ''}">${C.esc(s.value)}</div></div>`).join('');

  const samples = m.samples || [];
  const labels = samples.map((_, i) => '#' + (i + 1));
  const throughput = samples.map(s => s.records_per_sec || 0);
  const latency = samples.map(s => s.task_latency_ms || 0);

  Charts.barChart(document.getElementById('c-throughput'), {
    labels, values: throughput, format: v => Math.round(v).toString(), height: 220,
  });
  Charts.barChart(document.getElementById('c-latency'), {
    labels, values: latency, format: v => Math.round(v) + 'ms', height: 220,
  });
}

async function loadCluster() {
  let cm;
  try { cm = await API.get('/api/cluster/metrics'); } catch (e) { return; }
  workers = cm.per_worker || [];
  Charts.barChart(document.getElementById('c-worker-cpu'), {
    labels: workers.map(w => w.name),
    values: workers.map(w => w.cpu_avg || 0),
    format: v => Math.round(v) + '%', height: 220,
  });
  // populate worker selector
  const sel = document.getElementById('worker-sel');
  const prev = sel.value;
  sel.innerHTML = workers.map(w => `<option value="${w.worker_id}">${C.esc(w.name)}</option>`).join('');
  if (prev && workers.some(w => w.worker_id === prev)) sel.value = prev;
  loadResource(sel.value || (workers[0] && workers[0].worker_id));
}

async function loadResource(workerId) {
  if (!workerId) return;
  let w;
  try { w = await API.get('/api/workers/' + workerId + '/metrics'); } catch (e) { return; }
  const series = w.cpu_series || [];
  Charts.lineChart(document.getElementById('c-resource'), {
    labels: series.map(s => s.ts),
    series: [{ name: 'CPU %', values: series.map(s => s.v) }],
    yFormat: v => Math.round(v) + '%',
    xFormat: (ts) => C.fmtTime(ts),
    height: 220,
  });
}

document.getElementById('worker-sel').addEventListener('change', e => loadResource(e.target.value));
window.addEventListener('themechange', () => { loadJobMetrics(); loadCluster(); });

C.jobPicker('job-picker', (id) => { currentJob = id; loadJobMetrics(); });
loadCluster();
C.poll(() => { loadJobMetrics(); loadCluster(); }, 3000).start();
