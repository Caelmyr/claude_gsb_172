/* 结果导出 Results */
Components.init('results');
const C = Components;

let currentJob = '';

async function render() {
  if (!currentJob) return;
  let d;
  try { d = await API.get('/api/jobs/' + currentJob + '/results?limit=100'); } catch (e) { return; }

  document.getElementById('dl-json').href = '/api/jobs/' + currentJob + '/results/download?format=json';
  document.getElementById('dl-csv').href = '/api/jobs/' + currentJob + '/results/download?format=csv';

  document.getElementById('stats').innerHTML = [
    { label: '结果记录 Total records', value: C.fmtNum(d.total) },
    { label: '分区数 Partitions', value: d.partitions.length },
    { label: '作业状态 Status', value: d.status },
    { label: '是否截断 Truncated', value: d.truncated ? '是 yes' : '否 no' },
  ].map(s => `<div class="stat"><div class="label">${s.label}</div><div class="value">${C.esc(s.value)}</div></div>`).join('');

  document.getElementById('partitions').innerHTML = d.partitions.length
    ? C.table([
        { key: 'partition_name', label: '分区 Partition', render: r => `<span class="mono">${C.esc(r.partition_name)}</span>` },
        { key: 'count', label: '记录数 Count', render: r => C.fmtNum(r.count), num: true },
        { key: 'task_id', label: 'Reduce 任务 Task', render: r => `<span class="mono">${C.esc(r.task_id)}</span>` },
      ], d.partitions)
    : C.empty('暂无结果 No results — 作业可能尚未完成');

  const records = d.records || [];
  document.getElementById('preview').innerHTML = records.length
    ? C.table([
        { key: 'key', label: 'Key', render: r => `<b>${C.esc(r.key)}</b>` },
        { key: 'value', label: 'Value', render: r => C.valueCell(r) },
      ], records)
    : C.empty('暂无结果 No results');
}

C.jobPicker('job-picker', (id) => { currentJob = id; render(); });
C.poll(render, 3000).start();
