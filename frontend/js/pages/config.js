/* 配置管理 Config */
Components.init('config');
const C = Components;

const CLUSTER_FIELDS = [
  { key: 'heartbeat_interval_sec', label: '心跳间隔 Heartbeat interval (s)', type: 'number', step: 0.5, min: 0.2 },
  { key: 'heartbeat_timeout_sec', label: '心跳超时 Heartbeat timeout (s)', type: 'number', step: 0.5, min: 1 },
  { key: 'task_timeout_sec', label: '任务超时 Task timeout (s)', type: 'number', step: 5, min: 5 },
  { key: 'max_attempts', label: '最大重试次数 Max attempts', type: 'number', step: 1, min: 1 },
  { key: 'retry_backoff_base_sec', label: '重试退避基数 Backoff base (s)', type: 'number', step: 0.1, min: 0.1 },
  { key: 'speculative_execution', label: '推测执行 Speculative execution', type: 'checkbox' },
  { key: 'speculation_threshold', label: '推测阈值 Speculation threshold (×median)', type: 'number', step: 0.1, min: 1 },
  { key: 'shuffle_fetch_batch', label: 'Shuffle 拉取批次 Fetch batch', type: 'number', step: 1, min: 1 },
  { key: 'shuffle_spill_records', label: 'Shuffle 溢写阈值 Spill records', type: 'number', step: 100, min: 100 },
  { key: 'map_parallelism_factor', label: 'Map 并行因子 Map parallelism', type: 'number', step: 0.5, min: 0.5 },
  { key: 'reduce_parallelism_factor', label: 'Reduce 并行因子 Reduce parallelism', type: 'number', step: 0.5, min: 0.5 },
  { key: 'scheduler_tick_sec', label: '调度周期 Scheduler tick (s)', type: 'number', step: 0.05, min: 0.05 },
  { key: 'metric_interval_sec', label: '指标采样周期 Metric interval (s)', type: 'number', step: 0.5, min: 0.5 },
  { key: 'demo_mode', label: '演示模式 Demo mode', type: 'checkbox' },
  { key: 'default_input_rows', label: '默认输入行数 Default input rows', type: 'number', step: 100, min: 10 },
];

const DEFAULT_FIELDS = [
  { key: 'mapper', label: '默认 Map 函数 Default mapper', type: 'text' },
  { key: 'reducer', label: '默认 Reduce 函数 Default reducer', type: 'text' },
  { key: 'num_map_tasks', label: '默认 Map 任务数 Map tasks', type: 'number', step: 1, min: 1 },
  { key: 'num_reduce_tasks', label: '默认 Reduce 任务数 Reduce tasks', type: 'number', step: 1, min: 1 },
  { key: 'input_rows', label: '默认输入行数 Input rows', type: 'number', step: 100, min: 10 },
];

function renderForm(hostId, fields, data) {
  const host = document.getElementById(hostId);
  host.innerHTML = fields.map(f => {
    if (f.type === 'checkbox') {
      return `<label style="display:flex;align-items:center;gap:8px;margin:10px 0 3px">
        <input type="checkbox" id="${hostId}-${f.key}" style="width:auto" ${data[f.key] ? 'checked' : ''}>
        ${C.esc(f.label)}
      </label>`;
    }
    return `<label>${C.esc(f.label)}</label>
      <input type="${f.type}" id="${hostId}-${f.key}" value="${C.esc(data[f.key])}"
        ${f.step ? `step="${f.step}"` : ''} ${f.min != null ? `min="${f.min}"` : ''}>`;
  }).join('');
}

function readForm(hostId, fields) {
  const out = {};
  fields.forEach(f => {
    const el = document.getElementById(`${hostId}-${f.key}`);
    if (f.type === 'checkbox') out[f.key] = el.checked;
    else if (f.type === 'number') out[f.key] = parseFloat(el.value) || 0;
    else out[f.key] = el.value;
  });
  return out;
}

async function load() {
  const cfg = await API.get('/api/config');
  renderForm('cluster-form', CLUSTER_FIELDS, cfg);
  const defaults = await API.get('/api/config/defaults');
  renderForm('defaults-form', DEFAULT_FIELDS, defaults);
}

document.getElementById('save-cluster').addEventListener('click', async () => {
  const body = readForm('cluster-form', CLUSTER_FIELDS);
  try {
    await API.put('/api/config', body);
    C.toast('集群配置已保存 Cluster config saved', 'ok');
  } catch (e) { C.toast('保存失败 ' + e.message, 'error'); }
});

document.getElementById('save-defaults').addEventListener('click', async () => {
  const body = readForm('defaults-form', DEFAULT_FIELDS);
  try {
    await API.put('/api/config/defaults', body);
    C.toast('默认值已保存 Defaults saved', 'ok');
  } catch (e) { C.toast('保存失败 ' + e.message, 'error'); }
});

load();
