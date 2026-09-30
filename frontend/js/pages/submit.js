/* 作业提交 Submit */
Components.init('submit');
const C = Components;

let SAMPLES = [];

async function init() {
  const funcs = await API.get('/api/functions');
  SAMPLES = await API.get('/api/samples');

  fillSelect('mapper', funcs.mappers);
  fillSelect('reducer', funcs.reducers);

  const preset = document.getElementById('preset');
  preset.innerHTML = SAMPLES.map(s => `<option value="${s.name}">${C.esc(s.name)}</option>`).join('');
  preset.addEventListener('change', () => {
    const s = SAMPLES.find(x => x.name === preset.value);
    if (s) fillFromSample(s);
  });

  document.getElementById('form').addEventListener('submit', onSubmit);

  document.getElementById('func-list').innerHTML =
    '<h3>Map</h3>' + funcs.mappers.map(f =>
      `<div class="small" style="padding:2px 0"><span class="mono">${C.esc(f.name)}</span> — ${C.esc(f.description)}</div>`).join('') +
    '<h3 class="mt">Reduce</h3>' + funcs.reducers.map(f =>
      `<div class="small" style="padding:2px 0"><span class="mono">${C.esc(f.name)}</span> — ${C.esc(f.description)}</div>`).join('');

  loadRecent();
}

function fillSelect(id, items) {
  document.getElementById(id).innerHTML = items
    .map(f => `<option value="${f.name}">${C.esc(f.name)}</option>`).join('');
}

function fillFromSample(s) {
  document.getElementById('name').value = s.name;
  document.getElementById('mapper').value = s.mapper;
  document.getElementById('reducer').value = s.reducer;
  document.getElementById('num_map_tasks').value = s.num_map_tasks;
  document.getElementById('num_reduce_tasks').value = s.num_reduce_tasks;
  document.getElementById('input_rows').value = s.input_rows;
  document.getElementById('simulate_failure').checked = false;
}

async function onSubmit(ev) {
  ev.preventDefault();
  const body = {
    name: document.getElementById('name').value.trim(),
    mapper: document.getElementById('mapper').value,
    reducer: document.getElementById('reducer').value,
    num_map_tasks: parseInt(document.getElementById('num_map_tasks').value, 10),
    num_reduce_tasks: parseInt(document.getElementById('num_reduce_tasks').value, 10),
    input_rows: parseInt(document.getElementById('input_rows').value, 10),
    params: {},
  };
  if (document.getElementById('simulate_failure').checked) body.params.simulate_failure = true;
  const btn = ev.target.querySelector('button[type=submit]');
  btn.disabled = true;
  try {
    const job = await API.post('/api/jobs', body);
    C.toast('作业已提交 Job submitted: ' + job.job_id, 'ok');
    setTimeout(() => location.href = 'monitor.html', 600);
  } catch (e) {
    C.toast('提交失败 ' + e.message, 'error');
    btn.disabled = false;
  }
}

async function loadRecent() {
  const d = await API.get('/api/jobs');
  const jobs = d.jobs || [];
  document.getElementById('recent').innerHTML = jobs.length
    ? C.table([
        { key: 'name', label: '作业 Job' },
        { key: 'status', label: '状态 Status', render: r => C.stateBadge(r.status, true) },
        { key: 'mapper', label: 'Mapper' },
        { key: 'reducer', label: 'Reducer' },
        { key: 'created_ms', label: '时间 Time', render: r => C.fmtTime(r.created_ms) },
        { key: 'link', label: '', render: r => `<a href="monitor.html">监控→</a>` },
      ], jobs)
    : C.empty();
}

init();
C.poll(loadRecent, 4000).start();
