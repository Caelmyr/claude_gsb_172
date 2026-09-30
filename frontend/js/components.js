/* Shared UI components: navigation, formatting, badges, tables, polling. */
const Components = (() => {
  const PAGES = [
    { key: 'home', href: 'index.html', label: '总览 Overview' },
    { key: 'submit', href: 'submit.html', label: '作业提交 Submit' },
    { key: 'monitor', href: 'monitor.html', label: '作业监控 Monitor' },
    { key: 'nodes', href: 'nodes.html', label: '节点管理 Nodes' },
    { key: 'shards', href: 'shards.html', label: '分片管理 Shards' },
    { key: 'shuffle', href: 'shuffle.html', label: 'Shuffle 排序' },
    { key: 'logs', href: 'logs.html', label: '日志搜索 Logs' },
    { key: 'metrics', href: 'metrics.html', label: '性能指标 Metrics' },
    { key: 'fault', href: 'fault.html', label: '故障恢复 Fault' },
    { key: 'config', href: 'config.html', label: '配置管理 Config' },
    { key: 'results', href: 'results.html', label: '结果导出 Results' },
  ];

  const LABELS = {
    PENDING: '待调度 Pending', SHARDING: '分片 Sharding', MAP: 'Map', SHUFFLE: 'Shuffle',
    REDUCE: 'Reduce', SUCCEEDED: '成功 Succeeded', FAILED: '失败 Failed', CANCELLED: '已取消 Cancelled',
    ASSIGNED: '已分配 Assigned', RUNNING: '运行中 Running', RETRYING: '重试 Retrying',
    alive: '存活 Alive', dead: '失联 Dead', ready: '就绪 Ready', done: '完成 Done',
  };
  const CLASS = {
    SUCCEEDED: 'good', FAILED: 'bad', CANCELLED: 'muted', RUNNING: 'run', MAP: 'run',
    REDUCE: 'aqua', SHUFFLE: 'warn', RETRYING: 'warn', ASSIGNED: 'aqua', PENDING: 'muted',
    SHARDING: 'muted', alive: 'good', dead: 'bad', ready: 'muted', done: 'good',
  };

  // ------------------------------------------------------------------
  function esc(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  function fmtNum(n) {
    if (n == null || isNaN(n)) return '-';
    return Number(n).toLocaleString('en-US');
  }

  function fmtBytes(n) {
    if (n == null || isNaN(n)) return '-';
    if (n < 1024) return n + ' B';
    if (n < 1024 * 1024) return (n / 1024).toFixed(1) + ' KB';
    if (n < 1024 * 1024 * 1024) return (n / 1024 / 1024).toFixed(2) + ' MB';
    return (n / 1024 / 1024 / 1024).toFixed(2) + ' GB';
  }

  function fmtTime(ms) {
    if (!ms) return '-';
    const d = new Date(ms);
    return d.toLocaleTimeString('zh-CN', { hour12: false });
  }

  function fmtDur(ms) {
    if (ms == null || isNaN(ms)) return '-';
    if (ms < 1000) return ms + ' ms';
    if (ms < 60000) return (ms / 1000).toFixed(1) + ' s';
    return (ms / 60000).toFixed(1) + ' min';
  }

  function fmtPct(x) {
    return (x == null || isNaN(x)) ? '-' : (Number(x).toFixed(1) + '%');
  }

  // ------------------------------------------------------------------
  function stateBadge(state, withDot) {
    const cls = CLASS[state] || 'muted';
    const label = LABELS[state] || String(state);
    return `<span class="badge ${cls}${withDot ? ' badge-dot' : ''}">${esc(label)}</span>`;
  }

  function progress(pct, label) {
    const p = Math.max(0, Math.min(100, Number(pct) || 0));
    const fillCls = p >= 100 ? 'good' : (p >= 60 ? '' : '');
    return `<div class="progress-label"><span>${esc(label || '')}</span><span class="tabular">${p.toFixed(0)}%</span></div>
      <div class="progress"><div class="fill ${fillCls}" style="width:${p}%"></div></div>`;
  }

  function meter(pct, label) {
    const p = Math.max(0, Math.min(100, Number(pct) || 0));
    const cls = p >= 90 ? 'crit' : (p >= 70 ? 'high' : (p >= 50 ? 'warn' : 'ok'));
    return `<div class="flex between small"><span class="muted">${esc(label || '')}</span><span class="tabular bold">${p.toFixed(0)}%</span></div>
      <div class="meter"><div class="fill ${cls}" style="width:${p}%"></div></div>`;
  }

  function empty(msg) {
    return `<div class="empty">${esc(msg || '暂无数据 No data')}</div>`;
  }

  // headers: [{key, label, num, render(row), width}]
  function table(headers, rows, opts) {
    if (!rows || !rows.length) return empty();
    const thead = '<tr>' + headers.map(h =>
      `<th class="${h.num ? 'num' : ''}"${h.width ? ` style="width:${h.width}"` : ''}>${esc(h.label)}</th>`
    ).join('') + '</tr>';
    const tbody = rows.map((row, ri) => {
      const tds = headers.map(h => {
        const val = h.render ? h.render(row, ri) : row[h.key];
        return `<td class="${h.num ? 'num tabular' : ''}">${val == null ? '-' : val}</td>`;
      }).join('');
      const click = (opts && opts.onClick) ? ` data-row="${ri}"` : '';
      return `<tr${click}>${tds}</tr>`;
    }).join('');
    const html = `<div class="table-wrap"><table class="table"><thead>${thead}</thead><tbody>${tbody}</tbody></table></div>`;
    if (opts && opts.onClick) {
      // attach click handler by returning element instead; handled via data attribute
      return html;
    }
    return html;
  }

  // ------------------------------------------------------------------
  function renderNav(active) {
    const host = document.getElementById('app-nav');
    if (!host) return;
    host.innerHTML =
      `<span class="brand">分布式 MapReduce<small>Distributed</small></span>` +
      PAGES.map(p =>
        `<a class="nav-link${p.key === active ? ' active' : ''}" href="${p.href}">${esc(p.label)}</a>`
      ).join('') +
      `<span class="spacer"></span>` +
      `<button class="theme-toggle" id="theme-toggle" title="切换主题 Theme">◐</button>`;
    const toggle = document.getElementById('theme-toggle');
    toggle.addEventListener('click', () => {
      const root = document.documentElement;
      const cur = root.getAttribute('data-theme');
      const next = cur === 'dark' ? 'light' : 'dark';
      root.setAttribute('data-theme', next);
      try { localStorage.setItem('mr-theme', next); } catch (e) {}
      window.dispatchEvent(new Event('themechange'));
    });
  }

  function initTheme() {
    try {
      const saved = localStorage.getItem('mr-theme');
      if (saved) document.documentElement.setAttribute('data-theme', saved);
    } catch (e) {}
  }

  function init(active) {
    initTheme();
    renderNav(active);
  }

  // ------------------------------------------------------------------
  function toast(msg, type) {
    let host = document.querySelector('.toast-host');
    if (!host) { host = document.createElement('div'); host.className = 'toast-host'; document.body.appendChild(host); }
    const t = document.createElement('div');
    t.className = 'toast ' + (type || '');
    t.textContent = msg;
    host.appendChild(t);
    setTimeout(() => t.remove(), 4000);
  }

  function poll(fn, ms) {
    let timer = null;
    let stopped = false;
    async function run() {
      if (stopped) return;
      try { await fn(); } catch (e) { /* transient */ }
      if (!stopped) timer = setTimeout(run, ms);
    }
    return {
      start() { run(); },
      stop() { stopped = true; if (timer) clearTimeout(timer); },
    };
  }

  function valueCell(rec) {
    // Render a result record generically: key -> value / values.
    const keys = Object.keys(rec).filter(k => k !== 'key');
    if (keys.length === 1) return esc(rec[keys[0]]);
    return esc(keys.map(k => `${k}=${rec[k]}`).join(', '));
  }

  // Build a job <select> in hostId once, auto-select the first job.
  function jobPicker(hostId, onSelect) {
    const host = document.getElementById(hostId);
    if (!host) return;
    API.get('/api/jobs').then(d => {
      const jobs = d.jobs || [];
      let html = '<select class="job-select"><option value="">选择作业 Select job…</option>';
      jobs.forEach(j => { html += `<option value="${j.job_id}">${esc(j.name)} — ${j.status}</option>`; });
      html += '</select>';
      host.innerHTML = html;
      const sel = host.querySelector('select');
      sel.addEventListener('change', () => onSelect(sel.value));
      if (jobs.length) { sel.value = jobs[0].job_id; onSelect(sel.value); }
    }).catch(() => { host.innerHTML = empty('无法连接 Master (Cannot reach master)'); });
  }

  return {
    PAGES, LABELS, CLASS, esc, fmtNum, fmtBytes, fmtTime, fmtDur, fmtPct,
    stateBadge, progress, meter, empty, table, renderNav, init, toast, poll, valueCell, jobPicker,
  };
})();
