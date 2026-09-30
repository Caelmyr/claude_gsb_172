/* Lightweight canvas charts (no dependencies).
 *
 * Mark specs follow the data-viz system: 2px lines, ~10% area wash, >=8px end
 * markers with a 2px surface ring, 1px hairline gridlines, and bars <=24px with
 * a 4px rounded data-end growing from a single baseline.  Colors are read from
 * CSS custom properties so light/dark both theme from one place.
 */
const Charts = (() => {
  const SERIES_VARS = ['--series-1', '--series-2', '--series-3', '--series-4',
    '--series-5', '--series-6', '--series-7', '--series-8'];

  function cssVar(name) {
    return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  }

  function prepare(canvas, height) {
    const dpr = window.devicePixelRatio || 1;
    const rect = canvas.getBoundingClientRect();
    const w = Math.max(40, rect.width || canvas.clientWidth || 600);
    const h = height || Math.max(40, canvas.clientHeight || 220);
    canvas.width = Math.round(w * dpr);
    canvas.height = Math.round(h * dpr);
    canvas.style.height = h + 'px';
    const ctx = canvas.getContext('2d');
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    return { ctx, w, h };
  }

  function ink() {
    return { ink2: cssVar('--ink-2'), muted: cssVar('--muted'), gridline: cssVar('--gridline'), baseline: cssVar('--baseline'), surface: cssVar('--surface') };
  }

  // ------------------------------------------------------------------
  // Line chart — time series. Single series: no legend box. Multi: legend.
  // opts: { labels: [], series: [{name, values}], yFormat, xFormat, height }
  function lineChart(canvas, opts) {
    const { ctx, w, h } = prepare(canvas, opts.height || 220);
    const c = ink();
    const pad = { top: 10, right: 14, bottom: 24, left: 48 };
    const plotW = w - pad.left - pad.right;
    const plotH = h - pad.top - pad.bottom;
    const n = Math.max(1, opts.labels.length);
    const allVals = opts.series.flatMap(s => s.values).filter(v => v != null && !isNaN(v));
    const maxV = Math.max(1, ...allVals) * 1.12;
    const x = (i) => pad.left + (n === 1 ? plotW / 2 : plotW * i / (n - 1));
    const y = (v) => pad.top + plotH - plotH * (Math.max(0, v)) / maxV;

    // gridlines + y ticks (clean numbers)
    ctx.font = '10px system-ui';
    ctx.lineWidth = 1;
    const ticks = 4;
    for (let t = 0; t <= ticks; t++) {
      const v = maxV * t / ticks;
      const yy = y(v);
      ctx.strokeStyle = c.gridline;
      ctx.beginPath(); ctx.moveTo(pad.left, yy); ctx.lineTo(w - pad.right, yy); ctx.stroke();
      ctx.fillStyle = c.muted; ctx.textAlign = 'right';
      const label = opts.yFormat ? opts.yFormat(v) : Math.round(v).toString();
      ctx.fillText(label, pad.left - 6, yy + 3);
    }
    // x labels: first / middle / last
    ctx.fillStyle = c.muted; ctx.textAlign = 'center';
    const xIdxs = [...new Set([0, Math.floor((n - 1) / 2), n - 1])];
    xIdxs.forEach(i => {
      const label = opts.xFormat ? opts.xFormat(opts.labels[i], i) : String(opts.labels[i]);
      ctx.fillText(label, x(i), h - 6);
    });
    // baseline
    ctx.strokeStyle = c.baseline;
    ctx.beginPath(); ctx.moveTo(pad.left, y(0)); ctx.lineTo(w - pad.right, y(0)); ctx.stroke();

    opts.series.forEach((s, si) => {
      const color = s.color || cssVar(SERIES_VARS[si] || SERIES_VARS[0]);
      if (s.values.length > 1) {
        ctx.strokeStyle = color;
        ctx.lineWidth = 2;
        ctx.lineJoin = 'round';
        ctx.lineCap = 'round';
        ctx.beginPath();
        s.values.forEach((v, i) => (i === 0 ? ctx.moveTo(x(i), y(v)) : ctx.lineTo(x(i), y(v))));
        ctx.stroke();

        // area wash ~10%
        if (opts.series.length === 1) {
          ctx.globalAlpha = 0.10;
          ctx.fillStyle = color;
          ctx.beginPath();
          ctx.moveTo(x(0), y(0));
          s.values.forEach((v, i) => ctx.lineTo(x(i), y(v)));
          ctx.lineTo(x(s.values.length - 1), y(0));
          ctx.closePath();
          ctx.fill();
          ctx.globalAlpha = 1;
        }
      }
      // end marker (>=8px, 2px surface ring)
      const li = s.values.length - 1;
      if (li >= 0 && s.values[li] != null) {
        ctx.beginPath(); ctx.arc(x(li), y(s.values[li]), 5, 0, Math.PI * 2);
        ctx.fillStyle = c.surface; ctx.fill();
        ctx.beginPath(); ctx.arc(x(li), y(s.values[li]), 3.5, 0, Math.PI * 2);
        ctx.fillStyle = color; ctx.fill();
      }
    });

    // legend (only when >= 2 series)
    const legend = document.getElementById(canvas.id + '-legend');
    if (legend) {
      legend.innerHTML = opts.series.length >= 2
        ? opts.series.map((s, si) => {
            const color = s.color || cssVar(SERIES_VARS[si] || SERIES_VARS[0]);
            return `<span class="item"><span class="swatch line" style="background:${color}"></span>${Components.esc(s.name)}</span>`;
          }).join('')
        : '';
    }
  }

  // ------------------------------------------------------------------
  // Bar chart — categorical magnitude. Bars <=24px, 4px rounded top, 2px gap.
  // opts: { labels: [], values: [], format, height }
  function barChart(canvas, opts) {
    const { ctx, w, h } = prepare(canvas, opts.height || 220);
    const c = ink();
    const pad = { top: 10, right: 10, bottom: 24, left: 44 };
    const plotW = w - pad.left - pad.right;
    const plotH = h - pad.top - pad.bottom;
    const n = Math.max(1, opts.values.length);
    const maxV = Math.max(1, ...opts.values) * 1.12;
    const slot = plotW / n;
    const barW = Math.min(24, slot * 0.7);
    const y = (v) => pad.top + plotH - plotH * v / maxV;

    // gridlines
    ctx.font = '10px system-ui'; ctx.lineWidth = 1;
    const ticks = 4;
    for (let t = 0; t <= ticks; t++) {
      const v = maxV * t / ticks; const yy = y(v);
      ctx.strokeStyle = c.gridline;
      ctx.beginPath(); ctx.moveTo(pad.left, yy); ctx.lineTo(w - pad.right, yy); ctx.stroke();
      ctx.fillStyle = c.muted; ctx.textAlign = 'right';
      ctx.fillText(opts.format ? opts.format(v) : Math.round(v).toString(), pad.left - 6, yy + 3);
    }
    ctx.strokeStyle = c.baseline;
    ctx.beginPath(); ctx.moveTo(pad.left, y(0)); ctx.lineTo(w - pad.right, y(0)); ctx.stroke();

    const color = opts.color || cssVar('--series-1');
    opts.values.forEach((v, i) => {
      const cx = pad.left + slot * i + slot / 2;
      const x0 = cx - barW / 2;
      const yTop = y(v); const yBase = y(0);
      const r = Math.min(4, barW / 2, yBase - yTop);
      ctx.fillStyle = color;
      ctx.beginPath();
      ctx.moveTo(x0, yBase);
      ctx.lineTo(x0, yTop + r);
      ctx.quadraticCurveTo(x0, yTop, x0 + r, yTop);
      ctx.lineTo(x0 + barW - r, yTop);
      ctx.quadraticCurveTo(x0 + barW, yTop, x0 + barW, yTop + r);
      ctx.lineTo(x0 + barW, yBase);
      ctx.closePath();
      ctx.fill();
      // x label
      ctx.fillStyle = c.muted; ctx.textAlign = 'center';
      ctx.fillText(String(opts.labels[i]).slice(0, 18), cx, h - 6);
    });
  }

  // ------------------------------------------------------------------
  function donut(canvas, parts, opts) {
    // parts: [{label, value, color}]; renders a donut with center total.
    const { ctx, w, h } = prepare(canvas, opts && opts.height || 200);
    const c = ink();
    const cx = w / 2, cy = h / 2;
    const outer = Math.min(w, h) / 2 - 10;
    const inner = outer * 0.62;
    const total = parts.reduce((s, p) => s + p.value, 0) || 1;
    let a0 = -Math.PI / 2;
    parts.forEach(p => {
      const a1 = a0 + (p.value / total) * Math.PI * 2;
      ctx.beginPath();
      ctx.moveTo(cx, cy);
      ctx.arc(cx, cy, outer, a0, a1);
      ctx.closePath();
      ctx.fillStyle = p.color || cssVar('--series-1');
      ctx.fill();
      a0 = a1;
    });
    // inner hole (surface)
    ctx.beginPath(); ctx.arc(cx, cy, inner, 0, Math.PI * 2);
    ctx.fillStyle = c.surface; ctx.fill();
    ctx.fillStyle = cssVar('--ink');
    ctx.font = '600 20px system-ui'; ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
    ctx.fillText(Components.fmtNum(total), cx, cy - 8);
    ctx.fillStyle = c.muted; ctx.font = '11px system-ui';
    ctx.fillText(opts && opts.centerLabel || 'total', cx, cy + 10);
    ctx.textBaseline = 'alphabetic';
  }

  return { lineChart, barChart, donut, SERIES_VARS };
})();
