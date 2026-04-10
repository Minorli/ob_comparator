// ob-lens frontend — vanilla JS, no dependencies
(function () {
  'use strict';

  const DATA = window.__OB_LENS_DATA__ || { runs: [], current_run: null, fixup_scripts: [] };
  const run = DATA.current_run;

  // ---- Tab Navigation ----
  document.querySelectorAll('.nav-tab').forEach(tab => {
    tab.addEventListener('click', () => {
      const target = tab.dataset.page;
      document.querySelectorAll('.nav-tab').forEach(t => t.classList.remove('active'));
      document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
      tab.classList.add('active');
      document.getElementById('page-' + target).classList.add('active');
    });
  });

  // ---- Dashboard ----
  function initDashboard() {
    if (!run) return;
    document.getElementById('dash-ts').textContent = run.run_ts_display;
    document.getElementById('dash-consistent').textContent = run.consistent;
    document.getElementById('dash-missing').textContent = run.missing_total;
    document.getElementById('dash-incompatible').textContent = run.unsupported_or_blocked;
    document.getElementById('dash-extra').textContent = run.extra;

    const total = run.missing_total || 1;
    const done = run.missing_supported || 0;
    const pct = Math.round((done / total) * 100);
    document.getElementById('progress-fill').style.width = pct + '%';
    document.getElementById('progress-text').textContent =
      done + '/' + total + ' 可自动修补 (' + pct + '%)';
  }

  // ---- Problem Browser ----
  const REASON_LABELS = {
    'BLACKLIST_IOT': '索引组织表（IOT），OceanBase 不支持',
    'BLACKLIST_TEMPORARY_TABLE': '全局临时表结构不兼容，需改写',
    'BLACKLIST_SPE': '包含不支持的字段类型',
    'BLACKLIST_DIY': '包含自定义黑名单类型',
    'VIEW_SYS_OBJ': '视图引用了 Oracle 系统对象',
    'DEPENDENCY_UNSUPPORTED': '依赖对象不兼容，需先处理依赖',
    'DEPENDENCY_TARGET_TABLE_MISSING': '依赖的表尚未在目标端创建',
  };

  const STATE_BADGE = {
    'SUPPORTED':   '<span class="badge badge-supported">✗ 缺失可修补</span>',
    'UNSUPPORTED': '<span class="badge badge-unsupported">⚡ 不兼容</span>',
    'BLOCKED':     '<span class="badge badge-blocked">⛔ 阻断</span>',
  };

  let currentFilter = { state: 'all', type: 'all', schema: 'all', q: '' };

  function applyFilter(objects) {
    return objects.filter(o => {
      if (currentFilter.state !== 'all' && o.state !== currentFilter.state) return false;
      if (currentFilter.type !== 'all' && o.obj_type !== currentFilter.type) return false;
      if (currentFilter.schema !== 'all') {
        const schema = o.src_full.split('.')[0] || '';
        if (schema !== currentFilter.schema) return false;
      }
      if (currentFilter.q) {
        const q = currentFilter.q.toLowerCase();
        if (!o.src_full.toLowerCase().includes(q) &&
            !o.reason.toLowerCase().includes(q) &&
            !o.reason_code.toLowerCase().includes(q)) return false;
      }
      return true;
    });
  }

  function renderObjects() {
    if (!run) return;
    const objects = run.objects || [];
    const filtered = applyFilter(objects);
    const container = document.getElementById('object-list');
    document.getElementById('filter-count').textContent =
      filtered.length + ' / ' + objects.length + ' 个对象';

    if (filtered.length === 0) {
      container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">🔍</div>' +
        '<div class="empty-state-text">没有匹配的对象</div></div>';
      return;
    }

    container.innerHTML = filtered.map((o, i) => {
      const badge = STATE_BADGE[o.state] || '';
      const typeBadge = '<span class="badge badge-type">' + o.obj_type + '</span>';
      const reasonLabel = REASON_LABELS[o.reason_code] || o.reason || '';
      const depHtml = (o.dependency && o.dependency !== '-')
        ? '<a href="#" onclick="filterByName(\'' + escHtml(o.dependency) + '\'); return false;">' +
          escHtml(o.dependency) + ' →</a>'
        : '—';
      const warnHtml = (o.detail && o.detail.includes('DDL_REWRITE'))
        ? '<div class="detail-warn">⚠ DDL_REWRITE：含语义改写，建议人工复核后再执行</div>'
        : '';
      const detailExtra = (o.detail && o.detail !== '-' && !o.detail.includes('DDL_REWRITE'))
        ? '<div class="detail-grid"><span class="detail-label">补充</span>' +
          '<span class="detail-value">' + escHtml(o.detail) + '</span></div>'
        : '';

      return `<div class="object-item" data-idx="${i}">
        <div class="object-header" onclick="toggleItem(this)">
          <span class="object-name" title="${escHtml(o.src_full)}">${escHtml(o.src_full)}</span>
          ${typeBadge}
          ${badge}
          <span class="object-chevron">▶</span>
        </div>
        <div class="object-detail">
          <div class="detail-grid">
            <span class="detail-label">问题</span>
            <span class="detail-value">${escHtml(o.reason || '缺失，已生成修补脚本')}</span>
            ${o.reason_code ? `<span class="detail-label">原因</span>
            <span class="detail-value">${escHtml(reasonLabel)}</span>` : ''}
            <span class="detail-label">建议操作</span>
            <span class="detail-value">${escHtml(o.action)}</span>
            ${(o.dependency && o.dependency !== '-') ? `<span class="detail-label">阻断来源</span>
            <span class="detail-value">${depHtml}</span>` : ''}
            <span class="detail-label">目标对象</span>
            <span class="detail-value">${escHtml(o.tgt_full)}</span>
          </div>
          ${detailExtra}
          ${warnHtml}
          <div class="detail-actions">
            <button class="btn-sm" onclick="copyToClip('${escHtml(o.src_full)}')">📋 复制名称</button>
          </div>
        </div>
      </div>`;
    }).join('');
  }

  window.toggleItem = function(header) {
    header.parentElement.classList.toggle('expanded');
  };

  window.filterByName = function(name) {
    const q = document.getElementById('search-input');
    if (q) { q.value = name; currentFilter.q = name.toLowerCase(); renderObjects(); }
    document.querySelector('[data-page="problems"]').click();
  };

  window.copyToClip = function(text) {
    navigator.clipboard.writeText(text).catch(() => {});
  };

  function escHtml(str) {
    return String(str)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;')
      .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  function populateFilters() {
    if (!run) return;
    const objects = run.objects || [];
    // Unique types
    const types = [...new Set(objects.map(o => o.obj_type))].sort();
    const typeSelect = document.getElementById('filter-type');
    types.forEach(t => {
      const opt = document.createElement('option');
      opt.value = t; opt.textContent = t;
      typeSelect.appendChild(opt);
    });
    // Unique schemas
    const schemas = [...new Set(objects.map(o => o.src_full.split('.')[0]))].sort();
    const schemaSelect = document.getElementById('filter-schema');
    schemas.forEach(s => {
      const opt = document.createElement('option');
      opt.value = s; opt.textContent = s;
      schemaSelect.appendChild(opt);
    });
  }

  function initProblems() {
    populateFilters();
    document.getElementById('search-input').addEventListener('input', e => {
      currentFilter.q = e.target.value.toLowerCase();
      renderObjects();
    });
    document.getElementById('filter-state').addEventListener('change', e => {
      currentFilter.state = e.target.value;
      renderObjects();
    });
    document.getElementById('filter-type').addEventListener('change', e => {
      currentFilter.type = e.target.value;
      renderObjects();
    });
    document.getElementById('filter-schema').addEventListener('change', e => {
      currentFilter.schema = e.target.value;
      renderObjects();
    });
    renderObjects();
  }

  // ---- History ----
  function initHistory() {
    const tbody = document.getElementById('history-tbody');
    const runs = (DATA.runs || []).slice().reverse();
    if (runs.length === 0) {
      tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;color:var(--text-secondary);padding:32px">暂无历史运行</td></tr>';
      return;
    }
    tbody.innerHTML = runs.map(r => {
      const isCurrent = run && r.run_id === run.run_id;
      return `<tr${isCurrent ? ' style="background:rgba(0,122,255,0.04)"' : ''}>
        <td><span style="font-family:var(--mono);font-size:13px">${escHtml(r.run_ts_display)}</span>
          ${isCurrent ? ' <span class="badge badge-ok" style="font-size:11px">当前</span>' : ''}</td>
        <td>${r.missing_total > 0 ? '<span class="badge badge-unsupported">需处理</span>'
          : '<span class="badge badge-ok">✓ 通过</span>'}</td>
        <td>${r.consistent}</td>
        <td>${r.missing_total}</td>
        <td>${r.unsupported_or_blocked}</td>
        <td>${r.extra}</td>
      </tr>`;
    }).join('');
  }

  // ---- Init ----
  initDashboard();
  initProblems();
  initHistory();

})();
