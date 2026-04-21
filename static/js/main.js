/**
 * main.js — Shared application state and utilities for MediaManager UI.
 *
 * Provides: App.api, App.toast, App.openPanel, App.closePanel,
 *           App.openPathModal, App.toggleApi, and header status refresh.
 */

const App = {
  config: {},
  _pathCallback: null,
  _pathCurrent: '',

  async init() {
    // Load config into App.config
    try {
      App.config = await App.api.get('/api/config');
    } catch (e) {
      console.warn('Could not load config:', e);
    }

    // Update header status every 10s
    App.refreshHeaderStatus();
    setInterval(App.refreshHeaderStatus, 10000);

    // Update sidebar LLM name
    App.updateSidebarLlm();
  },

  // ── REST helpers ───────────────────────────────────────────────────────────

  api: {
    async get(url) {
      const r = await fetch(url);
      if (!r.ok) throw new Error(`GET ${url} → ${r.status}`);
      return r.json();
    },
    async post(url, data) {
      const r = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
      });
      if (!r.ok) throw new Error(`POST ${url} → ${r.status}`);
      return r.json();
    },
    async put(url, data) {
      const r = await fetch(url, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
      });
      if (!r.ok) throw new Error(`PUT ${url} → ${r.status}`);
      return r.json();
    },
  },

  // ── Header API pills ───────────────────────────────────────────────────────

  async refreshHeaderStatus() {
    try {
      const data = await App.api.get('/api/status');
      const apiState = data.api_state || {};

      ['tmdb', 'omdb'].forEach(name => {
        const dot   = document.getElementById(`${name}-dot`);
        const count = document.getElementById(`${name}-count`);
        const s = apiState[name] || {};
        if (!dot) return;
        dot.className = 'dot ' + (s.paused ? 'paused' : Object.keys(s).length ? 'ok' : 'off');
        if (count) count.textContent = s.requests_today != null ? `${s.requests_today}` : '—';
      });
    } catch (e) {
      // silently ignore network errors on polling
    }

    // LLM pill
    try {
      const cfg = App.config?.llm || {};
      const dot  = document.getElementById('llm-dot');
      const lbl  = document.getElementById('llm-label');
      if (cfg.provider && cfg.provider !== 'none' && cfg.model) {
        const pdata = await App.api.get('/api/llm/providers');
        const p = (pdata.providers || []).find(x => x.id === cfg.provider);
        if (dot) dot.className = 'dot ' + (p?.available ? 'ok' : 'paused');
        if (lbl) lbl.textContent = cfg.model || cfg.provider;
      } else {
        if (dot) dot.className = 'dot off';
        if (lbl) lbl.textContent = 'LLM';
      }
    } catch (e) { /* ignore */ }
  },

  updateSidebarLlm() {
    const cfg = App.config?.llm || {};
    const el  = document.getElementById('sidebar-llm-name');
    if (!el) return;
    if (cfg.provider && cfg.provider !== 'none') {
      el.textContent = `${cfg.provider}${cfg.model ? ` · ${cfg.model}` : ''}`;
    } else {
      el.textContent = 'Not configured';
    }
  },

  // ── API rate-limit controls ────────────────────────────────────────────────

  async toggleApi(name) {
    const dot = document.getElementById(`${name}-dot`);
    const isPaused = dot?.classList.contains('paused');
    if (isPaused) {
      await App.resumeApi(name);
    } else {
      await App.pauseApi(name);
    }
    App.refreshHeaderStatus();
  },

  async resumeApi(name) {
    await App.api.post(`/api/ratelimit/${name}/resume`, {});
    App.toast(`${name.toUpperCase()} resumed`, 'success');
    App.refreshHeaderStatus();
  },

  async pauseApi(name) {
    await App.api.post(`/api/ratelimit/${name}/pause`, {});
    App.toast(`${name.toUpperCase()} paused`, 'warning');
    App.refreshHeaderStatus();
  },

  // ── Side panel ─────────────────────────────────────────────────────────────

  openPanel() {
    document.getElementById('panel-overlay').classList.add('open');
    document.getElementById('side-panel').classList.add('open');
  },

  closePanel() {
    document.getElementById('panel-overlay').classList.remove('open');
    document.getElementById('side-panel').classList.remove('open');
  },

  // ── Toast notifications ────────────────────────────────────────────────────

  toast(message, type = 'info', duration = 3500) {
    const icons = { success: '✓', error: '✗', warning: '⚠', info: 'ℹ' };
    const container = document.getElementById('toast-container');
    const el = document.createElement('div');
    el.className = `toast ${type}`;
    el.innerHTML = `<span>${icons[type] || 'ℹ'}</span><span>${message}</span>`;
    container.appendChild(el);
    setTimeout(() => {
      el.style.transition = 'opacity .3s';
      el.style.opacity = '0';
      setTimeout(() => el.remove(), 300);
    }, duration);
  },

  // ── Path picker modal ──────────────────────────────────────────────────────

  openPathModal(callback) {
    App._pathCallback = callback;
    App._pathCurrent  = '';
    document.getElementById('path-modal').classList.add('open');
    App.browseTo('');
    document.getElementById('path-select-btn').onclick = () => {
      if (App._pathCurrent && App._pathCallback) {
        App._pathCallback(App._pathCurrent);
      }
      App.closePathModal();
    };
  },

  closePathModal() {
    document.getElementById('path-modal').classList.remove('open');
  },

  async browseTo(path) {
    App._pathCurrent = path;
    const data = await App.api.get('/api/browse?path=' + encodeURIComponent(path));
    if (data.error) { App.toast(data.error, 'error'); return; }

    // Breadcrumb
    const bc = document.getElementById('path-breadcrumb');
    if (!path) {
      bc.innerHTML = '<span>This PC</span>';
    } else {
      const parts = path.replace(/\\/g, '/').split('/').filter(Boolean);
      let built = '';
      bc.innerHTML = '<span style="cursor:pointer;color:var(--accent-l)" onclick="App.browseTo(\'\')">Root</span>';
      parts.forEach((part, i) => {
        built += (built ? '\\' : '') + part;
        const nav = built;
        bc.innerHTML += ` <span style="color:var(--text-4)">›</span> <span style="cursor:pointer;color:var(--accent-l)" onclick="App.browseTo('${nav}')">${part}</span>`;
      });
    }

    // Current selection display
    if (path) {
      document.getElementById('path-select-btn').textContent = 'Select "' + path.split('\\').pop() + '"';
    } else {
      document.getElementById('path-select-btn').textContent = 'Select This Folder';
    }

    // Entries
    const entries = document.getElementById('path-entries');
    let html = '';
    if (data.parent !== null && data.parent !== undefined) {
      html += `<div class="path-entry" onclick="App.browseTo(${JSON.stringify(data.parent)})">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M19 12H5m7-7-7 7 7 7"/></svg>
        <span style="color:var(--text-3)">.. (up)</span>
      </div>`;
    }
    (data.entries || []).forEach(e => {
      html += `<div class="path-entry" onclick="App.browseTo(${JSON.stringify(e.path)})">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/></svg>
        <span>${e.name}</span>
      </div>`;
    });
    if (!html && path) html = '<div class="text-muted text-small" style="padding:12px">No subfolders</div>';
    entries.innerHTML = html;
  },
};
