/**
 * JobPulse Dashboard — Frontend
 */
(() => {
  'use strict';

  const API = '/api';
  let currentPage = 1;
  const pageSize = 50;

  // Charts
  let tagsChart, salaryChart, companiesChart, historyChart;

  // ── Scrape Button ─────────────────────────────────────────
  const scrapeBtn = document.getElementById('scrape-btn');
  const scrapeStatus = document.getElementById('scrape-status');

  scrapeBtn.addEventListener('click', async () => {
    scrapeBtn.disabled = true;
    scrapeStatus.className = 'status-badge running';
    scrapeStatus.textContent = 'Scraping...';

    try {
      const res = await fetch(`${API}/scrape`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sources: ['remoteok', 'arbeitnow'] }),
      });
      const { job_id } = await res.json();

      // Poll for completion
      const result = await pollJob(job_id);

      scrapeStatus.className = `status-badge ${result.status}`;
      scrapeStatus.textContent = result.status === 'completed' ? 'Done' : 'Failed';

      // Refresh data
      await Promise.all([loadJobs(), loadTrends()]);
    } catch (e) {
      scrapeStatus.className = 'status-badge failed';
      scrapeStatus.textContent = 'Error';
      console.error(e);
    } finally {
      scrapeBtn.disabled = false;
      setTimeout(() => { scrapeStatus.className = 'status-badge hidden'; }, 4000);
    }
  });

  async function pollJob(jobId) {
    while (true) {
      await sleep(2000);
      const res = await fetch(`${API}/scrape/${jobId}`);
      const data = await res.json();
      if (data.status === 'completed' || data.status === 'failed') return data;
    }
  }

  function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

  // ── Filters ───────────────────────────────────────────────
  document.getElementById('filter-btn').addEventListener('click', () => {
    currentPage = 1;
    loadJobs();
  });

  // ── Pagination ────────────────────────────────────────────
  document.getElementById('prev-page').addEventListener('click', () => {
    if (currentPage > 1) { currentPage--; loadJobs(); }
  });
  document.getElementById('next-page').addEventListener('click', () => {
    currentPage++;
    loadJobs();
  });

  // ── Load Jobs ─────────────────────────────────────────────
  async function loadJobs() {
    const params = new URLSearchParams({ page: currentPage, limit: pageSize });
    const role = document.getElementById('filter-role').value;
    const location = document.getElementById('filter-location').value;
    const salary = document.getElementById('filter-salary').value;
    const source = document.getElementById('filter-source').value;

    if (role) params.set('role', role);
    if (location) params.set('location', location);
    if (salary) params.set('salary_min', salary);
    if (source) params.set('source', source);

    try {
      const res = await fetch(`${API}/jobs?${params}`);
      const data = await res.json();
      renderTable(data.listings);
      updatePagination(data.total, data.page);

      // Update stat
      document.querySelector('#stat-total .stat-value').textContent = data.total.toLocaleString();
    } catch (e) {
      console.error('Failed to load jobs:', e);
    }
  }

  function renderTable(listings) {
    const tbody = document.querySelector('#jobs-table tbody');
    tbody.innerHTML = listings.map(job => `
      <tr>
        <td>${job.url ? `<a href="${job.url}" target="_blank">${esc(job.title)}</a>` : esc(job.title)}</td>
        <td>${esc(job.company)}</td>
        <td>${esc(job.location || '—')}</td>
        <td>${formatSalary(job.salary_min, job.salary_max, job.currency)}</td>
        <td>${(job.tags || []).slice(0, 4).map(t => `<span class="tag">${esc(t)}</span>`).join('')}</td>
        <td><span class="source-badge ${job.source}">${job.source}</span></td>
      </tr>
    `).join('');
  }

  function updatePagination(total, page) {
    const maxPage = Math.max(1, Math.ceil(total / pageSize));
    document.getElementById('page-info').textContent = `Page ${page} of ${maxPage}`;
    document.getElementById('prev-page').disabled = page <= 1;
    document.getElementById('next-page').disabled = page >= maxPage;
  }

  function formatSalary(min, max, currency) {
    if (!min && !max) return '—';
    const fmt = n => `${(n / 1000).toFixed(0)}k`;
    const sym = currency === 'EUR' ? '\u20ac' : currency === 'GBP' ? '\u00a3' : '$';
    if (min && max && min !== max) return `${sym}${fmt(min)} - ${sym}${fmt(max)}`;
    return `${sym}${fmt(min || max)}`;
  }

  function esc(str) {
    if (!str) return '';
    const d = document.createElement('div');
    d.textContent = str;
    return d.innerHTML;
  }

  // ── Load Trends ───────────────────────────────────────────
  async function loadTrends() {
    try {
      const res = await fetch(`${API}/trends`);
      const data = await res.json();

      renderTagsChart(data.top_tags);
      renderSalaryChart(data.salary_distribution);
      renderCompaniesChart(data.top_companies);
      renderHistoryChart(data.scrape_history);

      // Update stats
      const sourcesCount = data.sources_breakdown.length;
      document.querySelector('#stat-sources .stat-value').textContent = sourcesCount;

      if (data.scrape_history.length > 0) {
        const latest = data.scrape_history[0];
        const date = new Date(latest.date);
        document.querySelector('#stat-latest .stat-value').textContent =
          date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
      }
    } catch (e) {
      console.error('Failed to load trends:', e);
    }
  }

  const chartDefaults = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { display: false },
    },
    scales: {
      x: { ticks: { color: '#8b8fa3', font: { size: 11 } }, grid: { color: '#2a2d3a' } },
      y: { ticks: { color: '#8b8fa3', font: { size: 11 } }, grid: { color: '#2a2d3a' } },
    },
  };

  function renderTagsChart(tags) {
    const ctx = document.getElementById('tags-chart');
    if (tagsChart) tagsChart.destroy();
    tagsChart = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: tags.map(t => t.tag),
        datasets: [{
          data: tags.map(t => t.count),
          backgroundColor: '#6366f1',
          borderRadius: 4,
        }],
      },
      options: { ...chartDefaults, indexAxis: 'y' },
    });
  }

  function renderSalaryChart(dist) {
    const ctx = document.getElementById('salary-chart');
    if (salaryChart) salaryChart.destroy();
    salaryChart = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: dist.map(d => d.range),
        datasets: [{
          data: dist.map(d => d.count),
          backgroundColor: '#22c55e',
          borderRadius: 4,
        }],
      },
      options: chartDefaults,
    });
  }

  function renderCompaniesChart(companies) {
    const ctx = document.getElementById('companies-chart');
    if (companiesChart) companiesChart.destroy();
    companiesChart = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: companies.map(c => c.company),
        datasets: [{
          data: companies.map(c => c.count),
          backgroundColor: '#f97316',
          borderRadius: 4,
        }],
      },
      options: { ...chartDefaults, indexAxis: 'y' },
    });
  }

  function renderHistoryChart(history) {
    const ctx = document.getElementById('history-chart');
    if (historyChart) historyChart.destroy();

    const sorted = [...history].reverse();
    historyChart = new Chart(ctx, {
      type: 'line',
      data: {
        labels: sorted.map(h => new Date(h.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })),
        datasets: [
          {
            label: 'Added',
            data: sorted.map(h => h.added),
            borderColor: '#22c55e',
            backgroundColor: 'rgba(34,197,94,0.1)',
            fill: true, tension: 0.3,
          },
          {
            label: 'Removed',
            data: sorted.map(h => h.removed),
            borderColor: '#ef4444',
            backgroundColor: 'rgba(239,68,68,0.1)',
            fill: true, tension: 0.3,
          },
        ],
      },
      options: {
        ...chartDefaults,
        plugins: {
          legend: { display: true, labels: { color: '#8b8fa3' } },
        },
      },
    });
  }

  // ── Init ──────────────────────────────────────────────────
  loadJobs();
  loadTrends();
})();
