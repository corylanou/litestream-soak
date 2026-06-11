(function () {
  "use strict";

  var REFRESH_INTERVAL = 10;
  var countdown = REFRESH_INTERVAL;
  var paused = false;
  var refreshing = false;
  var charts = [];

  var css = getComputedStyle(document.documentElement);
  var theme = {
    text: css.getPropertyValue("--muted").trim() || "#8b949e",
    faint: css.getPropertyValue("--faint").trim() || "#484f58",
    grid: "rgba(48,54,61,0.5)",
    green: css.getPropertyValue("--green").trim() || "#3fb950",
    amber: css.getPropertyValue("--amber").trim() || "#d29922",
    red: css.getPropertyValue("--red").trim() || "#f85149",
    blue: css.getPropertyValue("--blue").trim() || "#58a6ff"
  };

  function hourLabel(iso) {
    var d = new Date(iso);
    if (isNaN(d)) return iso;
    return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  }

  function durationLabel(ms) {
    if (ms == null) return "";
    if (ms >= 60000) return (ms / 60000).toFixed(1) + "m";
    if (ms >= 1000) return (ms / 1000).toFixed(1) + "s";
    return ms + "ms";
  }

  function baseOptions() {
    return {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: {
          display: true,
          labels: { color: theme.text, boxWidth: 10, boxHeight: 10, font: { size: 10 } }
        },
        tooltip: { backgroundColor: "#1c2129", borderColor: "#30363d", borderWidth: 1 }
      },
      scales: {
        x: {
          ticks: { color: theme.faint, font: { size: 10 }, maxTicksLimit: 6, maxRotation: 0 },
          grid: { display: false }
        },
        y: {
          ticks: { color: theme.faint, font: { size: 10 }, maxTicksLimit: 5 },
          grid: { color: theme.grid }
        }
      }
    };
  }

  function lineDataset(label, data, color, dashed) {
    return {
      label: label,
      data: data,
      borderColor: color,
      backgroundColor: "transparent",
      borderWidth: 1.5,
      borderDash: dashed ? [4, 3] : [],
      pointRadius: 0,
      pointHitRadius: 8,
      spanGaps: true,
      tension: 0.25
    };
  }

  function destroyCharts() {
    charts.forEach(function (chart) { chart.destroy(); });
    charts = [];
  }

  function initCharts() {
    if (typeof Chart === "undefined") return;
    var holder = document.getElementById("chart-data");
    if (!holder) return;
    var data;
    try {
      data = JSON.parse(holder.dataset.chart || holder.textContent);
    } catch (_) {
      return;
    }
    destroyCharts();

    var labels = (data.labels || []).map(hourLabel);

    var passCanvas = document.getElementById("chart-passrate");
    if (passCanvas && data.pass_rate_series && data.pass_rate_series.length) {
      var palette = [theme.blue, theme.amber, theme.green, theme.red];
      var passOpts = baseOptions();
      passOpts.scales.y.min = 0;
      passOpts.scales.y.max = 100;
      passOpts.scales.y.ticks.callback = function (v) { return v + "%"; };
      passOpts.plugins.legend.display = data.pass_rate_series.length > 1;
      charts.push(new Chart(passCanvas, {
        type: "line",
        data: {
          labels: labels,
          datasets: data.pass_rate_series.map(function (series, i) {
            return lineDataset(series.label, series.data, palette[i % palette.length], false);
          })
        },
        options: passOpts
      }));
    }

    var durCanvas = document.getElementById("chart-duration");
    if (durCanvas && data.p50) {
      var durOpts = baseOptions();
      durOpts.scales.y.beginAtZero = true;
      durOpts.scales.y.ticks.callback = function (v) { return durationLabel(v); };
      durOpts.plugins.tooltip.callbacks = {
        label: function (ctx) { return ctx.dataset.label + ": " + durationLabel(ctx.parsed.y); }
      };
      charts.push(new Chart(durCanvas, {
        type: "line",
        data: {
          labels: labels,
          datasets: [
            lineDataset("p50", data.p50, theme.blue, false),
            lineDataset("p95", data.p95, theme.amber, true)
          ]
        },
        options: durOpts
      }));
    }

    var failCanvas = document.getElementById("chart-failures");
    if (failCanvas && data.failures) {
      var failOpts = baseOptions();
      failOpts.plugins.legend.display = false;
      failOpts.scales.y.beginAtZero = true;
      failOpts.scales.y.ticks.precision = 0;
      charts.push(new Chart(failCanvas, {
        type: "bar",
        data: {
          labels: labels,
          datasets: [{
            label: "failures",
            data: data.failures,
            backgroundColor: "rgba(248,81,73,0.6)",
            borderRadius: 2
          }]
        },
        options: failOpts
      }));
    }

    var workerDurCanvas = document.getElementById("chart-worker-durations");
    if (workerDurCanvas && data.worker_durations) {
      var wdOpts = baseOptions();
      wdOpts.plugins.legend.display = false;
      wdOpts.scales.y.beginAtZero = true;
      wdOpts.scales.y.ticks.callback = function (v) { return durationLabel(v); };
      wdOpts.plugins.tooltip.callbacks = {
        label: function (ctx) { return durationLabel(ctx.parsed.y); }
      };
      charts.push(new Chart(workerDurCanvas, {
        type: "bar",
        data: {
          labels: (data.worker_durations.labels || []).map(hourLabel),
          datasets: [{
            label: "duration",
            data: data.worker_durations.values,
            backgroundColor: (data.worker_durations.passed || []).map(function (ok) {
              return ok ? "rgba(63,185,80,0.55)" : "rgba(248,81,73,0.75)";
            }),
            borderRadius: 2
          }]
        },
        options: wdOpts
      }));
    }
  }

  function updateRefreshIndicator() {
    var el = document.getElementById("refresh-count");
    var dot = document.getElementById("refresh-dot");
    if (dot) {
      dot.classList.toggle("paused", paused);
      dot.classList.toggle("busy", refreshing);
    }
    if (!el) return;
    if (paused) { el.textContent = "paused"; return; }
    if (refreshing) { el.textContent = "updating"; return; }
    el.textContent = countdown + "s";
  }

  async function refreshHome() {
    if (paused || refreshing) return;
    refreshing = true;
    updateRefreshIndicator();
    try {
      var resp = await fetch(window.location.origin + "/ui/partials/home" + window.location.search, {
        credentials: "same-origin",
        headers: { "X-Requested-With": "fetch" }
      });
      if (!resp.ok) return;
      var root = document.getElementById("home-live-root");
      if (!root) return;
      var activeTab = root.querySelector(".tab.active");
      var activeTabName = activeTab ? activeTab.dataset.tab : null;
      root.innerHTML = await resp.text();
      if (activeTabName) selectTab(activeTabName);
      initCharts();
    } finally {
      refreshing = false;
      countdown = REFRESH_INTERVAL;
      updateRefreshIndicator();
    }
  }

  function startCountdown() {
    updateRefreshIndicator();
    setInterval(async function () {
      if (paused || refreshing) return;
      if (countdown > 0) {
        countdown--;
        updateRefreshIndicator();
        return;
      }
      await refreshHome();
    }, 1000);
  }

  function toggleRefresh() {
    paused = !paused;
    if (!paused && countdown <= 0) countdown = REFRESH_INTERVAL;
    updateRefreshIndicator();
  }

  function selectTab(name) {
    document.querySelectorAll(".tab[data-tab]").forEach(function (tab) {
      tab.classList.toggle("active", tab.dataset.tab === name);
    });
    document.querySelectorAll("[data-tab-panel]").forEach(function (panel) {
      panel.classList.toggle("active", panel.dataset.tabPanel === name);
    });
  }

  function bindGlobalInteractions() {
    document.addEventListener("click", function (e) {
      var tab = e.target.closest(".tab[data-tab]");
      if (tab) {
        selectTab(tab.dataset.tab);
        return;
      }
      if (e.target.closest('[data-action="toggle-refresh"]')) {
        toggleRefresh();
        return;
      }
      var copyBtn = e.target.closest('[data-action="copy-prompt"]');
      if (copyBtn) {
        copyPrompt(copyBtn);
        return;
      }
      var modeBtn = e.target.closest(".mode-btn");
      if (modeBtn) {
        loadPromptMode(modeBtn);
        return;
      }
      if (e.target.closest("a,button,summary,textarea,details")) return;
      var row = e.target.closest(".clickable-row");
      if (!row) return;
      if (row.dataset.href) window.location = row.dataset.href;
    });

    document.addEventListener("click", async function (e) {
      var button = e.target.closest("[data-copy],[data-copy-url]");
      if (!button) return;
      e.preventDefault();
      if (!navigator.clipboard) return;
      var original = button.textContent;
      try {
        var text = button.getAttribute("data-copy");
        var copyURL = button.getAttribute("data-copy-url");
        if (!text && copyURL) {
          var resp = await fetch(copyURL, { credentials: "same-origin" });
          if (!resp.ok) return;
          text = await resp.text();
        }
        if (!text) return;
        await navigator.clipboard.writeText(text);
        button.textContent = "Copied";
        setTimeout(function () { button.textContent = original; }, 1200);
      } catch (_) {}
    });
  }

  async function copyPrompt(btn) {
    var box = document.getElementById("prompt-box");
    if (!box) return;
    try {
      await navigator.clipboard.writeText(box.value);
    } catch (_) {
      box.focus();
      box.select();
      document.execCommand("copy");
    }
    btn.textContent = "Copied";
    btn.classList.add("copied");
    setTimeout(function () {
      btn.textContent = "Copy AI Prompt";
      btn.classList.remove("copied");
    }, 1800);
  }

  async function loadPromptMode(btn) {
    var box = document.getElementById("prompt-box");
    var endpointBase = document.body.dataset.promptEndpoint;
    if (!box || !endpointBase) return;
    var resp = await fetch(endpointBase + "?mode=" + encodeURIComponent(btn.dataset.mode), { credentials: "same-origin" });
    if (!resp.ok) return;
    box.value = await resp.text();
    var label = document.getElementById("prompt-mode-label");
    var summary = document.getElementById("prompt-mode-summary");
    if (label) label.textContent = btn.dataset.label;
    if (summary) summary.textContent = btn.dataset.summary;
    document.querySelectorAll(".mode-btn").forEach(function (el) { el.classList.remove("active"); });
    btn.classList.add("active");
  }

  document.addEventListener("DOMContentLoaded", function () {
    bindGlobalInteractions();
    initCharts();
    if (document.getElementById("home-live-root")) startCountdown();
  });
})();
