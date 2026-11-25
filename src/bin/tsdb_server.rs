use std::collections::HashMap;
use std::env;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use lsm_engine::engine::EngineOptions;
use lsm_engine::tsdb::{Aggregation, Tsdb};
use lsm_engine::wal::FsyncPolicy;

/// Material 3 Expressive + Motion-Physics UI on a strict 60-30-10 Palette:
/// - 60% Background: Petrol Blue (#002D37 / #001D24)
/// - 30% Secondary:  Ivory (#FEF0DD)
/// - 10% Accent:     Saffron Gold (#F7A724)
const DASHBOARD_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>LSM-TSDB — Expressive Market Cockpit</title>
<style>
  :root {
    /* 60% Background — Petrol Blue */
    --petrol: #002D37;
    --petrol-deep: #001D24;
    --petrol-surface: #003744;
    --petrol-line: rgba(254, 240, 221, 0.16);
    /* 30% Secondary — Ivory */
    --ivory: #FEF0DD;
    --ivory-dim: #F5E2C8;
    --ivory-muted: rgba(254, 240, 221, 0.76);
    --ink: #002D37;
    --ink-soft: #2A5863;
    /* 10% Accent — Saffron Gold */
    --saffron: #F7A724;
    --saffron-bright: #FFC154;
    --saffron-glow: rgba(247, 167, 36, 0.42);
    /* Semantic */
    --up: #10B981;
    --down: #F87171;
    /* M3 Expressive Motion Curves */
    --spring-spatial: cubic-bezier(0.34, 1.56, 0.64, 1);
    --spring-fast: cubic-bezier(0.22, 1.35, 0.36, 1);
    --spring-standard: cubic-bezier(0.2, 0, 0, 1);
    /* M3 Expressive Typography */
    --display: "Avenir Next", "SF Pro Display", "Segoe UI", system-ui, -apple-system, sans-serif;
    --body: "SF Pro Text", "Avenir Next", system-ui, -apple-system, sans-serif;
    --mono: "SF Mono", "JetBrains Mono", ui-monospace, Menlo, Monaco, monospace;
  }
  * { box-sizing: border-box; }
  html, body {
    margin: 0;
    min-height: 100%;
    background: var(--petrol);
    color: var(--ivory);
    font-family: var(--body);
    -webkit-font-smoothing: antialiased;
    overflow-x: hidden;
  }
  #fractalBg {
    position: fixed;
    inset: 0;
    width: 100vw;
    height: 100vh;
    z-index: 0;
    pointer-events: none;
    opacity: 0.68;
    transition: opacity 420ms var(--spring-standard);
  }
  #fractalBg.dimmed { opacity: 0.10; }
  .petrol-aura {
    position: fixed;
    inset: 0;
    z-index: 0;
    pointer-events: none;
    background:
      radial-gradient(780px circle at 8% 4%, rgba(0, 86, 104, 0.56), transparent 60%),
      radial-gradient(620px circle at 92% 12%, rgba(247, 167, 36, 0.13), transparent 55%),
      radial-gradient(720px circle at 50% 100%, rgba(0, 22, 28, 0.88), transparent 68%);
  }
  .shell {
    position: relative;
    z-index: 1;
    max-width: 1420px;
    margin: 0 auto;
    padding: 24px 28px 56px;
  }

  /* M3 Expressive Top App Bar */
  .topbar {
    display: flex;
    flex-wrap: wrap;
    justify-content: space-between;
    align-items: center;
    gap: 16px;
    padding-bottom: 20px;
    border-bottom: 1px solid var(--petrol-line);
    margin-bottom: 24px;
    animation: m3Enter 640ms var(--spring-spatial) both;
  }
  .brand { display: flex; align-items: center; gap: 16px; }
  .brand-mark {
    width: 52px;
    height: 52px;
    display: grid;
    place-items: center;
    background: var(--petrol-deep);
    border: 2px solid var(--saffron);
    border-radius: 18px;
    box-shadow: 0 8px 24px rgba(247, 167, 36, 0.24);
    transition: transform 380ms var(--spring-spatial), border-radius 320ms var(--spring-spatial);
  }
  .brand-mark:hover {
    transform: rotate(-8deg) scale(1.08);
    border-radius: 26px;
  }
  .brand-title {
    margin: 0;
    font-family: var(--display);
    font-size: clamp(24px, 2.7vw, 34px);
    font-weight: 800;
    letter-spacing: -0.03em;
    line-height: 1.02;
    color: var(--ivory);
  }
  .brand-title span { color: var(--saffron); }
  .brand-sub {
    font-size: 11.5px;
    font-weight: 700;
    letter-spacing: 0.10em;
    text-transform: uppercase;
    color: var(--ivory-muted);
    margin-top: 5px;
  }

  /* Expressive Status Chip */
  .status-pill {
    display: inline-flex;
    align-items: center;
    gap: 9px;
    padding: 9px 16px;
    border-radius: 9999px;
    border: 1.5px solid rgba(254, 240, 221, 0.24);
    background: var(--petrol-deep);
    font-family: var(--mono);
    font-size: 11.5px;
    font-weight: 700;
    color: var(--ivory);
  }
  .dot {
    width: 9px;
    height: 9px;
    border-radius: 9999px;
    background: var(--ivory-muted);
    position: relative;
  }
  .dot.live {
    background: var(--saffron);
    box-shadow: 0 0 12px var(--saffron);
  }
  .dot.live::after {
    content: "";
    position: absolute;
    inset: -5px;
    border-radius: 9999px;
    border: 2px solid var(--saffron);
    animation: pulseRing 1.8s cubic-bezier(0.2, 0.8, 0.2, 1) infinite;
  }
  .dot.err { background: var(--down); box-shadow: 0 0 10px rgba(248, 113, 113, 0.75); }

  /* M3 Expressive Shape-Morphing Buttons + Ripple */
  .actions { display: flex; flex-wrap: wrap; align-items: center; gap: 10px; }
  button.m3-btn {
    position: relative;
    overflow: hidden;
    font-family: var(--display);
    font-size: 13.5px;
    font-weight: 700;
    letter-spacing: 0.01em;
    cursor: pointer;
    height: 44px;
    padding: 0 22px;
    border-radius: 9999px;
    border: none;
    transition:
      transform 320ms var(--spring-spatial),
      border-radius 240ms var(--spring-fast),
      background 200ms ease,
      box-shadow 260ms ease;
    user-select: none;
  }
  button.m3-btn:hover { transform: translateY(-2px) scale(1.025); }
  button.m3-btn:active {
    transform: scale(0.94);
    border-radius: 14px; /* Signature M3 Expressive press shape-morph */
  }
  .btn-accent {
    background: var(--saffron);
    color: var(--petrol-deep);
    box-shadow: 0 6px 20px rgba(247, 167, 36, 0.34);
  }
  .btn-accent:hover { background: var(--saffron-bright); box-shadow: 0 10px 26px rgba(247, 167, 36, 0.48); }
  .btn-ivory {
    background: var(--ivory);
    color: var(--petrol-deep);
    box-shadow: 0 6px 18px rgba(0, 17, 22, 0.28);
  }
  .btn-ivory:hover { background: #FFFFFF; }
  .btn-tonal {
    background: rgba(0, 29, 36, 0.82);
    color: var(--ivory);
    border: 1.5px solid rgba(254, 240, 221, 0.28) !important;
  }
  .btn-tonal:hover { border-color: var(--saffron) !important; color: var(--saffron); }
  .btn-dark {
    background: var(--petrol);
    color: var(--ivory);
  }
  .btn-dark:hover { background: var(--petrol-surface); }

  .m3-ripple {
    position: absolute;
    border-radius: 50%;
    transform: scale(0);
    background: rgba(255, 255, 255, 0.42);
    pointer-events: none;
    animation: rippleWave 560ms cubic-bezier(0.2, 0, 0, 1) forwards;
  }

  /* 30% Ivory Secondary KPI Cards — M3 Expressive Large-Radius Sculptural Cards */
  .kpi-grid {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 16px;
    margin-bottom: 24px;
  }
  .kpi-card {
    position: relative;
    background: var(--ivory);
    color: var(--ink);
    padding: 20px 22px;
    border-radius: 26px;
    border: 1.5px solid rgba(254, 240, 221, 0.95);
    box-shadow: 0 12px 30px rgba(0, 15, 20, 0.34);
    transition:
      transform 420ms var(--spring-spatial),
      border-radius 320ms var(--spring-fast),
      box-shadow 300ms ease;
    animation: m3Enter 680ms var(--spring-spatial) both;
  }
  .kpi-card:nth-child(1) { animation-delay: 40ms; }
  .kpi-card:nth-child(2) { animation-delay: 95ms; }
  .kpi-card:nth-child(3) { animation-delay: 150ms; }
  .kpi-card:nth-child(4) { animation-delay: 205ms; }
  .kpi-card:hover {
    transform: translateY(-5px) scale(1.015);
    border-radius: 30px;
    box-shadow: 0 18px 38px rgba(0, 15, 20, 0.44);
  }
  .kpi-card.featured {
    background: linear-gradient(155deg, #FEF0DD 0%, #FBE6C7 100%);
    box-shadow: 0 14px 36px rgba(0, 15, 20, 0.42), inset 0 5px 0 0 var(--saffron);
  }
  .overline {
    font-family: var(--display);
    font-size: 11px;
    font-weight: 800;
    letter-spacing: 0.13em;
    text-transform: uppercase;
    margin: 0 0 8px 0;
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .overline::before {
    content: "";
    width: 8px;
    height: 8px;
    border-radius: 3px;
    transform: rotate(45deg);
    background: var(--saffron);
    display: inline-block;
  }
  .kpi-card .overline, .panel-ivory .overline { color: var(--ink-soft); }
  .panel-petrol .overline { color: var(--ivory-muted); }
  .kpi-val {
    font-family: var(--display);
    font-size: clamp(28px, 3vw, 42px);
    font-weight: 800;
    letter-spacing: -0.035em;
    line-height: 1.04;
    margin-top: 6px;
    color: var(--ink);
    font-variant-numeric: tabular-nums;
    transform-origin: left center;
    will-change: transform;
  }
  .kpi-meta {
    margin-top: 9px;
    font-size: 12px;
    font-weight: 600;
    color: var(--ink-soft);
    font-family: var(--mono);
  }

  /* Main Layout */
  .main-grid { display: grid; grid-template-columns: 2.25fr 1fr; gap: 18px; align-items: start; }
  .panel-petrol {
    background: rgba(0, 29, 36, 0.88);
    backdrop-filter: blur(18px);
    -webkit-backdrop-filter: blur(18px);
    border: 1.5px solid rgba(254, 240, 221, 0.22);
    border-radius: 28px;
    padding: 22px 24px;
    box-shadow: 0 18px 42px rgba(0, 12, 16, 0.48);
    animation: m3Enter 740ms var(--spring-spatial) 220ms both;
  }
  .panel-ivory {
    background: var(--ivory);
    color: var(--ink);
    border: 1.5px solid rgba(254, 240, 221, 0.94);
    border-radius: 26px;
    padding: 22px 24px;
    box-shadow: 0 14px 34px rgba(0, 12, 16, 0.36);
    transition: transform 380ms var(--spring-spatial), box-shadow 280ms ease;
    animation: m3Enter 740ms var(--spring-spatial) 280ms both;
  }
  .panel-ivory:hover {
    transform: translateY(-3px);
    box-shadow: 0 18px 40px rgba(0, 12, 16, 0.44);
  }
  .panel-head {
    display: flex;
    flex-wrap: wrap;
    justify-content: space-between;
    align-items: flex-start;
    gap: 12px;
    margin-bottom: 16px;
  }

  /* M3 Expressive Spring-Sliding Segmented Pill Bar */
  .metric-pills {
    position: relative;
    display: inline-flex;
    flex-wrap: wrap;
    gap: 6px;
    padding: 5px;
    background: var(--petrol-deep);
    border: 1.5px solid rgba(254, 240, 221, 0.20);
    border-radius: 9999px;
  }
  .pill-indicator {
    position: absolute;
    top: 5px;
    left: 5px;
    height: calc(100% - 10px);
    background: var(--saffron);
    border-radius: 9999px;
    box-shadow: 0 4px 18px var(--saffron-glow);
    pointer-events: none;
    z-index: 0;
    will-change: transform, width;
  }
  .pill {
    position: relative;
    z-index: 1;
    height: 36px;
    padding: 0 15px;
    border-radius: 9999px;
    border: none;
    background: transparent;
    color: var(--ivory);
    font-family: var(--mono);
    font-size: 12px;
    font-weight: 700;
    cursor: pointer;
    display: inline-flex;
    align-items: center;
    gap: 7px;
    transition: color 200ms ease, transform 240ms var(--spring-fast);
  }
  .pill:active { transform: scale(0.94); }
  .pill .pill-price {
    font-weight: 600;
    color: var(--ivory-muted);
    font-size: 11.5px;
    transition: color 200ms ease;
  }
  .pill:hover:not(.active) { color: var(--saffron-bright); }
  .pill.active { color: var(--petrol-deep); }
  .pill.active .pill-price { color: var(--petrol-deep); opacity: 0.86; }

  .controls-bar { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
  .panel-petrol select {
    height: 40px;
    background: var(--petrol);
    color: var(--ivory);
    border: 1.5px solid rgba(254, 240, 221, 0.28);
    border-radius: 14px;
    padding: 0 12px;
    font-family: var(--mono);
    font-size: 12.5px;
    font-weight: 600;
    cursor: pointer;
    transition: border-color 180ms ease, transform 240ms var(--spring-fast);
  }
  .panel-petrol select:hover { border-color: var(--saffron); }
  .panel-petrol select:focus { outline: none; border-color: var(--saffron); transform: scale(1.02); }

  .chart-wrap {
    position: relative;
    background: var(--petrol-deep);
    border: 1.5px solid rgba(254, 240, 221, 0.16);
    border-radius: 20px;
    padding: 12px;
  }
  canvas#chart { width: 100%; height: 356px; display: block; cursor: crosshair; }
  .chart-footer {
    display: flex;
    justify-content: space-between;
    align-items: center;
    flex-wrap: wrap;
    gap: 12px;
    margin-top: 14px;
    padding-top: 12px;
    border-top: 1px solid var(--petrol-line);
    font-family: var(--mono);
    font-size: 12.5px;
    color: var(--ivory-muted);
  }
  .chart-footer strong { color: var(--ivory); }
  .chart-footer .hl { color: var(--saffron); font-weight: 700; }

  /* Right Sidebar Controls */
  .form-row { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 14px; }
  label.field {
    display: flex;
    flex-direction: column;
    gap: 6px;
    font-family: var(--display);
    font-size: 11px;
    font-weight: 800;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: var(--ink-soft);
  }
  .panel-ivory input {
    height: 42px;
    background: #FFFFFF;
    color: var(--ink);
    border: 1.5px solid rgba(0, 45, 55, 0.22);
    border-radius: 14px;
    padding: 0 13px;
    font-family: var(--mono);
    font-size: 13px;
    font-weight: 700;
    transition: border-color 180ms ease, box-shadow 220ms ease, transform 240ms var(--spring-fast);
  }
  .panel-ivory input:focus {
    outline: none;
    border-color: var(--saffron);
    box-shadow: 0 0 0 4px rgba(247, 167, 36, 0.28);
    transform: translateY(-1px);
  }

  .console {
    margin-top: 14px;
    padding: 12px 14px;
    background: var(--petrol-deep);
    border-left: 4px solid var(--saffron);
    border-radius: 14px;
    font-family: var(--mono);
    font-size: 11.5px;
    color: var(--ivory);
    min-height: 46px;
    word-break: break-word;
  }
  .levels-list { display: flex; flex-direction: column; gap: 10px; margin-top: 8px; }
  .lvl-row {
    display: grid;
    grid-template-columns: 32px 1fr 34px;
    align-items: center;
    gap: 10px;
    font-family: var(--mono);
    font-size: 12.5px;
    font-weight: 700;
    color: var(--ink);
  }
  .lvl-track {
    height: 11px;
    background: rgba(0, 45, 55, 0.14);
    border-radius: 9999px;
    overflow: hidden;
  }
  .lvl-fill {
    height: 100%;
    background: linear-gradient(90deg, var(--petrol) 0%, var(--saffron) 100%);
    width: 0%;
    border-radius: 9999px;
    transition: width 560ms var(--spring-spatial);
  }

  @keyframes m3Enter {
    0% { opacity: 0; transform: translateY(22px) scale(0.96); }
    100% { opacity: 1; transform: translateY(0) scale(1); }
  }
  @keyframes pulseRing {
    0% { transform: scale(0.8); opacity: 0.9; }
    100% { transform: scale(2.2); opacity: 0; }
  }
  @keyframes rippleWave {
    to { transform: scale(3.8); opacity: 0; }
  }
  @media (prefers-reduced-motion: reduce) {
    *, *::before, *::after { animation-duration: 0.01ms !important; transition-duration: 0.01ms !important; }
  }
  @media (max-width: 1060px) { .kpi-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } .main-grid { grid-template-columns: 1fr; } }
  @media (max-width: 600px) { .kpi-grid { grid-template-columns: 1fr; } }
</style>
</head>
<body>
<canvas id="fractalBg"></canvas>
<div class="petrol-aura"></div>
<div class="shell">
  <header class="topbar">
    <div class="brand">
      <div class="brand-mark" aria-hidden="true">
        <svg viewBox="0 0 40 40" width="28" height="28" fill="none">
          <polygon points="20,4 36,33 4,33" stroke="#F7A724" stroke-width="1.9" stroke-linejoin="round"/>
          <polygon points="20,18.5 28,33 12,33" stroke="#FEF0DD" stroke-width="1.35" stroke-linejoin="round"/>
          <polygon points="20,4 28,18.5 12,18.5" stroke="#FEF0DD" stroke-width="1.35" stroke-linejoin="round"/>
          <polygon points="20,18.5 24,25.8 16,25.8" fill="#F7A724"/>
        </svg>
      </div>
      <div>
        <h1 class="brand-title">LSM-TSDB <span>Market Cockpit</span></h1>
        <div class="brand-sub">Live Crypto Tick Engine &bull; Rust LSM-Tree &bull; WAL + 4KB SSTables + MVCC</div>
      </div>
    </div>
    <div class="actions">
      <div class="status-pill"><span class="dot live" id="liveDot"></span><span id="liveLabel">AUTO-SYNC 1s</span></div>
      <button class="m3-btn btn-tonal" id="fractalBtn" onclick="toggleFractal()">Fractal: ON</button>
      <button class="m3-btn btn-accent" id="toggleAuto" onclick="toggleAuto()">Pause Feed</button>
      <button class="m3-btn btn-ivory" onclick="triggerCompact()">Flush &amp; Compact</button>
      <button class="m3-btn btn-tonal" onclick="triggerRetention()">Prune Old (&gt;5m)</button>
    </div>
  </header>

  <div class="kpi-grid">
    <div class="kpi-card featured">
      <div class="overline">Live Spot Price</div>
      <div class="kpi-val" id="sSpot">—</div>
      <div class="kpi-meta" id="sSpotMeta">btc.usd &bull; waiting for ticks</div>
    </div>
    <div class="kpi-card">
      <div class="overline">Write Amplification (WAF)</div>
      <div class="kpi-val" id="sWaf">0.00x</div>
      <div class="kpi-meta" id="sLogical">0 B logical ingested</div>
    </div>
    <div class="kpi-card">
      <div class="overline">WAL + SSTable Footprint</div>
      <div class="kpi-val" id="sDisk">0 B</div>
      <div class="kpi-meta" id="sDiskBreak">WAL 0 B &bull; SST 0 B</div>
    </div>
    <div class="kpi-card">
      <div class="overline">Points in Window</div>
      <div class="kpi-val" id="sPts">0</div>
      <div class="kpi-meta" id="sCmp">0 B compacted</div>
    </div>
  </div>

  <div class="main-grid">
    <section class="panel-petrol">
      <div class="panel-head">
        <div>
          <div class="overline" style="margin-bottom:8px;">Live Market Tick Stream</div>
          <div class="metric-pills" id="metricPills">
            <div class="pill-indicator" id="pillIndicator"></div>
            <button class="pill active" data-metric="btc.usd" onclick="selectMetric('btc.usd', this)">BTC <span class="pill-price" id="p_btc.usd">—</span></button>
            <button class="pill" data-metric="eth.usd" onclick="selectMetric('eth.usd', this)">ETH <span class="pill-price" id="p_eth.usd">—</span></button>
            <button class="pill" data-metric="sol.usd" onclick="selectMetric('sol.usd', this)">SOL <span class="pill-price" id="p_sol.usd">—</span></button>
            <button class="pill" data-metric="btc.spread_bps" onclick="selectMetric('btc.spread_bps', this)">SPREAD <span class="pill-price" id="p_btc.spread_bps">—</span></button>
          </div>
        </div>
        <div class="controls-bar">
          <select id="win" onchange="refreshAll(true)" style="width:118px;">
            <option value="60000" selected>Last 1m</option>
            <option value="300000">Last 5m</option>
            <option value="900000">Last 15m</option>
            <option value="3600000">Last 1h</option>
            <option value="0">All Time</option>
          </select>
          <select id="bucket" onchange="refreshAll(true)" style="width:128px;">
            <option value="0">Raw Points</option>
            <option value="2000">2s Buckets</option>
            <option value="5000">5s Buckets</option>
            <option value="15000">15s Buckets</option>
          </select>
          <select id="agg" onchange="refreshAll(true)" style="width:88px;">
            <option value="avg">AVG</option>
            <option value="max">MAX</option>
            <option value="min">MIN</option>
            <option value="sum">SUM</option>
            <option value="count">COUNT</option>
          </select>
        </div>
      </div>
      <div class="chart-wrap"><canvas id="chart"></canvas></div>
      <div class="chart-footer">
        <div>LOW <strong id="fMin">—</strong> &nbsp;|&nbsp; AVG <strong id="fAvg">—</strong> &nbsp;|&nbsp; HIGH <strong id="fMax">—</strong> &nbsp;|&nbsp; &Delta; <strong id="fDelta">—</strong></div>
        <div class="hl" id="hoverInfo">Hover curve to inspect exact tick</div>
      </div>
    </section>

    <aside style="display:flex; flex-direction:column; gap:16px;">
      <section class="panel-ivory">
        <div class="overline">Manual Tick Ingest</div>
        <div class="form-row">
          <label class="field">Metric Name<input id="metric" value="btc.usd"></label>
          <label class="field">Float64 Value<input id="val" type="number" step="0.01" value="60000.00"></label>
        </div>
        <div style="display:flex; gap:8px;">
          <button class="m3-btn btn-accent" style="flex:1;" onclick="ingestOne()">Ingest Tick</button>
          <button class="m3-btn btn-dark" onclick="refreshAll(true)">Refresh</button>
        </div>
        <div class="console" id="status">$ auto-syncing live ticks every 1s — run ./scripts/run_crypto_cockpit.sh</div>
      </section>

      <section class="panel-ivory">
        <div class="overline">LSM-Tree SSTable Levels (L0 &rarr; L6)</div>
        <div class="levels-list" id="levelsList"></div>
      </section>
    </aside>
  </div>
</div>
<script>
/* =====================================================================
 * 2nd-Order Spring Physics Engine (Damped Harmonic Oscillator)
 * Drives segmented pill morphing, live KPI spot ticker, and chart hover.
 * ===================================================================== */
class Spring {
  constructor(val = 0, stiffness = 260, damping = 22) {
    this.val = val; this.target = val; this.vel = 0;
    this.k = stiffness; this.d = damping;
  }
  set(t, snap = false) {
    this.target = t;
    if (snap) { this.val = t; this.vel = 0; }
  }
  impulse(v) { this.vel += v; }
  step(dt) {
    const f = -this.k * (this.val - this.target) - this.d * this.vel;
    this.vel += f * dt;
    this.val += this.vel * dt;
    return Math.abs(this.val - this.target) > 0.0005 || Math.abs(this.vel) > 0.0005;
  }
}

/* M3 Expressive Ripple Effect on all m3-btn buttons */
document.addEventListener('pointerdown', e => {
  const btn = e.target.closest('button.m3-btn');
  if (!btn) return;
  const rect = btn.getBoundingClientRect();
  const rip = document.createElement('span');
  const size = Math.max(rect.width, rect.height);
  rip.className = 'm3-ripple';
  rip.style.width = rip.style.height = size + 'px';
  rip.style.left = (e.clientX - rect.left - size / 2) + 'px';
  rip.style.top = (e.clientY - rect.top - size / 2) + 'px';
  btn.appendChild(rip);
  rip.addEventListener('animationend', () => rip.remove());
});

/* Spring-driven Sliding Pill Indicator */
const pillX = new Spring(0, 320, 24);
const pillW = new Spring(80, 320, 24);
function syncPillTarget(snap = false) {
  const active = document.querySelector('#metricPills .pill.active');
  const wrap = document.getElementById('metricPills');
  if (!active || !wrap) return;
  const x = active.offsetLeft;
  const w = active.offsetWidth;
  pillX.set(x, snap);
  pillW.set(w, snap);
}

/* Spring-driven Spot Price & Pop Scale */
const spotSpring = new Spring(0, 170, 20);
const spotScale = new Spring(1, 380, 18);
let spotInitialized = false;
let lastSpotMetric = 'btc.usd';
let lastSpotRaw = null;

/* Spring-driven Chart Hover & Live Beacon */
const hoverSpringX = new Spring(0, 340, 26);
const hoverSpringY = new Spring(0, 340, 26);
const beaconPulse = new Spring(0, 220, 14);
let hoverActive = false;

/* Fractal Canvas + Unified 60fps Motion-Physics Loop */
let fractalOn = true;
const fractalEnergy = new Spring(0, 140, 12);
function toggleFractal() {
  fractalOn = !fractalOn;
  document.getElementById('fractalBg').classList.toggle('dimmed', !fractalOn);
  document.getElementById('fractalBtn').textContent = fractalOn ? 'Fractal: ON' : 'Fractal: DIM';
}

const DEFAULT_VALS = { 'btc.usd': 60000, 'eth.usd': 1700, 'sol.usd': 70, 'btc.spread_bps': 0.15 };
let latestCache = {};
let autoTimer = null, lastPoints = [], hoverIdx = -1, prevCount = 0;
let reqSeq = 0, isFetching = false;

(function initMotionLoop() {
  const canvas = document.getElementById('fractalBg');
  const ctx = canvas.getContext('2d');
  let W = 0, H = 0, angle = 0, lastT = performance.now();

  function resize() {
    const dpr = Math.min(window.devicePixelRatio || 1, 2);
    W = window.innerWidth; H = window.innerHeight;
    canvas.width = Math.floor(W * dpr); canvas.height = Math.floor(H * dpr);
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    syncPillTarget(true);
  }
  resize();
  window.addEventListener('resize', resize);

  function drawSierpinski(ax, ay, bx, by, cx, cy, depth) {
    if (depth === 0) { ctx.moveTo(ax, ay); ctx.lineTo(bx, by); ctx.lineTo(cx, cy); ctx.closePath(); return; }
    const abx = (ax + bx) * 0.5, aby = (ay + by) * 0.5;
    const bcx = (bx + cx) * 0.5, bcy = (by + cy) * 0.5;
    const cax = (cx + ax) * 0.5, cay = (cy + ay) * 0.5;
    drawSierpinski(ax, ay, abx, aby, cax, cay, depth - 1);
    drawSierpinski(abx, aby, bx, by, bcx, bcy, depth - 1);
    drawSierpinski(cax, cay, bcx, bcy, cx, cy, depth - 1);
  }
  function drawFractalBranch(x, y, len, theta, depth, spread) {
    if (depth === 0 || len < 6) return;
    const x2 = x + Math.cos(theta) * len, y2 = y + Math.sin(theta) * len;
    ctx.moveTo(x, y); ctx.lineTo(x2, y2);
    drawFractalBranch(x2, y2, len * 0.67, theta - spread, depth - 1, spread);
    drawFractalBranch(x2, y2, len * 0.67, theta + spread, depth - 1, spread);
  }

  function tick(now) {
    const dt = Math.min(0.04, (now - lastT) / 1000);
    lastT = now;

    // 1. Step segmented pill spring
    pillX.step(dt);
    pillW.step(dt);
    const ind = document.getElementById('pillIndicator');
    if (ind) {
      ind.style.transform = `translate3d(${pillX.val - 5}px, 0, 0)`;
      ind.style.width = `${Math.max(40, pillW.val)}px`;
    }

    // 2. Step spot price & scale-pop spring
    const spotMoving = spotSpring.step(dt);
    const scaleMoving = spotScale.step(dt);
    const spotEl = document.getElementById('sSpot');
    if (spotEl && spotInitialized && (spotMoving || scaleMoving)) {
      spotEl.textContent = fmtVal(lastSpotMetric, spotSpring.val);
      spotEl.style.transform = `scale(${spotScale.val.toFixed(4)})`;
    }

    // 3. Step chart hover & beacon springs
    const hxMoving = hoverSpringX.step(dt);
    const hyMoving = hoverSpringY.step(dt);
    const bpMoving = beaconPulse.step(dt);
    if (lastPoints.length && (hxMoving || hyMoving || bpMoving)) {
      drawChart(lastPoints, lastSpotMetric);
    }

    // 4. Step fractal energy spring & draw background
    fractalEnergy.step(dt);
    const pulse = Math.max(0, fractalEnergy.val);
    angle += 0.0014 + pulse * 0.004;
    ctx.clearRect(0, 0, W, H);
    const r = Math.min(W, H) * (0.42 + pulse * 0.04);
    const cx = W * 0.78, cy = H * 0.36;
    const pts = [0, 1, 2].map(i => { const a = angle + (i * Math.PI * 2) / 3; return [cx + Math.cos(a) * r, cy + Math.sin(a) * r]; });
    ctx.beginPath(); drawSierpinski(pts[0][0], pts[0][1], pts[1][0], pts[1][1], pts[2][0], pts[2][1], 4);
    ctx.strokeStyle = `rgba(254, 240, 221, ${0.075 + pulse * 0.11})`; ctx.lineWidth = 0.85; ctx.stroke();

    const r2 = r * 0.54;
    const pts2 = [0, 1, 2].map(i => { const a = -angle * 1.35 + (i * Math.PI * 2) / 3; return [cx + Math.cos(a) * r2, cy + Math.sin(a) * r2]; });
    ctx.beginPath(); drawSierpinski(pts2[0][0], pts2[0][1], pts2[1][0], pts2[1][1], pts2[2][0], pts2[2][1], 3);
    ctx.strokeStyle = `rgba(247, 167, 36, ${0.11 + pulse * 0.20})`; ctx.lineWidth = 0.95; ctx.stroke();

    const spread = 0.44 + Math.sin(angle * 2.8) * 0.05;
    ctx.beginPath(); drawFractalBranch(W * 0.10, H * 1.02, Math.min(W, H) * 0.15, -Math.PI / 2 + 0.14, 6, spread);
    ctx.strokeStyle = `rgba(254, 240, 221, ${0.09 + pulse * 0.08})`; ctx.lineWidth = 0.9; ctx.stroke();

    requestAnimationFrame(tick);
  }
  requestAnimationFrame(tick);
})();

function fmtBytes(b) {
  if (b < 1024) return b + ' B';
  if (b < 1048576) return (b / 1024).toFixed(1) + ' KB';
  if (b < 1073741824) return (b / 1048576).toFixed(2) + ' MB';
  return (b / 1073741824).toFixed(2) + ' GB';
}
function fmtVal(m, v) {
  if (m.endsWith('.usd')) {
    const dec = Math.abs(v) >= 1000 ? 2 : 3;
    return '$' + v.toLocaleString(undefined, { minimumFractionDigits: dec, maximumFractionDigits: dec });
  }
  if (m.endsWith('_bps')) return v.toFixed(3) + ' bps';
  return v.toFixed(3);
}
function fmtAxis(m, v, span) {
  if (m.endsWith('.usd')) {
    const dec = span < 1 ? 3 : (span < 20 ? 2 : (span < 200 ? 1 : 0));
    return '$' + v.toLocaleString(undefined, { minimumFractionDigits: dec, maximumFractionDigits: dec });
  }
  const dec = span < 0.05 ? 4 : (span < 1 ? 3 : 2);
  return m.endsWith('_bps') ? v.toFixed(dec) + 'bp' : v.toFixed(dec);
}
function logMsg(msg) { document.getElementById('status').textContent = '$ ' + msg; }

function selectMetric(name, el) {
  document.getElementById('metric').value = name;
  const cached = latestCache[name];
  document.getElementById('val').value = (cached !== undefined ? cached : (DEFAULT_VALS[name] ?? 100)).toFixed(2);
  document.querySelectorAll('.pill').forEach(p => p.classList.remove('active'));
  if (el) el.classList.add('active');
  syncPillTarget(false);
  hoverIdx = -1;
  hoverActive = false;
  prevCount = 0;
  spotInitialized = false;
  lastSpotMetric = name;
  lastSpotRaw = null;
  fractalEnergy.impulse(4.2);
  refreshAll(true);
}

async function ingestOne() {
  const m = document.getElementById('metric').value.trim();
  const v = document.getElementById('val').value;
  if (!m) return;
  fractalEnergy.impulse(5.5);
  try {
    const r = await fetch('/api/ingest', { method: 'POST', body: `${m} ${v} ${Date.now()}\n` });
    logMsg(`POST /api/ingest -> ${await r.text()}`);
    await refreshAll(true);
  } catch (e) { logMsg(`ingest failed: ${e.message}`); }
}
async function triggerCompact() {
  fractalEnergy.impulse(6.5);
  try {
    const r = await fetch('/api/compact', { method: 'POST' });
    logMsg(`POST /api/compact -> ${await r.text()}`);
    await refreshAll(true);
  } catch (e) { logMsg(`compact failed: ${e.message}`); }
}
async function triggerRetention() {
  const m = encodeURIComponent(document.getElementById('metric').value.trim() || 'btc.usd');
  fractalEnergy.impulse(4.5);
  try {
    const r = await fetch(`/api/retention?metric=${m}&older_than=${Date.now() - 300000}`, { method: 'POST' });
    logMsg(`POST /api/retention -> ${await r.text()} (tombstones written)`);
    await refreshAll(true);
  } catch (e) { logMsg(`retention failed: ${e.message}`); }
}
function toggleAuto() {
  const btn = document.getElementById('toggleAuto'), dot = document.getElementById('liveDot'), lbl = document.getElementById('liveLabel');
  if (autoTimer) {
    clearInterval(autoTimer); autoTimer = null;
    btn.textContent = 'Resume Feed'; dot.classList.remove('live', 'err'); lbl.textContent = 'PAUSED';
    logMsg('auto-refresh paused');
    return;
  }
  btn.textContent = 'Pause Feed'; dot.classList.remove('err'); dot.classList.add('live'); lbl.textContent = 'AUTO-SYNC 1s';
  logMsg('auto-syncing live ticks every 1s');
  refreshAll(true);
  autoTimer = setInterval(() => refreshAll(false), 1000);
}
function renderLevels(levels) {
  const maxCnt = Math.max(4, ...levels);
  document.getElementById('levelsList').innerHTML = levels.map((cnt, i) => {
    const pct = Math.min(100, (cnt / maxCnt) * 100);
    return `<div class="lvl-row"><span style="color:var(--ink-soft)">L${i}</span><div class="lvl-track"><div class="lvl-fill" style="width:${pct}%"></div></div><span style="text-align:right; color:${cnt > 0 ? 'var(--petrol-deep)' : 'var(--ink-soft)'}">${cnt}</span></div>`;
  }).join('');
}

async function refreshAll(force = false) {
  if (isFetching && !force) return;
  isFetching = true;
  const mySeq = ++reqSeq;
  const metricRaw = document.getElementById('metric').value.trim() || 'btc.usd';
  lastSpotMetric = metricRaw;
  const m = encodeURIComponent(metricRaw);
  const bucket = parseInt(document.getElementById('bucket').value || '0', 10);
  const agg = document.getElementById('agg').value;
  const winMs = parseInt(document.getElementById('win').value || '60000', 10);
  const now = Date.now(), end = now + 5000;
  const start = winMs > 0 ? Math.max(0, now - winMs) : 0;

  try {
    const [pts, st, latest] = await Promise.all([
      fetch(`/api/query?metric=${m}&start=${start}&end=${end}&bucket=${bucket}&agg=${agg}`).then(r => r.json()),
      fetch('/api/stats').then(r => r.json()),
      fetch('/api/latest?metrics=btc.usd,eth.usd,sol.usd,btc.spread_bps').then(r => r.json())
    ]);
    if (mySeq !== reqSeq) return;

    const dot = document.getElementById('liveDot'), lbl = document.getElementById('liveLabel');
    if (autoTimer) { dot.classList.remove('err'); dot.classList.add('live'); lbl.textContent = 'AUTO-SYNC 1s'; }

    let pillResized = false;
    for (const [k, v] of Object.entries(latest)) {
      if (v !== null && v !== undefined) {
        latestCache[k] = v;
        const el = document.getElementById('p_' + k);
        if (el) {
          const nextTxt = fmtVal(k, v);
          if (el.textContent !== nextTxt) { el.textContent = nextTxt; pillResized = true; }
        }
      }
    }
    if (pillResized) requestAnimationFrame(() => syncPillTarget(false));

    if (pts.length > prevCount) {
      fractalEnergy.impulse(2.8);
      beaconPulse.impulse(14);
    }
    prevCount = pts.length;
    lastPoints = pts;

    const spotVal = pts.length ? pts[pts.length - 1].val : latestCache[metricRaw];
    const spotEl = document.getElementById('sSpot');
    if (spotVal !== undefined) {
      if (!spotInitialized) {
        spotSpring.set(spotVal, true);
        spotEl.textContent = fmtVal(metricRaw, spotVal);
        spotInitialized = true;
      } else if (lastSpotRaw !== spotVal) {
        spotSpring.set(spotVal, false);
        spotScale.impulse(lastSpotRaw !== null && spotVal > lastSpotRaw ? 0.85 : -0.65);
        spotEl.style.color = lastSpotRaw !== null && spotVal > lastSpotRaw ? '#047857' : (lastSpotRaw !== null && spotVal < lastSpotRaw ? '#B91C1C' : 'var(--ink)');
        setTimeout(() => { spotEl.style.color = 'var(--ink)'; }, 420);
      }
      lastSpotRaw = spotVal;
    } else {
      spotEl.textContent = '—';
    }
    document.getElementById('sSpotMeta').textContent = pts.length
      ? `${metricRaw} • last tick ${new Date(pts[pts.length - 1].ts).toLocaleTimeString([], { hour12: false })}`
      : `${metricRaw} • waiting for ticks`;

    document.getElementById('sWaf').textContent = st.waf.toFixed(2) + 'x';
    document.getElementById('sLogical').textContent = fmtBytes(st.logical_bytes) + ' logical ingested';
    const sstBytes = st.flush_bytes + st.compaction_bytes;
    document.getElementById('sDisk').textContent = fmtBytes(st.wal_bytes + sstBytes);
    document.getElementById('sDiskBreak').textContent = `WAL ${fmtBytes(st.wal_bytes)} • SST ${fmtBytes(sstBytes)}`;
    document.getElementById('sPts').textContent = pts.length.toLocaleString();
    document.getElementById('sCmp').textContent = fmtBytes(st.compaction_bytes) + ' compacted';

    if (pts.length) {
      const vals = pts.map(p => p.val);
      const first = vals[0], last = vals[vals.length - 1];
      const pct = first !== 0 ? ((last - first) / first) * 100 : 0;
      const deltaEl = document.getElementById('fDelta');
      deltaEl.textContent = (pct >= 0 ? '+' : '') + pct.toFixed(3) + '%';
      deltaEl.style.color = pct >= 0 ? '#34D399' : '#F87171';
      document.getElementById('fMin').textContent = fmtVal(metricRaw, Math.min(...vals));
      document.getElementById('fMax').textContent = fmtVal(metricRaw, Math.max(...vals));
      document.getElementById('fAvg').textContent = fmtVal(metricRaw, vals.reduce((a, b) => a + b, 0) / vals.length);
    } else {
      document.getElementById('fMin').textContent = '—';
      document.getElementById('fMax').textContent = '—';
      document.getElementById('fAvg').textContent = '—';
      document.getElementById('fDelta').textContent = '—';
      document.getElementById('fDelta').style.color = 'var(--ivory)';
    }
    renderLevels(st.levels);
    drawChart(pts, metricRaw);
  } catch (err) {
    const dot = document.getElementById('liveDot'), lbl = document.getElementById('liveLabel');
    dot.classList.remove('live'); dot.classList.add('err'); lbl.textContent = 'RECONNECTING';
  } finally {
    if (mySeq === reqSeq) isFetching = false;
  }
}

function drawChart(pts, metricRaw = (document.getElementById('metric').value.trim() || 'btc.usd')) {
  const c = document.getElementById('chart');
  const dpr = Math.min(window.devicePixelRatio || 1, 2);
  const rect = c.getBoundingClientRect();
  const W = Math.max(320, rect.width), H = 356;
  c.width = Math.floor(W * dpr); c.height = Math.floor(H * dpr);
  const ctx = c.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, W, H);

  if (!pts.length) {
    ctx.fillStyle = 'rgba(254, 240, 221, 0.74)';
    ctx.font = '600 14px "Avenir Next", sans-serif';
    ctx.fillText('Waiting for live ticks... Run ./scripts/run_crypto_cockpit.sh or widen the time window.', 26, H / 2);
    return;
  }

  const padL = 88, padR = 82, padT = 20, padB = 34;
  const plotW = W - padL - padR, plotH = H - padT - padB;
  const minT = pts[0].ts, maxT = Math.max(pts[pts.length - 1].ts, minT + 1000);
  let minV = Math.min(...pts.map(p => p.val)), maxV = Math.max(...pts.map(p => p.val));
  if (maxV === minV) {
    const pad = Math.max(Math.abs(maxV) * 0.001, 0.01);
    maxV += pad; minV -= pad;
  }
  const rawSpan = maxV - minV;
  minV -= rawSpan * 0.10; maxV += rawSpan * 0.10;
  const spanV = maxV - minV;

  const xOf = ts => padL + ((ts - minT) / (maxT - minT)) * plotW;
  const yOf = val => padT + (1 - (val - minV) / spanV) * plotH;

  // Horizontal grid + Ivory Y-axis labels
  ctx.strokeStyle = 'rgba(254, 240, 221, 0.11)'; ctx.lineWidth = 1;
  ctx.font = '600 11px ui-monospace, monospace';
  for (let i = 0; i <= 4; i++) {
    const y = padT + plotH * (i / 4);
    const val = maxV - spanV * (i / 4);
    ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(W - padR, y); ctx.stroke();
    ctx.fillStyle = 'rgba(254, 240, 221, 0.80)';
    ctx.textAlign = 'right';
    ctx.fillText(fmtAxis(metricRaw, val, spanV), padL - 10, y + 4);
  }
  ctx.textAlign = 'left';

  // X-axis timestamp ticks
  ctx.fillStyle = 'rgba(254, 240, 221, 0.72)'; ctx.font = '500 10.5px ui-monospace, monospace';
  const tickCount = pts.length === 1 ? 0 : 4;
  for (let i = 0; i <= tickCount; i++) {
    const frac = tickCount === 0 ? 0 : i / tickCount;
    const tTick = minT + (maxT - minT) * frac;
    const x = padL + plotW * frac;
    const label = new Date(tTick).toLocaleTimeString([], { hour12: false });
    ctx.textAlign = i === 0 ? 'left' : (i === tickCount ? 'right' : 'center');
    ctx.fillText(label, x, H - 10);
  }
  ctx.textAlign = 'left';

  // Idle-gap segment splitting
  let gapThreshold = 12000;
  if (pts.length > 2) {
    const dts = [];
    for (let i = 1; i < pts.length; i++) dts.push(pts[i].ts - pts[i - 1].ts);
    dts.sort((a, b) => a - b);
    const med = dts[Math.floor(dts.length / 2)] || 1000;
    gapThreshold = Math.max(6000, med * 8);
  }
  const segments = [];
  let seg = [pts[0]];
  for (let i = 1; i < pts.length; i++) {
    if (pts[i].ts - pts[i - 1].ts > gapThreshold) { segments.push(seg); seg = [pts[i]]; }
    else seg.push(pts[i]);
  }
  segments.push(seg);

  const fillGrad = ctx.createLinearGradient(0, padT, 0, H - padB);
  fillGrad.addColorStop(0, 'rgba(247, 167, 36, 0.36)');
  fillGrad.addColorStop(0.6, 'rgba(247, 167, 36, 0.08)');
  fillGrad.addColorStop(1, 'rgba(0, 29, 36, 0.0)');

  const strokeGrad = ctx.createLinearGradient(padL, 0, W - padR, 0);
  strokeGrad.addColorStop(0, '#FEF0DD');
  strokeGrad.addColorStop(0.28, '#F7A724');
  strokeGrad.addColorStop(1, '#FFC154');

  for (const s of segments) {
    const sc = s.map(p => [xOf(p.ts), yOf(p.val)]);
    if (sc.length === 1) {
      ctx.fillStyle = '#F7A724';
      ctx.beginPath(); ctx.arc(sc[0][0], sc[0][1], 4, 0, Math.PI * 2); ctx.fill();
      continue;
    }
    function traceSeg() {
      ctx.beginPath(); ctx.moveTo(sc[0][0], sc[0][1]);
      for (let i = 1; i < sc.length - 1; i++) {
        const mx = (sc[i][0] + sc[i + 1][0]) * 0.5, my = (sc[i][1] + sc[i + 1][1]) * 0.5;
        ctx.quadraticCurveTo(sc[i][0], sc[i][1], mx, my);
      }
      ctx.lineTo(sc[sc.length - 1][0], sc[sc.length - 1][1]);
    }
    traceSeg(); ctx.lineTo(sc[sc.length - 1][0], H - padB); ctx.lineTo(sc[0][0], H - padB); ctx.closePath();
    ctx.fillStyle = fillGrad; ctx.fill();

    ctx.save();
    ctx.shadowColor = 'rgba(247, 167, 36, 0.48)';
    ctx.shadowBlur = 12;
    traceSeg(); ctx.strokeStyle = strokeGrad; ctx.lineWidth = 2.8; ctx.stroke();
    ctx.restore();
  }

  // Live-price dashed guide + spring-pulsing beacon + M3 Expressive pill badge
  const lastPt = pts[pts.length - 1];
  const lx = xOf(lastPt.ts), ly = yOf(lastPt.val);
  ctx.strokeStyle = 'rgba(247, 167, 36, 0.45)'; ctx.setLineDash([4, 4]); ctx.lineWidth = 1;
  ctx.beginPath(); ctx.moveTo(padL, ly); ctx.lineTo(W - padR, ly); ctx.stroke();
  ctx.setLineDash([]);

  const ringR = Math.max(7, 8 + beaconPulse.val * 0.9);
  ctx.fillStyle = 'rgba(247, 167, 36, 0.28)';
  ctx.beginPath(); ctx.arc(lx, ly, ringR, 0, Math.PI * 2); ctx.fill();
  ctx.fillStyle = '#F7A724';
  ctx.beginPath(); ctx.arc(lx, ly, 4.2, 0, Math.PI * 2); ctx.fill();

  const tagText = fmtAxis(metricRaw, lastPt.val, spanV);
  const tagX = W - padR + 7, tagW = padR - 12, tagH = 22;
  const tagY = Math.max(padT + 11, Math.min(H - padB - 11, ly)) - tagH / 2;
  ctx.fillStyle = '#F7A724';
  ctx.beginPath();
  ctx.roundRect(tagX, tagY, tagW, tagH, 11);
  ctx.fill();
  ctx.fillStyle = '#001D24';
  ctx.font = '700 10.5px ui-monospace, monospace';
  ctx.textAlign = 'center';
  ctx.fillText(tagText, tagX + tagW / 2, tagY + 14.5);
  ctx.textAlign = 'left';

  // Spring-interpolated hover crosshair + M3 diamond marker
  if (hoverActive && hoverIdx >= 0 && hoverIdx < pts.length) {
    const hx = hoverSpringX.val, hy = hoverSpringY.val;
    ctx.strokeStyle = 'rgba(254, 240, 221, 0.52)'; ctx.setLineDash([3, 4]);
    ctx.beginPath(); ctx.moveTo(hx, padT); ctx.lineTo(hx, H - padB); ctx.stroke(); ctx.setLineDash([]);
    ctx.fillStyle = '#FEF0DD';
    ctx.beginPath(); ctx.arc(hx, hy, 6.5, 0, Math.PI * 2); ctx.fill();
    ctx.strokeStyle = '#F7A724'; ctx.lineWidth = 2.4; ctx.stroke();
  }
}

document.getElementById('chart').addEventListener('mousemove', e => {
  if (!lastPoints.length) return;
  const rect = e.target.getBoundingClientRect();
  const padL = 88, padR = 82, padT = 20, padB = 34;
  const plotW = Math.max(1, rect.width - padL - padR), plotH = 356 - padT - padB;
  const relX = Math.max(0, Math.min(1, (e.clientX - rect.left - padL) / plotW));
  const minT = lastPoints[0].ts, maxT = Math.max(lastPoints[lastPoints.length - 1].ts, minT + 1000);
  const targetTs = minT + relX * (maxT - minT);
  let best = 0, bestDist = Infinity;
  for (let i = 0; i < lastPoints.length; i++) {
    const d = Math.abs(lastPoints[i].ts - targetTs);
    if (d < bestDist) { bestDist = d; best = i; }
  }
  hoverIdx = best;
  const pt = lastPoints[hoverIdx];
  let minV = Math.min(...lastPoints.map(p => p.val)), maxV = Math.max(...lastPoints.map(p => p.val));
  if (maxV === minV) { const p = Math.max(Math.abs(maxV) * 0.001, 0.01); maxV += p; minV -= p; }
  const rSpan = maxV - minV; minV -= rSpan * 0.10; maxV += rSpan * 0.10;
  const tx = padL + ((pt.ts - minT) / (maxT - minT)) * plotW;
  const ty = padT + (1 - (pt.val - minV) / (maxV - minV)) * plotH;
  if (!hoverActive) { hoverSpringX.set(tx, true); hoverSpringY.set(ty, true); hoverActive = true; }
  else { hoverSpringX.set(tx, false); hoverSpringY.set(ty, false); }

  const mName = document.getElementById('metric').value.trim() || 'btc.usd';
  document.getElementById('hoverInfo').textContent = `${new Date(pt.ts).toLocaleTimeString([], { hour12: false })}  ->  ${fmtVal(mName, pt.val)}`;
  drawChart(lastPoints, mName);
});
document.getElementById('chart').addEventListener('mouseleave', () => {
  hoverIdx = -1; hoverActive = false;
  document.getElementById('hoverInfo').textContent = 'Hover curve to inspect exact tick';
  drawChart(lastPoints);
});
window.addEventListener('resize', () => { syncPillTarget(true); drawChart(lastPoints); });
requestAnimationFrame(() => syncPillTarget(true));
refreshAll(true);
autoTimer = setInterval(() => refreshAll(false), 1000);
</script>
</body>
</html>
"##;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn parse_query(qs: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for pair in qs.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            map.insert(k.to_string(), url_decode(v));
        }
    }
    map
}

fn url_decode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(hex) = std::str::from_utf8(&bytes[i + 1..i + 3]) {
                if let Ok(val) = u8::from_str_radix(hex, 16) {
                    out.push(val as char);
                    i += 3;
                    continue;
                }
            }
        } else if bytes[i] == b'+' {
            out.push(' ');
            i += 1;
            continue;
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn write_http_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &str,
) {
    let header = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nCache-Control: no-store\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(header.as_bytes());
    let _ = stream.write_all(body.as_bytes());
    let _ = stream.flush();
}

fn handle_client(mut stream: TcpStream, db: Arc<Tsdb>) {
    let mut reader = match stream.try_clone() {
        Ok(s) => BufReader::new(s),
        Err(_) => return,
    };

    let mut request_line = String::new();
    if reader.read_line(&mut request_line).unwrap_or(0) == 0 {
        return;
    }
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let target = parts.next().unwrap_or("/");

    let mut content_length: usize = 0;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" || line == "\n" {
            break;
        }
        if let Some((name, val)) = line.split_once(':') {
            if name.trim().eq_ignore_ascii_case("content-length") {
                content_length = val.trim().parse().unwrap_or(0);
            }
        }
    }

    let mut body = vec![0u8; content_length.min(16 * 1024 * 1024)];
    if !body.is_empty() && reader.read_exact(&mut body).is_err() {
        write_http_response(&mut stream, "400 Bad Request", "text/plain", "incomplete body");
        return;
    }
    let body_str = String::from_utf8_lossy(&body);

    let (path, qs) = target.split_once('?').unwrap_or((target, ""));
    let params = parse_query(qs);

    match (method, path) {
        ("GET", "/") => {
            write_http_response(&mut stream, "200 OK", "text/html; charset=utf-8", DASHBOARD_HTML);
        }
        ("POST", "/api/ingest") => {
            let mut ingested = 0usize;
            for line in body_str.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let cols: Vec<&str> = trimmed.split_whitespace().collect();
                if cols.len() < 2 {
                    write_http_response(
                        &mut stream,
                        "400 Bad Request",
                        "text/plain",
                        "expected: <metric> <value> [timestamp_ms]",
                    );
                    return;
                }
                let metric = cols[0];
                let val: f64 = match cols[1].parse() {
                    Ok(v) => v,
                    Err(_) => {
                        write_http_response(&mut stream, "400 Bad Request", "text/plain", "invalid float value");
                        return;
                    }
                };
                let ts: u64 = if cols.len() >= 3 {
                    match cols[2].parse() {
                        Ok(t) => t,
                        Err(_) => {
                            write_http_response(&mut stream, "400 Bad Request", "text/plain", "invalid timestamp_ms");
                            return;
                        }
                    }
                } else {
                    now_ms()
                };

                if let Err(err) = db.ingest(metric, ts, val) {
                    write_http_response(&mut stream, "500 Internal Server Error", "text/plain", &err.to_string());
                    return;
                }
                ingested += 1;
            }
            write_http_response(
                &mut stream,
                "200 OK",
                "application/json",
                &format!("{{\"ingested\":{ingested}}}"),
            );
        }
        ("GET", "/api/query") => {
            let metric = match params.get("metric") {
                Some(m) if !m.is_empty() => m.as_str(),
                _ => {
                    write_http_response(&mut stream, "400 Bad Request", "text/plain", "missing metric param");
                    return;
                }
            };
            let start: u64 = params.get("start").and_then(|s| s.parse().ok()).unwrap_or(0);
            let end: u64 = params.get("end").and_then(|s| s.parse().ok()).unwrap_or(u64::MAX);
            let bucket: u64 = params.get("bucket").and_then(|s| s.parse().ok()).unwrap_or(0);
            let agg_str = params.get("agg").map(|s| s.as_str()).unwrap_or("avg");

            let points_res = if bucket > 0 {
                match Aggregation::from_str(agg_str) {
                    Ok(agg) => db.downsample(metric, start, end, bucket, agg),
                    Err(e) => Err(e),
                }
            } else {
                db.query_range(metric, start, end)
            };

            match points_res {
                Ok(points) => {
                    let mut json = String::from("[");
                    for (i, pt) in points.iter().enumerate() {
                        if i > 0 {
                            json.push(',');
                        }
                        json.push_str(&format!("{{\"ts\":{},\"val\":{}}}", pt.timestamp_ms, pt.value));
                    }
                    json.push(']');
                    write_http_response(&mut stream, "200 OK", "application/json", &json);
                }
                Err(err) => {
                    write_http_response(&mut stream, "400 Bad Request", "text/plain", &err.to_string());
                }
            }
        }
        ("GET", "/api/latest") => {
            let metrics_param = params.get("metrics").map(|s| s.as_str()).unwrap_or("");
            let now = now_ms();
            let start = now.saturating_sub(3_600_000);
            let mut json = String::from("{");
            let mut first = true;
            for raw_m in metrics_param.split(',') {
                let metric = raw_m.trim();
                if metric.is_empty() {
                    continue;
                }
                if !first {
                    json.push(',');
                }
                first = false;
                let latest_val = db
                    .query_range(metric, start, now.saturating_add(5_000))
                    .ok()
                    .and_then(|pts| pts.last().map(|p| p.value));
                match latest_val {
                    Some(v) => json.push_str(&format!("\"{metric}\":{v}")),
                    None => json.push_str(&format!("\"{metric}\":null")),
                }
            }
            json.push('}');
            write_http_response(&mut stream, "200 OK", "application/json", &json);
        }
        ("POST", "/api/retention") => {
            let metric = match params.get("metric") {
                Some(m) if !m.is_empty() => m.as_str(),
                _ => {
                    write_http_response(&mut stream, "400 Bad Request", "text/plain", "missing metric param");
                    return;
                }
            };
            let older_than: u64 = match params.get("older_than").and_then(|s| s.parse().ok()) {
                Some(t) => t,
                None => {
                    write_http_response(&mut stream, "400 Bad Request", "text/plain", "missing older_than param");
                    return;
                }
            };
            match db.enforce_retention(metric, older_than) {
                Ok(deleted) => write_http_response(
                    &mut stream,
                    "200 OK",
                    "application/json",
                    &format!("{{\"deleted\":{deleted}}}"),
                ),
                Err(err) => write_http_response(&mut stream, "500 Internal Server Error", "text/plain", &err.to_string()),
            }
        }
        ("POST", "/api/compact") => {
            if let Err(err) = db.flush().and_then(|_| db.compact_now()) {
                write_http_response(&mut stream, "500 Internal Server Error", "text/plain", &err.to_string());
                return;
            }
            write_http_response(&mut stream, "200 OK", "application/json", "{\"status\":\"compacted\"}");
        }
        ("GET", "/api/stats") => {
            let st = db.stats();
            let levels = db.level_counts();
            let json = format!(
                "{{\"wal_bytes\":{},\"flush_bytes\":{},\"compaction_bytes\":{},\"logical_bytes\":{},\"waf\":{:.3},\"levels\":{:?}}}",
                st.wal_bytes_written,
                st.flush_bytes_written,
                st.compaction_bytes_written,
                st.logical_user_bytes,
                st.write_amplification(),
                levels,
            );
            write_http_response(&mut stream, "200 OK", "application/json", &json);
        }
        _ => {
            write_http_response(&mut stream, "404 Not Found", "text/plain", "not found");
        }
    }
}

fn main() {
    let mut addr = String::from("127.0.0.1:8080");
    let mut data_dir = PathBuf::from("./tsdb_data");

    let args: Vec<String> = env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--addr" if i + 1 < args.len() => {
                addr = args[i + 1].clone();
                i += 2;
            }
            "--dir" if i + 1 < args.len() => {
                data_dir = PathBuf::from(&args[i + 1]);
                i += 2;
            }
            _ => i += 1,
        }
    }

    let opts = EngineOptions {
        fsync_policy: FsyncPolicy::EveryN(100),
        ..Default::default()
    };
    let db = Arc::new(Tsdb::open(&data_dir, opts).expect("failed to open TSDB"));
    let listener = TcpListener::bind(&addr).expect("failed to bind HTTP address");
    println!("LSM-TSDB listening on http://{addr} (data dir: {})", data_dir.display());

    for incoming in listener.incoming() {
        if let Ok(stream) = incoming {
            let db_ref = Arc::clone(&db);
            thread::spawn(move || handle_client(stream, db_ref));
        }
    }
}
