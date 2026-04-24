const APP_VERSION = "2026.03.07";

const TRANSFORM_LEVEL = "level";
const TRANSFORM_PCT_OF = "pct_of";
const TRANSFORM_LVL_CHANGE = "lvl_change";
const TRANSFORM_PCT_CHANGE = "pct_change";

const COMPARE_NONE = "none";
const COMPARE_DIFF_VS_RUN = "diff_vs_run";
const COMPARE_PCT_DIFF_VS_RUN = "pct_diff_vs_run";

const DEFAULT_DENOMINATOR = "GDP";

/* ── Expression parser (recursive descent, no eval) ────────────── */

function tokenize(expr) {
  const tokens = [];
  let i = 0;
  while (i < expr.length) {
    if (/\s/.test(expr[i])) { i++; continue; }
    if ("+-*/()".includes(expr[i])) { tokens.push({ type: "op", value: expr[i] }); i++; continue; }
    if (/[0-9]/.test(expr[i]) || (expr[i] === "." && i + 1 < expr.length && /[0-9]/.test(expr[i + 1]))) {
      let num = "";
      while (i < expr.length && (/[0-9]/.test(expr[i]) || expr[i] === ".")) { num += expr[i]; i++; }
      tokens.push({ type: "num", value: Number(num) });
      continue;
    }
    if (/[A-Za-z_]/.test(expr[i])) {
      let name = "";
      while (i < expr.length && /[A-Za-z0-9_]/.test(expr[i])) { name += expr[i]; i++; }
      tokens.push({ type: "var", value: name.toUpperCase() });
      continue;
    }
    throw new Error(`Unexpected character: '${expr[i]}'`);
  }
  return tokens;
}

function parseExpression(tokens) {
  let pos = 0;
  function peek() { return tokens[pos] || null; }
  function consume() { return tokens[pos++]; }

  function parseExpr() { return parseAddSub(); }

  function parseAddSub() {
    let left = parseMulDiv();
    while (peek() && (peek().value === "+" || peek().value === "-")) {
      const op = consume().value;
      left = { type: "binary", op, left, right: parseMulDiv() };
    }
    return left;
  }

  function parseMulDiv() {
    let left = parseUnary();
    while (peek() && (peek().value === "*" || peek().value === "/")) {
      const op = consume().value;
      left = { type: "binary", op, left, right: parseUnary() };
    }
    return left;
  }

  function parseUnary() {
    if (peek() && peek().value === "-") { consume(); return { type: "unary", op: "-", operand: parseUnary() }; }
    if (peek() && peek().value === "+") { consume(); return parseUnary(); }
    return parsePrimary();
  }

  function parsePrimary() {
    const tok = peek();
    if (!tok) throw new Error("Unexpected end of expression");
    if (tok.type === "num") { consume(); return { type: "num", value: tok.value }; }
    if (tok.type === "var") { consume(); return { type: "var", value: tok.value }; }
    if (tok.value === "(") {
      consume();
      const inner = parseExpr();
      const closing = peek();
      if (!closing || closing.value !== ")") throw new Error("Expected ')'");
      consume();
      return inner;
    }
    throw new Error(`Unexpected token: '${tok.value}'`);
  }

  const ast = parseExpr();
  if (pos < tokens.length) throw new Error(`Unexpected token: '${tokens[pos].value}'`);
  return ast;
}

function extractVariables(ast) {
  const vars = new Set();
  (function walk(node) {
    if (node.type === "var") vars.add(node.value);
    if (node.type === "binary") { walk(node.left); walk(node.right); }
    if (node.type === "unary") walk(node.operand);
  })(ast);
  return [...vars];
}

function evaluateAst(ast, getVar) {
  if (ast.type === "num") return ast.value;
  if (ast.type === "var") return getVar(ast.value);
  if (ast.type === "unary" && ast.op === "-") {
    const v = evaluateAst(ast.operand, getVar);
    return v === null ? null : -v;
  }
  if (ast.type === "binary") {
    const l = evaluateAst(ast.left, getVar);
    const r = evaluateAst(ast.right, getVar);
    if (l === null || r === null) return null;
    if (ast.op === "+") return l + r;
    if (ast.op === "-") return l - r;
    if (ast.op === "*") return l * r;
    if (ast.op === "/") return r === 0 ? null : l / r;
  }
  return null;
}

let equationIdCounter = 0;

const state = {
  manifest: null,
  presets: [],
  dictionary: new Map(),
  equationCatalog: new Map(),
  runMeta: [],
  activeHorizonId: "",
  selectedRunIds: [],
  selectedPresetIds: [],
  selectedVariables: [],
  runCache: new Map(),
  variableConfigs: new Map(),
  equationConfigs: new Map(),
  variableSearchQuery: "",
  selectedRunInfoId: "",
  equations: [],
  runSelectExpanded: false,
};

const dom = {
  pageTitle: document.querySelector("#pageTitle"),
  runPanel: document.querySelector("#runPanel"),
  runPanelBackdrop: document.querySelector("#runPanelBackdrop"),
  runSelect: document.querySelector("#runSelect"),
  runSelectToggle: document.querySelector("#runSelectToggle"),
  horizonControls: document.querySelector("#horizonControls"),
  presetSelect: document.querySelector("#presetSelect"),
  applyPresetButton: document.querySelector("#applyPresetButton"),
  variableSearch: document.querySelector("#variableSearch"),
  variableSelect: document.querySelector("#variableSelect"),
  runInfoSelect: document.querySelector("#runInfoSelect"),
  runInfo: document.querySelector("#runInfo"),
  equationInput: document.querySelector("#equationInput"),
  equationPlotButton: document.querySelector("#equationPlotButton"),
  equationError: document.querySelector("#equationError"),
  equationCharts: document.querySelector("#equationCharts"),
  charts: document.querySelector("#charts"),
  chartEmpty: document.querySelector("#chartEmpty"),
  dictionarySearch: document.querySelector("#dictionarySearch"),
  dictionaryResults: document.querySelector("#dictionaryResults"),
  equationSearch: document.querySelector("#equationSearch"),
  equationExplorerResults: document.querySelector("#equationExplorerResults"),
  themeToggle: document.querySelector("#themeToggle"),
};

/* ── Theme toggle ─────────────────────────────────────────────── */

const THEME_KEY = "rptr-theme";

const ICON_SUN = '<svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/></svg>';
const ICON_MOON = '<svg viewBox="0 0 24 24"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>';

function isLightMode() {
  return document.documentElement.getAttribute("data-theme") === "light";
}

function getPlotlyTheme() {
  const style = getComputedStyle(document.documentElement);
  return {
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: style.getPropertyValue("--plot-bg").trim(),
    font: { family: "IBM Plex Sans, sans-serif", color: style.getPropertyValue("--plot-text").trim() },
    gridcolor: style.getPropertyValue("--plot-grid").trim(),
    zerolinecolor: style.getPropertyValue("--plot-zero").trim(),
  };
}

function relayoutAllCharts() {
  const theme = getPlotlyTheme();
  const surfaces = document.querySelectorAll(".chart-surface");
  for (const el of surfaces) {
    if (el.data) {
      Plotly.relayout(el, {
        paper_bgcolor: theme.paper_bgcolor,
        plot_bgcolor: theme.plot_bgcolor,
        font: theme.font,
        "yaxis.gridcolor": theme.gridcolor,
        "yaxis.zerolinecolor": theme.zerolinecolor,
      });
    }
  }
}

function syncThemeIcon() {
  if (!dom.themeToggle) return;
  dom.themeToggle.innerHTML = isLightMode() ? ICON_MOON : ICON_SUN;
  dom.themeToggle.setAttribute("aria-label", isLightMode() ? "Switch to dark mode" : "Switch to light mode");
}

function toggleTheme() {
  const next = isLightMode() ? "dark" : "light";
  document.documentElement.setAttribute("data-theme", next);
  localStorage.setItem(THEME_KEY, next);
  syncThemeIcon();
  relayoutAllCharts();
}

syncThemeIcon();

function resolveAssetUrl(relativePath) {
  return new URL(relativePath, window.location.href).toString();
}

async function fetchJson(relativePath) {
  const url = resolveAssetUrl(relativePath);
  const sep = url.includes("?") ? "&" : "?";
  const response = await fetch(`${url}${sep}_v=${APP_VERSION}`);
  if (!response.ok) {
    throw new Error(`Failed to load ${relativePath}: ${response.status}`);
  }
  return response.json();
}

function sortPeriodTokens(tokens) {
  return [...tokens].sort((left, right) => {
    const leftParts = `${left}`.split(".");
    const rightParts = `${right}`.split(".");
    const leftYear = Number.parseInt(leftParts[0] || "0", 10);
    const leftSub = Number.parseInt(leftParts[1] || "0", 10);
    const rightYear = Number.parseInt(rightParts[0] || "0", 10);
    const rightSub = Number.parseInt(rightParts[1] || "0", 10);
    if (leftYear !== rightYear) {
      return leftYear - rightYear;
    }
    if (leftSub !== rightSub) {
      return leftSub - rightSub;
    }
    return `${left}`.localeCompare(`${right}`);
  });
}

function formatPeriodToken(token) {
  const [year, sub] = `${token}`.split(".");
  return year && sub ? `${year} Q${sub}` : `${token}`;
}

function unique(values) {
  return [...new Set(values)];
}

function getRunMeta(runId) {
  return state.runMeta.find((item) => item.run_id === runId) || null;
}

function getReferenceRuns() {
  return state.runMeta.filter((run) => {
    const horizonId = `${run?.horizon_id || ""}`.trim();
    const group = `${run?.group || ""}`.trim().toLowerCase();
    const familyId = `${run?.family_id || ""}`.trim();
    return !horizonId || group === "reference" || familyId === "stock_fm_baseline";
  });
}

function getAvailableHorizonIds() {
  const ids = unique(
    state.runMeta
      .map((run) => `${run?.horizon_id || ""}`.trim())
      .filter(Boolean),
  );
  return ids.sort((left, right) => {
    const leftYears = Number.parseInt(left, 10);
    const rightYears = Number.parseInt(right, 10);
    if (Number.isFinite(leftYears) && Number.isFinite(rightYears) && leftYears !== rightYears) {
      return leftYears - rightYears;
    }
    return left.localeCompare(right);
  });
}

function getHorizonLabel(horizonId) {
  const match = state.runMeta.find((run) => `${run?.horizon_id || ""}`.trim() === horizonId);
  return `${match?.horizon_label || horizonId}`.trim() || horizonId.toUpperCase();
}

function getVisibleRunMeta() {
  return state.runMeta.filter((run) => {
    const horizonId = `${run?.horizon_id || ""}`.trim();
    if (getReferenceRuns().some((item) => item.run_id === run.run_id)) {
      return true;
    }
    return !horizonId || !state.activeHorizonId || horizonId === state.activeHorizonId;
  });
}

function getRunForActiveHorizon(run) {
  if (!run) {
    return null;
  }
  const familyId = `${run?.family_id || ""}`.trim();
  const horizonId = `${run?.horizon_id || ""}`.trim();
  if (getReferenceRuns().some((item) => item.run_id === run.run_id)) {
    return run;
  }
  if (!familyId || !horizonId || !state.activeHorizonId || horizonId === state.activeHorizonId) {
    return run;
  }
  return state.runMeta.find((item) => {
    return `${item?.family_id || ""}`.trim() === familyId && `${item?.horizon_id || ""}`.trim() === state.activeHorizonId;
  }) || run;
}

function normalizeSelectedRunIdsForActiveHorizon(runIds) {
  const visibleRunIds = new Set(getVisibleRunMeta().map((run) => run.run_id));
  const mapped = new Set();
  for (const runId of runIds) {
    const target = getRunForActiveHorizon(getRunMeta(runId));
    if (target && visibleRunIds.has(target.run_id)) {
      mapped.add(target.run_id);
    }
  }
  return getVisibleRunMeta()
    .map((run) => run.run_id)
    .filter((runId) => mapped.has(runId));
}

function getRunGroupLabel(run) {
  return `${run?.group || ""}`.trim() || "Other";
}

function buildRunGroups(runs) {
  const ordered = [];
  const byLabel = new Map();
  for (const run of runs) {
    const label = getRunGroupLabel(run);
    if (!byLabel.has(label)) {
      const group = { label, runs: [] };
      byLabel.set(label, group);
      ordered.push(group);
    }
    byLabel.get(label).runs.push(run);
  }
  return ordered;
}

async function ensureRunsLoaded(runIds) {
  const pending = runIds
    .filter((runId) => !state.runCache.has(runId))
    .map(async (runId) => {
      const runMeta = getRunMeta(runId);
      if (!runMeta) {
        return;
      }
      const payload = await fetchJson(runMeta.data_path);
      state.runCache.set(runId, payload);
    });
  await Promise.all(pending);
}

function getDictionaryRecord(variable) {
  return state.dictionary.get(variable) || {
    code: variable,
    short_name: "",
    description: "",
    units: "",
    defined_by_equation: null,
    used_in_equations: [],
  };
}

function getEquationRecord(eqId) {
  if (eqId === null || eqId === undefined) {
    return null;
  }
  const normalized = `${eqId}`.trim();
  if (!normalized) {
    return null;
  }
  return state.equationCatalog.get(normalized) || null;
}

function inferDenominator() {
  if (state.manifest.available_variables.includes(DEFAULT_DENOMINATOR)) {
    return DEFAULT_DENOMINATOR;
  }
  return state.manifest.available_variables[0] || DEFAULT_DENOMINATOR;
}

function buildDefaultConfig() {
  const runIds = state.selectedRunIds;
  return {
    transformMode: TRANSFORM_LEVEL,
    denominator: inferDenominator(),
    compareMode: COMPARE_NONE,
    referenceRunId: runIds[0] || "",
  };
}

function sanitizeSeriesConfig(input) {
  const base = buildDefaultConfig();
  const next = { ...base, ...(input || {}) };
  if (![TRANSFORM_LEVEL, TRANSFORM_PCT_OF, TRANSFORM_LVL_CHANGE, TRANSFORM_PCT_CHANGE].includes(next.transformMode)) {
    next.transformMode = TRANSFORM_LEVEL;
  }
  if (![COMPARE_NONE, COMPARE_DIFF_VS_RUN, COMPARE_PCT_DIFF_VS_RUN].includes(next.compareMode)) {
    next.compareMode = COMPARE_NONE;
  }
  if (!state.manifest.available_variables.includes(next.denominator)) {
    next.denominator = inferDenominator();
  }
  if (!state.selectedRunIds.includes(next.referenceRunId)) {
    next.referenceRunId = state.selectedRunIds[0] || "";
  }
  if (state.selectedRunIds.length < 2) {
    next.compareMode = COMPARE_NONE;
  }
  return next;
}

function sanitizeVariableConfig(variable, input) {
  const next = sanitizeSeriesConfig(input);
  state.variableConfigs.set(variable, next);
  return next;
}

function getVariableConfig(variable) {
  return sanitizeVariableConfig(variable, state.variableConfigs.get(variable));
}

function sanitizeEquationConfig(equationId, input) {
  const next = sanitizeSeriesConfig(input);
  state.equationConfigs.set(equationId, next);
  return next;
}

function getEquationConfig(equationId) {
  return sanitizeEquationConfig(equationId, state.equationConfigs.get(equationId));
}

function applyPresetSelection() {
  const selectedPresets = state.presets.filter((item) => state.selectedPresetIds.includes(item.id));
  const nextVariables = unique(selectedPresets.flatMap((item) => item.variables || []))
    .filter((name) => state.manifest.available_variables.includes(name));
  if (nextVariables.length > 0) {
    state.selectedVariables = nextVariables;
  }

  const nextConfigs = new Map();
  for (const variable of state.selectedVariables) {
    let nextConfig = buildDefaultConfig();
    for (const preset of selectedPresets) {
      if (preset.transforms && preset.transforms[variable]) {
        nextConfig = {
          ...nextConfig,
          transformMode: preset.transforms[variable].mode || nextConfig.transformMode,
          denominator: preset.transforms[variable].denominator || nextConfig.denominator,
        };
      }
      if (preset.run_comparisons && preset.run_comparisons[variable]) {
        const compare = preset.run_comparisons[variable];
        const referenceRunId = state.selectedRunIds.includes(compare.reference_run_id)
          ? compare.reference_run_id
          : nextConfig.referenceRunId;
        nextConfig = {
          ...nextConfig,
          compareMode: compare.mode || nextConfig.compareMode,
          referenceRunId,
        };
      }
    }
    nextConfigs.set(variable, sanitizeVariableConfig(variable, nextConfig));
  }
  state.variableConfigs = nextConfigs;
  syncVariableSelect();
  renderCharts();
}

function createChecklistItem({ checked, title, note, tooltip, onToggle }) {
  const label = document.createElement("label");
  label.className = "check-item";
  if (tooltip) {
    label.title = tooltip;
  }

  const input = document.createElement("input");
  input.type = "checkbox";
  input.checked = checked;
  input.addEventListener("change", () => onToggle(input.checked));

  const copy = document.createElement("span");
  copy.className = "check-copy";

  const titleSpan = document.createElement("span");
  titleSpan.className = "check-title";
  titleSpan.textContent = title;
  copy.appendChild(titleSpan);

  if (note) {
    const noteSpan = document.createElement("span");
    noteSpan.className = "check-note";
    noteSpan.textContent = note;
    copy.appendChild(noteSpan);
  }

  label.appendChild(input);
  label.appendChild(copy);
  return label;
}

function getRunDescription(run) {
  const parts = [];
  const summary = `${run?.summary || ""}`.trim();
  if (summary) {
    parts.push(summary);
  }
  const details = Array.isArray(run?.details) ? run.details.filter(Boolean) : [];
  if (details.length > 0) {
    const leadDetail = `${details[0]}`.trim();
    if (leadDetail && !parts.includes(leadDetail)) {
      parts.push(leadDetail);
    }
  }
  const forecastStart = `${run?.forecast_start || ""}`.trim();
  const forecastEnd = `${run?.forecast_end || ""}`.trim();
  if (forecastStart && forecastEnd) {
    const forecastCopy = `Forecast ${forecastStart} to ${forecastEnd}.`;
    if (!parts.includes(forecastCopy)) {
      parts.push(forecastCopy);
    }
  }
  return parts.join(" ");
}

function getRunTooltip(run) {
  const details = Array.isArray(run?.details) ? run.details.filter(Boolean) : [];
  return [
    run?.label || "",
    `${run?.summary || ""}`.trim(),
    ...details,
    `${run?.forecast_start || ""}`.trim() && `${run?.forecast_end || ""}`.trim()
      ? `Forecast ${run.forecast_start} to ${run.forecast_end}.`
      : "",
    run?.scenario_name ? `Scenario: ${run.scenario_name}` : "",
  ]
    .filter(Boolean)
    .join("\n");
}

function getRunSelectColumnCount() {
  if (!state.runSelectExpanded || !dom.runPanel) {
    return 1;
  }
  const width = dom.runPanel.clientWidth || window.innerWidth || 0;
  if (width >= 1180) {
    return 3;
  }
  if (width >= 760) {
    return 2;
  }
  return 1;
}

function estimateRunGroupWeight(group) {
  const baseWeight = 96;
  return group.runs.reduce((total, run) => {
    const description = getRunDescription(run);
    const summary = `${run?.summary || ""}`.trim();
    return total + 68 + Math.ceil((description.length + summary.length) / 48) * 16;
  }, baseWeight);
}

function createRunGroupSection(group) {
  const section = document.createElement("section");
  section.className = "run-group";

  const header = document.createElement("div");
  header.className = "run-group-header";

  const titleWrap = document.createElement("div");
  const title = document.createElement("p");
  title.className = "run-group-title";
  title.textContent = group.label;
  titleWrap.appendChild(title);

  const note = document.createElement("p");
  note.className = "run-group-note";
  note.textContent = `${group.runs.length} runs`;
  titleWrap.appendChild(note);
  header.appendChild(titleWrap);

  const actions = document.createElement("div");
  actions.className = "run-group-actions";
  for (const [label, mode] of [["All", "all"], ["None", "none"]]) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "run-group-action";
    button.textContent = label;
    button.addEventListener("click", async () => {
      const selected = new Set(state.selectedRunIds);
      for (const run of group.runs) {
        if (mode === "all") {
          selected.add(run.run_id);
        } else {
          selected.delete(run.run_id);
        }
      }
      await applyRunSelection(selected);
    });
    actions.appendChild(button);
  }
  header.appendChild(actions);
  section.appendChild(header);

  const list = document.createElement("div");
  list.className = "checklist checklist-group";
  for (const run of group.runs) {
    list.appendChild(
      createChecklistItem({
        checked: state.selectedRunIds.includes(run.run_id),
        title: run.label,
        note: getRunDescription(run),
        tooltip: getRunTooltip(run),
        onToggle: async (checked) => {
          const selected = new Set(state.selectedRunIds);
          if (checked) {
            selected.add(run.run_id);
          } else {
            selected.delete(run.run_id);
          }
          await applyRunSelection(selected);
        },
      }),
    );
  }
  section.appendChild(list);
  return section;
}

function syncRunSelectExpansion() {
  if (!dom.runPanel || !dom.runSelect || !dom.runSelectToggle || !dom.runPanelBackdrop) {
    return;
  }
  document.body.classList.toggle("panel-overlay-open", state.runSelectExpanded);
  dom.runPanel.classList.toggle("panel-card-expanded", state.runSelectExpanded);
  dom.runSelect.classList.toggle("checklist-expanded", state.runSelectExpanded);
  dom.runPanelBackdrop.hidden = !state.runSelectExpanded;
  dom.runSelectToggle.textContent = state.runSelectExpanded ? "Collapse" : "Expand";
  dom.runSelectToggle.setAttribute("aria-expanded", state.runSelectExpanded ? "true" : "false");
}

function setRunSelectExpanded(nextValue) {
  const normalized = Boolean(nextValue);
  const didChange = state.runSelectExpanded !== normalized;
  state.runSelectExpanded = normalized;
  syncRunSelectExpansion();
  if (didChange) {
    syncRunSelect();
  }
}

function collapseRunSelect() {
  if (!state.runSelectExpanded) {
    return;
  }
  setRunSelectExpanded(false);
}

function syncHorizonControls() {
  if (!dom.horizonControls) {
    return;
  }
  const horizonIds = getAvailableHorizonIds();
  dom.horizonControls.hidden = horizonIds.length <= 1;
  dom.horizonControls.innerHTML = "";
  for (const horizonId of horizonIds) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `segmented-button${state.activeHorizonId === horizonId ? " active" : ""}`;
    button.textContent = getHorizonLabel(horizonId);
    button.setAttribute("aria-pressed", state.activeHorizonId === horizonId ? "true" : "false");
    button.addEventListener("click", async () => {
      await setActiveHorizon(horizonId);
    });
    dom.horizonControls.appendChild(button);
  }
}

async function setActiveHorizon(horizonId) {
  if (!horizonId || state.activeHorizonId === horizonId) {
    return;
  }
  state.activeHorizonId = horizonId;
  const normalized = normalizeSelectedRunIdsForActiveHorizon(state.selectedRunIds);
  const fallback = normalized.length > 0
    ? normalized
    : normalizeSelectedRunIdsForActiveHorizon(state.manifest.default_run_ids || []);
  state.selectedRunIds = fallback;
  await ensureRunsLoaded(state.selectedRunIds);
  const nextRunInfo = getRunForActiveHorizon(getRunMeta(state.selectedRunInfoId));
  state.selectedRunInfoId = state.selectedRunIds.includes(nextRunInfo?.run_id)
    ? nextRunInfo.run_id
    : (state.selectedRunIds[0] || getVisibleRunMeta()[0]?.run_id || "");
  syncHorizonControls();
  syncRunSelect();
  syncRunInfoSelect();
  renderCharts();
  renderEquationCharts();
  renderRunInfo();
}

async function applyRunSelection(selected) {
  state.selectedRunIds = getVisibleRunMeta()
    .map((item) => item.run_id)
    .filter((runId) => selected.has(runId));
  await ensureRunsLoaded(state.selectedRunIds);
  for (const variable of state.selectedVariables) {
    sanitizeVariableConfig(variable, state.variableConfigs.get(variable));
  }
  for (const equation of state.equations) {
    sanitizeEquationConfig(equation.id, state.equationConfigs.get(equation.id));
  }
  if (!state.selectedRunIds.includes(state.selectedRunInfoId)) {
    state.selectedRunInfoId = state.selectedRunIds[0] || getVisibleRunMeta()[0]?.run_id || "";
  }
  syncRunSelect();
  syncRunInfoSelect();
  renderCharts();
  renderEquationCharts();
  renderRunInfo();
}

function syncRunSelect() {
  dom.runSelect.innerHTML = "";
  const groups = buildRunGroups(getVisibleRunMeta());
  if (state.runSelectExpanded) {
    const columnCount = getRunSelectColumnCount();
    dom.runSelect.style.setProperty("--run-select-columns", `${columnCount}`);
    const columns = Array.from({ length: columnCount }, () => {
      const column = document.createElement("div");
      column.className = "run-group-column";
      return column;
    });
    const weights = columns.map(() => 0);
    for (const group of groups) {
      let targetIndex = 0;
      for (let index = 1; index < columns.length; index += 1) {
        if (weights[index] < weights[targetIndex]) {
          targetIndex = index;
        }
      }
      columns[targetIndex].appendChild(createRunGroupSection(group));
      weights[targetIndex] += estimateRunGroupWeight(group);
    }
    for (const column of columns) {
      dom.runSelect.appendChild(column);
    }
    return;
  }

  dom.runSelect.style.removeProperty("--run-select-columns");
  for (const group of groups) {
    dom.runSelect.appendChild(createRunGroupSection(group));
  }
}

function syncPresetSelect() {
  dom.presetSelect.innerHTML = "";
  for (const preset of state.presets) {
    dom.presetSelect.appendChild(
      createChecklistItem({
        checked: state.selectedPresetIds.includes(preset.id),
        title: preset.label,
        note: `${(preset.variables || []).length} variables`,
        onToggle: (checked) => {
          const selected = new Set(state.selectedPresetIds);
          if (checked) {
            selected.add(preset.id);
          } else {
            selected.delete(preset.id);
          }
          state.selectedPresetIds = state.presets
            .map((item) => item.id)
            .filter((presetId) => selected.has(presetId));
          syncPresetSelect();
        },
      }),
    );
  }
}

function syncRunInfoSelect() {
  dom.runInfoSelect.innerHTML = "";
  for (const group of buildRunGroups(getVisibleRunMeta())) {
    const optgroup = document.createElement("optgroup");
    optgroup.label = group.label;
    for (const run of group.runs) {
      const option = document.createElement("option");
      option.value = run.run_id;
      option.textContent = run.label;
      option.selected = run.run_id === state.selectedRunInfoId;
      optgroup.appendChild(option);
    }
    dom.runInfoSelect.appendChild(optgroup);
  }
}

function syncVariableSelect() {
  dom.variableSelect.innerHTML = "";
  const query = `${state.variableSearchQuery || ""}`.trim().toLowerCase();
  for (const variable of state.manifest.available_variables) {
    const record = getDictionaryRecord(variable);
    const haystack = [variable, record.short_name, record.description]
      .join(" ")
      .toLowerCase();
    if (query && !haystack.includes(query)) {
      continue;
    }
    dom.variableSelect.appendChild(
      createChecklistItem({
        checked: state.selectedVariables.includes(variable),
        title: variable,
        note: record.short_name || record.units || "",
        onToggle: (checked) => {
          const selected = new Set(state.selectedVariables);
          if (checked) {
            selected.add(variable);
          } else {
            selected.delete(variable);
          }
          state.selectedVariables = state.manifest.available_variables
            .filter((name) => selected.has(name));
          const nextConfigs = new Map();
          for (const name of state.selectedVariables) {
            nextConfigs.set(name, sanitizeVariableConfig(name, state.variableConfigs.get(name)));
          }
          state.variableConfigs = nextConfigs;
          syncVariableSelect();
          renderCharts();
          renderDictionary();
        },
      }),
    );
  }
}

function makeSelect(options, value, onChange) {
  const select = document.createElement("select");
  for (const optionSpec of options) {
    const option = document.createElement("option");
    option.value = optionSpec.value;
    option.textContent = optionSpec.label;
    option.selected = optionSpec.value === value;
    select.appendChild(option);
  }
  select.addEventListener("change", onChange);
  return select;
}

function buildCompactControl({ label, value, options, onChange, variable }) {
  const field = document.createElement("label");
  field.className = "control-field";

  const tag = document.createElement("span");
  tag.className = "control-tag";
  tag.textContent = label;
  field.appendChild(tag);

  const select = makeSelect(options, value, onChange);
  select.setAttribute("aria-label", `${variable} ${label}`);
  field.appendChild(select);
  return field;
}

function createChartControls({ seriesKey, cfg, onConfigChange, onRender }) {
  const controls = document.createElement("div");
  controls.className = "chart-controls";

  controls.appendChild(
    buildCompactControl({
      label: "Transform",
      value: cfg.transformMode,
      variable: seriesKey,
      options: [
        { value: TRANSFORM_LEVEL, label: "Level" },
        { value: TRANSFORM_PCT_OF, label: "% of denominator" },
        { value: TRANSFORM_LVL_CHANGE, label: "Lvl change" },
        { value: TRANSFORM_PCT_CHANGE, label: "% change" },
      ],
      onChange: (event) => {
        onConfigChange({
          ...cfg,
          transformMode: event.target.value,
        });
        onRender();
      },
    }),
  );

  if (cfg.transformMode === TRANSFORM_PCT_OF) {
    controls.appendChild(
      buildCompactControl({
        label: "Denom",
        value: cfg.denominator,
        variable: seriesKey,
        options: state.manifest.available_variables.map((name) => ({
          value: name,
          label: name,
        })),
        onChange: (event) => {
          onConfigChange({
            ...cfg,
            denominator: event.target.value,
          });
          onRender();
        },
      }),
    );
  }

  controls.appendChild(
    buildCompactControl({
      label: "Compare",
      value: cfg.compareMode,
      variable: seriesKey,
      options: [
        { value: COMPARE_NONE, label: "None" },
        { value: COMPARE_DIFF_VS_RUN, label: "Diff vs run" },
        { value: COMPARE_PCT_DIFF_VS_RUN, label: "% diff vs run" },
      ],
      onChange: (event) => {
        onConfigChange({
          ...cfg,
          compareMode: event.target.value,
        });
        onRender();
      },
    }),
  );

  if (cfg.compareMode !== COMPARE_NONE && state.selectedRunIds.length > 1) {
    controls.appendChild(
      buildCompactControl({
        label: "Ref",
        value: cfg.referenceRunId,
        variable: seriesKey,
        options: state.selectedRunIds.map((runId) => getRunMeta(runId)).filter(Boolean).map((run) => ({
          value: run.run_id,
          label: run.label,
        })),
        onChange: (event) => {
          onConfigChange({
            ...cfg,
            referenceRunId: event.target.value,
          });
          onRender();
        },
      }),
    );
  }

  return controls;
}

function renderRunInfo() {
  dom.runInfo.innerHTML = "";
  const run = getRunMeta(state.selectedRunInfoId);
  if (!run) {
    dom.runInfo.innerHTML = `<div class="empty-state">Select a scenario to see its details.</div>`;
    return;
  }
  const card = document.createElement("article");
  card.className = "run-info-card";

  const detailItems = Array.isArray(run.details) ? run.details : [];
  const detailList = detailItems.length > 0
    ? detailItems.map((item) => `<p class="run-info-text">${item}</p>`).join("")
    : `<p class="hint">No additional scenario notes were exported for this run.</p>`;

  card.innerHTML = `
    <p class="run-info-title">${run.label}</p>
    ${run.group ? `<p class="run-info-text">Group: ${run.group}</p>` : ""}
    <p class="run-info-text">Scenario: ${run.scenario_name || "Unknown"}</p>
    <p class="run-info-text">Forecast: ${run.forecast_start || "?"} to ${run.forecast_end || "?"}</p>
    ${run.summary ? `<p class="run-info-text">${run.summary}</p>` : ""}
    ${detailList}
  `;
  dom.runInfo.appendChild(card);
}

function valueOrNull(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function pctValue(numerator, denominator) {
  const numer = Number(numerator);
  const denom = Number(denominator);
  if (!Number.isFinite(numer) || !Number.isFinite(denom) || denom === 0) {
    return null;
  }
  return 100 * numer / denom;
}

function levelChangeValue(current, previous) {
  const left = Number(current);
  const right = Number(previous);
  if (!Number.isFinite(left) || !Number.isFinite(right)) {
    return null;
  }
  return left - right;
}

function pctChangeValue(current, previous) {
  const left = Number(current);
  const right = Number(previous);
  if (!Number.isFinite(left) || !Number.isFinite(right) || right === 0) {
    return null;
  }
  return 100 * (left / right - 1);
}

function transformSeries(values, denominatorValues, mode) {
  if (mode === TRANSFORM_PCT_OF) {
    return values.map((value, index) => pctValue(value, denominatorValues[index]));
  }
  if (mode === TRANSFORM_LVL_CHANGE) {
    return values.map((value, index) => (index === 0 ? null : levelChangeValue(value, values[index - 1])));
  }
  if (mode === TRANSFORM_PCT_CHANGE) {
    return values.map((value, index) => (index === 0 ? null : pctChangeValue(value, values[index - 1])));
  }
  return values.map(valueOrNull);
}

function applyRunComparison(values, referenceValues, mode) {
  if (mode === COMPARE_NONE) {
    return [...values];
  }
  if (!Array.isArray(referenceValues)) {
    return values.map(() => null);
  }
  return values.map((value, index) => {
    const reference = referenceValues[index];
    if (mode === COMPARE_DIFF_VS_RUN) {
      return levelChangeValue(value, reference);
    }
    if (mode === COMPARE_PCT_DIFF_VS_RUN) {
      return pctChangeValue(value, reference);
    }
    return valueOrNull(value);
  });
}

function alignSeries(runPayload, variable, periods) {
  const periodIndex = new Map(runPayload.periods.map((period, index) => [period, index]));
  const values = runPayload.series[variable] || [];
  return periods.map((period) => {
    const index = periodIndex.get(period);
    return index === undefined ? null : valueOrNull(values[index]);
  });
}

function buildSeriesTitle(titleInput, unitsInput, config) {
  let title = titleInput;
  let units = unitsInput || "";
  if (config.transformMode === TRANSFORM_PCT_OF) {
    title = `${title} (% of ${config.denominator})`;
    units = `% of ${config.denominator}`;
  } else if (config.transformMode === TRANSFORM_LVL_CHANGE) {
    title = `${title} (Lvl change)`;
  } else if (config.transformMode === TRANSFORM_PCT_CHANGE) {
    title = `${title} (% change)`;
    units = "%";
  }
  const referenceRun = getRunMeta(config.referenceRunId);
  if (config.compareMode === COMPARE_DIFF_VS_RUN && referenceRun) {
    title = `${title} (Diff vs ${referenceRun.label})`;
  } else if (config.compareMode === COMPARE_PCT_DIFF_VS_RUN && referenceRun) {
    title = `${title} (% diff vs ${referenceRun.label})`;
    units = "%";
  }
  return { title, units };
}

function buildChartTitle(variable, config) {
  const record = getDictionaryRecord(variable);
  return buildSeriesTitle(record.short_name || variable, record.units || "", config);
}

/* ── CSV download helpers ────────────────────────────────────── */

function csvEscape(value) {
  const str = `${value}`;
  if (str.includes(",") || str.includes('"') || str.includes("\n")) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

function buildCsvContent(xLabels, traces) {
  const header = ["Period", ...traces.map((t) => csvEscape(t.name))].join(",");
  const rows = xLabels.map((label, i) => {
    const values = traces.map((t) => {
      const v = t.y[i];
      return v === null || v === undefined ? "" : v;
    });
    return [csvEscape(label), ...values].join(",");
  });
  return [header, ...rows].join("\n");
}

function triggerCsvDownload(filename, content) {
  const blob = new Blob([content], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.style.display = "none";
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

function createDownloadButton(filename, xLabels, traces) {
  const btn = document.createElement("button");
  btn.className = "chart-download";
  btn.textContent = "\u2193 CSV";
  btn.title = "Download chart data as CSV";
  btn.addEventListener("click", () => {
    triggerCsvDownload(filename, buildCsvContent(xLabels, traces));
  });
  return btn;
}

function renderCharts() {
  dom.charts.innerHTML = "";
  const hasSelections = state.selectedRunIds.length > 0 && state.selectedVariables.length > 0;
  dom.chartEmpty.hidden = hasSelections;
  if (!hasSelections) {
    return;
  }

  const selectedRuns = state.selectedRunIds
    .map((runId) => ({ meta: getRunMeta(runId), payload: state.runCache.get(runId) }))
    .filter((item) => item.meta && item.payload);

  if (selectedRuns.length === 0) {
    dom.chartEmpty.hidden = false;
    dom.chartEmpty.textContent = "No run payloads are loaded yet.";
    return;
  }

  const periods = sortPeriodTokens(
    unique(selectedRuns.flatMap((item) => item.payload.periods || [])),
  );
  const xLabels = periods.map(formatPeriodToken);

  for (const variable of state.selectedVariables) {
    const config = getVariableConfig(variable);
    const titleMeta = buildChartTitle(variable, config);
    const transformedByRun = new Map();

    for (const item of selectedRuns) {
      const baseValues = alignSeries(item.payload, variable, periods);
      let denominatorValues = periods.map(() => null);
      if (config.transformMode === TRANSFORM_PCT_OF) {
        denominatorValues = alignSeries(item.payload, config.denominator, periods);
      }
      transformedByRun.set(
        item.meta.run_id,
        transformSeries(baseValues, denominatorValues, config.transformMode),
      );
    }

    const referenceValues = transformedByRun.get(config.referenceRunId) || null;
    const traces = [];
    for (const item of selectedRuns) {
      if (config.compareMode !== COMPARE_NONE && item.meta.run_id === config.referenceRunId) {
        continue;
      }
      const values = applyRunComparison(
        transformedByRun.get(item.meta.run_id) || periods.map(() => null),
        referenceValues,
        config.compareMode,
      );
      traces.push({
        type: "scatter",
        mode: "lines+markers",
        name: item.meta.label,
        x: xLabels,
        y: values,
        hovertemplate: `<b>${item.meta.label}</b><br>%{x}<br>%{y:,.4f}<extra></extra>`,
        connectgaps: false,
      });
    }

    const card = document.createElement("article");
    card.className = "chart-card";
    const heading = document.createElement("h3");
    heading.textContent = titleMeta.title;
    const chart = document.createElement("div");
    chart.className = "chart-surface";
    card.appendChild(heading);
    card.appendChild(chart);
    const controls = createChartControls({
      seriesKey: variable,
      cfg: config,
      onConfigChange: (nextConfig) => sanitizeVariableConfig(variable, nextConfig),
      onRender: () => renderCharts(),
    });
    controls.appendChild(createDownloadButton(`${variable}.csv`, xLabels, traces));
    card.appendChild(controls);
    dom.charts.appendChild(card);

    const pt = getPlotlyTheme();
    Plotly.newPlot(
      chart,
      traces,
      {
        margin: { t: 30, r: 12, b: 46, l: 54 },
        paper_bgcolor: pt.paper_bgcolor,
        plot_bgcolor: pt.plot_bgcolor,
        font: pt.font,
        xaxis: {
          title: { text: "Period" },
          automargin: true,
        },
        yaxis: {
          title: { text: titleMeta.units || variable },
          zerolinecolor: pt.zerolinecolor,
          gridcolor: pt.gridcolor,
          automargin: true,
        },
        legend: {
          orientation: "h",
          yanchor: "bottom",
          y: 1.02,
          xanchor: "left",
          x: 0,
        },
      },
      {
        responsive: true,
        displaylogo: false,
      },
    );
  }
}

function renderDictionary() {
  const query = `${dom.dictionarySearch.value || ""}`.trim().toLowerCase();
  const visibleVariables = query
    ? state.manifest.available_variables
    : state.selectedVariables.length > 0
      ? state.selectedVariables
      : state.manifest.available_variables;

  const cards = visibleVariables
    .map((variable) => getDictionaryRecord(variable))
    .filter((record) => {
      if (!query) {
        return true;
      }
      return [
        record.code,
        record.short_name,
        record.description,
        record.units,
      ]
        .join(" ")
        .toLowerCase()
        .includes(query);
    });

  dom.dictionaryResults.innerHTML = "";
  if (cards.length === 0) {
    dom.dictionaryResults.innerHTML = `<div class="empty-state">No variables match the current filter.</div>`;
    return;
  }

  for (const record of cards) {
    const card = document.createElement("article");
    card.className = "dict-card";
    const shortName = record.short_name ? `<span class="dict-short-name">${record.short_name}</span>` : "";
    card.innerHTML = `
      <div class="dict-header">
        <strong>${record.code}</strong>
        ${shortName}
        <span>${record.units || ""}</span>
      </div>
      <p>${record.description || "No description available."}</p>
    `;
    dom.dictionaryResults.appendChild(card);
  }
}

function parseEquationIdQuery(rawQuery) {
  const match = `${rawQuery || ""}`.trim().toUpperCase().match(/^(?:EQ(?:UATION)?\s*)?(\d+)$/);
  if (!match) {
    return null;
  }
  return Number.parseInt(match[1], 10);
}

function normalizeEquationRef(value) {
  if (value === null || value === undefined) {
    return null;
  }
  const normalized = `${value}`.trim();
  if (!normalized) {
    return null;
  }
  return normalized;
}

function collectEquationRefsForVariable(variable) {
  const record = getDictionaryRecord(variable);
  const defining = new Set();
  const usage = new Set();

  const linkedDefinition = normalizeEquationRef(record.defined_by_equation);
  if (linkedDefinition) {
    const linkedEquation = getEquationRecord(linkedDefinition);
    const linkedLhs = `${linkedEquation?.lhs_expr || ""}`.trim().toUpperCase();
    if (!linkedEquation || linkedLhs === variable) {
      defining.add(linkedDefinition);
    } else {
      usage.add(linkedDefinition);
    }
  }
  for (const value of Array.isArray(record.used_in_equations) ? record.used_in_equations : []) {
    const normalized = normalizeEquationRef(value);
    if (normalized) {
      usage.add(normalized);
    }
  }

  for (const equation of state.equationCatalog.values()) {
    const lhs = `${equation.lhs_expr || ""}`.trim().toUpperCase();
    const rhsVariables = Array.isArray(equation.rhs_variables)
      ? equation.rhs_variables.map((name) => `${name}`.trim().toUpperCase())
      : [];
    const equationId = normalizeEquationRef(equation.id);
    if (!equationId) {
      continue;
    }
    if (lhs === variable) {
      defining.add(equationId);
    }
    if (rhsVariables.includes(variable)) {
      usage.add(equationId);
    }
  }

  for (const definitionId of defining) {
    usage.delete(definitionId);
  }

  return {
    defining: [...defining],
    usage: [...usage],
  };
}

function compareEquationIds(left, right) {
  const leftIsNumeric = /^\d+$/.test(left);
  const rightIsNumeric = /^\d+$/.test(right);
  if (leftIsNumeric && rightIsNumeric) {
    return Number(left) - Number(right);
  }
  if (leftIsNumeric) {
    return -1;
  }
  if (rightIsNumeric) {
    return 1;
  }
  return left.localeCompare(right);
}

function equationSourceRunCount(equationId) {
  const record = getEquationRecord(equationId);
  return Array.isArray(record?.source_runs) ? record.source_runs.length : 0;
}

function equationFormulaLength(equationId) {
  const record = getEquationRecord(equationId);
  return `${record?.formula || ""}`.length;
}

function isStructuralModelEquation(record) {
  return record?.type === "scenario_equation" && record.model_eq_id !== undefined && record.model_eq_id !== null;
}

function compareDefiningEquationIds(left, right) {
  const leftRecord = getEquationRecord(left);
  const rightRecord = getEquationRecord(right);
  const leftStructural = isStructuralModelEquation(leftRecord);
  const rightStructural = isStructuralModelEquation(rightRecord);
  if (leftStructural !== rightStructural) {
    return leftStructural ? -1 : 1;
  }
  const leftRuns = equationSourceRunCount(left);
  const rightRuns = equationSourceRunCount(right);
  if ((leftRuns > 0) !== (rightRuns > 0)) {
    return leftRuns > 0 ? -1 : 1;
  }
  if (leftRuns !== rightRuns) {
    return rightRuns - leftRuns;
  }
  const leftLength = equationFormulaLength(left);
  const rightLength = equationFormulaLength(right);
  if (leftLength !== rightLength) {
    return rightLength - leftLength;
  }
  return compareEquationIds(left, right);
}

function getPfPriceEquationIds() {
  return [...state.equationCatalog.values()]
    .filter((equation) => {
      const lhs = `${equation.lhs_expr || ""}`.trim().toUpperCase();
      const formula = `${equation.formula || ""}`.trim().toUpperCase();
      return lhs === "LPF" && isStructuralModelEquation(equation) && formula.startsWith("LPF ");
    })
    .map((equation) => `${equation.id}`)
    .sort(compareDefiningEquationIds);
}

function isPfPriceEquationQuery(rawQuery) {
  const normalized = `${rawQuery || ""}`.trim().toUpperCase().replace(/\s+/g, " ");
  return ["PF EQUATION", "PRICE EQUATION", "LPF LPF", "EQ 10 LPF"].includes(normalized);
}

function collectDirectInputDefinitionIds(equationIds, rootVariable) {
  const inputDefinitionIds = new Set();
  const rootIds = new Set(equationIds.map((value) => `${value}`));
  const root = `${rootVariable || ""}`.trim().toUpperCase();
  for (const equationId of rootIds) {
    const equation = getEquationRecord(equationId);
    const rhsVariables = Array.isArray(equation?.rhs_variables) ? equation.rhs_variables : [];
    for (const rhsVariable of rhsVariables) {
      const variable = `${rhsVariable}`.trim().toUpperCase();
      if (!variable || !state.manifest.available_variables.includes(variable)) {
        continue;
      }
      for (const definitionId of collectEquationRefsForVariable(variable).defining) {
        const normalized = `${definitionId}`;
        const definition = getEquationRecord(normalized);
        const definitionRhsVariables = Array.isArray(definition?.rhs_variables)
          ? definition.rhs_variables.map((name) => `${name}`.trim().toUpperCase())
          : [];
        if (!rootIds.has(normalized) && (!root || !definitionRhsVariables.includes(root))) {
          inputDefinitionIds.add(normalized);
        }
      }
    }
  }
  return [...inputDefinitionIds].sort(compareDefiningEquationIds);
}

function formatEquationId(record) {
  if (record.display_id) {
    return record.display_id;
  }
  if (/^\d+$/.test(`${record.id}`)) {
    return `Eq ${record.id}`;
  }
  return `${record.id}`;
}

function formatSourceRuns(record) {
  const runs = Array.isArray(record.source_runs)
    ? record.source_runs.map((item) => `${item}`.trim()).filter(Boolean)
    : [];
  return runs.length > 0 ? `Runs: ${runs.join(", ")}` : "";
}

function createTextElement(tagName, text, className = "") {
  const element = document.createElement(tagName);
  if (className) {
    element.className = className;
  }
  element.textContent = text;
  return element;
}

function formatEquationText(value) {
  return `${value || ""}`
    .replace(/\\{2,}/g, "\n")
    .replace(/\\\(/g, "")
    .replace(/\\\)/g, "")
    .replace(/\\cdot/g, " x ")
    .replace(/\\left/g, "")
    .replace(/\\right/g, "")
    .replace(/\\frac\s*\{([^{}]+)\}\s*\{([^{}]+)\}/g, "($1)/($2)")
    .replace(/\\frac\s*\{/g, "")
    .replace(/\{tabular\}/g, "")
    .replace(/·/g, " x ")
    .replace(/[ \t\f\v]+/g, " ")
    .replace(/\s*\n\s*/g, "\n")
    .replace(/\\+\s*(?=\n|$)/g, "")
    .trim();
}

function createEquationCard(record, note = "") {
  const card = document.createElement("article");
  card.className = "equation-card";

  const meta = document.createElement("div");
  meta.className = "equation-meta";
  meta.appendChild(createTextElement("span", formatEquationId(record), "equation-id"));
  meta.appendChild(
    createTextElement(
      "span",
      record.type ? record.type.replace(/_/g, " ") : "Equation",
      "equation-type",
    ),
  );
  card.appendChild(meta);

  const heading = createTextElement(
    "h4",
    formatEquationText(record.label || record.lhs_expr || `Equation ${record.id}`),
  );
  card.appendChild(heading);

  const lhs = createTextElement(
    "p",
    `LHS: ${formatEquationText(record.lhs_expr) || "Unavailable"}`,
    "equation-code",
  );
  card.appendChild(lhs);

  const formula = createTextElement(
    "p",
    `Formula:\n${formatEquationText(record.formula) || "Unavailable"}`,
    "equation-code",
  );
  card.appendChild(formula);

  if (Array.isArray(record.rhs_variables) && record.rhs_variables.length > 0) {
    card.appendChild(
      createTextElement(
        "p",
        `Variables: ${record.rhs_variables.join(", ")}`,
        "equation-note",
      ),
    );
  }

  const sourceRuns = formatSourceRuns(record);
  if (sourceRuns) {
    card.appendChild(createTextElement("p", sourceRuns, "equation-note"));
  }

  if (note) {
    card.appendChild(createTextElement("p", note, "equation-note"));
  }

  return card;
}

function createMissingEquationCard(eqId, note) {
  const card = document.createElement("article");
  card.className = "equation-card";
  const heading = createTextElement("h4", /^\d+$/.test(`${eqId}`) ? `Eq ${eqId}` : `${eqId}`);
  const copy = createTextElement(
    "p",
    note || "This equation id is referenced by the exported dictionary, but no equation body was exported.",
    "equation-note",
  );
  card.appendChild(heading);
  card.appendChild(copy);
  return card;
}

function appendEquationGroup(container, title, cards) {
  if (cards.length === 0) {
    return;
  }
  const section = document.createElement("section");
  section.className = "equation-group";
  section.appendChild(createTextElement("h3", title));
  for (const card of cards) {
    section.appendChild(card);
  }
  container.appendChild(section);
}

function createPfPriceEquationCards(note) {
  return getPfPriceEquationIds()
    .map((equationId) => getEquationRecord(equationId))
    .filter(Boolean)
    .map((equation) => createEquationCard(equation, note));
}

function renderEquationExplorerEmpty(message) {
  dom.equationExplorerResults.innerHTML = "";
  const empty = document.createElement("div");
  empty.className = "empty-state";
  empty.textContent = message;
  dom.equationExplorerResults.appendChild(empty);
}

function renderPfPriceEquationLookup() {
  dom.equationExplorerResults.innerHTML = "";
  appendEquationGroup(
    dom.equationExplorerResults,
    "PF Price Equations (LPF)",
    createPfPriceEquationCards("This is the structural price equation; PF is EXP(LPF)."),
  );
}

function renderVariableEquationLookup(variable) {
  dom.equationExplorerResults.innerHTML = "";
  const record = getDictionaryRecord(variable);
  const summary = document.createElement("article");
  summary.className = "dict-card";
  summary.appendChild(
    createTextElement(
      "p",
      `${variable}${record.short_name ? ` — ${record.short_name}` : ""}`,
    ),
  );
  summary.appendChild(
    createTextElement(
      "p",
      record.description || "No variable description is available in the exported dictionary.",
    ),
  );
  dom.equationExplorerResults.appendChild(summary);

  const refs = collectEquationRefsForVariable(variable);
  const definingIds = refs.defining.sort(compareDefiningEquationIds);
  const usedIds = refs.usage.sort(compareEquationIds);

  const definingCards = [];
  for (const definingId of definingIds) {
    const equation = getEquationRecord(definingId);
    definingCards.push(
      equation
        ? createEquationCard(equation, `Defines ${variable}.`)
        : createMissingEquationCard(
          definingId,
          `Eq ${definingId} is referenced as the defining equation for ${variable}, but no equation body was exported.`,
        ),
    );
  }

  const usageCards = [];
  for (const eqId of usedIds) {
    const equation = getEquationRecord(eqId);
    usageCards.push(
      equation
        ? createEquationCard(equation, `${variable} appears in this equation.`)
        : createMissingEquationCard(
          eqId,
          `Eq ${eqId} is referenced as using ${variable}, but no equation body was exported.`,
        ),
    );
  }

  appendEquationGroup(dom.equationExplorerResults, "Defines This Variable", definingCards);

  if (variable === "PF" || variable === "LPF") {
    appendEquationGroup(
      dom.equationExplorerResults,
      "PF Price Equations (LPF)",
      createPfPriceEquationCards(variable === "PF" ? "This is the structural price equation; PF is EXP(LPF)." : "This is the structural price equation."),
    );
  }

  const directInputCards = [];
  if (variable !== "PF" && variable !== "LPF") {
    for (const inputDefinitionId of collectDirectInputDefinitionIds(definingIds, variable)) {
      const equation = getEquationRecord(inputDefinitionId);
      if (equation) {
        directInputCards.push(createEquationCard(equation, `Defines an input used to define ${variable}.`));
      }
    }
  }
  appendEquationGroup(dom.equationExplorerResults, "Direct Input Definitions", directInputCards);

  appendEquationGroup(dom.equationExplorerResults, "Uses This Variable", usageCards);

  if (definingCards.length === 0 && directInputCards.length === 0 && usageCards.length === 0) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent =
      `${variable} has no exported model-equation links. It may be a derived, helper, or scenario-only series.`;
    dom.equationExplorerResults.appendChild(empty);
  }
}

function scoreEquationMatch(record, rawQuery) {
  const query = rawQuery.toLowerCase();
  const queryUpper = rawQuery.toUpperCase();
  const sourceRunCount = Array.isArray(record.source_runs) ? record.source_runs.length : 0;
  let score = 0;
  if ((record.label || "").toUpperCase() === queryUpper) score += 120;
  if ((record.lhs_expr || "").toUpperCase() === queryUpper) score += 110;
  if ((record.lhs_expr || "").toUpperCase() === queryUpper && sourceRunCount > 0) score += 140;
  if ((record.rhs_variables || []).includes(queryUpper)) score += 100;
  if (`${record.id}` === rawQuery || `${record.display_id || ""}`.toUpperCase() === queryUpper) score += 95;
  if ((record.label || "").toLowerCase().startsWith(query)) score += 20;
  if ((record.lhs_expr || "").toLowerCase().includes(query)) score += 15;
  if ((record.formula || "").toLowerCase().includes(query)) score += 10;
  if ((`${record.display_id || ""}`).toLowerCase().includes(query)) score += 12;
  if (sourceRunCount > 0) score += 5;
  if ((record.rhs_variables || []).some((name) => `${name}`.toLowerCase().includes(query))) {
    score += 8;
  }
  return score;
}

function renderEquationExplorer() {
  if (!state.manifest) {
    renderEquationExplorerEmpty("Loading exported equation metadata...");
    return;
  }
  const rawQuery = `${dom.equationSearch.value || ""}`.trim();
  if (!rawQuery) {
    renderEquationExplorerEmpty("Search for a variable like GDP or an equation like 82.");
    return;
  }

  if (isPfPriceEquationQuery(rawQuery)) {
    renderPfPriceEquationLookup();
    return;
  }

  const exactVariable = rawQuery.toUpperCase();
  if (state.manifest.available_variables.includes(exactVariable)) {
    renderVariableEquationLookup(exactVariable);
    return;
  }

  const exactEquationId = parseEquationIdQuery(rawQuery);
  if (exactEquationId !== null) {
    const equation = getEquationRecord(exactEquationId);
    dom.equationExplorerResults.innerHTML = "";
    dom.equationExplorerResults.appendChild(
      equation
        ? createEquationCard(equation)
        : createMissingEquationCard(
          exactEquationId,
          `Eq ${exactEquationId} is not present in the exported equation catalog for this bundle.`,
        ),
    );
    return;
  }

  const matches = [...state.equationCatalog.values()]
    .filter((record) => {
      const haystack = [
        `${record.id}`,
        record.display_id,
        record.label,
        record.lhs_expr,
        record.formula,
        ...(record.source_runs || []),
        ...(record.rhs_variables || []),
      ]
        .join(" ")
        .toLowerCase();
      return haystack.includes(rawQuery.toLowerCase());
    })
    .sort((left, right) => {
      const scoreDelta = scoreEquationMatch(right, rawQuery) - scoreEquationMatch(left, rawQuery);
      if (scoreDelta !== 0) {
        return scoreDelta;
      }
      return compareEquationIds(`${left.id}`, `${right.id}`);
    });

  if (matches.length === 0) {
    renderEquationExplorerEmpty(`No exported equations match "${rawQuery}".`);
    return;
  }

  dom.equationExplorerResults.innerHTML = "";
  appendEquationGroup(
    dom.equationExplorerResults,
    `Matching Equations (${matches.length})`,
    matches.map((record) => createEquationCard(record)),
  );
}

function applyDeepLinkedEquationSearch() {
  const params = new URLSearchParams(window.location.search);
  const query = params.get("eq") || params.get("equation");
  if (!query || !dom.equationSearch) {
    return;
  }
  dom.equationSearch.value = query;
}

function addEquation(expression) {
  const trimmed = expression.trim();
  if (!trimmed) return;

  dom.equationError.hidden = true;

  let ast;
  try {
    const tokens = tokenize(trimmed);
    ast = parseExpression(tokens);
  } catch (err) {
    dom.equationError.textContent = `Parse error: ${err.message}`;
    dom.equationError.hidden = false;
    return;
  }

  const vars = extractVariables(ast);
  const missing = vars.filter((v) => !state.manifest.available_variables.includes(v));
  if (missing.length > 0) {
    dom.equationError.textContent = `Unknown variable${missing.length > 1 ? "s" : ""}: ${missing.join(", ")}`;
    dom.equationError.hidden = false;
    return;
  }

  equationIdCounter++;
  state.equations.push({ id: equationIdCounter, expression: trimmed, ast });
  sanitizeEquationConfig(equationIdCounter, state.equationConfigs.get(equationIdCounter));
  dom.equationInput.value = "";
  renderEquationCharts();
}

function removeEquation(id) {
  state.equations = state.equations.filter((eq) => eq.id !== id);
  state.equationConfigs.delete(id);
  renderEquationCharts();
}

function renderEquationCharts() {
  dom.equationCharts.innerHTML = "";
  if (state.equations.length === 0) return;

  const selectedRuns = state.selectedRunIds
    .map((runId) => ({ meta: getRunMeta(runId), payload: state.runCache.get(runId) }))
    .filter((item) => item.meta && item.payload);

  if (selectedRuns.length === 0) return;

  const periods = sortPeriodTokens(
    unique(selectedRuns.flatMap((item) => item.payload.periods || [])),
  );
  const xLabels = periods.map(formatPeriodToken);

  for (const eq of state.equations) {
    const config = getEquationConfig(eq.id);
    const titleMeta = buildSeriesTitle(eq.expression, "", config);
    const transformedByRun = new Map();

    for (const item of selectedRuns) {
      const periodIndex = new Map(item.payload.periods.map((p, i) => [p, i]));
      const baseValues = periods.map((period) => {
        const idx = periodIndex.get(period);
        if (idx === undefined) return null;
        return evaluateAst(eq.ast, (varName) => {
          const series = item.payload.series[varName];
          if (!series || idx >= series.length) return null;
          return valueOrNull(series[idx]);
        });
      });
      let denominatorValues = periods.map(() => null);
      if (config.transformMode === TRANSFORM_PCT_OF) {
        denominatorValues = alignSeries(item.payload, config.denominator, periods);
      }
      transformedByRun.set(
        item.meta.run_id,
        transformSeries(baseValues, denominatorValues, config.transformMode),
      );
    }

    const referenceValues = transformedByRun.get(config.referenceRunId) || null;
    const traces = [];
    for (const item of selectedRuns) {
      if (config.compareMode !== COMPARE_NONE && item.meta.run_id === config.referenceRunId) {
        continue;
      }
      const values = applyRunComparison(
        transformedByRun.get(item.meta.run_id) || periods.map(() => null),
        referenceValues,
        config.compareMode,
      );
      traces.push({
        type: "scatter",
        mode: "lines+markers",
        name: item.meta.label,
        x: xLabels,
        y: values,
        hovertemplate: `<b>${item.meta.label}</b><br>%{x}<br>%{y:,.4f}<extra></extra>`,
        connectgaps: false,
      });
    }

    const card = document.createElement("article");
    card.className = "chart-card";

    const header = document.createElement("div");
    header.style.display = "flex";
    header.style.justifyContent = "space-between";
    header.style.alignItems = "center";

    const heading = document.createElement("h3");
    heading.textContent = titleMeta.title;

    const removeBtn = document.createElement("button");
    removeBtn.className = "equation-remove";
    removeBtn.textContent = "\u00d7";
    removeBtn.title = "Remove equation chart";
    removeBtn.addEventListener("click", () => removeEquation(eq.id));

    header.appendChild(heading);
    header.appendChild(removeBtn);

    const chart = document.createElement("div");
    chart.className = "chart-surface";

    card.appendChild(header);
    card.appendChild(chart);
    const safeName = eq.expression.replace(/[^A-Za-z0-9]/g, "_").replace(/_+/g, "_").replace(/^_|_$/g, "");
    const eqControls = createChartControls({
      seriesKey: eq.expression,
      cfg: config,
      onConfigChange: (nextConfig) => sanitizeEquationConfig(eq.id, nextConfig),
      onRender: () => renderEquationCharts(),
    });
    eqControls.appendChild(createDownloadButton(`${safeName}.csv`, xLabels, traces));
    card.appendChild(eqControls);
    dom.equationCharts.appendChild(card);

    const pt2 = getPlotlyTheme();
    Plotly.newPlot(
      chart,
      traces,
      {
        margin: { t: 30, r: 12, b: 46, l: 54 },
        paper_bgcolor: pt2.paper_bgcolor,
        plot_bgcolor: pt2.plot_bgcolor,
        font: pt2.font,
        xaxis: { title: { text: "Period" }, automargin: true },
        yaxis: {
          title: { text: titleMeta.units || eq.expression },
          zerolinecolor: pt2.zerolinecolor,
          gridcolor: pt2.gridcolor,
          automargin: true,
        },
        legend: { orientation: "h", yanchor: "bottom", y: 1.02, xanchor: "left", x: 0 },
      },
      { responsive: true, displaylogo: false },
    );
  }
}

async function initialize() {
  const manifest = await fetchJson("./manifest.json");
  const presetsPayload = await fetchJson(manifest.presets_path);
  const dictionaryPayload = await fetchJson(manifest.dictionary_path);

  state.manifest = manifest;
  state.runMeta = manifest.runs || [];
  state.presets = Array.isArray(presetsPayload)
    ? presetsPayload
    : (presetsPayload.presets || []);
  state.dictionary = new Map(
    Object.entries(dictionaryPayload.variables || {}).map(([key, value]) => [key, value]),
  );
  state.equationCatalog = new Map(
    Object.values(dictionaryPayload.equations || {}).map((value) => [`${value.id}`, value]),
  );
  const horizonIds = getAvailableHorizonIds();
  state.activeHorizonId = horizonIds[0] || "";
  state.selectedRunIds = normalizeSelectedRunIdsForActiveHorizon(manifest.default_run_ids || []);
  state.selectedPresetIds = [...(manifest.default_preset_ids || [])];

  const selectedPresets = state.presets.filter((preset) => state.selectedPresetIds.includes(preset.id));
  state.selectedVariables = unique(selectedPresets.flatMap((item) => item.variables || []));
  if (state.selectedVariables.length === 0) {
    state.selectedVariables = manifest.available_variables.slice(0, 6);
  }

  dom.pageTitle.textContent = manifest.title || "Model Runs Explorer";

  syncHorizonControls();
  syncRunSelectExpansion();
  syncRunSelect();
  syncPresetSelect();
  syncVariableSelect();
  state.selectedRunInfoId = state.selectedRunIds[0] || getVisibleRunMeta()[0]?.run_id || "";
  syncRunInfoSelect();

  await ensureRunsLoaded(state.selectedRunIds);
  renderRunInfo();
  renderCharts();
  renderDictionary();
  applyDeepLinkedEquationSearch();
  renderEquationExplorer();
}

dom.runSelectToggle?.addEventListener("click", () => {
  setRunSelectExpanded(!state.runSelectExpanded);
});

dom.runPanelBackdrop?.addEventListener("click", () => {
  collapseRunSelect();
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    collapseRunSelect();
  }
});

document.addEventListener("pointerdown", (event) => {
  if (!state.runSelectExpanded || !dom.runPanel || !dom.runSelectToggle) {
    return;
  }
  const target = event.target;
  if (!(target instanceof Node)) {
    return;
  }
  if (dom.runPanel.contains(target) || dom.runSelectToggle.contains(target)) {
    return;
  }
  collapseRunSelect();
});

window.addEventListener("resize", () => {
  if (state.runSelectExpanded) {
    syncRunSelect();
  }
});

dom.runInfoSelect.addEventListener("change", () => {
  state.selectedRunInfoId = dom.runInfoSelect.value;
  renderRunInfo();
});

dom.applyPresetButton.addEventListener("click", async () => {
  applyPresetSelection();
  await ensureRunsLoaded(state.selectedRunIds);
  renderDictionary();
});

dom.variableSearch.addEventListener("input", () => {
  state.variableSearchQuery = `${dom.variableSearch.value || ""}`;
  syncVariableSelect();
});

dom.dictionarySearch.addEventListener("input", () => {
  renderDictionary();
});

dom.equationSearch.addEventListener("input", () => {
  renderEquationExplorer();
});

dom.equationPlotButton.addEventListener("click", () => {
  addEquation(dom.equationInput.value);
});

if (dom.themeToggle) {
  dom.themeToggle.addEventListener("click", toggleTheme);
}

dom.equationInput.addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    addEquation(dom.equationInput.value);
  }
});

initialize().catch((error) => {
  console.error(error);
  dom.chartEmpty.hidden = false;
  dom.chartEmpty.textContent = `Failed to load the exported bundle: ${error.message}`;
});
