const TRANSFORM_LEVEL = "level";
const TRANSFORM_PCT_OF = "pct_of";
const TRANSFORM_LVL_CHANGE = "lvl_change";
const TRANSFORM_PCT_CHANGE = "pct_change";

const COMPARE_NONE = "none";
const COMPARE_DIFF_VS_RUN = "diff_vs_run";
const COMPARE_PCT_DIFF_VS_RUN = "pct_diff_vs_run";

const DEFAULT_DENOMINATOR = "GDP";

const state = {
  manifest: null,
  presets: [],
  dictionary: new Map(),
  runMeta: [],
  selectedRunIds: [],
  selectedPresetIds: [],
  selectedVariables: [],
  runCache: new Map(),
  variableConfigs: new Map(),
  variableSearchQuery: "",
  selectedRunInfoId: "",
};

const dom = {
  pageTitle: document.querySelector("#pageTitle"),
  generatedAt: document.querySelector("#generatedAt"),
  runCount: document.querySelector("#runCount"),
  variableCount: document.querySelector("#variableCount"),
  runSelect: document.querySelector("#runSelect"),
  presetSelect: document.querySelector("#presetSelect"),
  applyPresetButton: document.querySelector("#applyPresetButton"),
  variableSearch: document.querySelector("#variableSearch"),
  variableSelect: document.querySelector("#variableSelect"),
  runInfoSelect: document.querySelector("#runInfoSelect"),
  runInfo: document.querySelector("#runInfo"),
  variableControls: document.querySelector("#variableControls"),
  charts: document.querySelector("#charts"),
  chartEmpty: document.querySelector("#chartEmpty"),
  dictionarySearch: document.querySelector("#dictionarySearch"),
  dictionaryResults: document.querySelector("#dictionaryResults"),
};

function resolveAssetUrl(relativePath) {
  return new URL(relativePath, window.location.href).toString();
}

async function fetchJson(relativePath) {
  const response = await fetch(resolveAssetUrl(relativePath));
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
  };
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

function sanitizeVariableConfig(variable, input) {
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
  state.variableConfigs.set(variable, next);
  return next;
}

function getVariableConfig(variable) {
  return sanitizeVariableConfig(variable, state.variableConfigs.get(variable));
}

function applyPresetSelection() {
  const selectedPresetIds = [...dom.presetSelect.selectedOptions].map((item) => item.value);
  state.selectedPresetIds = selectedPresetIds;
  const selectedPresets = state.presets.filter((item) => selectedPresetIds.includes(item.id));
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
  renderVariableControls();
  renderCharts();
}

function syncRunSelect() {
  dom.runSelect.innerHTML = "";
  for (const run of state.runMeta) {
    const option = document.createElement("option");
    option.value = run.run_id;
    option.textContent = run.label;
    option.selected = state.selectedRunIds.includes(run.run_id);
    dom.runSelect.appendChild(option);
  }
}

function syncPresetSelect() {
  dom.presetSelect.innerHTML = "";
  for (const preset of state.presets) {
    const option = document.createElement("option");
    option.value = preset.id;
    option.textContent = preset.label;
    option.selected = state.selectedPresetIds.includes(preset.id);
    dom.presetSelect.appendChild(option);
  }
}

function syncRunInfoSelect() {
  dom.runInfoSelect.innerHTML = "";
  for (const run of state.runMeta) {
    const option = document.createElement("option");
    option.value = run.run_id;
    option.textContent = run.label;
    option.selected = run.run_id === state.selectedRunInfoId;
    dom.runInfoSelect.appendChild(option);
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
    const option = document.createElement("option");
    option.value = variable;
    option.textContent = record.short_name ? `${variable} - ${record.short_name}` : variable;
    option.selected = state.selectedVariables.includes(variable);
    dom.variableSelect.appendChild(option);
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

function renderVariableControls() {
  dom.variableControls.innerHTML = "";
  for (const variable of state.selectedVariables) {
    const record = getDictionaryRecord(variable);
    const cfg = getVariableConfig(variable);
    const wrapper = document.createElement("article");
    wrapper.className = "var-card";

    const header = document.createElement("div");
    header.className = "var-card-header";
    header.innerHTML = `
      <strong>${variable}</strong>
      <span>${record.short_name || ""}</span>
    `;

    const grid = document.createElement("div");
    grid.className = "var-grid";
    grid.appendChild(
      buildCompactControl({
        label: "Transform",
        value: cfg.transformMode,
        variable,
        options: [
          { value: TRANSFORM_LEVEL, label: "Level" },
          { value: TRANSFORM_PCT_OF, label: "% of denominator" },
          { value: TRANSFORM_LVL_CHANGE, label: "Lvl change" },
          { value: TRANSFORM_PCT_CHANGE, label: "% change" },
        ],
        onChange: (event) => {
          sanitizeVariableConfig(variable, {
            ...cfg,
            transformMode: event.target.value,
          });
          renderVariableControls();
          renderCharts();
        },
      }),
    );

    if (cfg.transformMode === TRANSFORM_PCT_OF) {
      grid.appendChild(
        buildCompactControl({
          label: "Denom",
          value: cfg.denominator,
          variable,
          options: state.manifest.available_variables.map((name) => ({
            value: name,
            label: name,
          })),
          onChange: (event) => {
            sanitizeVariableConfig(variable, {
              ...cfg,
              denominator: event.target.value,
            });
            renderCharts();
          },
        }),
      );
    }

    grid.appendChild(
      buildCompactControl({
        label: "Compare",
        value: cfg.compareMode,
        variable,
        options: [
          { value: COMPARE_NONE, label: "None" },
          { value: COMPARE_DIFF_VS_RUN, label: "Diff vs run" },
          { value: COMPARE_PCT_DIFF_VS_RUN, label: "% diff vs run" },
        ],
        onChange: (event) => {
          sanitizeVariableConfig(variable, {
            ...cfg,
            compareMode: event.target.value,
          });
          renderVariableControls();
          renderCharts();
        },
      }),
    );

    if (cfg.compareMode !== COMPARE_NONE && state.selectedRunIds.length > 1) {
      grid.appendChild(
        buildCompactControl({
          label: "Ref",
          value: cfg.referenceRunId,
          variable,
          options: state.runMeta.map((run) => ({
            value: run.run_id,
            label: run.label,
          })),
          onChange: (event) => {
            sanitizeVariableConfig(variable, {
              ...cfg,
              referenceRunId: event.target.value,
            });
            renderCharts();
          },
        }),
      );
    }

    wrapper.appendChild(header);
    wrapper.appendChild(grid);
    dom.variableControls.appendChild(wrapper);
  }
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

function buildChartTitle(variable, config) {
  const record = getDictionaryRecord(variable);
  let title = record.short_name || variable;
  let units = record.units || "";
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
    dom.charts.appendChild(card);

    Plotly.newPlot(
      chart,
      traces,
      {
        margin: { t: 30, r: 12, b: 46, l: 54 },
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(255,255,255,0.72)",
        font: { family: "IBM Plex Sans, sans-serif", color: "#18201b" },
        xaxis: {
          title: { text: "Period" },
          automargin: true,
        },
        yaxis: {
          title: { text: titleMeta.units || variable },
          zerolinecolor: "rgba(24,32,27,0.16)",
          gridcolor: "rgba(24,32,27,0.08)",
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
  const visibleVariables = state.selectedVariables.length > 0
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
    card.innerHTML = `
      <div class="dict-header">
        <strong>${record.code}</strong>
        <span>${record.units || "Units unavailable"}</span>
      </div>
      <p><strong>${record.short_name || "No short name"}</strong></p>
      <p>${record.description || "No description available in the exported dictionary."}</p>
    `;
    dom.dictionaryResults.appendChild(card);
  }
}

async function initialize() {
  const manifest = await fetchJson("./manifest.json");
  const presetsPayload = await fetchJson(manifest.presets_path);
  const dictionaryPayload = await fetchJson(manifest.dictionary_path);

  state.manifest = manifest;
  state.runMeta = manifest.runs || [];
  state.presets = presetsPayload.presets || [];
  state.dictionary = new Map(
    Object.entries(dictionaryPayload.variables || {}).map(([key, value]) => [key, value]),
  );
  state.selectedRunIds = [...(manifest.default_run_ids || [])];
  state.selectedPresetIds = [...(manifest.default_preset_ids || [])];

  const selectedPresets = state.presets.filter((preset) => state.selectedPresetIds.includes(preset.id));
  state.selectedVariables = unique(selectedPresets.flatMap((item) => item.variables || []));
  if (state.selectedVariables.length === 0) {
    state.selectedVariables = manifest.available_variables.slice(0, 6);
  }

  dom.pageTitle.textContent = manifest.title || "Model Runs Explorer";
  dom.generatedAt.textContent = manifest.generated_at || "Unknown";
  dom.runCount.textContent = `${state.runMeta.length}`;
  dom.variableCount.textContent = `${manifest.available_variables.length}`;

  syncRunSelect();
  syncPresetSelect();
  syncVariableSelect();
  state.selectedRunInfoId = state.selectedRunIds[0] || state.runMeta[0]?.run_id || "";
  syncRunInfoSelect();

  await ensureRunsLoaded(state.selectedRunIds);
  renderRunInfo();
  renderVariableControls();
  renderCharts();
  renderDictionary();
}

dom.runSelect.addEventListener("change", async () => {
  state.selectedRunIds = [...dom.runSelect.selectedOptions].map((item) => item.value);
  await ensureRunsLoaded(state.selectedRunIds);
  for (const variable of state.selectedVariables) {
    sanitizeVariableConfig(variable, state.variableConfigs.get(variable));
  }
  renderVariableControls();
  renderCharts();
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

dom.variableSelect.addEventListener("change", () => {
  const visibleVariables = [...dom.variableSelect.options].map((item) => item.value);
  const hiddenSelected = state.selectedVariables.filter((variable) => !visibleVariables.includes(variable));
  const visibleSelected = [...dom.variableSelect.selectedOptions].map((item) => item.value);
  state.selectedVariables = unique([...hiddenSelected, ...visibleSelected]);
  const nextConfigs = new Map();
  for (const variable of state.selectedVariables) {
    nextConfigs.set(variable, sanitizeVariableConfig(variable, state.variableConfigs.get(variable)));
  }
  state.variableConfigs = nextConfigs;
  renderVariableControls();
  renderCharts();
  renderDictionary();
});

dom.variableSearch.addEventListener("input", () => {
  state.variableSearchQuery = `${dom.variableSearch.value || ""}`;
  syncVariableSelect();
});

dom.dictionarySearch.addEventListener("input", () => {
  renderDictionary();
});

initialize().catch((error) => {
  console.error(error);
  dom.chartEmpty.hidden = false;
  dom.chartEmpty.textContent = `Failed to load the exported bundle: ${error.message}`;
});
