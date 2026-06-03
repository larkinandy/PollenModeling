const fs = require("fs");
const path = require("path");

const outputRoot = path.join(__dirname, "outputs");
const inputPath = path.join(outputRoot, "meeker_pollen_hierarchy_daily_concentrations.csv");
const outputPath = path.join(outputRoot, "meeker_pollen_hierarchy_area_timeseries.svg");

const levels = [
  { key: "General pollen", color: "#0f766e" },
  { key: "Broad pollen type", color: "#c2410c" },
  { key: "Genus/family", color: "#7e22ce" },
  { key: "Species/subspecies", color: "#78350f" },
];

function parseCsvLine(line) {
  const values = [];
  let value = "";
  let quoted = false;

  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    const next = line[i + 1];

    if (char === '"' && quoted && next === '"') {
      value += '"';
      i++;
    } else if (char === '"') {
      quoted = !quoted;
    } else if (char === "," && !quoted) {
      values.push(value);
      value = "";
    } else {
      value += char;
    }
  }

  values.push(value);
  return values;
}

function esc(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function fmt(value, digits = 1) {
  return Number(value).toLocaleString("en-US", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function dateText(date) {
  return date.toISOString().slice(5, 10);
}

if (!fs.existsSync(inputPath)) {
  throw new Error(`Missing input CSV: ${inputPath}`);
}

const text = fs.readFileSync(inputPath, "utf8").replace(/\0/g, "").replace(/^\uFEFF/, "").trim();
if (!text) {
  throw new Error(`Input CSV has no rows: ${inputPath}`);
}

const lines = text.split(/\r?\n/);
const header = parseCsvLine(lines[0]).map((key) => key.replace(/^[^\w]+/, ""));
const rows = lines.slice(1).map((line) => {
  const values = parseCsvLine(line);
  return Object.fromEntries(header.map((key, index) => [key, values[index]]));
});

const byDate = new Map();
for (const row of rows) {
  const dateKey = row.metric_date;
  if (!byDate.has(dateKey)) {
    byDate.set(dateKey, {
      dateText: dateKey,
      date: new Date(`${dateKey}T00:00:00Z`),
      nFlowMeasurements: Number(row.n_flow_measurements),
      values: Object.fromEntries(levels.map((level) => [level.key, 0])),
    });
  }
  byDate.get(dateKey).values[row.hierarchy_level] = Number(row.concentration);
}

const data = [...byDate.values()].sort((a, b) => a.date - b.date);
const width = 1260;
const height = 760;
const margin = { top: 92, right: 245, bottom: 96, left: 92 };
const plotWidth = width - margin.left - margin.right;
const plotHeight = height - margin.top - margin.bottom;
const minDate = data[0].date;
const maxDate = data[data.length - 1].date;
const dayMs = 24 * 60 * 60 * 1000;
const span = Math.max(dayMs, maxDate.getTime() - minDate.getTime());

let yMax = Math.max(
  ...data.map((row) => levels.reduce((total, level) => total + row.values[level.key], 0))
);
yMax = Math.ceil(yMax / 10) * 10 || 10;

function xScale(date) {
  return margin.left + ((date.getTime() - minDate.getTime()) / span) * plotWidth;
}

function yScale(value) {
  return margin.top + plotHeight - (value / yMax) * plotHeight;
}

const stacked = data.map((row) => {
  let low = 0;
  const bands = {};
  for (const level of levels) {
    const high = low + row.values[level.key];
    bands[level.key] = { low, high };
    low = high;
  }
  return { ...row, bands, total: low };
});

function areaPath(levelKey) {
  const top = stacked
    .map((row) => `${xScale(row.date).toFixed(2)},${yScale(row.bands[levelKey].high).toFixed(2)}`)
    .join(" L ");
  const bottom = [...stacked]
    .reverse()
    .map((row) => `${xScale(row.date).toFixed(2)},${yScale(row.bands[levelKey].low).toFixed(2)}`)
    .join(" L ");
  return `M ${top} L ${bottom} Z`;
}

const xTicks = [];
for (
  let tick = new Date(Date.UTC(minDate.getUTCFullYear(), minDate.getUTCMonth(), minDate.getUTCDate()));
  tick <= maxDate;
  tick = new Date(tick.getTime() + 7 * dayMs)
) {
  xTicks.push(tick);
}

const yStep = Math.max(10, Math.ceil(yMax / 6 / 10) * 10);
const yTicks = [];
for (let value = 0; value <= yMax; value += yStep) yTicks.push(value);
if (yTicks[yTicks.length - 1] !== yMax) yTicks.push(yMax);

let svg = `<?xml version="1.0" encoding="UTF-8"?>\n`;
svg += `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">\n`;
svg += `<rect width="100%" height="100%" fill="#ffffff"/>\n`;
svg += `<text x="${margin.left}" y="42" font-family="Arial, sans-serif" font-size="26" font-weight="700" fill="#111827">Meeker Pollen by Prediction Specificity</text>\n`;
svg += `<text x="${margin.left}" y="68" font-family="Arial, sans-serif" font-size="15" fill="#4b5563">Ann Arbor, MI; mutually exclusive POL hierarchy levels, ${data[0].dateText} to ${data[data.length - 1].dateText}</text>\n`;

for (const value of yTicks) {
  const y = yScale(value);
  svg += `<line x1="${margin.left}" y1="${y.toFixed(2)}" x2="${margin.left + plotWidth}" y2="${y.toFixed(2)}" stroke="#e5e7eb" stroke-width="1"/>\n`;
  svg += `<text x="${margin.left - 12}" y="${(y + 5).toFixed(2)}" text-anchor="end" font-family="Arial, sans-serif" font-size="12" fill="#374151">${value}</text>\n`;
}

for (const tick of xTicks) {
  const x = xScale(tick);
  svg += `<line x1="${x.toFixed(2)}" y1="${margin.top}" x2="${x.toFixed(2)}" y2="${margin.top + plotHeight}" stroke="#f3f4f6" stroke-width="1"/>\n`;
  svg += `<text x="${x.toFixed(2)}" y="${margin.top + plotHeight + 28}" text-anchor="middle" font-family="Arial, sans-serif" font-size="12" fill="#374151">${dateText(tick)}</text>\n`;
}

for (const level of levels) {
  svg += `<path d="${areaPath(level.key)}" fill="${level.color}" fill-opacity="0.72" stroke="#ffffff" stroke-width="1.2">\n`;
  svg += `<title>${esc(level.key)}</title></path>\n`;
}

const totalPoints = stacked
  .map((row) => `${xScale(row.date).toFixed(2)},${yScale(row.total).toFixed(2)}`)
  .join(" ");
svg += `<polyline points="${totalPoints}" fill="none" stroke="#111827" stroke-width="2.2" stroke-linejoin="round" stroke-linecap="round"/>\n`;

svg += `<line x1="${margin.left}" y1="${margin.top + plotHeight}" x2="${margin.left + plotWidth}" y2="${margin.top + plotHeight}" stroke="#111827" stroke-width="1.5"/>\n`;
svg += `<line x1="${margin.left}" y1="${margin.top}" x2="${margin.left}" y2="${margin.top + plotHeight}" stroke="#111827" stroke-width="1.5"/>\n`;
svg += `<text x="${margin.left + plotWidth / 2}" y="${height - 30}" text-anchor="middle" font-family="Arial, sans-serif" font-size="14" fill="#111827">Date</text>\n`;
svg += `<text transform="translate(28 ${margin.top + plotHeight / 2}) rotate(-90)" text-anchor="middle" font-family="Arial, sans-serif" font-size="14" fill="#111827">Daily concentration (pollen grains/m3)</text>\n`;

const legendX = margin.left + plotWidth + 34;
let legendY = margin.top + 8;
svg += `<text x="${legendX}" y="${legendY}" font-family="Arial, sans-serif" font-size="15" font-weight="700" fill="#111827">Hierarchy level</text>\n`;
legendY += 26;
for (const level of [...levels].reverse()) {
  const levelTotal = data.reduce((sum, row) => sum + row.values[level.key], 0);
  svg += `<rect x="${legendX}" y="${legendY - 12}" width="18" height="18" fill="${level.color}" fill-opacity="0.72" stroke="#ffffff"/>\n`;
  svg += `<text x="${legendX + 28}" y="${legendY + 2}" font-family="Arial, sans-serif" font-size="13" fill="#111827">${esc(level.key)}</text>\n`;
  svg += `<text x="${legendX + 28}" y="${legendY + 19}" font-family="Arial, sans-serif" font-size="11" fill="#6b7280">mean ${fmt(levelTotal / data.length)}</text>\n`;
  legendY += 44;
}

legendY += 6;
svg += `<line x1="${legendX}" y1="${legendY}" x2="${legendX + 28}" y2="${legendY}" stroke="#111827" stroke-width="2.2" stroke-linecap="round"/>\n`;
svg += `<text x="${legendX + 38}" y="${legendY + 5}" font-family="Arial, sans-serif" font-size="13" fill="#111827">Total pollen</text>\n`;

const coverageY = height - 62;
svg += `<text x="${margin.left}" y="${coverageY}" font-family="Arial, sans-serif" font-size="12" fill="#4b5563">Coverage: ${data.length} days; median ${Math.round(data.map((d) => d.nFlowMeasurements).sort((a, b) => a - b)[Math.floor(data.length / 2)])} hourly flow measurements/day.</text>\n`;

svg += `</svg>\n`;

fs.writeFileSync(outputPath, svg);
console.log(`Wrote ${outputPath} with ${data.length} days.`);
