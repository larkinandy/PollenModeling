const fs = require("fs");

const inputPath = "ann_arbor_acer_daily_concentrations.csv";
const outputPath = "ann_arbor_acer_timeseries.svg";

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

function escapeXml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

const lines = fs.readFileSync(inputPath, "utf8").trim().split(/\r?\n/);
const header = parseCsvLine(lines[0]);
const rows = lines.slice(1).map((line) => {
  const values = parseCsvLine(line);
  return Object.fromEntries(header.map((key, index) => [key, values[index]]));
});

const data = rows.map((row) => ({
  siteId: row.site_id,
  siteName: row.site_name || row.site_id.slice(0, 8),
  date: new Date(`${row.metric_date}T00:00:00Z`),
  dateText: row.metric_date,
  concentration: Number(row.concentration),
}));

const width = 1200;
const height = 720;
const margin = { top: 88, right: 260, bottom: 88, left: 86 };
const plotWidth = width - margin.left - margin.right;
const plotHeight = height - margin.top - margin.bottom;

const minDate = new Date(Math.min(...data.map((d) => d.date.getTime())));
const maxDate = new Date(Math.max(...data.map((d) => d.date.getTime())));
const maxValue = Math.max(...data.map((d) => d.concentration));
const yMax = Math.ceil(maxValue / 10) * 10;
const dayMs = 24 * 60 * 60 * 1000;

const sites = [...new Map(data.map((d) => [d.siteId, d.siteName])).entries()]
  .map(([siteId, siteName]) => ({ siteId, siteName }))
  .sort((a, b) => a.siteName.localeCompare(b.siteName));

const colors = [
  "#2563eb",
  "#dc2626",
  "#059669",
  "#9333ea",
  "#ea580c",
  "#0891b2",
];

function xScale(date) {
  const span = maxDate.getTime() - minDate.getTime();
  return margin.left + ((date.getTime() - minDate.getTime()) / span) * plotWidth;
}

function yScale(value) {
  return margin.top + plotHeight - (value / yMax) * plotHeight;
}

function fmtDate(date) {
  return date.toISOString().slice(5, 10);
}

const yTicks = [];
for (let value = 0; value <= yMax; value += Math.max(10, Math.ceil(yMax / 6 / 10) * 10)) {
  yTicks.push(value);
}
if (yTicks[yTicks.length - 1] !== yMax) yTicks.push(yMax);

const xTicks = [];
for (
  let tick = new Date(Date.UTC(minDate.getUTCFullYear(), minDate.getUTCMonth(), minDate.getUTCDate()));
  tick <= maxDate;
  tick = new Date(tick.getTime() + 7 * dayMs)
) {
  xTicks.push(tick);
}

let svg = `<?xml version="1.0" encoding="UTF-8"?>\n`;
svg += `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">\n`;
svg += `<rect width="100%" height="100%" fill="#ffffff"/>\n`;
svg += `<text x="${margin.left}" y="42" font-family="Arial, sans-serif" font-size="26" font-weight="700" fill="#111827">Acer (Maple) Daily Pollen Concentration</text>\n`;
svg += `<text x="${margin.left}" y="68" font-family="Arial, sans-serif" font-size="15" fill="#4b5563">Ann Arbor, MI monitors, ${minDate.toISOString().slice(0, 10)} to ${maxDate.toISOString().slice(0, 10)}</text>\n`;

for (const value of yTicks) {
  const y = yScale(value);
  svg += `<line x1="${margin.left}" y1="${y.toFixed(2)}" x2="${margin.left + plotWidth}" y2="${y.toFixed(2)}" stroke="#e5e7eb" stroke-width="1"/>\n`;
  svg += `<text x="${margin.left - 12}" y="${(y + 5).toFixed(2)}" text-anchor="end" font-family="Arial, sans-serif" font-size="12" fill="#374151">${value}</text>\n`;
}

for (const date of xTicks) {
  const x = xScale(date);
  svg += `<line x1="${x.toFixed(2)}" y1="${margin.top}" x2="${x.toFixed(2)}" y2="${margin.top + plotHeight}" stroke="#f3f4f6" stroke-width="1"/>\n`;
  svg += `<text x="${x.toFixed(2)}" y="${margin.top + plotHeight + 28}" text-anchor="middle" font-family="Arial, sans-serif" font-size="12" fill="#374151">${fmtDate(date)}</text>\n`;
}

svg += `<line x1="${margin.left}" y1="${margin.top + plotHeight}" x2="${margin.left + plotWidth}" y2="${margin.top + plotHeight}" stroke="#111827" stroke-width="1.5"/>\n`;
svg += `<line x1="${margin.left}" y1="${margin.top}" x2="${margin.left}" y2="${margin.top + plotHeight}" stroke="#111827" stroke-width="1.5"/>\n`;
svg += `<text x="${margin.left + plotWidth / 2}" y="${height - 28}" text-anchor="middle" font-family="Arial, sans-serif" font-size="14" fill="#111827">Date</text>\n`;
svg += `<text transform="translate(24 ${margin.top + plotHeight / 2}) rotate(-90)" text-anchor="middle" font-family="Arial, sans-serif" font-size="14" fill="#111827">Daily concentration (pollen grains/m3)</text>\n`;

for (const [siteIndex, site] of sites.entries()) {
  const siteData = data
    .filter((d) => d.siteId === site.siteId)
    .sort((a, b) => a.date - b.date);
  const color = colors[siteIndex % colors.length];
  const points = siteData
    .map((d) => `${xScale(d.date).toFixed(2)},${yScale(d.concentration).toFixed(2)}`)
    .join(" ");

  svg += `<polyline points="${points}" fill="none" stroke="${color}" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"/>\n`;
  for (const d of siteData) {
    svg += `<circle cx="${xScale(d.date).toFixed(2)}" cy="${yScale(d.concentration).toFixed(2)}" r="2.8" fill="${color}"><title>${escapeXml(site.siteName)} ${d.dateText}: ${d.concentration.toFixed(2)}</title></circle>\n`;
  }
}

const legendX = margin.left + plotWidth + 36;
let legendY = margin.top + 8;
svg += `<text x="${legendX}" y="${legendY}" font-family="Arial, sans-serif" font-size="15" font-weight="700" fill="#111827">Site</text>\n`;
legendY += 26;
for (const [siteIndex, site] of sites.entries()) {
  const color = colors[siteIndex % colors.length];
  svg += `<line x1="${legendX}" y1="${legendY}" x2="${legendX + 28}" y2="${legendY}" stroke="${color}" stroke-width="3" stroke-linecap="round"/>\n`;
  svg += `<text x="${legendX + 38}" y="${legendY + 5}" font-family="Arial, sans-serif" font-size="13" fill="#111827">${escapeXml(site.siteName)}</text>\n`;
  legendY += 24;
}

svg += `</svg>\n`;

fs.writeFileSync(outputPath, svg);
console.log(`Wrote ${outputPath} with ${data.length} records across ${sites.length} sites.`);
