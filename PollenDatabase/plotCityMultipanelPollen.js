const fs = require("fs");
const path = require("path");

const inputPath = "city_site_daily_concentrations.csv";
const outputDir = "city_pollen_multipanel_figures";

const allergenOrder = [
  "Total Pollen",
  "Total Tree Pollen",
  "Quercus (Oak)",
  "Cupressaceae (Cypress)",
  "Morus (Mulberry)",
  "Ulmus (Elm)",
  "Fraxinus (Ash)",
  "Betula (Birch)",
  "Acer (Maple)",
  "Populus (Poplar)",
  "Pinaceae (Pine)",
  "Total Grass Pollen",
  "Ambrosia (Ragweed)",
  "Poaceae (Grasses)",
  "Total Mold",
];

const palette = [
  "#2563eb",
  "#dc2626",
  "#059669",
  "#9333ea",
  "#ea580c",
  "#0891b2",
  "#be123c",
  "#4d7c0f",
  "#7c3aed",
  "#0f766e",
  "#b45309",
  "#1d4ed8",
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

function escapeXml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function slugify(value) {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function formatDate(date) {
  return date.toISOString().slice(5, 10);
}

function niceCeiling(value) {
  if (!Number.isFinite(value) || value <= 0) return 1;
  const exponent = Math.floor(Math.log10(value));
  const magnitude = 10 ** exponent;
  const normalized = value / magnitude;
  const nice = normalized <= 1 ? 1 : normalized <= 2 ? 2 : normalized <= 5 ? 5 : 10;
  return nice * magnitude;
}

const lines = fs.readFileSync(inputPath, "utf8").trim().split(/\r?\n/);
const header = parseCsvLine(lines[0]);
const rows = lines.slice(1).map((line) => {
  const values = parseCsvLine(line);
  return Object.fromEntries(header.map((key, index) => [key, values[index]]));
});

const data = rows.map((row) => ({
  cityId: row.city_id,
  cityName: row.city_name,
  siteId: row.site_id,
  siteName: row.site_name || row.site_id.slice(0, 8),
  date: new Date(`${row.metric_date}T00:00:00Z`),
  dateText: row.metric_date,
  allergenType: row.allergen_type,
  concentration: Number(row.concentration),
}));

fs.mkdirSync(outputDir, { recursive: true });

const cities = [...new Map(data.map((d) => [`${d.cityId}|${d.cityName}`, {
  cityId: d.cityId,
  cityName: d.cityName,
}])).values()].sort((a, b) => a.cityName.localeCompare(b.cityName));

const outputs = [];

for (const city of cities) {
  const cityData = data.filter((d) => d.cityId === city.cityId);
  const sites = [...new Map(cityData.map((d) => [d.siteId, d.siteName])).entries()]
    .map(([siteId, siteName]) => ({ siteId, siteName }))
    .sort((a, b) => a.siteName.localeCompare(b.siteName));
  const colorBySite = new Map(sites.map((site, index) => [site.siteId, palette[index % palette.length]]));

  const minDate = new Date(Math.min(...cityData.map((d) => d.date.getTime())));
  const maxDate = new Date(Math.max(...cityData.map((d) => d.date.getTime())));
  const dayMs = 24 * 60 * 60 * 1000;
  const span = Math.max(dayMs, maxDate.getTime() - minDate.getTime());

  const pageWidth = 1500;
  const columns = 2;
  const panelWidth = 610;
  const panelHeight = 260;
  const panelGapX = 58;
  const panelGapY = 58;
  const headerHeight = 126;
  const leftMargin = 74;
  const topMargin = headerHeight;
  const legendX = leftMargin + columns * panelWidth + panelGapX + 20;
  const rowsNeeded = Math.ceil(allergenOrder.length / columns);
  const pageHeight = topMargin + rowsNeeded * panelHeight + (rowsNeeded - 1) * panelGapY + 72;

  function panelX(column) {
    return leftMargin + column * (panelWidth + panelGapX);
  }

  function panelY(row) {
    return topMargin + row * (panelHeight + panelGapY);
  }

  function xScale(date, x0) {
    return x0 + ((date.getTime() - minDate.getTime()) / span) * panelWidth;
  }

  function yScale(value, y0, yMax) {
    return y0 + panelHeight - (value / yMax) * panelHeight;
  }

  const xTicks = [];
  const firstTick = new Date(Date.UTC(minDate.getUTCFullYear(), minDate.getUTCMonth(), minDate.getUTCDate()));
  for (let tick = firstTick; tick <= maxDate; tick = new Date(tick.getTime() + 30 * dayMs)) {
    xTicks.push(tick);
  }
  if (xTicks.length === 0 || xTicks[xTicks.length - 1].getTime() !== maxDate.getTime()) {
    xTicks.push(maxDate);
  }

  let svg = `<?xml version="1.0" encoding="UTF-8"?>\n`;
  svg += `<svg xmlns="http://www.w3.org/2000/svg" width="${pageWidth}" height="${pageHeight}" viewBox="0 0 ${pageWidth} ${pageHeight}">\n`;
  svg += `<rect width="100%" height="100%" fill="#ffffff"/>\n`;
  svg += `<text x="${leftMargin}" y="44" font-family="Arial, sans-serif" font-size="28" font-weight="700" fill="#111827">${escapeXml(city.cityName)} Daily Pollen Concentrations</text>\n`;
  svg += `<text x="${leftMargin}" y="72" font-family="Arial, sans-serif" font-size="15" fill="#4b5563">${minDate.toISOString().slice(0, 10)} to ${maxDate.toISOString().slice(0, 10)}. Lines are monitor sites; concentration units are pollen grains/m3.</text>\n`;
  svg += `<text x="${legendX}" y="44" font-family="Arial, sans-serif" font-size="15" font-weight="700" fill="#111827">Sites</text>\n`;

  let legendY = 70;
  for (const site of sites) {
    const color = colorBySite.get(site.siteId);
    svg += `<line x1="${legendX}" y1="${legendY}" x2="${legendX + 28}" y2="${legendY}" stroke="${color}" stroke-width="3" stroke-linecap="round"/>\n`;
    svg += `<text x="${legendX + 38}" y="${legendY + 5}" font-family="Arial, sans-serif" font-size="13" fill="#111827">${escapeXml(site.siteName)}</text>\n`;
    legendY += 21;
  }

  for (const [panelIndex, allergenType] of allergenOrder.entries()) {
    const column = panelIndex % columns;
    const row = Math.floor(panelIndex / columns);
    const x0 = panelX(column);
    const y0 = panelY(row);
    const panelData = cityData.filter((d) => d.allergenType === allergenType);
    const yMax = niceCeiling(Math.max(...panelData.map((d) => d.concentration), 0));
    const yTicks = [0, yMax / 2, yMax];

    svg += `<text x="${x0}" y="${y0 - 14}" font-family="Arial, sans-serif" font-size="16" font-weight="700" fill="#111827">${escapeXml(allergenType)}</text>\n`;
    svg += `<rect x="${x0}" y="${y0}" width="${panelWidth}" height="${panelHeight}" fill="#ffffff" stroke="#d1d5db" stroke-width="1"/>\n`;

    for (const value of yTicks) {
      const y = yScale(value, y0, yMax);
      svg += `<line x1="${x0}" y1="${y.toFixed(2)}" x2="${x0 + panelWidth}" y2="${y.toFixed(2)}" stroke="#e5e7eb" stroke-width="1"/>\n`;
      svg += `<text x="${x0 - 8}" y="${(y + 4).toFixed(2)}" text-anchor="end" font-family="Arial, sans-serif" font-size="11" fill="#374151">${Number(value.toPrecision(3))}</text>\n`;
    }

    for (const date of xTicks) {
      const x = xScale(date, x0);
      svg += `<line x1="${x.toFixed(2)}" y1="${y0}" x2="${x.toFixed(2)}" y2="${y0 + panelHeight}" stroke="#f3f4f6" stroke-width="1"/>\n`;
      svg += `<text x="${x.toFixed(2)}" y="${y0 + panelHeight + 18}" text-anchor="middle" font-family="Arial, sans-serif" font-size="10" fill="#374151">${formatDate(date)}</text>\n`;
    }

    for (const site of sites) {
      const siteData = panelData
        .filter((d) => d.siteId === site.siteId)
        .sort((a, b) => a.date - b.date);
      if (siteData.length === 0) continue;

      const color = colorBySite.get(site.siteId);
      const points = siteData
        .map((d) => `${xScale(d.date, x0).toFixed(2)},${yScale(d.concentration, y0, yMax).toFixed(2)}`)
        .join(" ");
      svg += `<polyline points="${points}" fill="none" stroke="${color}" stroke-width="1.8" stroke-linejoin="round" stroke-linecap="round"/>\n`;
      for (const d of siteData) {
        svg += `<circle cx="${xScale(d.date, x0).toFixed(2)}" cy="${yScale(d.concentration, y0, yMax).toFixed(2)}" r="1.8" fill="${color}"><title>${escapeXml(site.siteName)} ${d.dateText}: ${d.concentration.toFixed(2)}</title></circle>\n`;
      }
    }
  }

  const outputPath = path.join(outputDir, `${slugify(city.cityName)}_${city.cityId}_pollen_multipanel.svg`);
  svg += `</svg>\n`;
  fs.writeFileSync(outputPath, svg);
  outputs.push(outputPath);
}

console.log(`Wrote ${outputs.length} city multipanel SVG files to ${outputDir}.`);
for (const output of outputs) {
  console.log(output);
}
