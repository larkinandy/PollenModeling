const fs = require("fs");
const path = require("path");

const inputPath = "study_city_site_daily_concentrations.csv";
const outputDir = "study_city_pollen_multipanel_figures";

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
  "#2563eb", "#dc2626", "#059669", "#9333ea", "#ea580c", "#0891b2",
  "#be123c", "#4d7c0f", "#7c3aed", "#0f766e", "#b45309", "#1d4ed8",
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
  return value.toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
}

function niceCeiling(value) {
  if (!Number.isFinite(value) || value <= 0) return 1;
  const exponent = Math.floor(Math.log10(value));
  const magnitude = 10 ** exponent;
  const normalized = value / magnitude;
  const nice = normalized <= 1 ? 1 : normalized <= 2 ? 2 : normalized <= 5 ? 5 : 10;
  return nice * magnitude;
}

function formatDate(date) {
  return date.toISOString().slice(5, 10);
}

const lines = fs.readFileSync(inputPath, "utf8").trim().split(/\r?\n/);
const header = parseCsvLine(lines[0]);
const data = lines.slice(1).map((line) => {
  const values = parseCsvLine(line);
  const row = Object.fromEntries(header.map((key, index) => [key, values[index]]));
  return {
    cityDesignation: row.city_designation,
    siteId: row.site_id,
    siteName: row.site_name || row.site_id.slice(0, 8),
    date: new Date(`${row.metric_date}T00:00:00Z`),
    dateText: row.metric_date,
    allergenType: row.allergen_type,
    concentration: Number(row.concentration),
    nFlowMeasurements: Number(row.n_flow_measurements),
  };
});

fs.mkdirSync(outputDir, { recursive: true });

const cities = [...new Set(data.map((d) => d.cityDesignation))].sort((a, b) => a.localeCompare(b));
const outputs = [];

for (const cityDesignation of cities) {
  const cityData = data.filter((d) => d.cityDesignation === cityDesignation);
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
  const coverageTop = topMargin + rowsNeeded * panelHeight + (rowsNeeded - 1) * panelGapY + 88;
  const coverageRowHeight = 22;
  const coverageHeight = 56 + sites.length * coverageRowHeight;
  const pageHeight = coverageTop + coverageHeight + 72;

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
  for (
    let tick = new Date(Date.UTC(minDate.getUTCFullYear(), minDate.getUTCMonth(), minDate.getUTCDate()));
    tick <= maxDate;
    tick = new Date(tick.getTime() + 30 * dayMs)
  ) {
    xTicks.push(tick);
  }
  if (xTicks.length === 0 || xTicks[xTicks.length - 1].getTime() !== maxDate.getTime()) {
    xTicks.push(maxDate);
  }

  const allDates = [];
  for (
    let day = new Date(Date.UTC(minDate.getUTCFullYear(), minDate.getUTCMonth(), minDate.getUTCDate()));
    day <= maxDate;
    day = new Date(day.getTime() + dayMs)
  ) {
    allDates.push(new Date(day.getTime()));
  }

  const coverageBySiteDate = new Map();
  for (const row of cityData) {
    const key = `${row.siteId}|${row.dateText}`;
    const previous = coverageBySiteDate.get(key) || 0;
    coverageBySiteDate.set(key, Math.max(previous, row.nFlowMeasurements || 0));
  }

  let svg = `<?xml version="1.0" encoding="UTF-8"?>\n`;
  svg += `<svg xmlns="http://www.w3.org/2000/svg" width="${pageWidth}" height="${pageHeight}" viewBox="0 0 ${pageWidth} ${pageHeight}">\n`;
  svg += `<rect width="100%" height="100%" fill="#ffffff"/>\n`;
  svg += `<text x="${leftMargin}" y="44" font-family="Arial, sans-serif" font-size="28" font-weight="700" fill="#111827">${escapeXml(cityDesignation)} Daily Pollen Concentrations</text>\n`;
  svg += `<text x="${leftMargin}" y="72" font-family="Arial, sans-serif" font-size="15" fill="#4b5563">${minDate.toISOString().slice(0, 10)} to ${maxDate.toISOString().slice(0, 10)}. Lines are monitor sites; concentration units are pollen grains/m3.</text>\n`;
  svg += `<text x="${legendX}" y="44" font-family="Arial, sans-serif" font-size="15" font-weight="700" fill="#111827">Sites</text>\n`;

  let legendY = 70;
  for (const site of sites) {
    const color = colorBySite.get(site.siteId);
    svg += `<rect x="${legendX}" y="${legendY - 8}" width="14" height="14" fill="${color}" stroke="#111827" stroke-width="0.4"/>\n`;
    svg += `<text x="${legendX + 38}" y="${legendY + 5}" font-family="Arial, sans-serif" font-size="13" fill="#111827">${escapeXml(site.siteName)}</text>\n`;
    legendY += 21;
  }

  for (const [panelIndex, allergenType] of allergenOrder.entries()) {
    const column = panelIndex % columns;
    const row = Math.floor(panelIndex / columns);
    const x0 = panelX(column);
    const y0 = panelY(row);
    const panelData = cityData.filter(
      (d) => d.allergenType === allergenType && d.nFlowMeasurements > 12
    );
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
      const siteData = panelData.filter((d) => d.siteId === site.siteId).sort((a, b) => a.date - b.date);
      if (siteData.length === 0) continue;
      const color = colorBySite.get(site.siteId);
      const points = siteData.map((d) => `${xScale(d.date, x0).toFixed(2)},${yScale(d.concentration, y0, yMax).toFixed(2)}`).join(" ");
      svg += `<polyline points="${points}" fill="none" stroke="${color}" stroke-width="1.8" stroke-linejoin="round" stroke-linecap="round"/>\n`;
      for (const d of siteData) {
        svg += `<circle cx="${xScale(d.date, x0).toFixed(2)}" cy="${yScale(d.concentration, y0, yMax).toFixed(2)}" r="1.8" fill="${color}"><title>${escapeXml(site.siteName)} ${d.dateText}: ${d.concentration.toFixed(2)}</title></circle>\n`;
      }
    }
  }

  const coverageX0 = leftMargin;
  const coverageY0 = coverageTop;
  const labelWidth = 154;
  const coverageWidth = columns * panelWidth + panelGapX;
  const gridX0 = coverageX0 + labelWidth;
  const gridWidth = coverageWidth - labelWidth;
  const cellWidth = gridWidth / allDates.length;
  const gridHeight = sites.length * coverageRowHeight;

  svg += `<text x="${coverageX0}" y="${coverageY0 - 34}" font-family="Arial, sans-serif" font-size="18" font-weight="700" fill="#111827">Daily coverage by site</text>\n`;
  svg += `<text x="${coverageX0}" y="${coverageY0 - 12}" font-family="Arial, sans-serif" font-size="13" fill="#4b5563">Green cells have more than 12 positive-flow measurements. Gray cells do not meet the threshold or have no daily record.</text>\n`;
  svg += `<rect x="${coverageX0}" y="${coverageY0}" width="${coverageWidth}" height="${gridHeight}" fill="#ffffff" stroke="#d1d5db" stroke-width="1"/>\n`;

  for (const [siteIndex, site] of sites.entries()) {
    const y = coverageY0 + siteIndex * coverageRowHeight;
    svg += `<rect x="${coverageX0}" y="${y}" width="${labelWidth}" height="${coverageRowHeight}" fill="${siteIndex % 2 === 0 ? "#ffffff" : "#f9fafb"}" stroke="#e5e7eb" stroke-width="1"/>\n`;
    svg += `<text x="${coverageX0 + 8}" y="${y + 15}" font-family="Arial, sans-serif" font-size="12" fill="#111827">${escapeXml(site.siteName)}</text>\n`;

    for (const [dateIndex, date] of allDates.entries()) {
      const dateText = date.toISOString().slice(0, 10);
      const nFlow = coverageBySiteDate.get(`${site.siteId}|${dateText}`) || 0;
      const x = gridX0 + dateIndex * cellWidth;
      const fill = nFlow > 12 ? "#16a34a" : nFlow === 0 ? "#fca5a5" : "#e5e7eb";
      svg += `<rect x="${x.toFixed(2)}" y="${y}" width="${Math.max(cellWidth, 0.8).toFixed(2)}" height="${coverageRowHeight}" fill="${fill}" stroke="#ffffff" stroke-width="0.4"><title>${escapeXml(site.siteName)} ${dateText}: ${nFlow} positive-flow measurements</title></rect>\n`;
    }
  }

  for (const date of xTicks) {
    const x = gridX0 + ((date.getTime() - minDate.getTime()) / Math.max(dayMs, maxDate.getTime() - minDate.getTime() + dayMs)) * gridWidth;
    svg += `<line x1="${x.toFixed(2)}" y1="${coverageY0}" x2="${x.toFixed(2)}" y2="${coverageY0 + gridHeight}" stroke="#9ca3af" stroke-width="0.7"/>\n`;
    svg += `<text x="${x.toFixed(2)}" y="${coverageY0 + gridHeight + 18}" text-anchor="middle" font-family="Arial, sans-serif" font-size="10" fill="#374151">${formatDate(date)}</text>\n`;
  }

  svg += `<rect x="${coverageX0}" y="${coverageY0 + gridHeight + 34}" width="13" height="13" fill="#16a34a"/>\n`;
  svg += `<text x="${coverageX0 + 20}" y="${coverageY0 + gridHeight + 45}" font-family="Arial, sans-serif" font-size="12" fill="#111827">&gt;50% daily coverage</text>\n`;
  svg += `<rect x="${coverageX0 + 162}" y="${coverageY0 + gridHeight + 34}" width="13" height="13" fill="#e5e7eb"/>\n`;
  svg += `<text x="${coverageX0 + 182}" y="${coverageY0 + gridHeight + 45}" font-family="Arial, sans-serif" font-size="12" fill="#111827">1-12 measurements</text>\n`;
  svg += `<rect x="${coverageX0 + 314}" y="${coverageY0 + gridHeight + 34}" width="13" height="13" fill="#fca5a5"/>\n`;
  svg += `<text x="${coverageX0 + 334}" y="${coverageY0 + gridHeight + 45}" font-family="Arial, sans-serif" font-size="12" fill="#111827">0 measurements</text>\n`;

  const outputPath = path.join(outputDir, `${slugify(cityDesignation)}_pollen_multipanel.svg`);
  svg += `</svg>\n`;
  fs.writeFileSync(outputPath, svg);
  outputs.push(outputPath);
}

console.log(`Wrote ${outputs.length} study-city multipanel SVG files to ${outputDir}.`);
for (const output of outputs) console.log(output);
