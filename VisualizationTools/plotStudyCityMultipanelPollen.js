const fs = require("fs");
const path = require("path");

const outputRoot = "outputs";
const inputPath = path.join(outputRoot, "study_city_site_daily_concentrations.csv");
const outputDir = path.join(outputRoot, "study_city_pollen_multipanel_figures");
const threshold = 12;
const excludedSiteNames = new Set([
  "ch1", "ch1-out", "ch2", "ch2-out", "ch3", "ch3-out", "ch4", "ch4-out",
]);

const allergens = [
  "Total Pollen", "Total Tree Pollen", "Quercus (Oak)", "Cupressaceae (Cypress)",
  "Morus (Mulberry)", "Ulmus (Elm)", "Fraxinus (Ash)", "Betula (Birch)",
  "Acer (Maple)", "Populus (Poplar)", "Pinaceae (Pine)", "Total Grass Pollen",
  "Ambrosia (Ragweed)", "Poaceae (Grasses)", "Total Mold",
];

const colors = [
  "#2563eb", "#dc2626", "#059669", "#9333ea", "#ea580c", "#0891b2",
  "#be123c", "#4d7c0f", "#7c3aed", "#0f766e", "#b45309", "#1d4ed8",
  "#db2777", "#64748b", "#84cc16", "#0ea5e9", "#f97316", "#14b8a6",
];

function csvLine(line) {
  const out = [];
  let cur = "";
  let quoted = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"' && quoted && line[i + 1] === '"') {
      cur += '"'; i++;
    } else if (ch === '"') {
      quoted = !quoted;
    } else if (ch === "," && !quoted) {
      out.push(cur); cur = "";
    } else {
      cur += ch;
    }
  }
  out.push(cur);
  return out;
}

function esc(x) {
  return String(x).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function slug(x) {
  return x.toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
}

function niceMax(x) {
  if (!Number.isFinite(x) || x <= 0) return 1;
  const m = 10 ** Math.floor(Math.log10(x));
  const n = x / m;
  return (n <= 1 ? 1 : n <= 2 ? 2 : n <= 5 ? 5 : 10) * m;
}

const lines = fs.readFileSync(inputPath, "utf8").trim().split(/\r?\n/);
const header = csvLine(lines[0]);
const data = lines.slice(1).map((line) => {
  const values = csvLine(line);
  const row = Object.fromEntries(header.map((h, i) => [h, values[i]]));
  return {
    city: row.city_designation,
    siteId: row.site_id,
    siteName: row.site_name || row.site_id.slice(0, 8),
    dateText: row.metric_date,
    date: new Date(`${row.metric_date}T00:00:00Z`),
    allergen: row.allergen_type,
    concentration: Number(row.concentration),
    nFlow: Number(row.n_flow_measurements || 0),
  };
}).filter((row) => !excludedSiteNames.has(row.siteName.trim().toLowerCase()));

fs.mkdirSync(outputDir, { recursive: true });

const cities = [...new Set(data.map((d) => d.city))].sort();
const dayMs = 86400000;

for (const city of cities) {
  const cityData = data.filter((d) => d.city === city);
  const sites = [...new Map(cityData.map((d) => [d.siteId, d.siteName])).entries()]
    .map(([siteId, siteName]) => ({ siteId, siteName }))
    .sort((a, b) => a.siteName.localeCompare(b.siteName));
  const color = new Map(sites.map((s, i) => [s.siteId, colors[i % colors.length]]));
  const minDate = new Date(Math.min(...cityData.map((d) => d.date)));
  const maxDate = new Date(Math.max(...cityData.map((d) => d.date)));
  const span = Math.max(dayMs, maxDate - minDate);

  const W = 1500, panelW = 610, panelH = 260, gapX = 58, gapY = 58, left = 74, top = 126;
  const rows = Math.ceil(allergens.length / 2);
  const coverageTop = top + rows * panelH + (rows - 1) * gapY + 88;
  const rowH = 22, labelW = 154, gridW = panelW * 2 + gapX - labelW;
  const H = coverageTop + sites.length * rowH + 128;
  let svg = `<?xml version="1.0" encoding="UTF-8"?>\n<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}">\n<rect width="100%" height="100%" fill="#fff"/>\n`;
  svg += `<text x="${left}" y="44" font-family="Arial" font-size="28" font-weight="700" fill="#111827">${esc(city)} Daily Pollen Concentrations</text>\n`;
  svg += `<text x="${left}" y="72" font-family="Arial" font-size="15" fill="#4b5563">${minDate.toISOString().slice(0,10)} to ${maxDate.toISOString().slice(0,10)}. Pollen panels only show days with &gt;50% positive-flow measurements. Units: pollen grains/m3.</text>\n`;

  const legendX = 1372;
  svg += `<text x="${legendX}" y="44" font-family="Arial" font-size="15" font-weight="700" fill="#111827">Sites</text>\n`;
  sites.forEach((s, i) => {
    const y = 70 + i * 21;
    svg += `<rect x="${legendX}" y="${y - 8}" width="14" height="14" fill="${color.get(s.siteId)}" stroke="#111827" stroke-width="0.4"/>\n`;
    svg += `<text x="${legendX + 22}" y="${y + 5}" font-family="Arial" font-size="13" fill="#111827">${esc(s.siteName)}</text>\n`;
  });

  function xScale(date, x0) { return x0 + ((date - minDate) / span) * panelW; }
  function yScale(v, y0, ymax) { return y0 + panelH - (v / ymax) * panelH; }
  const ticks = [];
  for (let t = new Date(minDate); t <= maxDate; t = new Date(t.getTime() + 30 * dayMs)) ticks.push(new Date(t));
  if (!ticks.length || ticks[ticks.length - 1].getTime() !== maxDate.getTime()) ticks.push(maxDate);

  allergens.forEach((allergen, i) => {
    const col = i % 2, row = Math.floor(i / 2);
    const x0 = left + col * (panelW + gapX), y0 = top + row * (panelH + gapY);
    const panel = cityData.filter((d) => d.allergen === allergen && d.nFlow > threshold);
    const ymax = niceMax(Math.max(0, ...panel.map((d) => d.concentration)));
    svg += `<text x="${x0}" y="${y0 - 14}" font-family="Arial" font-size="16" font-weight="700" fill="#111827">${esc(allergen)}</text>\n`;
    svg += `<rect x="${x0}" y="${y0}" width="${panelW}" height="${panelH}" fill="#fff" stroke="#d1d5db"/>\n`;
    [0, ymax / 2, ymax].forEach((v) => {
      const y = yScale(v, y0, ymax);
      svg += `<line x1="${x0}" y1="${y}" x2="${x0 + panelW}" y2="${y}" stroke="#e5e7eb"/>
<text x="${x0 - 8}" y="${y + 4}" text-anchor="end" font-family="Arial" font-size="11">${Number(v.toPrecision(3))}</text>\n`;
    });
    ticks.forEach((t) => {
      const x = xScale(t, x0);
      svg += `<line x1="${x}" y1="${y0}" x2="${x}" y2="${y0 + panelH}" stroke="#f3f4f6"/>
<text x="${x}" y="${y0 + panelH + 18}" text-anchor="middle" font-family="Arial" font-size="10">${t.toISOString().slice(5,10)}</text>\n`;
    });
    sites.forEach((s) => {
      const sd = panel.filter((d) => d.siteId === s.siteId).sort((a, b) => a.date - b.date);
      if (!sd.length) return;
      const pts = sd.map((d) => `${xScale(d.date, x0).toFixed(2)},${yScale(d.concentration, y0, ymax).toFixed(2)}`).join(" ");
      svg += `<polyline points="${pts}" fill="none" stroke="${color.get(s.siteId)}" stroke-width="1.8" stroke-linejoin="round" stroke-linecap="round"/>\n`;
    });
  });

  const dates = [];
  for (let d = new Date(minDate); d <= maxDate; d = new Date(d.getTime() + dayMs)) dates.push(new Date(d));
  const cov = new Map();
  cityData.forEach((d) => {
    const key = `${d.siteId}|${d.dateText}`;
    cov.set(key, Math.max(cov.get(key) || 0, d.nFlow));
  });
  const gridX = left + labelW, gridY = coverageTop, cellW = gridW / dates.length;
  const gridHeight = sites.length * rowH;
  svg += `<text x="${left}" y="${coverageTop - 34}" font-family="Arial" font-size="18" font-weight="700">Daily coverage by site</text>\n`;
  svg += `<text x="${left}" y="${coverageTop - 12}" font-family="Arial" font-size="13" fill="#4b5563">Green: &gt;12 measurements. Gray: 1-12 measurements. Pink: 0 measurements.</text>\n`;
  svg += `<rect x="${left}" y="${gridY}" width="${labelW + gridW}" height="${gridHeight}" fill="#ffffff" stroke="#9ca3af" stroke-width="1"/>\n`;
  sites.forEach((s, i) => {
    const y = gridY + i * rowH;
    svg += `<rect x="${left}" y="${y}" width="${labelW}" height="${rowH}" fill="${i % 2 ? "#f9fafb" : "#fff"}" stroke="#d1d5db" stroke-width="0.7"/>\n`;
    svg += `<text x="${left + 8}" y="${y + 15}" font-family="Arial" font-size="12">${esc(s.siteName)}</text>\n`;
    dates.forEach((d, j) => {
      const ds = d.toISOString().slice(0, 10);
      const n = cov.get(`${s.siteId}|${ds}`) || 0;
      const fill = n > threshold ? "#16a34a" : n === 0 ? "#fca5a5" : "#e5e7eb";
      svg += `<rect x="${(gridX + j * cellW).toFixed(2)}" y="${y}" width="${Math.max(cellW, 0.8).toFixed(2)}" height="${rowH}" fill="${fill}" stroke="#ffffff" stroke-width="0.25"><title>${esc(s.siteName)} ${ds}: ${n} measurements</title></rect>\n`;
    });
  });
  for (let i = 0; i <= sites.length; i++) {
    const y = gridY + i * rowH;
    svg += `<line x1="${left}" y1="${y}" x2="${left + labelW + gridW}" y2="${y}" stroke="#9ca3af" stroke-width="0.45"/>\n`;
  }
  for (let j = 0; j <= dates.length; j++) {
    const x = gridX + j * cellW;
    svg += `<line x1="${x.toFixed(2)}" y1="${gridY}" x2="${x.toFixed(2)}" y2="${gridY + gridHeight}" stroke="#9ca3af" stroke-width="0.35"/>\n`;
  }
  svg += `<line x1="${gridX}" y1="${gridY}" x2="${gridX}" y2="${gridY + gridHeight}" stroke="#111827" stroke-width="1"/>\n`;
  ticks.forEach((t) => {
    const dayIndex = Math.round((t - minDate) / dayMs);
    const x = gridX + Math.max(0, Math.min(dates.length - 1, dayIndex)) * cellW + cellW / 2;
    svg += `<line x1="${x.toFixed(2)}" y1="${gridY}" x2="${x.toFixed(2)}" y2="${gridY + gridHeight}" stroke="#374151" stroke-width="0.8"/>\n`;
    svg += `<text x="${x.toFixed(2)}" y="${gridY + gridHeight + 18}" text-anchor="middle" font-family="Arial" font-size="10" fill="#374151">${t.toISOString().slice(5,10)}</text>\n`;
  });
  const legendY = gridY + gridHeight + 38;
  svg += `<rect x="${left}" y="${legendY - 11}" width="13" height="13" fill="#16a34a" stroke="#9ca3af" stroke-width="0.4"/>\n`;
  svg += `<text x="${left + 20}" y="${legendY}" font-family="Arial" font-size="12" fill="#111827">&gt;50% daily coverage</text>\n`;
  svg += `<rect x="${left + 172}" y="${legendY - 11}" width="13" height="13" fill="#e5e7eb" stroke="#9ca3af" stroke-width="0.4"/>\n`;
  svg += `<text x="${left + 192}" y="${legendY}" font-family="Arial" font-size="12" fill="#111827">1-12 measurements</text>\n`;
  svg += `<rect x="${left + 324}" y="${legendY - 11}" width="13" height="13" fill="#fca5a5" stroke="#9ca3af" stroke-width="0.4"/>\n`;
  svg += `<text x="${left + 344}" y="${legendY}" font-family="Arial" font-size="12" fill="#111827">0 measurements</text>\n`;
  svg += `</svg>\n`;
  fs.writeFileSync(path.join(outputDir, `${slug(city)}_pollen_multipanel.svg`), svg);
}

console.log(`Wrote ${cities.length} SVG files to ${outputDir}.`);


