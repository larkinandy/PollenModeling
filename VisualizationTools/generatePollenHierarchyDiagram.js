const fs = require("fs");
const path = require("path");

const outputPath = path.join(
  __dirname,
  "outputs",
  "pollen_multiplot_hierarchy_diagram.svg"
);

function esc(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

const width = 1800;
const height = 1280;

const nodes = [
  {
    id: "total",
    x: 690,
    y: 70,
    w: 420,
    h: 92,
    title: "Total Pollen",
    subtitle: "Direct POL category row",
    fill: "#dbeafe",
    stroke: "#1d4ed8",
  },
  {
    id: "generic",
    x: 90,
    y: 255,
    w: 260,
    h: 74,
    title: "Generic Pollen",
    subtitle: "POL only",
    fill: "#eef2ff",
    stroke: "#4f46e5",
  },
  {
    id: "tree",
    x: 425,
    y: 245,
    w: 315,
    h: 92,
    title: "Total Tree Pollen",
    subtitle: "Direct TRE category row",
    fill: "#dcfce7",
    stroke: "#15803d",
  },
  {
    id: "grass",
    x: 820,
    y: 245,
    w: 315,
    h: 92,
    title: "Total Grass Pollen",
    subtitle: "Direct GRA category row",
    fill: "#fef9c3",
    stroke: "#ca8a04",
  },
  {
    id: "weed",
    x: 1215,
    y: 245,
    w: 315,
    h: 92,
    title: "Weed/Shrub Branch",
    subtitle: "WEE hierarchy context",
    fill: "#fce7f3",
    stroke: "#be185d",
  },
  {
    id: "mold",
    x: 1465,
    y: 70,
    w: 255,
    h: 92,
    title: "Total Mold",
    subtitle: "Separate non-pollen panel",
    fill: "#f3f4f6",
    stroke: "#6b7280",
    dashed: true,
  },
];

const leafNodes = [
  ["Quercus (Oak)", "QUE: 41 categories", 60, 470, "#ecfdf5", "#16a34a"],
  ["Cupressaceae (Cypress)", "CUP: 16 categories", 330, 470, "#ecfdf5", "#16a34a"],
  ["Morus (Mulberry)", "MOR: 3 categories", 600, 470, "#ecfdf5", "#16a34a"],
  ["Ulmus (Elm)", "ULM: 9 categories", 870, 470, "#ecfdf5", "#16a34a"],
  ["Fraxinus (Ash)", "FRA: 11 categories", 1140, 470, "#ecfdf5", "#16a34a"],
  ["Betula (Birch)", "BET: 10 categories", 195, 610, "#ecfdf5", "#16a34a"],
  ["Acer (Maple)", "ACE: 12 categories", 465, 610, "#ecfdf5", "#16a34a"],
  ["Populus (Poplar)", "POP: 11 categories", 735, 610, "#ecfdf5", "#16a34a"],
  ["Pinaceae (Pine)", "PIN: 29 categories", 1005, 610, "#ecfdf5", "#16a34a"],
  ["Other Tree Descendants", "Context only; not summed into TRE", 1275, 610, "#f0fdf4", "#86efac"],
  ["Poaceae (Grasses)", "POA: 1 category", 505, 805, "#fefce8", "#ca8a04"],
  ["Other Grass Descendants", "Context only; not summed into GRA", 775, 805, "#fefce8", "#eab308"],
  ["Ambrosia (Ragweed)", "AMB-IVA branch: 11 categories", 1110, 805, "#fdf2f8", "#be185d"],
  ["Other Weed/Shrub Descendants", "Context only; not summed into POL", 1380, 805, "#fdf2f8", "#f472b6"],
].map(([title, subtitle, x, y, fill, stroke], index) => ({
  id: `leaf${index}`,
  title,
  subtitle,
  x,
  y,
  w: 230,
  h: 78,
  fill,
  stroke,
}));

for (const node of leafNodes) nodes.push(node);

const byId = Object.fromEntries(nodes.map((node) => [node.id, node]));

function center(node) {
  return { x: node.x + node.w / 2, y: node.y + node.h / 2 };
}

function bottom(node) {
  return { x: node.x + node.w / 2, y: node.y + node.h };
}

function top(node) {
  return { x: node.x + node.w / 2, y: node.y };
}

const links = [
  ["total", "generic"],
  ["total", "tree"],
  ["total", "grass"],
  ["total", "weed"],
  ["tree", "leaf0"],
  ["tree", "leaf1"],
  ["tree", "leaf2"],
  ["tree", "leaf3"],
  ["tree", "leaf4"],
  ["tree", "leaf5"],
  ["tree", "leaf6"],
  ["tree", "leaf7"],
  ["tree", "leaf8"],
  ["tree", "leaf9"],
  ["grass", "leaf10"],
  ["grass", "leaf11"],
  ["weed", "leaf12"],
  ["weed", "leaf13"],
];

let svg = `<?xml version="1.0" encoding="UTF-8"?>\n`;
svg += `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">\n`;
svg += `<rect width="100%" height="100%" fill="#ffffff"/>\n`;
svg += `<text x="70" y="44" font-family="Arial" font-size="28" font-weight="700" fill="#111827">Category Context for the Multiplot Pollen Panels</text>\n`;
svg += `<text x="70" y="72" font-family="Arial" font-size="15" fill="#4b5563">Solid arrows show category hierarchy context. Plotted totals use direct aggregate rows such as POL, TRE, and GRA.</text>\n`;

function connector(sourceId, targetId, stroke = "#374151", dashed = false) {
  const source = bottom(byId[sourceId]);
  const target = top(byId[targetId]);
  const midY = (source.y + target.y) / 2;
  const dash = dashed ? ' stroke-dasharray="6 5"' : "";
  svg += `<polyline points="${source.x},${source.y} ${source.x},${midY} ${target.x},${midY} ${target.x},${target.y - 9}" fill="none" stroke="${stroke}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"${dash}/>\n`;
  svg += `<polygon points="${target.x - 6},${target.y - 9} ${target.x + 6},${target.y - 9} ${target.x},${target.y}" fill="${stroke}" stroke="${stroke}" stroke-width="1"/>\n`;
}

for (const [sourceId, targetId] of links) {
  connector(sourceId, targetId);
}

const mold = byId.mold;
svg += `<polyline points="${center(byId.total).x + 210},${center(byId.total).y} 1235,116 1335,116 ${mold.x - 9},${center(mold).y}" fill="none" stroke="#6b7280" stroke-width="2" stroke-dasharray="6 5" stroke-linejoin="round" stroke-linecap="round"/>\n`;
svg += `<polygon points="${mold.x - 9},${center(mold).y - 6} ${mold.x - 9},${center(mold).y + 6} ${mold.x},${center(mold).y}" fill="#6b7280" stroke="#6b7280" stroke-width="1"/>\n`;

for (const node of nodes) {
  svg += `<rect x="${node.x}" y="${node.y}" width="${node.w}" height="${node.h}" rx="8" fill="${node.fill}" stroke="${node.stroke}" stroke-width="2"${node.dashed ? ' stroke-dasharray="7 5"' : ""}/>\n`;
  svg += `<text x="${node.x + node.w / 2}" y="${node.y + 31}" text-anchor="middle" font-family="Arial" font-size="16" font-weight="700" fill="#111827">${esc(node.title)}</text>\n`;
  svg += `<text x="${node.x + node.w / 2}" y="${node.y + 56}" text-anchor="middle" font-family="Arial" font-size="13" fill="#374151">${esc(node.subtitle)}</text>\n`;
}

svg += `<rect x="70" y="1015" width="1660" height="170" rx="8" fill="#f9fafb" stroke="#d1d5db"/>\n`;
svg += `<text x="95" y="1050" font-family="Arial" font-size="18" font-weight="700" fill="#111827">How to read this diagram</text>\n`;
svg += `<text x="95" y="1080" font-family="Arial" font-size="14" fill="#374151">Total Pollen, Total Tree Pollen, and Total Grass Pollen are plotted from the direct POL, TRE, and GRA category rows.</text>\n`;
svg += `<text x="95" y="1106" font-family="Arial" font-size="14" fill="#374151">Specific panels such as Acer, Quercus, Cupressaceae, Poaceae, and Ambrosia are plotted from their direct category rows when available.</text>\n`;
svg += `<text x="95" y="1132" font-family="Arial" font-size="14" fill="#374151">Total Mold is a separate non-pollen panel and is not included under Total Pollen.</text>\n`;
svg += `<line x1="95" y1="1160" x2="145" y2="1160" stroke="#374151" stroke-width="2"/><polygon points="145,1154 145,1166 154,1160" fill="#374151"/><text x="160" y="1165" font-family="Arial" font-size="13" fill="#374151">Hierarchy context, not summed into total panel</text>\n`;
svg += `<line x1="390" y1="1160" x2="440" y2="1160" stroke="#6b7280" stroke-width="1.4" stroke-dasharray="6 5"/><text x="455" y="1165" font-family="Arial" font-size="13" fill="#374151">Separate panel, not under Total Pollen</text>\n`;
svg += `</svg>\n`;

fs.writeFileSync(outputPath, svg);
console.log(`Wrote ${outputPath}`);


