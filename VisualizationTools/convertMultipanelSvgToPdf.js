const fs = require("fs");
const path = require("path");

const inputDir = "study_city_pollen_multipanel_figures";

function parseAttrs(tag) {
  const attrs = {};
  const pattern = /([a-zA-Z_:.-]+)="([^"]*)"/g;
  let match;
  while ((match = pattern.exec(tag)) !== null) {
    attrs[match[1]] = match[2];
  }
  return attrs;
}

function decodeXml(value) {
  return String(value)
    .replace(/&gt;/g, ">")
    .replace(/&lt;/g, "<")
    .replace(/&quot;/g, '"')
    .replace(/&amp;/g, "&");
}

function pdfEscape(value) {
  return String(value)
    .replace(/\\/g, "\\\\")
    .replace(/\(/g, "\\(")
    .replace(/\)/g, "\\)")
    .replace(/[^\x20-\x7e]/g, "?");
}

function colorToRgb(color) {
  if (!color || color === "none") return null;
  const hex = color.trim().replace("#", "");
  if (hex.length !== 6) return [0, 0, 0];
  return [
    parseInt(hex.slice(0, 2), 16) / 255,
    parseInt(hex.slice(2, 4), 16) / 255,
    parseInt(hex.slice(4, 6), 16) / 255,
  ];
}

function num(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function textWidth(text, size) {
  return text.length * size * 0.53;
}

function formatNumber(value) {
  return Number(value).toFixed(3).replace(/\.?0+$/, "");
}

function commandsForRect(attrs, height) {
  const x = num(attrs.x);
  const y = num(attrs.y);
  const w = num(attrs.width);
  const h = num(attrs.height);
  const fill = colorToRgb(attrs.fill);
  const stroke = colorToRgb(attrs.stroke);
  const strokeWidth = num(attrs["stroke-width"], 1);
  let out = "";

  if (fill) {
    out += `${fill.map(formatNumber).join(" ")} rg\n`;
    out += `${formatNumber(x)} ${formatNumber(height - y - h)} ${formatNumber(w)} ${formatNumber(h)} re f\n`;
  }
  if (stroke && strokeWidth > 0) {
    out += `${stroke.map(formatNumber).join(" ")} RG\n${formatNumber(strokeWidth)} w\n`;
    out += `${formatNumber(x)} ${formatNumber(height - y - h)} ${formatNumber(w)} ${formatNumber(h)} re S\n`;
  }
  return out;
}

function commandsForLine(attrs, height) {
  const stroke = colorToRgb(attrs.stroke);
  if (!stroke) return "";
  const x1 = num(attrs.x1);
  const y1 = height - num(attrs.y1);
  const x2 = num(attrs.x2);
  const y2 = height - num(attrs.y2);
  const strokeWidth = num(attrs["stroke-width"], 1);
  return `${stroke.map(formatNumber).join(" ")} RG\n${formatNumber(strokeWidth)} w\n${formatNumber(x1)} ${formatNumber(y1)} m ${formatNumber(x2)} ${formatNumber(y2)} l S\n`;
}

function commandsForPolyline(attrs, height) {
  const stroke = colorToRgb(attrs.stroke);
  if (!stroke || !attrs.points) return "";
  const points = attrs.points.trim().split(/\s+/).map((point) => point.split(",").map(Number));
  if (points.length < 2) return "";
  const strokeWidth = num(attrs["stroke-width"], 1);
  let out = `${stroke.map(formatNumber).join(" ")} RG\n${formatNumber(strokeWidth)} w\n`;
  out += `${formatNumber(points[0][0])} ${formatNumber(height - points[0][1])} m\n`;
  for (const point of points.slice(1)) {
    out += `${formatNumber(point[0])} ${formatNumber(height - point[1])} l\n`;
  }
  out += "S\n";
  return out;
}

function commandsForCircle(attrs, height) {
  const fill = colorToRgb(attrs.fill);
  if (!fill) return "";
  const cx = num(attrs.cx);
  const cy = height - num(attrs.cy);
  const r = num(attrs.r);
  const k = 0.5522847498 * r;
  return `${fill.map(formatNumber).join(" ")} rg\n` +
    `${formatNumber(cx + r)} ${formatNumber(cy)} m\n` +
    `${formatNumber(cx + r)} ${formatNumber(cy + k)} ${formatNumber(cx + k)} ${formatNumber(cy + r)} ${formatNumber(cx)} ${formatNumber(cy + r)} c\n` +
    `${formatNumber(cx - k)} ${formatNumber(cy + r)} ${formatNumber(cx - r)} ${formatNumber(cy + k)} ${formatNumber(cx - r)} ${formatNumber(cy)} c\n` +
    `${formatNumber(cx - r)} ${formatNumber(cy - k)} ${formatNumber(cx - k)} ${formatNumber(cy - r)} ${formatNumber(cx)} ${formatNumber(cy - r)} c\n` +
    `${formatNumber(cx + k)} ${formatNumber(cy - r)} ${formatNumber(cx + r)} ${formatNumber(cy - k)} ${formatNumber(cx + r)} ${formatNumber(cy)} c f\n`;
}

function commandsForText(line, height) {
  const attrs = parseAttrs(line);
  const textMatch = line.match(/>([^<]*)<\/text>/);
  if (!textMatch) return "";
  const text = decodeXml(textMatch[1]);
  const size = num(attrs["font-size"], 12);
  const fill = colorToRgb(attrs.fill) || [0, 0, 0];
  const font = attrs["font-weight"] === "700" ? "/F2" : "/F1";
  const anchor = attrs["text-anchor"] || "start";
  let x = num(attrs.x);
  let y = num(attrs.y);
  const transform = attrs.transform || "";
  const rotateMatch = transform.match(/translate\(([-0-9.]+)\s+([-0-9.]+)\)\s+rotate\((-?90)\)/);

  if (anchor === "middle") x -= textWidth(text, size) / 2;
  if (anchor === "end") x -= textWidth(text, size);

  if (rotateMatch) {
    const tx = num(rotateMatch[1]);
    const ty = num(rotateMatch[2]);
    return `q\n${fill.map(formatNumber).join(" ")} rg\nBT\n${font} ${formatNumber(size)} Tf\n0 1 -1 0 ${formatNumber(tx)} ${formatNumber(height - ty)} Tm\n(${pdfEscape(text)}) Tj\nET\nQ\n`;
  }

  return `${fill.map(formatNumber).join(" ")} rg\nBT\n${font} ${formatNumber(size)} Tf\n${formatNumber(x)} ${formatNumber(height - y)} Td\n(${pdfEscape(text)}) Tj\nET\n`;
}

function makePdf(pageWidth, pageHeight, content) {
  const objects = [];
  function addObject(body) {
    objects.push(body);
    return objects.length;
  }

  const catalog = addObject("<< /Type /Catalog /Pages 2 0 R >>");
  const pages = addObject("<< /Type /Pages /Kids [3 0 R] /Count 1 >>");
  const page = addObject(`<< /Type /Page /Parent 2 0 R /MediaBox [0 0 ${pageWidth} ${pageHeight}] /Resources << /Font << /F1 5 0 R /F2 6 0 R >> >> /Contents 4 0 R >>`);
  const stream = addObject(`<< /Length ${Buffer.byteLength(content, "utf8")} >>\nstream\n${content}\nendstream`);
  const font1 = addObject("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>");
  const font2 = addObject("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>");
  void catalog; void pages; void page; void stream; void font1; void font2;

  let pdf = "%PDF-1.4\n";
  const offsets = [0];
  for (let i = 0; i < objects.length; i++) {
    offsets.push(Buffer.byteLength(pdf, "utf8"));
    pdf += `${i + 1} 0 obj\n${objects[i]}\nendobj\n`;
  }
  const xrefOffset = Buffer.byteLength(pdf, "utf8");
  pdf += `xref\n0 ${objects.length + 1}\n0000000000 65535 f \n`;
  for (let i = 1; i < offsets.length; i++) {
    pdf += `${String(offsets[i]).padStart(10, "0")} 00000 n \n`;
  }
  pdf += `trailer\n<< /Size ${objects.length + 1} /Root 1 0 R >>\nstartxref\n${xrefOffset}\n%%EOF\n`;
  return pdf;
}

function convertSvg(svgPath) {
  const svg = fs.readFileSync(svgPath, "utf8");
  const svgTag = svg.match(/<svg[^>]+>/)[0];
  const svgAttrs = parseAttrs(svgTag);
  const width = num(svgAttrs.width);
  const height = num(svgAttrs.height);
  let content = "1 J 1 j\n";

  for (const line of svg.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (trimmed.startsWith("<rect ")) content += commandsForRect(parseAttrs(trimmed), height);
    else if (trimmed.startsWith("<line ")) content += commandsForLine(parseAttrs(trimmed), height);
    else if (trimmed.startsWith("<polyline ")) content += commandsForPolyline(parseAttrs(trimmed), height);
    else if (trimmed.startsWith("<circle ")) content += commandsForCircle(parseAttrs(trimmed), height);
    else if (trimmed.startsWith("<text ")) content += commandsForText(trimmed, height);
  }

  const pdfPath = svgPath.replace(/\.svg$/i, ".pdf");
  fs.writeFileSync(pdfPath, makePdf(width, height, content), "binary");
  return pdfPath;
}

const svgFiles = fs.readdirSync(inputDir)
  .filter((file) => file.toLowerCase().endsWith(".svg"))
  .map((file) => path.join(inputDir, file));

const pdfFiles = svgFiles.map(convertSvg);
console.log(`Wrote ${pdfFiles.length} PDF files to ${inputDir}.`);
for (const pdfFile of pdfFiles) console.log(pdfFile);
