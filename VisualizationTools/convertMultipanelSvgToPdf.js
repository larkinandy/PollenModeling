const fs = require("fs");
const path = require("path");

const dir = path.join("outputs", "study_city_pollen_multipanel_figures");

function attrs(tag) {
  const out = {};
  for (const m of tag.matchAll(/([a-zA-Z_:.-]+)="([^"]*)"/g)) out[m[1]] = m[2];
  return out;
}
function n(x, d = 0) { const v = Number(x); return Number.isFinite(v) ? v : d; }
function rgb(c) {
  if (!c || c === "none" || !c.startsWith("#") || c.length !== 7) return null;
  return [parseInt(c.slice(1,3),16)/255, parseInt(c.slice(3,5),16)/255, parseInt(c.slice(5,7),16)/255];
}
function f(x) { return Number(x).toFixed(3).replace(/\.?0+$/, ""); }
function esc(s) { return String(s).replace(/\\/g,"\\\\").replace(/\(/g,"\\(").replace(/\)/g,"\\)").replace(/[^\x20-\x7e]/g, "?"); }
function xml(s) { return String(s).replace(/&gt;/g,">").replace(/&lt;/g,"<").replace(/&quot;/g,'"').replace(/&amp;/g,"&"); }
function tw(s, size) { return s.length * size * 0.53; }

function rect(a, H) {
  const x=n(a.x), y=n(a.y), w=n(a.width), h=n(a.height), fill=rgb(a.fill), stroke=rgb(a.stroke);
  let s = "";
  if (fill) s += `${fill.map(f).join(" ")} rg\n${f(x)} ${f(H-y-h)} ${f(w)} ${f(h)} re f\n`;
  if (stroke) s += `${stroke.map(f).join(" ")} RG\n${f(n(a["stroke-width"],1))} w\n${f(x)} ${f(H-y-h)} ${f(w)} ${f(h)} re S\n`;
  return s;
}
function line(a, H) {
  const stroke=rgb(a.stroke); if (!stroke) return "";
  return `${stroke.map(f).join(" ")} RG\n${f(n(a["stroke-width"],1))} w\n${f(n(a.x1))} ${f(H-n(a.y1))} m ${f(n(a.x2))} ${f(H-n(a.y2))} l S\n`;
}
function poly(a, H) {
  const stroke=rgb(a.stroke); if (!stroke || !a.points) return "";
  const pts=a.points.trim().split(/\s+/).map(p=>p.split(",").map(Number)); if (pts.length < 2) return "";
  let s=`${stroke.map(f).join(" ")} RG\n${f(n(a["stroke-width"],1))} w\n${f(pts[0][0])} ${f(H-pts[0][1])} m\n`;
  for (const p of pts.slice(1)) s += `${f(p[0])} ${f(H-p[1])} l\n`;
  return s + "S\n";
}
function text(tag, H) {
  const a=attrs(tag), m=tag.match(/>([^<]*)<\/text>/); if (!m) return "";
  const str=xml(m[1]), size=n(a["font-size"],12), fill=rgb(a.fill)||[0,0,0], font=a["font-weight"]==="700"?"/F2":"/F1";
  let x=n(a.x), y=n(a.y);
  if (a["text-anchor"] === "middle") x -= tw(str,size)/2;
  if (a["text-anchor"] === "end") x -= tw(str,size);
  return `${fill.map(f).join(" ")} rg\nBT\n${font} ${f(size)} Tf\n${f(x)} ${f(H-y)} Td\n(${esc(str)}) Tj\nET\n`;
}
function pdf(W,H,content) {
  const objs=[
    "<< /Type /Catalog /Pages 2 0 R >>",
    "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
    `<< /Type /Page /Parent 2 0 R /MediaBox [0 0 ${W} ${H}] /Resources << /Font << /F1 5 0 R /F2 6 0 R >> >> /Contents 4 0 R >>`,
    `<< /Length ${Buffer.byteLength(content)} >>\nstream\n${content}\nendstream`,
    "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>",
  ];
  let out="%PDF-1.4\n"; const off=[0];
  objs.forEach((o,i)=>{ off.push(Buffer.byteLength(out)); out += `${i+1} 0 obj\n${o}\nendobj\n`; });
  const xref=Buffer.byteLength(out);
  out += `xref\n0 ${objs.length+1}\n0000000000 65535 f \n`;
  for (let i=1;i<off.length;i++) out += `${String(off[i]).padStart(10,"0")} 00000 n \n`;
  return out + `trailer\n<< /Size ${objs.length+1} /Root 1 0 R >>\nstartxref\n${xref}\n%%EOF\n`;
}

let count = 0;
for (const file of fs.readdirSync(dir).filter(x => x.endsWith(".svg"))) {
  const svg = fs.readFileSync(path.join(dir,file), "utf8");
  const root = attrs(svg.match(/<svg[^>]+>/)[0]);
  const W=n(root.width), H=n(root.height);
  let content = "1 J 1 j\n";
  for (const raw of svg.split(/\r?\n/)) {
    const l = raw.trim();
    if (l.startsWith("<rect ")) content += rect(attrs(l), H);
    else if (l.startsWith("<line ")) content += line(attrs(l), H);
    else if (l.startsWith("<polyline ")) content += poly(attrs(l), H);
    else if (l.startsWith("<text ")) content += text(l, H);
  }
  pdfPath = path.join(dir,file.replace(/\.svg$/,".pdf"));
  try {
    fs.writeFileSync(pdfPath, pdf(W,H,content), "binary");
  } catch (error) {
    if (error.code !== "EBUSY") throw error;
    const fallbackPath = path.join(dir,file.replace(/\.svg$/,"_updated.pdf"));
    fs.writeFileSync(fallbackPath, pdf(W,H,content), "binary");
    console.log(`Locked: ${pdfPath}. Wrote ${fallbackPath} instead.`);
  }
  count++;
}
console.log(`Wrote ${count} PDF files to ${dir}.`);

