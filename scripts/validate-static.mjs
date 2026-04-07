import { readFileSync, existsSync } from "node:fs";
import { resolve } from "node:path";

const root = resolve(process.cwd());
const requiredFiles = [
  "frontend/static/index.html",
  "frontend/static/assets/css/app.css",
  "frontend/static/assets/js/app.js",
];

const missing = requiredFiles.filter((path) => !existsSync(resolve(root, path)));
if (missing.length) {
  console.error("Missing static bundle files:");
  for (const file of missing) {
    console.error(`- ${file}`);
  }
  process.exit(1);
}

const html = readFileSync(resolve(root, "frontend/static/index.html"), "utf8");
const cssHref = '/assets/css/app.css';
const jsSrc = '/assets/js/app.js';

if (!html.includes(cssHref) || !html.includes(jsSrc)) {
  console.error("index.html must reference the local CSS and JS assets.");
  process.exit(1);
}

const externalAssetPattern = /<(?:script|link)[^>]+(?:src|href)="https?:\/\//i;
if (externalAssetPattern.test(html)) {
  console.error("Static shell must not depend on external CDN assets.");
  process.exit(1);
}

console.log("Static bundle looks valid.");
