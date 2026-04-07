import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync, existsSync } from "node:fs";
import { resolve } from "node:path";

const root = resolve(process.cwd());
const indexPath = resolve(root, "frontend/static/index.html");
const cssPath = resolve(root, "frontend/static/assets/css/app.css");
const jsPath = resolve(root, "frontend/static/assets/js/app.js");

test("static shell files exist", () => {
  assert.equal(existsSync(indexPath), true);
  assert.equal(existsSync(cssPath), true);
  assert.equal(existsSync(jsPath), true);
});

test("index references local assets and core views", () => {
  const html = readFileSync(indexPath, "utf8");
  assert.match(html, /href="\/assets\/css\/app\.css"/);
  assert.match(html, /src="\/assets\/js\/app\.js"/);
  assert.match(html, /id="view-dashboard"/);
  assert.match(html, /id="view-health"/);
  assert.match(html, /id="view-settings"/);
  assert.match(html, /id="view-diagnostics"/);
});

test("frontend script targets runtime endpoints", () => {
  const script = readFileSync(jsPath, "utf8");
  assert.match(script, /\/api\/v1\/health\/live/);
  assert.match(script, /\/api\/v1\/health\/ready/);
  assert.match(script, /\/api\/v1\/diagnostics\/startup/);
  assert.match(script, /\/api\/v1\/diagnostics\/runtime/);
  assert.match(script, /\/api\/v1\/settings\/effective/);
  assert.match(script, /\/api\/v1\/settings\/schema/);
});

test("frontend stylesheet includes theme tokens and responsive layout", () => {
  const css = readFileSync(cssPath, "utf8");
  assert.match(css, /--accent:/);
  assert.match(css, /@keyframes revealUp/);
  assert.match(css, /@media \(max-width: 960px\)/);
});
