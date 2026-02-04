import { readFileSync } from "node:fs";
import { parseArgs } from "node:util";

type Dataset = {
  title: string;
  category: "Online" | "OTB";
  downloadUrl: string;
  key: string;
};

function decodeHtmlEntities(input: string): string {
  // Minimal decoding for the strings we see on the page.
  return input
    .replaceAll("&amp;", "&")
    .replaceAll("&quot;", '"')
    .replaceAll("&#039;", "'")
    .replaceAll("&lt;", "<")
    .replaceAll("&gt;", ">")
    .replace(/&#(\d+);/g, (_m, n) => String.fromCharCode(Number(n)));
}

function normalizeTitle(input: string): string {
  return decodeHtmlEntities(input)
    .replace(/[\u2013\u2014]/g, "-") // en/em dash
    .replace(/\s+/g, " ")
    .trim();
}

function deriveKey(
  title: string,
): { key: string; category: Dataset["category"] } | null {
  const t = normalizeTitle(title);
  const otb = /^otb\b/i.test(t);
  const online = /^online\b/i.test(t);
  if (!otb && !online) return null;

  const category: Dataset["category"] = otb ? "OTB" : "Online";
  const remainder = t
    .replace(/^otb\b/i, "")
    .replace(/^online\b/i, "")
    .trim();

  let suffix: string | null = null;
  if (/^nodate$/i.test(remainder)) {
    suffix = "nodate";
  } else if (/partial release/i.test(remainder)) {
    const y = remainder.match(/\b(\d{4})\b/);
    suffix = y ? `${y[1]}_partial` : "partial";
  } else {
    // Year range: 1995-2009, 0001-1899, 2020-2024
    const yr = remainder.match(/^(\d{4})\s*-\s*(\d{4})$/);
    if (yr?.[1] && yr?.[2]) suffix = `${yr[1]}_${yr[2]}`;

    // Single year: 2025
    const y = remainder.match(/^(\d{4})$/);
    if (!suffix && y?.[1]) suffix = y[1];

    // Year-month: 2026-02
    const ym = remainder.match(/^(\d{4})\s*-\s*(\d{2})$/);
    if (!suffix && ym?.[1] && ym?.[2]) suffix = `${ym[1]}_${ym[2]}`;
  }

  if (!suffix) return null;
  const key = `lumbras_${category.toLowerCase()}_${suffix}`;
  return { key, category };
}

function extractDownloadLinks(html: string, baseUrl: string): Dataset[] {
  const datasets: Dataset[] = [];
  const anchorRe = /<a\b[^>]*>/gi;
  let m: RegExpExecArray | null = anchorRe.exec(html);
  while (m) {
    const tag = m[0];
    const anchorIndex = m.index;
    m = anchorRe.exec(html);
    if (!/\bwpdm-download-link\b/i.test(tag)) continue;

    const dataUrl = tag.match(/\bdata-downloadurl\s*=\s*(["'])(.*?)\1/i)?.[2];
    if (!dataUrl) continue;
    const downloadUrl = new URL(
      decodeHtmlEntities(dataUrl),
      baseUrl,
    ).toString();

    // Find the nearest preceding <strong>Title</strong>.
    // Titles are often a bit above the download button in the card HTML.
    // Use a generous lookback window to avoid missing it when markup changes.
    const windowStart = Math.max(0, anchorIndex - 20000);
    const back = html.slice(windowStart, anchorIndex);
    const strongRe =
      /<strong\b[^>]*>\s*(?:<a\b[^>]*>\s*)?([^<]+?)\s*(?:<\/a>\s*)?<\/strong>/gi;
    let lastStrong: string | null = null;
    let sm: RegExpExecArray | null = strongRe.exec(back);
    while (sm) {
      const s = sm[1];
      if (s) lastStrong = s;

      sm = strongRe.exec(back);
    }
    if (!lastStrong) continue;

    const title = normalizeTitle(lastStrong);
    const derived = deriveKey(title);
    if (!derived) continue;

    datasets.push({
      title,
      category: derived.category,
      downloadUrl,
      key: derived.key,
    });
  }

  // De-dupe by key (keep first occurrence)
  const seen = new Set<string>();
  const deduped: Dataset[] = [];
  for (const d of datasets) {
    if (seen.has(d.key)) continue;
    seen.add(d.key);
    deduped.push(d);
  }
  return deduped;
}

async function resolveMegaKey(
  downloadUrl: string,
  timeoutMs: number,
): Promise<string | null> {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(downloadUrl, {
      method: "HEAD",
      redirect: "manual",
      signal: controller.signal,
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
      },
    });

    const loc = res.headers.get("location");
    if (!loc) return null;

    const resolved = new URL(loc, downloadUrl).toString();
    const mm = resolved.match(
      /https?:\/\/mega\.nz\/file\/([^#?/]+)#([^?\s]+)/i,
    );
    if (!mm) return null;
    return `${mm[1]}#${mm[2]}`;
  } finally {
    clearTimeout(t);
  }
}

function existingSourceKeys(paramsYamlPath: string): Set<string> {
  try {
    const txt = readFileSync(paramsYamlPath, "utf8");
    const keys = new Set<string>();
    for (const line of txt.split(/\r?\n/)) {
      const m = line.match(/^\s{2}([A-Za-z0-9_]+):\s*$/);
      if (m?.[1]) keys.add(m[1]);
    }
    return keys;
  } catch {
    return new Set();
  }
}

function printYamlEntries(
  entries: Array<{ key: string; category: string; mega: string }>,
) {
  for (const e of entries) {
    console.log(`  ${e.key}:`);
    console.log(`    mega: "${e.mega}"`);
    console.log(`    category: "${e.category}"`);
    console.log("");
  }
}

const { values } = parseArgs({
  args: Bun.argv,
  options: {
    url: { type: "string" },
    params: { type: "string" },
    all: { type: "boolean" },
    noResolve: { type: "boolean" },
    timeoutMs: { type: "string" },
  },
  strict: true,
  allowPositionals: true,
});

const url =
  (values.url as string | undefined) ??
  "https://lumbrasgigabase.com/en/download-in-pgn-format-en/";
const paramsPath = (values.params as string | undefined) ?? "params.yaml";
const includeExisting = Boolean(values.all);
const resolve = !values.noResolve;
const timeoutMs = Number((values.timeoutMs as string | undefined) ?? "30000");
if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
  throw new Error(
    "Invalid --timeoutMs; expected a positive number (milliseconds)",
  );
}

console.log(`# Source: ${url}`);
console.log(`# Generated: ${new Date().toISOString()}`);
console.log("# Paste under `sources:` in params.yaml");
console.log("");

const page = await fetch(url, {
  headers: {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
  },
});
if (!page.ok) {
  throw new Error(`Failed to fetch ${url} (${page.status} ${page.statusText})`);
}
const html = await page.text();

const datasets = extractDownloadLinks(html, url).sort((a, b) =>
  a.key.localeCompare(b.key),
);
const existing = existingSourceKeys(paramsPath);

const toPrint: Array<{ key: string; category: string; mega: string }> = [];
for (const d of datasets) {
  if (!includeExisting && existing.has(d.key)) continue;
  if (!resolve) {
    console.log(`# ${d.title}`);
    console.log(`# download: ${d.downloadUrl}`);
    console.log("");
    continue;
  }

  const mega = await resolveMegaKey(d.downloadUrl, timeoutMs);
  if (!mega) {
    console.log(`# ${d.title}`);
    console.log(`# download: ${d.downloadUrl}`);
    console.log(
      "# WARNING: could not resolve mega key (no Location header or non-MEGA target)",
    );
    console.log("");
    continue;
  }

  toPrint.push({ key: d.key, category: d.category, mega });
}

const online = toPrint.filter((e) => e.category === "Online");
const otb = toPrint.filter((e) => e.category === "OTB");

if (online.length) {
  console.log("  # Online datasets");
  printYamlEntries(online);
}

if (otb.length) {
  console.log("  # OTB datasets");
  printYamlEntries(otb);
}

if (!includeExisting && resolve && !toPrint.length) {
  console.log(`# No new datasets found relative to ${paramsPath}`);
}

if (!resolve && !toPrint.length) {
  // When --noResolve is used we print comments per dataset instead of YAML.
  // This message helps avoid a silent run when all keys already exist.
  if (!includeExisting) {
    console.log(`# No new datasets found relative to ${paramsPath}`);
  }
}
