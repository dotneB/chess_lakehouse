import { readdirSync, readFileSync } from "node:fs";
import { join, relative } from "node:path";

function isTruthyEnvValue(v: string | undefined): boolean {
  if (!v) return false;
  switch (v.trim().toLowerCase()) {
    case "1":
    case "true":
    case "yes":
    case "y":
    case "on":
      return true;
    default:
      return false;
  }
}

export function duckdbCliArgs(extra?: string[]): string[] {
  // Configure the DuckDB CLI invocation via env vars.
  // Also auto-enable unsigned extensions when params.yaml uses a local chess extension.
  // - DUCKDB_UNSIGNED=1 enables loading unsigned extensions (adds -unsigned).
  // - DUCKDB_CLI_ARGS="..." appends additional CLI args (simple whitespace split).
  const out: string[] = [];

  if (shouldForceDuckdbUnsignedFromParamsYaml()) {
    out.push("-unsigned");
  } else if (isTruthyEnvValue(process.env.DUCKDB_UNSIGNED)) {
    out.push("-unsigned");
  }

  const extra_env = process.env.DUCKDB_CLI_ARGS?.trim();
  if (extra_env) {
    out.push(...extra_env.split(/\s+/g).filter(Boolean));
  }
  if (extra) {
    out.push(...extra);
  }

  return out;
}

let cachedForceUnsigned: boolean | undefined;

function shouldForceDuckdbUnsignedFromParamsYaml(): boolean {
  if (cachedForceUnsigned !== undefined) return cachedForceUnsigned;

  try {
    // Very small "parser" for the single key we care about; avoid adding a YAML dependency.
    const raw = readFileSync("params.yaml", "utf8");
    const m = raw.match(/^\s*chess_ext_version\s*:\s*['"]?([^'"\n#]+)['"]?/m);
    const v = m?.[1]?.trim();
    cachedForceUnsigned = !!v && v.toLowerCase().includes("local");
  } catch {
    cachedForceUnsigned = false;
  }

  return cachedForceUnsigned;
}

export function sqlStringLiteral(s: string): string {
  // DuckDB SQL string literal escaping.
  return s.replaceAll("'", "''");
}

export function sqlPath(p: string): string {
  // Prefer forward slashes for DuckDB string paths.
  return sqlStringLiteral(p.replaceAll("\\", "/"));
}

export function listFilesByExtensionRecursively(
  dir: string,
  extLower: string,
): string[] {
  const out: string[] = [];
  const entries = readdirSync(dir, { withFileTypes: true });

  for (const ent of entries) {
    const full = join(dir, ent.name);
    if (ent.isDirectory()) {
      out.push(...listFilesByExtensionRecursively(full, extLower));
      continue;
    }
    if (ent.isFile() && ent.name.toLowerCase().endsWith(extLower)) {
      out.push(full);
    }
  }

  return out;
}

export function outputStemFromRelativePath(
  rootDir: string,
  filePath: string,
  stripExtLower: string,
): string {
  const rel = relative(rootDir, filePath);
  return rel
    .replace(new RegExp(`${escapeRegExp(stripExtLower)}$`, "i"), "")
    .replace(/[\\/]/g, "__");
}

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
