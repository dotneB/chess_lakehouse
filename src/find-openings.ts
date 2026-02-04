import { copyFileSync, mkdirSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { parseArgs } from "node:util";
import { $ } from "bun";
import {
  duckdbCliArgs,
  listFilesByExtensionRecursively,
  sqlStringLiteral,
} from "./lib/pipeline-utils";

type Args = {
  inDir: string;
  outDir: string;
  openingsDb: string;
  dataSource?: string;
  key?: string;
};

const { values } = parseArgs({
  args: Bun.argv,
  options: {
    inDir: { type: "string" },
    outDir: { type: "string" },
    openingsDb: { type: "string" },
    dataSource: { type: "string" },
    gigabase: { type: "string" },
    key: { type: "string" },
  },
  strict: true,
  allowPositionals: true,
});

const args = values as Partial<Args>;

if (!args.inDir || !args.outDir || !args.openingsDb || !args.dataSource) {
  throw new Error(
    "Usage: bun run src/find-openings.ts --inDir <dir> --outDir <dir> --openingsDb <path> --dataSource <name> [--key <name>]",
  );
}

mkdirSync(args.outDir, { recursive: true });

const dbs = listFilesByExtensionRecursively(args.inDir, ".duckdb");
if (!dbs.length) {
  throw new Error(`No .duckdb files found under ${args.inDir}`);
}

console.log(
  `Enriching ${dbs.length} DuckDB file(s) from ${args.inDir} to ${args.outDir}`,
);

for (const srcDb of dbs) {
  const rel = relative(args.inDir, srcDb);
  const dstDb = join(args.outDir, rel);
  mkdirSync(dirname(dstDb), { recursive: true });
  copyFileSync(srcDb, dstDb);

  const sql = [
    "LOAD chess;",
    "ALTER TABLE games ADD COLUMN IF NOT EXISTS DataSource VARCHAR;",
    `UPDATE games SET DataSource = '${sqlStringLiteral(args.dataSource)}';`,
    `ATTACH '${sqlStringLiteral(args.openingsDb)}' AS openings (READ_ONLY);`,
    "WITH",
    "  openings_with_ply AS (",
    "    SELECT array_length(string_split(uci, ' ')) AS opening_ply, o.* FROM openings.openings o",
    "  ),",
    "  target AS (",
    "    SELECT rowid as game_id, * FROM games WHERE Opening IS NULL",
    "  )",
    "UPDATE games m",
    "SET eco = o.eco, Opening = o.name",
    "FROM target t",
    "  JOIN LATERAL (",
    "    SELECT o.eco, o.name",
    "    FROM openings_with_ply o",
    "    WHERE CONTAINS(t.clean_movetext, o.pgn)",
    "    ORDER BY o.opening_ply DESC",
    "    LIMIT 1",
    "  ) o ON TRUE",
    "WHERE m.rowid = t.game_id;",
    "DETACH openings;",
  ].join("\n");

  await $`duckdb ${duckdbCliArgs()} ${dstDb} -c ${sql}`;
}
