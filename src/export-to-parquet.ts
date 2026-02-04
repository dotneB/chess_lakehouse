import { mkdirSync, rmSync } from "node:fs";
import { dirname } from "node:path";
import { parseArgs } from "node:util";
import { $ } from "bun";
import {
  duckdbCliArgs,
  listFilesByExtensionRecursively,
  sqlPath,
  sqlStringLiteral,
} from "./lib/pipeline-utils";

type Args = {
  inDir: string;
  outDir: string;
  outDb: string;
  dataSource?: string;
  key?: string;
};

const { values } = parseArgs({
  args: Bun.argv,
  options: {
    inDir: { type: "string" },
    outDir: { type: "string" },
    outDb: { type: "string" },
    dataSource: { type: "string" },
    key: { type: "string" },
  },
  strict: true,
  allowPositionals: true,
});

const args = values as Partial<Args>;
if (!args.inDir || !args.outDir || !args.outDb) {
  throw new Error(
    "Usage: bun run src/export-to-parquet.ts --inDir <dir> --outDir <dir> --outDb <path> [--dataSource <name>]",
  );
}

const dbs = listFilesByExtensionRecursively(args.inDir, ".duckdb").sort(
  (a, b) => a.localeCompare(b),
);
if (!dbs.length) {
  throw new Error(`No .duckdb files found under ${args.inDir}`);
}

console.log(
  `Exporting ${dbs.length} DuckDB file(s) from ${args.inDir} to ${args.outDir}`,
);

// DVC-friendly behavior: always produce a clean output.
rmSync(args.outDir, { recursive: true, force: true });
mkdirSync(args.outDir, { recursive: true });

rmSync(args.outDb, { force: true });
mkdirSync(dirname(args.outDb), { recursive: true });

let first = true;
const dataSource = args.dataSource ?? args.key;

for (const db of dbs) {
  const dataSourceSelect = dataSource
    ? `  '${sqlStringLiteral(dataSource)}' AS DataSource,`
    : "  DataSource,";

  const attachAndSelect = [
    `ATTACH '${sqlPath(db)}' AS src (READ_ONLY);`,
    first ? "CREATE TABLE combined AS" : "INSERT INTO combined",
    [
      "SELECT",
      "  Event,",
      "  Site,",
      "  White,",
      "  Black,",
      "  Result,",
      "  WhiteTitle,",
      "  BlackTitle,",
      "  WhiteElo,",
      "  BlackElo,",
      "  UTCDate,",
      "  UTCTime,",
      "  ECO,",
      "  Opening,",
      "  Termination,",
      "  TimeControl,",
      "  Source,",
      "  movetext,",
      dataSourceSelect,
      "  year(UTCDate) AS year,",
      "  strftime(UTCDate, '%m') AS month",
      "FROM src.games",
      "WHERE UTCDate IS NOT NULL",
      "AND year(UTCDate) >= 1500;",
    ].join("\n"),
    "DETACH src;",
  ].join("\n");

  await $`duckdb ${duckdbCliArgs()} ${args.outDb} -c ${attachAndSelect}`;
  first = false;
}

const output =
  await $`duckdb ${duckdbCliArgs(["-markdown", "-readonly"])} ${args.outDb} -c "SELECT COUNT(*) FROM combined;"`.text();
console.log(output);

const copySql = [
  "COPY combined",
  `TO ('${sqlPath(args.outDir)}')`,
  "  (FORMAT PARQUET, PARTITION_BY (DataSource, year, month));",
].join("\n");

await $`duckdb ${duckdbCliArgs()} ${args.outDb} -c ${copySql}`;
