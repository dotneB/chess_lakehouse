import { mkdirSync } from "node:fs";
import { join } from "node:path";
import { parseArgs } from "node:util";
import { $ } from "bun";
import {
  duckdbCliArgs,
  listFilesByExtensionRecursively,
  outputStemFromRelativePath,
  sqlStringLiteral,
} from "./lib/pipeline-utils";

type Args = {
  inDir: string;
  outDir: string;
  key?: string;
};

const { values } = parseArgs({
  args: Bun.argv,
  options: {
    inDir: { type: "string" },
    outDir: { type: "string" },
    key: { type: "string" },
  },
  strict: true,
  allowPositionals: true,
});

const args = values as Partial<Args>;

if (!args.inDir || !args.outDir) {
  throw new Error(
    "Usage: bun run src/read-pgn.ts --inDir <dir> --outDir <dir> [--key <name>]",
  );
}

async function readOnePgn(params: {
  inDir: string;
  pgnPath: string;
  outDir: string;
}) {
  const stem = outputStemFromRelativePath(params.inDir, params.pgnPath, ".pgn");
  const outDb = join(params.outDir, `${stem}.duckdb`);

  const sql = [
    "LOAD chess;",
    "CREATE TABLE IF NOT EXISTS games AS",
    "SELECT *, chess_moves_normalize(movetext) AS clean_movetext",
    `FROM read_pgn('${sqlStringLiteral(params.pgnPath)}');`,
  ].join("\n");

  await $`duckdb ${duckdbCliArgs()} ${outDb} -c ${sql}`;

  const output =
    await $`duckdb ${duckdbCliArgs(["-markdown", "-readonly"])} ${outDb} -c "SELECT COUNT(*) as 'Games' FROM games; SELECT COUNT(*) AS 'Games with Parse Errors' FROM games WHERE parse_error IS NOT NULL; SELECT * FROM games WHERE parse_error IS NOT NULL;"`.text();
  console.log(output);
}

mkdirSync(args.outDir, { recursive: true });

const pgns = listFilesByExtensionRecursively(args.inDir, ".pgn");
if (!pgns.length) {
  throw new Error(`No .pgn files found under ${args.inDir}`);
}

console.log(
  `Reading ${pgns.length} PGN(s) from ${args.inDir} to ${args.outDir}`,
);

for (const pgnPath of pgns) {
  await readOnePgn({ inDir: args.inDir, pgnPath, outDir: args.outDir });
}
