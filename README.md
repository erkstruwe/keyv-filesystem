# keyv-filesystem

Filesystem storage adapter for Keyv, optimized for binary files on disk with one file per entry.

[![CI](https://github.com/erkstruwe/keyv-filesystem/actions/workflows/ci.yml/badge.svg)](https://github.com/erkstruwe/keyv-filesystem/actions/workflows/ci.yml)

## Install

```shell
npm install --save keyv keyv-filesystem
```

## Runtime Dependencies

This package currently depends on:

- `better-sqlite3` (used when `useIndexFile: true`)

Development dependencies are used only for building, testing, and formatting.

## Usage

`path` is required when creating `KeyvFilesystem`.

```js
import Keyv from "keyv";
import { readFile } from "fs/promises";
import { KeyvFilesystem } from "keyv-filesystem";

const keyv = new Keyv({
  store: new KeyvFilesystem({
    path: "./node_modules/.cache/keyv-filesystem",
  }),
});

const fileBuffer = await readFile("./assets/image.bin");
await keyv.set("image", fileBuffer);
const value = await keyv.get("image");
```

### Default Serializer Input Types

The default serializer accepts exactly these input types:

```js
import { open, readFile } from "fs/promises";
import { createReadStream } from "fs";
import { Readable } from "stream";

// 1) Buffer
const fileBuffer = await readFile("./assets/image.bin");
await keyv.set("as-buffer", fileBuffer);

// 2) Node Readable
const readableFromFile = createReadStream("./assets/video.bin");
await keyv.set("as-node-readable", readableFromFile);

// 3) Web ReadableStream
const fileHandle = await open("./assets/archive.bin", "r");
await keyv.set("as-web-readable-stream", fileHandle.readableWebStream());
await fileHandle.close();
```

### JSON Object Example (Readable <-> Object)

```ts
import { Readable } from "stream";
import { KeyvFilesystem } from "keyv-filesystem";

type UserProfile = {
  id: string;
  name: string;
};

const store = new KeyvFilesystem<UserProfile, UserProfile>({
  path: "./node_modules/.cache/keyv-json-store",

  // Object -> JSON bytes -> Readable
  serialize: (value) => Readable.from([Buffer.from(JSON.stringify(value))]),

  // Readable -> bytes -> JSON -> Object
  deserialize: async (stream) => {
    const chunks: Buffer[] = [];
    for await (const chunk of stream) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }

    return JSON.parse(Buffer.concat(chunks).toString("utf8")) as UserProfile;
  },
});

await store.set("user:1", { id: "1", name: "Ada" });
const user = await store.get("user:1"); // typed as UserProfile | undefined
```

## How It Works

- One file per key under `path`.
- Expiration time is encoded in the entry filename as a suffix token.
- Non-expiring values use the special filename token `never`.
- Expired values are deleted on read and by periodic sweep (`expiredCheckDelay`).
- Optional SQLite metadata index (`useIndexFile`) can be used for index-driven sweeps.

## Options

- `path` (required): storage directory. There is no default.
- `expiredCheckDelay` (default callback): sweep schedule.
  - `number`: fixed interval in milliseconds.
  - `callback`: `(lastSweep) => number | Promise<number>` to compute the next interval dynamically.
  - The delay always starts after a sweep has finished.
    This means the interval is measured from end of previous sweep to start of next sweep, for both `number` and `callback` modes.
- `extension` (default `.bin`): file extension for entry files.
- `useIndexFile` (default `false`): store metadata in `.keyv-filesystem-index.sqlite` and run normal sweeps against the index file instead of directory scans.
- `serialize` (default serializer): `(Buffer | Readable | ReadableStream) -> Readable`.
- `deserialize` (default deserializer): `Readable -> Buffer`.
- `durability` (default `standard`): write durability strategy.
- `dialect` (default `redis`): Keyv compatibility hint for iterable adapter detection.

### Durability Modes

- `standard`:
  - Write entry payload to a temp file in the same directory.
  - Atomically rename temp file to the final key path.
  - This is fast and protects against most partial-write corruption scenarios.
- `strict`:
  - Same temp-file + rename flow as `standard`.
  - Also performs best-effort `fsync` on the temp file before rename.
  - Also performs best-effort directory `fsync` after rename.
  - This reduces data-loss risk during sudden power loss/crash at the cost of extra IO latency.

For most workloads, `standard` is enough. Use `strict` when durability is more important than write throughput.

### Default Optional Values

```js
{
  dialect: 'redis',
  expiredCheckDelay: (lastSweep) => number,
  extension: '.bin',
  useIndexFile: false,
  serialize: (value) => Readable,
  deserialize: async (stream) => Buffer,
  durability: 'standard',
}
```

### Adaptive `expiredCheckDelay`

When `expiredCheckDelay` is a callback, it receives metrics from the previous sweep:

```ts
type ExpireSweepStats = {
  totalFiles: number; // all regular files in the storage directory
  namespaceFiles: number; // files belonging to this store namespace
  deletedFiles: number; // expired files removed by the sweep
  durationMs: number; // runtime of the sweep
};

type ExpiredCheckDelayResolver = (
  lastSweep: ExpireSweepStats | undefined,
) => number | Promise<number>;
```

The default callback uses these metrics to adapt the next interval and includes a built-in minimum of 1 minute.
No global minimum is enforced for user-supplied numbers or user-supplied callbacks.
Scheduling is always end-to-start: the next timeout begins after the current sweep completes.

Example custom strategy:

```ts
import { KeyvFilesystem } from "keyv-filesystem";

const store = new KeyvFilesystem({
  path: "./node_modules/.cache/keyv-filesystem",
  expiredCheckDelay: (lastSweep) => {
    if (!lastSweep) {
      return 60_000;
    }

    if (lastSweep.deletedFiles > 1000) {
      return 30_000;
    }

    if (lastSweep.namespaceFiles > 200_000 || lastSweep.durationMs > 2000) {
      return 30 * 60_000;
    }

    return 5 * 60_000;
  },
});
```

### SQLite Index Mode

Set `useIndexFile: true` to enable a SQLite index file at:

`<path>/.keyv-filesystem-index.sqlite`

Behavior in this mode:

- If the index file does not exist yet, the adapter immediately starts a bootstrap sweep.
- The bootstrap sweep scans existing entry files once, removes stale files, and populates the SQLite index.
- Regular sweeps (`clearExpire` and scheduled sweeps) then operate based on the SQLite index.
- Expiration metadata still remains encoded in the entry filename suffix.
- In `set`, the index row is written before the payload file is written.
  On crash, this can leave extra/stale index rows, which are reconciled later when file operations hit `ENOENT` and during sweeps.
- During sweeps, SQLite cleanup is done by cutoff query (`DELETE ... WHERE expires_at <= cutoff`) for the active namespace scope.

Concurrency note for multiple instances sharing the same `path`:

- SQLite file integrity is safe across parallel processes (WAL + SQLite locking).
- Store-level operations are not fully cross-process transactional because payload files and index rows are updated in separate steps.
- In rare races, this can cause temporary drift (for example orphaned files or a recently written key being briefly missing from the index until a later operation/sweep reconciles state).
- Recommended for strict correctness: one writer process per storage path.

### When To Use `useIndexFile`

Enable `useIndexFile: true` when:

- Your store directory contains many files and directory-wide expiry sweeps become expensive.
- You run frequent sweeps and want sweep cost to scale with expired index rows, not with full directory scans.
- You want faster startup behavior after the first bootstrap pass for large stores.
- You are okay with an additional runtime dependency (`better-sqlite3`) and a local SQLite file in the storage directory.

Keep `useIndexFile: false` when:

- Your store is small or moderate and directory scans are already cheap.
- You prefer minimal operational complexity and no SQLite sidecar file.
- You run in environments where native module handling for `better-sqlite3` is undesirable.

Practical recommendation:

- Default to `false` for small caches or simple deployments.
- Switch to `true` for large, long-lived caches with high key churn and regular expiry cleanup.
- For multi-process writes to the same path, use `true` only if temporary index/file drift is acceptable, or enforce a single writer process.

## Behavior Notes

- Async file writes are always stream-based (`Readable` -> `Writable`).
- Async file reads are always stream-based (`Readable` from disk).
- `set` always routes values through `serialize` and expects a `Readable` result.
- `get` always routes the file `Readable` through `deserialize`.
- Default serializer accepts `Buffer`, Node `Readable`, and Web `ReadableStream` and converts to Node `Readable`.
- Default deserializer consumes a Node `Readable` and returns a `Buffer`.
- Custom serializers/deserializers must follow the same stream contracts at the boundaries.
- The adapter exposes async operations only; synchronous cache APIs are intentionally not supported.
- Writes encode TTL in the final filename suffix token and then atomically rename.
- When `namespace` is set by Keyv, files are isolated by namespace within the same directory.
- Only `ENOENT` is treated as cache miss; other IO errors are thrown.
- Empty-string keys are supported consistently across `get`, `iterator`, `clear`, and expiry sweep.
- Bulk methods `setMany` and `hasMany` are supported by the adapter.
- Tests should use temporary subfolders under `node_modules/.cache/`.

## License

MIT
