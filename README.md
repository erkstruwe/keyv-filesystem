# keyv-filesystem

Filesystem storage adapter for Keyv, optimized for binary files on disk with one file per entry.

[![CI](https://github.com/erkstruwe/keyv-filesystem/actions/workflows/ci.yml/badge.svg)](https://github.com/erkstruwe/keyv-filesystem/actions/workflows/ci.yml)

## Install

```shell
npm install --save keyv keyv-filesystem
```

## Runtime Dependencies

This package currently depends on:

- `better-sqlite3`

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
  serialize: undefined,
  deserialize: undefined,
});

const fileBuffer = await readFile("./assets/image.bin");
await keyv.set("image", fileBuffer);
const value = await keyv.get("image");
```

> [!IMPORTANT]
> Set `serialize` and `deserialize` to `undefined` when using `keyv-filesystem` via `Keyv`.
> Otherwise, Keyv's default JSON serialization runs first and the adapter will not receive the original binary/stream payload.

### Using Adapter Serialization Through Keyv

If you want this adapter's serializer/deserializer behavior (for `Buffer`, Node `Readable`, or Web `ReadableStream`) through `Keyv`, disable Keyv-level serialization so the adapter receives the original payload.

```js
import Keyv from "keyv";
import { createReadStream } from "fs";
import { KeyvFilesystem } from "keyv-filesystem";

const keyv = new Keyv({
  store: new KeyvFilesystem({
    path: "./node_modules/.cache/keyv-filesystem",
  }),
  serialize: undefined,
  deserialize: undefined,
});

await keyv.set("video", createReadStream("./assets/video.bin"));
```

Web `ReadableStream` example with the same Keyv config:

```js
import { open } from "fs/promises";

const fileHandle = await open("./assets/archive.bin", "r");
try {
  await keyv.set("archive", fileHandle.readableWebStream());
} finally {
  await fileHandle.close();
}
```

Why: Keyv's default serializer converts values to JSON-compatible data before they reach the store adapter. That means original stream bytes are no longer available to the adapter. With `serialize`/`deserialize` set to `undefined`, `keyv-filesystem` receives the original value and can write the intended bytes.

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
- File names are encoded identities (namespace + key), not raw key strings.
- Expiration time is stored in the SQLite index (`expires_at`), not in file names.
- Expired values are deleted on read and by periodic sweep (`expiredCheckDelay`).
- A SQLite metadata index is always used for lookups and sweeps.

## Options

- `path` (required): storage directory. There is no default.
- `expiredCheckDelay` (default callback): sweep schedule.
  - `number`: fixed interval in milliseconds.
  - `callback`: `(lastSweep) => number | Promise<number>` to compute the next interval dynamically.
  - The delay always starts after a sweep has finished.
    This means the interval is measured from end of previous sweep to start of next sweep, for both `number` and `callback` modes.
- `extension` (default `.bin`): file extension for entry files.
- `serialize` (default serializer): `(Buffer | Readable | ReadableStream) -> Readable`.
- `deserialize` (default deserializer): `Readable -> Buffer`.
- `durability` (default `standard`): write durability strategy.

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
  expiredCheckDelay: (lastSweep) => number,
  extension: '.bin',
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

### SQLite Index

The adapter always uses a SQLite index file at:

`<path>/.keyv-filesystem-index.sqlite`

Behavior:

- If the index file does not exist yet, the adapter creates a new empty SQLite index.
- Existing data files in the directory are left untouched and are not auto-imported into the index.
- Regular sweeps (`clearExpire` and scheduled sweeps) then operate based on the SQLite index.
- Lookups first resolve `(namespace, key)` in SQLite, then load or delete the data file depending on `expires_at`.
- In `set`, the index row is written before the payload file is written.
  On crash, this can leave extra/stale index rows, which are reconciled later when file operations hit `ENOENT` and during sweeps.
- During sweeps, SQLite cleanup is done by cutoff query (`DELETE ... WHERE expires_at <= cutoff`) for the active namespace scope.

Concurrency note for multiple instances sharing the same `path`:

- SQLite file integrity is safe across parallel processes (WAL + SQLite locking).
- Store-level operations are not fully cross-process transactional because payload files and index rows are updated in separate steps.
- In rare races, this can cause temporary drift (for example orphaned files or a recently written key being briefly missing from the index until a later operation/sweep reconciles state).
- Recommended for strict correctness: one writer process per storage path.

Practical recommendation:

- This adapter now assumes SQLite is available at runtime.
- For multi-process writes to the same path, temporary index/file drift can still occur; for strict correctness, use a single writer process per storage path.

## Behavior Notes

- Async file writes are always stream-based (`Readable` -> `Writable`).
- Async file reads are always stream-based (`Readable` from disk).
- `set` always routes values through `serialize` and expects a `Readable` result.
- `get` always routes the file `Readable` through `deserialize`.
- Default serializer accepts `Buffer`, Node `Readable`, and Web `ReadableStream` and converts to Node `Readable`.
- Default deserializer consumes a Node `Readable` and returns a `Buffer`.
- Custom serializers/deserializers must follow the same stream contracts at the boundaries.
- The adapter exposes async operations only; synchronous cache APIs are intentionally not supported.
- Writes update TTL metadata in SQLite and atomically write the payload file.
- When `namespace` is set by Keyv, files are isolated by namespace within the same directory.
- Only `ENOENT` is treated as cache miss; other IO errors are thrown.
- Empty-string keys are supported consistently across `get`, `iterator`, `clear`, and expiry sweep.
- Bulk methods `setMany` and `hasMany` are supported by the adapter.
- Tests should use temporary subfolders under `node_modules/.cache/`.

## License

MIT
