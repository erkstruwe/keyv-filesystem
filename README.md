# keyv-filesystem

Filesystem storage adapter for Keyv, optimized for binary files on disk with one file per entry.

[![publish](https://github.com/erkstruwe/keyv-filesystem/actions/workflows/publish.yml/badge.svg)](https://github.com/erkstruwe/keyv-filesystem/actions/workflows/publish.yml)

## Install

```shell
npm install --save keyv keyv-filesystem
```

## Runtime Dependencies

This package currently has no runtime dependencies.
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
await keyv.set(
  "as-web-readable-stream",
  fileHandle.readableWebStream(),
);
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
- File mtime stores expiration time.
- Non-expiring values use a configurable far-future sentinel timestamp.
- Expired values are deleted on read and by periodic sweep (`expiredCheckDelay`).

## Options

- `path` (required): storage directory. There is no default.
- `expiredCheckDelay` (default `86400000`): sweep interval in milliseconds.
- `sentinelExpire` (default `Date.parse('2100-01-01T00:00:00.000Z')`): far-future timestamp used to represent non-expiring entries.
- `extension` (default `.bin`): file extension for entry files.
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
  expiredCheckDelay: 24 * 3600 * 1000,
  sentinelExpire: Date.parse('2100-01-01T00:00:00.000Z'),
  extension: '.bin',
  serialize: (value) => Readable,
  deserialize: async (stream) => Buffer,
  durability: 'standard',
}
```

## Behavior Notes

- Async file writes are always stream-based (`Readable` -> `Writable`).
- Async file reads are always stream-based (`Readable` from disk).
- `set` always routes values through `serialize` and expects a `Readable` result.
- `get` always routes the file `Readable` through `deserialize`.
- Default serializer accepts `Buffer`, Node `Readable`, and Web `ReadableStream` and converts to Node `Readable`.
- Default deserializer consumes a Node `Readable` and returns a `Buffer`.
- Custom serializers/deserializers must follow the same stream contracts at the boundaries.
- The adapter exposes async operations only; synchronous cache APIs are intentionally not supported.
- Writes set TTL metadata on the temp file and then atomically rename.
- When `namespace` is set by Keyv, files are isolated by namespace within the same directory.
- Only `ENOENT` is treated as cache miss; other IO errors are thrown.
- Empty-string keys are supported consistently across `get`, `iterator`, `clear`, and expiry sweep.
- Bulk methods `setMany` and `hasMany` are supported by the adapter.
- Tests should use temporary subfolders under `node_modules/.cache/`.

## License

MIT
