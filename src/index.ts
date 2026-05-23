import { randomUUID } from "crypto";
import fs from "fs";
import { promises as fsp } from "fs";
import EventEmitter from "events";
import path from "path";
import { Readable } from "stream";
import { pipeline } from "stream/promises";
import { ReadableStream as WebReadableStream } from "stream/web";
import type { KeyvStoreAdapter } from "keyv";

export type DefaultSetValue = Buffer | Readable | WebReadableStream;

type Serializer<SetValue> = (value: SetValue) => Readable | Promise<Readable>;

type Deserializer<GetValue> = (value: Readable) => GetValue | Promise<GetValue>;

export interface Options<SetValue = DefaultSetValue, GetValue = Buffer> {
  /** Keyv adapter dialect hint used by Keyv internals. */
  dialect: string;
  /** Directory used for one-file-per-entry storage. */
  path: string;
  /** Scan interval for expiring files in milliseconds. */
  expiredCheckDelay: number;
  /** Far-future timestamp used for "never expires" entries. */
  sentinelExpire: number;
  /** File extension used for entry files. */
  extension: string;
  /** Optional serializer used when writing values. */
  serialize: Serializer<SetValue>;
  /** Optional deserializer used when reading bytes from disk. */
  deserialize: Deserializer<GetValue>;
  /**
   * Durability mode.
   * - standard: atomic temp+rename
   * - strict: atomic temp+rename with fsync best-effort
   */
  durability: "standard" | "strict";
}

export type KeyvFilesystemOptions<
  SetValue = DefaultSetValue,
  GetValue = Buffer,
> = {
  path: string;
} & Partial<Omit<Options<SetValue, GetValue>, "path">>;

function isNodeReadable(value: unknown): value is Readable {
  return value instanceof Readable;
}

function isWebReadableStream(value: unknown): value is WebReadableStream {
  return value instanceof WebReadableStream;
}

function defaultReadableSerializer(value: DefaultSetValue): Readable {
  if (Buffer.isBuffer(value)) {
    return Readable.from([value]);
  }

  if (isNodeReadable(value)) {
    return value;
  }

  if (isWebReadableStream(value)) {
    return Readable.fromWeb(value);
  }

  throw new TypeError(
    "Default serializer only accepts Buffer, Readable, or ReadableStream",
  );
}

async function defaultReadableDeserializer(stream: Readable): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    chunks.push(toBufferChunk(chunk));
  }

  return Buffer.concat(chunks);
}

export const defaultOpts: Omit<Options<DefaultSetValue, Buffer>, "path"> = {
  dialect: "redis",
  expiredCheckDelay: 24 * 3600 * 1000,
  sentinelExpire: Date.parse("2100-01-01T00:00:00.000Z"),
  extension: ".bin",
  serialize: defaultReadableSerializer,
  deserialize: defaultReadableDeserializer,
  durability: "standard",
};

function isNodeErrno(error: unknown): error is NodeJS.ErrnoException {
  return !!error && typeof error === "object" && "code" in error;
}

function toBufferChunk(chunk: unknown): Buffer {
  if (Buffer.isBuffer(chunk)) {
    return chunk;
  }

  if (typeof chunk === "string") {
    return Buffer.from(chunk);
  }

  if (chunk instanceof Uint8Array) {
    return Buffer.from(chunk);
  }

  if (chunk instanceof ArrayBuffer) {
    return Buffer.from(new Uint8Array(chunk));
  }

  if (ArrayBuffer.isView(chunk)) {
    return Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength);
  }

  throw new TypeError(
    "Stream chunks must be Buffer, Uint8Array, ArrayBuffer, DataView, or string",
  );
}

function keyToBaseName(key: string): string {
  return `k_${Buffer.from(key).toString("base64url")}`;
}

function namespaceToBaseName(namespace: string): string {
  return `n_${Buffer.from(namespace).toString("base64url")}`;
}

function baseNameToKey(baseName: string): string | undefined {
  if (!baseName.startsWith("k_")) {
    return;
  }

  try {
    return Buffer.from(baseName.slice(2), "base64url").toString("utf8");
  } catch {
    return;
  }
}

async function fsyncFile(filePath: string) {
  const handle = await fsp.open(filePath, "r");
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

async function fsyncDirectory(dirPath: string) {
  try {
    const handle = await fsp.open(dirPath, "r");
    try {
      await handle.sync();
    } finally {
      await handle.close();
    }
  } catch (error) {
    // Directory fsync is not supported on some platforms/filesystems.
    if (
      !isNodeErrno(error) ||
      (error.code !== "EINVAL" &&
        error.code !== "ENOTSUP" &&
        error.code !== "EPERM")
    ) {
      throw error;
    }
  }
}

export class KeyvFilesystem<SetValue = DefaultSetValue, GetValue = Buffer>
  extends EventEmitter
  implements KeyvStoreAdapter
{
  public ttlSupport = true;

  public namespace?: string;

  public readonly opts: Options<SetValue, GetValue>;

  private readonly directory: string;

  private gcInterval?: NodeJS.Timeout;

  private gcInFlight?: Promise<number>;

  constructor(options: KeyvFilesystemOptions<SetValue, GetValue>) {
    super();
    if (!options?.path || options.path.trim().length === 0) {
      throw new Error("KeyvFilesystem requires a non-empty options.path");
    }

    this.directory = path.resolve(options.path);
    this.opts = {
      ...(defaultOpts as unknown as Omit<Options<SetValue, GetValue>, "path">),
      ...options,
      serialize:
        options.serialize ??
        (defaultReadableSerializer as Serializer<SetValue>),
      deserialize:
        options.deserialize ??
        (defaultReadableDeserializer as Deserializer<GetValue>),
      path: this.directory,
    };

    this.ensureDirectory().catch((error) => this.emit("error", error));
    this.gcInterval = setInterval(() => {
      void this.clearExpire().catch((error) => this.emit("error", error));
    }, this.opts.expiredCheckDelay);
    this.gcInterval.unref?.();
  }

  private async ensureDirectory() {
    await fsp.mkdir(this.directory, { recursive: true });
  }

  private entryPath(key: string): string {
    const namespaceBaseName =
      this.namespace === undefined
        ? undefined
        : namespaceToBaseName(this.namespace);
    const keyBaseName = keyToBaseName(key);
    const fileBaseName = namespaceBaseName
      ? `${namespaceBaseName}__${keyBaseName}`
      : keyBaseName;

    return path.join(this.directory, `${fileBaseName}${this.opts.extension}`);
  }

  private decodeFileName(fileName: string): string | undefined {
    if (!fileName.endsWith(this.opts.extension)) {
      return;
    }

    const baseName = fileName.slice(0, -this.opts.extension.length);

    if (this.namespace === undefined) {
      if (baseName.startsWith("n_")) {
        return;
      }

      return baseNameToKey(baseName);
    }

    const namespaceBaseName = namespaceToBaseName(this.namespace);
    const namespacedPrefix = `${namespaceBaseName}__`;
    if (!baseName.startsWith(namespacedPrefix)) {
      return;
    }

    const keyBaseName = baseName.slice(namespacedPrefix.length);
    return baseNameToKey(keyBaseName);
  }

  private ttlToExpires(ttl?: number): number {
    if (typeof ttl !== "number" || ttl <= 0) {
      return this.opts.sentinelExpire;
    }

    return Date.now() + ttl;
  }

  private isExpired(expiresAt: number): boolean {
    return expiresAt < this.opts.sentinelExpire && expiresAt <= Date.now();
  }

  private async writeAtomicFromReadable(
    targetPath: string,
    payload: Readable,
    expiresAt: number,
  ): Promise<void> {
    await this.ensureDirectory();
    const tempPath = `${targetPath}.tmp-${process.pid}-${randomUUID()}`;

    try {
      await pipeline(payload, fs.createWriteStream(tempPath));
      const nowSec = Date.now() / 1000;
      await fsp.utimes(tempPath, nowSec, expiresAt / 1000);
      if (this.opts.durability === "strict") {
        await fsyncFile(tempPath);
      }

      await fsp.rename(tempPath, targetPath);
      if (this.opts.durability === "strict") {
        await fsyncDirectory(this.directory);
      }
    } catch (error) {
      await fsp.rm(tempPath, { force: true }).catch(() => undefined);
      throw error;
    }
  }

  private async statWithMiss(pathToStat: string) {
    try {
      return await fsp.stat(pathToStat);
    } catch (error) {
      if (isNodeErrno(error) && error.code === "ENOENT") {
        return;
      }

      throw error;
    }
  }

  public async get<Value = GetValue>(key: string): Promise<Value | undefined> {
    const entry = this.entryPath(key);
    const stat = await this.statWithMiss(entry);
    if (!stat) {
      return;
    }

    const expiresAt = stat.mtimeMs;
    if (this.isExpired(expiresAt)) {
      await this.delete(key);
      return;
    }

    const readable = fs.createReadStream(entry);
    try {
      return (await this.opts.deserialize(readable)) as Value;
    } catch (error) {
      readable.destroy();
      if (isNodeErrno(error) && error.code === "ENOENT") {
        return;
      }

      throw error;
    }
  }

  public async getMany<Value>(
    keys: string[],
  ): Promise<Array<Value | undefined>> {
    return Promise.all(keys.map((key) => this.get<Value>(key)));
  }

  public async set(key: string, value: SetValue, ttl?: number): Promise<void> {
    const entry = this.entryPath(key);
    const expiresAt = this.ttlToExpires(ttl);

    const payload = await this.opts.serialize(value);
    await this.writeAtomicFromReadable(entry, payload, expiresAt);
  }

  public async setMany(
    values: Array<{ key: string; value: SetValue; ttl?: number }>,
  ): Promise<void> {
    await Promise.all(
      values.map((entry) => this.set(entry.key, entry.value, entry.ttl)),
    );
  }

  public async delete(key: string): Promise<boolean> {
    try {
      await fsp.unlink(this.entryPath(key));
      return true;
    } catch (error) {
      if (isNodeErrno(error) && error.code === "ENOENT") {
        return false;
      }

      throw error;
    }
  }

  public async deleteMany(keys: string[]): Promise<boolean> {
    const result = await Promise.all(keys.map((key) => this.delete(key)));
    return result.every(Boolean);
  }

  public async clear(): Promise<void> {
    await this.ensureDirectory();
    const pending: Promise<void>[] = [];
    for await (const dirent of await fsp.opendir(this.directory)) {
      if (!dirent.isFile()) {
        continue;
      }

      const key = this.decodeFileName(dirent.name);
      if (key === undefined) {
        continue;
      }

      pending.push(fsp.unlink(path.join(this.directory, dirent.name)));
      if (pending.length >= 64) {
        await Promise.all(pending);
        pending.length = 0;
      }
    }

    if (pending.length > 0) {
      await Promise.all(pending);
    }

    return;
  }

  public async has(key: string): Promise<boolean> {
    return (await this.get(key)) !== undefined;
  }

  public async hasMany(keys: string[]): Promise<boolean[]> {
    return Promise.all(keys.map((key) => this.has(key)));
  }

  public async clearExpire(): Promise<number> {
    if (this.gcInFlight) {
      return this.gcInFlight;
    }

    this.gcInFlight = this.runExpireSweep();
    try {
      return await this.gcInFlight;
    } finally {
      this.gcInFlight = undefined;
    }
  }

  private async runExpireSweep(): Promise<number> {
    await this.ensureDirectory();
    let deleted = 0;

    for await (const dirent of await fsp.opendir(this.directory)) {
      if (!dirent.isFile()) {
        continue;
      }

      const fileName = dirent.name;
      const key = this.decodeFileName(fileName);
      if (key === undefined) {
        continue;
      }

      const entry = path.join(this.directory, fileName);
      const stat = await this.statWithMiss(entry);
      if (!stat) {
        continue;
      }

      if (!this.isExpired(stat.mtimeMs)) {
        continue;
      }

      try {
        await fsp.unlink(entry);
        deleted += 1;
      } catch (error) {
        if (!isNodeErrno(error) || error.code !== "ENOENT") {
          throw error;
        }
      }
    }

    return deleted;
  }

  public async disconnect(): Promise<void> {
    if (this.gcInterval) {
      clearInterval(this.gcInterval);
      this.gcInterval = undefined;
    }
  }

  public async *iterator<Value>(
    namespace?: string,
  ): AsyncGenerator<[string, Value], void> {
    await this.ensureDirectory();

    for await (const dirent of await fsp.opendir(this.directory)) {
      if (!dirent.isFile()) {
        continue;
      }

      const key = this.decodeFileName(dirent.name);
      if (key === undefined) {
        continue;
      }

      if (
        namespace &&
        !(key === namespace || key.startsWith(`${namespace}:`))
      ) {
        continue;
      }

      const value = await this.get<Value>(key);
      if (value !== undefined) {
        yield [key, value];
      }
    }
  }
}

export default KeyvFilesystem;
