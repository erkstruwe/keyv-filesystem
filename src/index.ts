import { randomUUID } from "crypto";
import fs from "fs";
import { promises as fsp } from "fs";
import EventEmitter from "events";
import path from "path";
import { Readable } from "stream";
import { pipeline } from "stream/promises";
import { ReadableStream as WebReadableStream } from "stream/web";
import Database from "better-sqlite3";
import type { KeyvStoreAdapter } from "keyv";

export type DefaultSetValue = Buffer | Readable | WebReadableStream;

type Serializer<SetValue> = (value: SetValue) => Readable | Promise<Readable>;

type Deserializer<GetValue> = (value: Readable) => GetValue | Promise<GetValue>;

type KeyvEnvelope<Value> = {
  value: Value;
  expires?: number;
};

export interface ExpireSweepStats {
  /** Number of regular files currently found in the storage directory. */
  totalFiles: number;
  /** Number of files in this adapter namespace that were considered by the sweep. */
  namespaceFiles: number;
  /** Number of expired files deleted by the sweep. */
  deletedFiles: number;
  /** Sweep wall-clock runtime in milliseconds. */
  durationMs: number;
}

export type ExpiredCheckDelayResolver = (
  lastSweep: ExpireSweepStats | undefined,
) => number | Promise<number>;

const MIN_DEFAULT_EXPIRE_SWEEP_DELAY = 60_000;
const INDEX_FILE_NAME = ".keyv-filesystem-index.sqlite";
const ADAPTER_DIALECT = "redis";

function defaultExpiredCheckDelay(
  lastSweep: ExpireSweepStats | undefined,
): number {
  if (!lastSweep) {
    return 5 * MIN_DEFAULT_EXPIRE_SWEEP_DELAY;
  }

  const deletedRatio =
    lastSweep.namespaceFiles === 0
      ? 0
      : lastSweep.deletedFiles / lastSweep.namespaceFiles;

  if (deletedRatio >= 0.1) {
    return MIN_DEFAULT_EXPIRE_SWEEP_DELAY;
  }

  if (deletedRatio >= 0.01) {
    return 5 * MIN_DEFAULT_EXPIRE_SWEEP_DELAY;
  }

  if (lastSweep.namespaceFiles >= 50_000 || lastSweep.durationMs >= 2_000) {
    return 30 * MIN_DEFAULT_EXPIRE_SWEEP_DELAY;
  }

  if (lastSweep.namespaceFiles >= 5_000 || lastSweep.durationMs >= 500) {
    return 15 * MIN_DEFAULT_EXPIRE_SWEEP_DELAY;
  }

  return 10 * MIN_DEFAULT_EXPIRE_SWEEP_DELAY;
}

export interface Options<SetValue = DefaultSetValue, GetValue = Buffer> {
  /** Directory used for one-file-per-entry storage. */
  path: string;
  /**
   * Scan interval for expiring files.
   * - number: fixed interval in milliseconds
   * - callback: computes the next interval from the last sweep metrics
   */
  expiredCheckDelay: number | ExpiredCheckDelayResolver;
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

type InternalOptions<SetValue = DefaultSetValue, GetValue = Buffer> = Options<
  SetValue,
  GetValue
> & {
  /** Keyv adapter dialect hint used by Keyv internals. */
  dialect: string;
};

function isNodeReadable(value: unknown): value is Readable {
  return value instanceof Readable;
}

function isWebReadableStream(value: unknown): value is WebReadableStream {
  return value instanceof WebReadableStream;
}

function isKeyvEnvelope<Value>(value: unknown): value is KeyvEnvelope<Value> {
  if (!value || typeof value !== "object") {
    return false;
  }

  return "value" in value;
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
  expiredCheckDelay: defaultExpiredCheckDelay,
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

  public readonly opts: InternalOptions<SetValue, GetValue>;

  private readonly directory: string;

  private indexDb?: Database.Database;

  private indexDbReady?: Promise<void>;

  private gcTimer?: NodeJS.Timeout;

  private isDisconnected = false;

  private gcInFlight?: Promise<ExpireSweepStats>;

  private lastExpireSweep?: ExpireSweepStats;

  private keyvEnvelopeMode = false;

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
      dialect: ADAPTER_DIALECT,
    };

    this.ensureDirectory().catch((error) => this.emit("error", error));
    this.ensureIndexDatabase().catch((error) => this.emit("error", error));
    void this.scheduleNextExpireSweep(undefined);
  }

  private indexFilePath(): string {
    return path.join(this.directory, INDEX_FILE_NAME);
  }

  private currentNamespace(): string {
    return this.namespace ?? "";
  }

  private requireIndexDb(): Database.Database {
    if (!this.indexDb) {
      throw new Error("Index database is not initialized");
    }

    return this.indexDb;
  }

  private async ensureIndexDatabase() {
    if (!this.indexDbReady) {
      this.indexDbReady = this.openIndexDatabase();
    }

    await this.indexDbReady;
  }

  private async openIndexDatabase() {
    await this.ensureDirectory();
    const indexPath = this.indexFilePath();
    const db = new Database(indexPath);
    this.indexDb = db;

    db.pragma("journal_mode = WAL");
    db.exec(`
      CREATE TABLE IF NOT EXISTS entries (
        namespace TEXT NOT NULL,
        key TEXT NOT NULL,
        file_name TEXT NOT NULL,
        expires_at INTEGER,
        PRIMARY KEY(namespace, key)
      );

      CREATE INDEX IF NOT EXISTS idx_entries_namespace_expires_at
      ON entries(namespace, expires_at);
    `);
  }

  private indexGet(
    namespace: string,
    key: string,
  ): { fileName: string; expiresAt: number | undefined } | undefined {
    const db = this.requireIndexDb();
    const row = db
      .prepare(
        `SELECT file_name as fileName, expires_at as expiresAt FROM entries WHERE namespace = ? AND key = ?`,
      )
      .get(namespace, key) as
      | { fileName: string; expiresAt: number | null }
      | undefined;

    if (!row) {
      return;
    }

    return {
      fileName: row.fileName,
      expiresAt: row.expiresAt === null ? undefined : row.expiresAt,
    };
  }

  private indexUpsert(
    namespace: string,
    key: string,
    fileName: string,
    expiresAt: number | undefined,
  ) {
    const db = this.requireIndexDb();
    db.prepare(
      `
      INSERT INTO entries(namespace, key, file_name, expires_at)
      VALUES (?, ?, ?, ?)
      ON CONFLICT(namespace, key)
      DO UPDATE SET
        file_name = excluded.file_name,
        expires_at = excluded.expires_at
    `,
    ).run(namespace, key, fileName, expiresAt ?? null);
  }

  private indexDelete(namespace: string, key: string) {
    const db = this.requireIndexDb();
    db.prepare(`DELETE FROM entries WHERE namespace = ? AND key = ?`).run(
      namespace,
      key,
    );
  }

  private async resolveNextExpireDelay(
    lastSweep: ExpireSweepStats | undefined,
  ): Promise<number> {
    const resolved =
      typeof this.opts.expiredCheckDelay === "number"
        ? this.opts.expiredCheckDelay
        : await this.opts.expiredCheckDelay(lastSweep);

    if (
      typeof resolved !== "number" ||
      !Number.isFinite(resolved) ||
      resolved <= 0
    ) {
      throw new TypeError(
        "expiredCheckDelay must resolve to a positive number",
      );
    }

    return resolved;
  }

  private async scheduleNextExpireSweep(
    lastSweep: ExpireSweepStats | undefined,
  ) {
    if (this.isDisconnected) {
      return;
    }

    let delay: number;
    try {
      delay = await this.resolveNextExpireDelay(lastSweep);
    } catch (error) {
      this.emit("error", error);
      delay = defaultExpiredCheckDelay(lastSweep);
    }

    this.gcTimer = setTimeout(() => {
      void this.runScheduledExpireSweep();
    }, delay);
    this.gcTimer.unref?.();
  }

  private async runScheduledExpireSweep(): Promise<void> {
    if (this.isDisconnected) {
      return;
    }

    try {
      await this.clearExpire();
    } catch (error) {
      this.emit("error", error);
    }

    await this.scheduleNextExpireSweep(this.lastExpireSweep);
  }

  private async ensureDirectory() {
    await fsp.mkdir(this.directory, { recursive: true });
  }

  private entryIdentity(namespace: string, key: string): string {
    const namespaceBaseName =
      namespace.length === 0 ? undefined : namespaceToBaseName(namespace);
    const keyBaseName = keyToBaseName(key);
    return namespaceBaseName
      ? `${namespaceBaseName}__${keyBaseName}`
      : keyBaseName;
  }

  private fileNameFromNamespaceAndKey(namespace: string, key: string): string {
    return `${this.entryIdentity(namespace, key)}${this.opts.extension}`;
  }

  private ttlToExpires(ttl?: number): number | undefined {
    if (typeof ttl !== "number" || ttl <= 0) {
      return;
    }

    return Date.now() + ttl;
  }

  private isExpired(expiresAt: number | undefined): boolean {
    return expiresAt !== undefined && expiresAt <= Date.now();
  }

  private async writeAtomicFromReadable(
    targetPath: string,
    payload: Readable,
  ): Promise<void> {
    await this.ensureDirectory();
    const tempPath = `${targetPath}.tmp-${process.pid}-${randomUUID()}`;

    try {
      await pipeline(payload, fs.createWriteStream(tempPath));
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

  public async get<Value = GetValue>(key: string): Promise<Value | undefined> {
    const namespace = this.currentNamespace();
    await this.ensureIndexDatabase();

    const indexed = this.indexGet(namespace, key);
    if (!indexed) {
      return;
    }

    if (this.isExpired(indexed.expiresAt)) {
      await this.delete(key);
      return;
    }

    const entry = path.join(this.directory, indexed.fileName);

    const readable = fs.createReadStream(entry);
    try {
      const openResult = await new Promise<
        { opened: true } | { opened: false; error: unknown }
      >((resolve) => {
        readable.once("open", () => resolve({ opened: true }));
        readable.once("error", (error) => resolve({ opened: false, error }));
      });

      if (!openResult.opened) {
        throw openResult.error;
      }

      const deserializeResult = await Promise.race<
        { type: "value"; value: GetValue } | { type: "error"; error: unknown }
      >([
        Promise.resolve(this.opts.deserialize(readable)).then((value) => ({
          type: "value" as const,
          value,
        })),
        new Promise<{ type: "error"; error: unknown }>((resolve) => {
          readable.once("error", (error) => resolve({ type: "error", error }));
        }),
      ]);

      if (deserializeResult.type === "error") {
        throw deserializeResult.error;
      }

      const deserialized = deserializeResult.value;
      if (this.keyvEnvelopeMode) {
        return {
          value: deserialized,
          expires: indexed.expiresAt,
        } as Value;
      }

      return deserialized as unknown as Value;
    } catch (error) {
      readable.destroy();
      if (isNodeErrno(error) && error.code === "ENOENT") {
        this.indexDelete(namespace, key);
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
    const namespace = this.currentNamespace();
    await this.ensureIndexDatabase();

    const previous = this.indexGet(namespace, key);
    const expiresAt = this.ttlToExpires(ttl);
    const fileName = this.fileNameFromNamespaceAndKey(namespace, key);
    const entry = path.join(this.directory, fileName);

    let valueToSerialize = value as unknown;
    if (isKeyvEnvelope<SetValue>(value)) {
      this.keyvEnvelopeMode = true;
      valueToSerialize = value.value;
    }

    const payload = await this.opts.serialize(valueToSerialize as SetValue);
    this.indexUpsert(namespace, key, fileName, expiresAt);
    await this.writeAtomicFromReadable(entry, payload);

    if (previous && previous.fileName !== fileName) {
      await fsp
        .unlink(path.join(this.directory, previous.fileName))
        .catch((error) => {
          if (!isNodeErrno(error) || error.code !== "ENOENT") {
            throw error;
          }
        });
    }
  }

  public async setMany(
    values: Array<{ key: string; value: SetValue; ttl?: number }>,
  ): Promise<void> {
    await Promise.all(
      values.map((entry) => this.set(entry.key, entry.value, entry.ttl)),
    );
  }

  public async delete(key: string): Promise<boolean> {
    const namespace = this.currentNamespace();
    await this.ensureIndexDatabase();

    const indexed = this.indexGet(namespace, key);
    if (!indexed) {
      return false;
    }

    try {
      await fsp.unlink(path.join(this.directory, indexed.fileName));
      this.indexDelete(namespace, key);
      return true;
    } catch (error) {
      if (isNodeErrno(error) && error.code === "ENOENT") {
        this.indexDelete(namespace, key);
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
    await this.ensureIndexDatabase();
    const db = this.requireIndexDb();
    const namespace = this.currentNamespace();
    const rows = db
      .prepare(
        `SELECT key, file_name as fileName FROM entries WHERE namespace = ?`,
      )
      .all(namespace) as Array<{ key: string; fileName: string }>;

    for (const row of rows) {
      await fsp
        .unlink(path.join(this.directory, row.fileName))
        .catch((error) => {
          if (!isNodeErrno(error) || error.code !== "ENOENT") {
            throw error;
          }
        });
      this.indexDelete(namespace, row.key);
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
      return (await this.gcInFlight).deletedFiles;
    }

    this.gcInFlight = this.runExpireSweep();
    try {
      const stats = await this.gcInFlight;
      this.lastExpireSweep = stats;
      return stats.deletedFiles;
    } finally {
      this.gcInFlight = undefined;
    }
  }

  private async runExpireSweep(): Promise<ExpireSweepStats> {
    const startedAt = Date.now();
    this.emit("sweep:start", {
      startedAt,
      useIndexFile: true,
      namespace: this.namespace,
    });

    try {
      await this.ensureIndexDatabase();
      const db = this.requireIndexDb();
      const now = Date.now();
      const namespace = this.currentNamespace();
      const totalFiles = (
        db.prepare(`SELECT COUNT(*) as count FROM entries`).get() as {
          count: number;
        }
      ).count;
      const namespaceFiles = (
        db
          .prepare(`SELECT COUNT(*) as count FROM entries WHERE namespace = ?`)
          .get(namespace) as { count: number }
      ).count;
      let deleted = 0;

      const expiredRows = db
        .prepare(
          `
          SELECT key, file_name as fileName, expires_at as expiresAt
          FROM entries
          WHERE namespace = ? AND expires_at IS NOT NULL AND expires_at <= ?
        `,
        )
        .all(namespace, now) as Array<{
        key: string;
        fileName: string;
        expiresAt: number;
      }>;

      for (const row of expiredRows) {
        try {
          await fsp.unlink(path.join(this.directory, row.fileName));
          deleted += 1;
          this.emit("sweep:fileDeleted", {
            identity: this.entryIdentity(namespace, row.key),
            key: row.key,
            fileName: row.fileName,
            expiresAt: row.expiresAt,
            reason: "expired" as const,
          });
        } catch (error) {
          if (isNodeErrno(error) && error.code === "ENOENT") {
            continue;
          }

          throw error;
        }
      }

      db.prepare(
        `
          DELETE FROM entries
          WHERE namespace = ? AND expires_at IS NOT NULL AND expires_at <= ?
        `,
      ).run(namespace, now);

      const stats: ExpireSweepStats = {
        totalFiles,
        namespaceFiles,
        deletedFiles: deleted,
        durationMs: Date.now() - startedAt,
      };
      this.emit("sweep:end", {
        ...stats,
        startedAt,
        endedAt: Date.now(),
      });

      return stats;
    } catch (error) {
      this.emit("sweep:error", {
        startedAt,
        durationMs: Date.now() - startedAt,
        error,
      });
      throw error;
    }
  }

  public async disconnect(): Promise<void> {
    this.isDisconnected = true;
    if (this.gcTimer) {
      clearTimeout(this.gcTimer);
      this.gcTimer = undefined;
    }

    if (this.indexDb) {
      this.indexDb.close();
      this.indexDb = undefined;
      this.indexDbReady = undefined;
    }
  }

  public async *iterator<Value>(
    namespace?: string,
  ): AsyncGenerator<[string, Value], void> {
    await this.ensureIndexDatabase();
    const db = this.requireIndexDb();
    const currentNamespace = this.currentNamespace();
    const rows = db
      .prepare(`SELECT key FROM entries WHERE namespace = ? ORDER BY key`)
      .all(currentNamespace) as Array<{ key: string }>;

    for (const row of rows) {
      const key = row.key;

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

    return;
  }
}

export default KeyvFilesystem;
