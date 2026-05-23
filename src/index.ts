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
const EXPIRY_SUFFIX = "__exp_";
const NO_TTL_EXPIRY_TOKEN = "never";
const INDEX_FILE_NAME = ".keyv-filesystem-index.sqlite";

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
  /** Keyv adapter dialect hint used by Keyv internals. */
  dialect: string;
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
  /** Use SQLite index file for metadata-driven operations and sweeps. */
  useIndexFile: boolean;
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
  expiredCheckDelay: defaultExpiredCheckDelay,
  extension: ".bin",
  useIndexFile: false,
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

function parseExpiryToken(token: string): number | undefined {
  if (token === NO_TTL_EXPIRY_TOKEN) {
    return;
  }

  if (!/^\d+$/.test(token)) {
    return;
  }

  const expiresAt = Number(token);
  if (!Number.isSafeInteger(expiresAt) || expiresAt <= 0) {
    return;
  }

  return expiresAt;
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

  private readonly entryIndex = new Map<
    string,
    { fileName: string; expiresAt: number | undefined }
  >();

  private entryIndexReady?: Promise<void>;

  private indexDb?: Database.Database;

  private indexDbReady?: Promise<void>;

  private gcTimer?: NodeJS.Timeout;

  private isDisconnected = false;

  private gcInFlight?: Promise<ExpireSweepStats>;

  private lastExpireSweep?: ExpireSweepStats;

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
    if (this.opts.useIndexFile) {
      this.ensureIndexDatabase().catch((error) => this.emit("error", error));
    }
    void this.scheduleNextExpireSweep(undefined);
  }

  private indexFilePath(): string {
    return path.join(this.directory, INDEX_FILE_NAME);
  }

  private namespaceIdentityPattern(): string {
    if (this.namespace === undefined) {
      return "k_%";
    }

    return `${namespaceToBaseName(this.namespace)}__k_%`;
  }

  private requireIndexDb(): Database.Database {
    if (!this.indexDb) {
      throw new Error("Index database is not initialized");
    }

    return this.indexDb;
  }

  private async ensureIndexDatabase() {
    if (!this.opts.useIndexFile) {
      return;
    }

    if (!this.indexDbReady) {
      this.indexDbReady = this.openIndexDatabase();
    }

    await this.indexDbReady;
  }

  private async openIndexDatabase() {
    await this.ensureDirectory();
    const indexPath = this.indexFilePath();
    const isNewDatabase = !fs.existsSync(indexPath);
    const db = new Database(indexPath);
    this.indexDb = db;

    db.pragma("journal_mode = WAL");
    db.exec(`
      CREATE TABLE IF NOT EXISTS entries (
        identity TEXT PRIMARY KEY,
        file_name TEXT NOT NULL,
        expires_at INTEGER
      );

      CREATE INDEX IF NOT EXISTS idx_entries_expires_at_identity
      ON entries(expires_at, identity);
    `);

    if (isNewDatabase) {
      await this.bootstrapIndexFromDirectory(db);
    }
  }

  private async bootstrapIndexFromDirectory(
    db: Database.Database,
  ): Promise<void> {
    await this.ensureDirectory();
    const now = Date.now();
    const validEntries = new Map<
      string,
      { fileName: string; expiresAt: number | undefined }
    >();

    for await (const dirent of await fsp.opendir(this.directory)) {
      if (!dirent.isFile() || dirent.name === INDEX_FILE_NAME) {
        continue;
      }

      const parsed = this.parseEntryFileName(dirent.name);
      if (!parsed) {
        continue;
      }

      const fullPath = path.join(this.directory, dirent.name);
      if (parsed.expiresAt !== undefined && parsed.expiresAt <= now) {
        await fsp.unlink(fullPath).catch((error) => {
          if (!isNodeErrno(error) || error.code !== "ENOENT") {
            throw error;
          }
        });
        continue;
      }

      const existing = validEntries.get(parsed.identity);
      if (!existing) {
        validEntries.set(parsed.identity, {
          fileName: dirent.name,
          expiresAt: parsed.expiresAt,
        });
        continue;
      }

      const existingWeight = existing.expiresAt ?? Number.POSITIVE_INFINITY;
      const parsedWeight = parsed.expiresAt ?? Number.POSITIVE_INFINITY;
      if (parsedWeight >= existingWeight) {
        await fsp
          .unlink(path.join(this.directory, existing.fileName))
          .catch((error) => {
            if (!isNodeErrno(error) || error.code !== "ENOENT") {
              throw error;
            }
          });
        validEntries.set(parsed.identity, {
          fileName: dirent.name,
          expiresAt: parsed.expiresAt,
        });
      } else {
        await fsp.unlink(fullPath).catch((error) => {
          if (!isNodeErrno(error) || error.code !== "ENOENT") {
            throw error;
          }
        });
      }
    }

    const clearStmt = db.prepare("DELETE FROM entries");
    const insertStmt = db.prepare(
      `INSERT INTO entries(identity, file_name, expires_at) VALUES (?, ?, ?)`,
    );
    const tx = db.transaction(() => {
      clearStmt.run();
      for (const [identity, entry] of validEntries) {
        insertStmt.run(identity, entry.fileName, entry.expiresAt ?? null);
      }
    });
    tx();

    this.entryIndex.clear();
    for (const [identity, entry] of validEntries) {
      this.entryIndex.set(identity, {
        fileName: entry.fileName,
        expiresAt: entry.expiresAt,
      });
    }
  }

  private indexGet(
    identity: string,
  ): { fileName: string; expiresAt: number | undefined } | undefined {
    const db = this.requireIndexDb();
    const row = db
      .prepare(
        `SELECT file_name as fileName, expires_at as expiresAt FROM entries WHERE identity = ?`,
      )
      .get(identity) as
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
    identity: string,
    fileName: string,
    expiresAt: number | undefined,
  ) {
    const db = this.requireIndexDb();
    db.prepare(
      `
      INSERT INTO entries(identity, file_name, expires_at)
      VALUES (?, ?, ?)
      ON CONFLICT(identity)
      DO UPDATE SET
        file_name = excluded.file_name,
        expires_at = excluded.expires_at
    `,
    ).run(identity, fileName, expiresAt ?? null);
  }

  private indexDelete(identity: string) {
    const db = this.requireIndexDb();
    db.prepare(`DELETE FROM entries WHERE identity = ?`).run(identity);
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

  private async ensureEntryIndex() {
    if (this.opts.useIndexFile) {
      return;
    }

    if (!this.entryIndexReady) {
      this.entryIndexReady = this.loadEntryIndex();
    }

    await this.entryIndexReady;
  }

  private async loadEntryIndex() {
    await this.ensureDirectory();
    this.entryIndex.clear();

    for await (const dirent of await fsp.opendir(this.directory)) {
      if (!dirent.isFile()) {
        continue;
      }

      const parsed = this.parseEntryFileName(dirent.name);
      if (!parsed) {
        continue;
      }

      const existing = this.entryIndex.get(parsed.identity);
      if (!existing) {
        this.entryIndex.set(parsed.identity, {
          fileName: dirent.name,
          expiresAt: parsed.expiresAt,
        });
        continue;
      }

      const existingWeight = existing.expiresAt ?? Number.POSITIVE_INFINITY;
      const parsedWeight = parsed.expiresAt ?? Number.POSITIVE_INFINITY;
      if (parsedWeight >= existingWeight) {
        this.entryIndex.set(parsed.identity, {
          fileName: dirent.name,
          expiresAt: parsed.expiresAt,
        });
      }
    }
  }

  private entryIdentity(key: string): string {
    const namespaceBaseName =
      this.namespace === undefined
        ? undefined
        : namespaceToBaseName(this.namespace);
    const keyBaseName = keyToBaseName(key);
    return namespaceBaseName
      ? `${namespaceBaseName}__${keyBaseName}`
      : keyBaseName;
  }

  private fileNameFromIdentity(
    identity: string,
    expiresAt: number | undefined,
  ): string {
    const token =
      expiresAt === undefined ? NO_TTL_EXPIRY_TOKEN : String(expiresAt);
    return `${identity}${EXPIRY_SUFFIX}${token}${this.opts.extension}`;
  }

  private parseEntryFileName(fileName: string):
    | {
        identity: string;
        key: string;
        expiresAt: number | undefined;
      }
    | undefined {
    if (!fileName.endsWith(this.opts.extension)) {
      return;
    }

    const baseName = fileName.slice(0, -this.opts.extension.length);
    const suffixIndex = baseName.lastIndexOf(EXPIRY_SUFFIX);
    if (suffixIndex <= 0) {
      return;
    }

    const identity = baseName.slice(0, suffixIndex);
    const token = baseName.slice(suffixIndex + EXPIRY_SUFFIX.length);
    const expiresAt = parseExpiryToken(token);
    if (token !== NO_TTL_EXPIRY_TOKEN && expiresAt === undefined) {
      return;
    }

    const key = this.decodeIdentityToKey(identity);
    if (key === undefined) {
      return;
    }

    return {
      identity,
      key,
      expiresAt,
    };
  }

  private decodeIdentityToKey(identity: string): string | undefined {
    if (this.namespace === undefined) {
      if (identity.startsWith("n_")) {
        return;
      }

      return baseNameToKey(identity);
    }

    const namespaceBaseName = namespaceToBaseName(this.namespace);
    const namespacedPrefix = `${namespaceBaseName}__`;
    if (!identity.startsWith(namespacedPrefix)) {
      return;
    }

    return baseNameToKey(identity.slice(namespacedPrefix.length));
  }

  private entryPath(key: string): string {
    if (this.opts.useIndexFile) {
      const identity = this.entryIdentity(key);
      const indexed = this.indexGet(identity);
      if (!indexed) {
        return path.join(
          this.directory,
          this.fileNameFromIdentity(identity, undefined),
        );
      }

      return path.join(this.directory, indexed.fileName);
    }

    const indexed = this.entryIndex.get(this.entryIdentity(key));
    if (!indexed) {
      return path.join(
        this.directory,
        this.fileNameFromIdentity(this.entryIdentity(key), undefined),
      );
    }

    return path.join(this.directory, indexed.fileName);
  }

  private decodeFileName(fileName: string): string | undefined {
    return this.parseEntryFileName(fileName)?.key;
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
    const identity = this.entryIdentity(key);

    if (this.opts.useIndexFile) {
      await this.ensureIndexDatabase();
    } else {
      await this.ensureEntryIndex();
    }

    const indexed = this.opts.useIndexFile
      ? this.indexGet(identity)
      : this.entryIndex.get(identity);
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
      return (await this.opts.deserialize(readable)) as Value;
    } catch (error) {
      readable.destroy();
      if (isNodeErrno(error) && error.code === "ENOENT") {
        if (this.opts.useIndexFile) {
          this.indexDelete(identity);
        } else {
          this.entryIndex.delete(identity);
        }
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
    const identity = this.entryIdentity(key);
    if (this.opts.useIndexFile) {
      await this.ensureIndexDatabase();
    } else {
      await this.ensureEntryIndex();
    }

    const previous = this.opts.useIndexFile
      ? this.indexGet(identity)
      : this.entryIndex.get(identity);
    const expiresAt = this.ttlToExpires(ttl);
    const fileName = this.fileNameFromIdentity(identity, expiresAt);
    const entry = path.join(this.directory, fileName);

    const payload = await this.opts.serialize(value);
    if (this.opts.useIndexFile) {
      this.indexUpsert(identity, fileName, expiresAt);
    } else {
      this.entryIndex.set(identity, {
        fileName,
        expiresAt,
      });
    }
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
    const identity = this.entryIdentity(key);
    if (this.opts.useIndexFile) {
      await this.ensureIndexDatabase();
    } else {
      await this.ensureEntryIndex();
    }

    const indexed = this.opts.useIndexFile
      ? this.indexGet(identity)
      : this.entryIndex.get(identity);
    if (!indexed) {
      return false;
    }

    try {
      await fsp.unlink(path.join(this.directory, indexed.fileName));
      if (this.opts.useIndexFile) {
        this.indexDelete(identity);
      } else {
        this.entryIndex.delete(identity);
      }
      return true;
    } catch (error) {
      if (isNodeErrno(error) && error.code === "ENOENT") {
        if (this.opts.useIndexFile) {
          this.indexDelete(identity);
        } else {
          this.entryIndex.delete(identity);
        }
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
    if (this.opts.useIndexFile) {
      await this.ensureIndexDatabase();
      const db = this.requireIndexDb();
      const pattern = this.namespaceIdentityPattern();
      const rows = db
        .prepare(
          `SELECT identity, file_name as fileName FROM entries WHERE identity LIKE ?`,
        )
        .all(pattern) as Array<{ identity: string; fileName: string }>;

      for (const row of rows) {
        await fsp
          .unlink(path.join(this.directory, row.fileName))
          .catch((error) => {
            if (!isNodeErrno(error) || error.code !== "ENOENT") {
              throw error;
            }
          });
        this.indexDelete(row.identity);
      }

      return;
    }

    await this.ensureEntryIndex();
    const pending: Promise<void>[] = [];
    for await (const dirent of await fsp.opendir(this.directory)) {
      if (!dirent.isFile()) {
        continue;
      }

      const parsed = this.parseEntryFileName(dirent.name);
      if (!parsed) {
        continue;
      }

      pending.push(
        fsp.unlink(path.join(this.directory, dirent.name)).then(() => {
          this.entryIndex.delete(parsed.identity);
        }),
      );
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
    if (this.opts.useIndexFile) {
      await this.ensureIndexDatabase();
      const db = this.requireIndexDb();
      const startedAt = Date.now();
      const now = Date.now();
      const pattern = this.namespaceIdentityPattern();
      const totalFiles = (
        db.prepare(`SELECT COUNT(*) as count FROM entries`).get() as {
          count: number;
        }
      ).count;
      const namespaceFiles = (
        db
          .prepare(
            `SELECT COUNT(*) as count FROM entries WHERE identity LIKE ?`,
          )
          .get(pattern) as { count: number }
      ).count;
      let deleted = 0;

      const expiredRows = db
        .prepare(
          `
          SELECT identity, file_name as fileName
          FROM entries
          WHERE identity LIKE ? AND expires_at IS NOT NULL AND expires_at <= ?
        `,
        )
        .all(pattern, now) as Array<{ identity: string; fileName: string }>;

      for (const row of expiredRows) {
        try {
          await fsp.unlink(path.join(this.directory, row.fileName));
          deleted += 1;
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
          WHERE identity LIKE ? AND expires_at IS NOT NULL AND expires_at <= ?
        `,
      ).run(pattern, now);

      return {
        totalFiles,
        namespaceFiles,
        deletedFiles: deleted,
        durationMs: Date.now() - startedAt,
      };
    }

    await this.ensureDirectory();
    await this.ensureEntryIndex();
    const startedAt = Date.now();
    const now = Date.now();
    let totalFiles = 0;
    let namespaceFiles = 0;
    let deleted = 0;

    for await (const dirent of await fsp.opendir(this.directory)) {
      if (!dirent.isFile()) {
        continue;
      }

      totalFiles += 1;

      const parsed = this.parseEntryFileName(dirent.name);
      if (!parsed) {
        continue;
      }

      namespaceFiles += 1;

      if (parsed.expiresAt === undefined || parsed.expiresAt > now) {
        continue;
      }

      try {
        await fsp.unlink(path.join(this.directory, dirent.name));
        this.entryIndex.delete(parsed.identity);
        deleted += 1;
      } catch (error) {
        if (!isNodeErrno(error) || error.code !== "ENOENT") {
          throw error;
        }
      }
    }

    return {
      totalFiles,
      namespaceFiles,
      deletedFiles: deleted,
      durationMs: Date.now() - startedAt,
    };
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
    if (this.opts.useIndexFile) {
      await this.ensureIndexDatabase();
      const db = this.requireIndexDb();
      const rows = db
        .prepare(
          `SELECT identity FROM entries WHERE identity LIKE ? ORDER BY identity`,
        )
        .all(this.namespaceIdentityPattern()) as Array<{ identity: string }>;

      for (const row of rows) {
        const key = this.decodeIdentityToKey(row.identity);
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

      return;
    }

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
