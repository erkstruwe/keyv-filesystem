import { describe, it, expect, vi } from "vitest";
import { promises as fsp } from "fs";
import path from "path";
import Keyv from "keyv";
import KeyvFilesystem from "../src/index.ts";
import { keyvDeserialize, keyvSerialize, randomTestPath } from "./helpers.js";

const INDEX_FILE_NAME = ".keyv-filesystem-index.sqlite";

function encodedFileName(key: string, expiresAt: number | "never"): string {
  const identity = `k_${Buffer.from(key).toString("base64url")}`;
  const token = expiresAt === "never" ? "never" : String(expiresAt);
  return `${identity}__exp_${token}.bin`;
}

describe("Store operations", () => {
  it("sets ttl metadata before rename and avoids post-rename utimes", async () => {
    const dir = randomTestPath("atomic-ttl");
    const store = new KeyvFilesystem({ path: dir, expiredCheckDelay: 60_000 });
    const realUtimes = fsp.utimes.bind(fsp);
    const utimesSpy = vi
      .spyOn(fsp, "utimes")
      .mockImplementation(async (target: any, atime: any, mtime: any) => {
        if (typeof target === "string" && !target.includes(".tmp-")) {
          throw new Error("utimes-on-final-path");
        }

        return realUtimes(target, atime, mtime);
      });

    try {
      await expect(store.set("k", Buffer.from("v"), 5_000)).resolves.toBe(
        undefined,
      );
      expect((await store.get<Buffer>("k"))?.toString()).toBe("v");
    } finally {
      utimesSpy.mockRestore();
      await store.disconnect();
    }
  });

  it("isolates namespaces in storage even when Keyv prefixing is disabled", async () => {
    const dir = randomTestPath("namespace-isolation");
    const usersStore = new KeyvFilesystem<string, string>({
      path: dir,
      expiredCheckDelay: 60_000,
      serialize: keyvSerialize,
      deserialize: keyvDeserialize,
    });
    const cacheStore = new KeyvFilesystem<string, string>({
      path: dir,
      expiredCheckDelay: 60_000,
      serialize: keyvSerialize,
      deserialize: keyvDeserialize,
    });
    const users = new Keyv({
      store: usersStore,
      namespace: "users",
      useKeyPrefix: false,
    });
    const cache = new Keyv({
      store: cacheStore,
      namespace: "cache",
      useKeyPrefix: false,
    });

    try {
      await users.set("same", "u");
      await cache.set("same", "c");

      expect(await users.get("same")).toBe("u");
      expect(await cache.get("same")).toBe("c");

      await users.clear();

      expect(await users.get("same")).toBeUndefined();
      expect(await cache.get("same")).toBe("c");
    } finally {
      await users.disconnect();
      await cache.disconnect();
      await usersStore.disconnect();
      await cacheStore.disconnect();
    }
  });

  it("supports setMany and hasMany bulk operations", async () => {
    const dir = randomTestPath("bulk-methods");
    const store = new KeyvFilesystem({ path: dir, expiredCheckDelay: 60_000 });

    try {
      await store.setMany([
        { key: "a", value: Buffer.from("va") },
        { key: "b", value: Buffer.from("vb"), ttl: 40 },
      ]);

      expect((await store.get<Buffer>("a"))?.toString()).toBe("va");
      expect((await store.get<Buffer>("b"))?.toString()).toBe("vb");

      await new Promise((resolve) => setTimeout(resolve, 80));

      const results = await store.hasMany(["a", "b", "missing"]);
      expect(results).toEqual([true, false, false]);
    } finally {
      await store.disconnect();
    }
  });

  it("returns false from deleteMany if at least one key is missing", async () => {
    const dir = randomTestPath("delete-many");
    const store = new KeyvFilesystem({ path: dir, expiredCheckDelay: 60_000 });

    try {
      await store.set("present", Buffer.from("v"));

      expect(await store.deleteMany(["present", "missing"])).toBe(false);
      expect(await store.get("present")).toBeUndefined();
    } finally {
      await store.disconnect();
    }
  });

  it("deduplicates concurrent clearExpire sweeps", async () => {
    const dir = randomTestPath("clear-expire-concurrency");
    const store = new KeyvFilesystem({ path: dir, expiredCheckDelay: 60_000 });

    try {
      await store.set("soon-expire", Buffer.from("v"), 5);
      await new Promise((resolve) => setTimeout(resolve, 20));

      const [first, second] = await Promise.all([
        store.clearExpire(),
        store.clearExpire(),
      ]);

      expect(first).toBe(1);
      expect(second).toBe(1);
      expect(await store.get("soon-expire")).toBeUndefined();
    } finally {
      await store.disconnect();
    }
  });

  it("emits sweep lifecycle events for expired deletions", async () => {
    const dir = randomTestPath("clear-expire-events");
    const store = new KeyvFilesystem({ path: dir, expiredCheckDelay: 60_000 });

    const starts: Array<{
      startedAt: number;
      useIndexFile: boolean;
      namespace: string | undefined;
    }> = [];
    const fileDeletes: Array<{
      identity: string;
      key: string | undefined;
      fileName: string;
      expiresAt: number | undefined;
      reason: "expired";
    }> = [];
    const ends: Array<{
      totalFiles: number;
      namespaceFiles: number;
      deletedFiles: number;
      durationMs: number;
      startedAt: number;
      endedAt: number;
    }> = [];
    const errors: Array<{
      startedAt: number;
      durationMs: number;
      error: unknown;
    }> = [];

    store.on("sweep:start", (event) => {
      starts.push(event);
    });
    store.on("sweep:fileDeleted", (event) => {
      fileDeletes.push(event);
    });
    store.on("sweep:end", (event) => {
      ends.push(event);
    });
    store.on("sweep:error", (event) => {
      errors.push(event);
    });

    try {
      await store.set("soon-expire", Buffer.from("v"), 5);
      await new Promise((resolve) => setTimeout(resolve, 20));

      expect(await store.clearExpire()).toBe(1);

      expect(starts).toHaveLength(1);
      expect(fileDeletes).toHaveLength(1);
      expect(fileDeletes[0]?.key).toBe("soon-expire");
      expect(fileDeletes[0]?.reason).toBe("expired");
      expect(ends).toHaveLength(1);
      expect(ends[0]?.deletedFiles).toBe(1);
      expect(ends[0]?.startedAt).toBe(starts[0]?.startedAt);
      expect(ends[0]?.endedAt).toBeGreaterThanOrEqual(ends[0]?.startedAt ?? 0);
      expect(errors).toHaveLength(0);
    } finally {
      await store.disconnect();
    }
  });

  it("supports callback-based expiredCheckDelay with sweep stats", async () => {
    const dir = randomTestPath("adaptive-expire-delay");
    const expiredCheckDelay = vi.fn(() => 5);
    const store = new KeyvFilesystem({ path: dir, expiredCheckDelay });

    try {
      await store.set("soon-expire", Buffer.from("v"), 5);
      await new Promise((resolve) => setTimeout(resolve, 80));

      const calls = expiredCheckDelay.mock.calls.map(
        (entry) =>
          entry[0] as
            | {
                totalFiles: number;
                namespaceFiles: number;
                deletedFiles: number;
                durationMs: number;
              }
            | undefined,
      );

      expect(calls.length).toBeGreaterThan(1);
      expect(calls[0]).toBeUndefined();
      expect(calls.some((stats) => (stats?.deletedFiles ?? 0) >= 1)).toBe(true);

      const files = await fsp.readdir(dir);
      expect(files.length).toBe(0);
    } finally {
      await store.disconnect();
    }
  });

  it("creates sqlite index and bootstraps from existing files", async () => {
    const dir = randomTestPath("sqlite-index-bootstrap");
    await fsp.mkdir(dir, { recursive: true });
    await fsp.writeFile(
      path.join(dir, encodedFileName("stale", Date.now() - 1_000)),
      Buffer.from("old"),
    );
    await fsp.writeFile(
      path.join(dir, encodedFileName("alive", Date.now() + 60_000)),
      Buffer.from("new"),
    );

    const store = new KeyvFilesystem({
      path: dir,
      useIndexFile: true,
      expiredCheckDelay: 60_000,
    });

    try {
      const startedAt = Date.now();
      while (Date.now() - startedAt < 2_000) {
        if ((await store.get<Buffer>("stale")) === undefined) {
          break;
        }

        await new Promise((resolve) => setTimeout(resolve, 25));
      }

      expect(await store.get("stale")).toBeUndefined();
      expect((await store.get<Buffer>("alive"))?.toString()).toBe("new");
      await expect(
        fsp.stat(path.join(dir, INDEX_FILE_NAME)),
      ).resolves.toBeTruthy();
    } finally {
      await store.disconnect();
    }
  });

  it("uses sqlite index during regular clearExpire sweeps", async () => {
    const dir = randomTestPath("sqlite-index-sweep");
    const store = new KeyvFilesystem({
      path: dir,
      useIndexFile: true,
      expiredCheckDelay: 60_000,
    });

    const opendirSpy = vi.spyOn(fsp, "opendir");
    try {
      await store.set("expires", Buffer.from("v"), 5);
      await new Promise((resolve) => setTimeout(resolve, 20));

      opendirSpy.mockClear();

      const deleted = await store.clearExpire();
      expect(deleted).toBe(1);
      expect(opendirSpy).not.toHaveBeenCalled();
    } finally {
      opendirSpy.mockRestore();
      await store.disconnect();
    }
  });

  it("filters iterator output with namespace argument", async () => {
    const dir = randomTestPath("iterator-namespace-filter");
    const store = new KeyvFilesystem({ path: dir, expiredCheckDelay: 60_000 });

    try {
      await store.set("users:1", Buffer.from("u1"));
      await store.set("users:2", Buffer.from("u2"));
      await store.set("sessions:1", Buffer.from("s1"));

      const keys: string[] = [];
      for await (const [key] of store.iterator("users")) {
        keys.push(key);
      }

      expect(keys.sort()).toEqual(["users:1", "users:2"]);
    } finally {
      await store.disconnect();
    }
  });
});
