import { describe, it, expect, vi } from "vitest";
import { promises as fsp } from "fs";
import Keyv from "keyv";
import KeyvFilesystem from "../lib/index.js";
import { keyvDeserialize, keyvSerialize, randomTestPath } from "./helpers.js";

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
        { key: "b", value: Buffer.from("vb"), ttl: 5 },
      ]);

      expect((await store.get<Buffer>("a"))?.toString()).toBe("va");
      expect((await store.get<Buffer>("b"))?.toString()).toBe("vb");

      await new Promise((resolve) => setTimeout(resolve, 20));

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