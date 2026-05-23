import { describe, it, expect, vi } from "vitest";
import { promises as fsp } from "fs";
import path from "path";
import { Readable } from "stream";
import { ReadableStream } from "stream/web";
import { defaultDeserialize, defaultSerialize } from "@keyv/serialize";
import Keyv from "keyv";
import KeyvFilesystem from "../lib/index.js";

const TEST_ROOT = path.join("node_modules", ".cache", "keyv-filesystem-tests");

async function streamToBuffer(stream: Readable): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  return Buffer.concat(chunks);
}

const keyvSerialize = (value: unknown) => {
  const serialized = defaultSerialize(value);
  return Readable.from([
    Buffer.isBuffer(serialized) ? serialized : Buffer.from(serialized),
  ]);
};

const keyvDeserialize = async (stream: Readable) =>
  defaultDeserialize((await streamToBuffer(stream)).toString());

describe("Key filename safety", () => {
  it("requires a non-empty path option", () => {
    expect(() => new KeyvFilesystem({ path: "" })).toThrow(
      "KeyvFilesystem requires a non-empty options.path",
    );
    expect(() => new KeyvFilesystem({} as any)).toThrow(
      "KeyvFilesystem requires a non-empty options.path",
    );
  });

  it("supports ASCII special characters and empty key", async () => {
    const dir = path.join(
      TEST_ROOT,
      `keyv-filename-${Math.random().toString(36).slice(2)}`,
    );
    const store = new KeyvFilesystem({ path: dir, expiredCheckDelay: 60_000 });

    const weirdKey = 'test/\\:*?"<>|file';
    await store.set(weirdKey, Buffer.from("ok"));
    await store.set("", Buffer.from("empty"));

    expect((await store.get<Buffer>(weirdKey))?.toString()).toBe("ok");
    expect((await store.get<Buffer>(""))?.toString()).toBe("empty");

    await store.disconnect();
  });

  it("expires encoded keys during explicit sweep", async () => {
    const dir = path.join(
      TEST_ROOT,
      `keyv-sweep-${Math.random().toString(36).slice(2)}`,
    );
    const absDir = path.resolve(dir);
    const store = new KeyvFilesystem({ path: dir, expiredCheckDelay: 60_000 });
    const key = "a/b:c?d*e|f";

    await store.set(key, Buffer.from("value"), 5);
    await new Promise((resolve) => setTimeout(resolve, 20));

    const deleted = await store.clearExpire();
    expect(deleted).toBe(1);
    expect(await store.get(key)).toBeUndefined();

    const files = await fsp.readdir(absDir);
    expect(files.length).toBe(0);

    await store.disconnect();
  });

  it("includes empty key in iterator and removes it during clear", async () => {
    const dir = path.join(
      TEST_ROOT,
      `keyv-empty-iterate-clear-${Math.random().toString(36).slice(2)}`,
    );
    const absDir = path.resolve(dir);
    const store = new KeyvFilesystem({ path: dir, expiredCheckDelay: 60_000 });

    await store.set("", Buffer.from("empty"));
    await store.set("normal", Buffer.from("value"));

    const keys: string[] = [];
    for await (const [key] of store.iterator<Buffer>()) {
      keys.push(key);
    }

    expect(keys).toContain("");
    expect(keys).toContain("normal");

    await store.clear();

    const files = await fsp.readdir(absDir);
    expect(files.length).toBe(0);

    await store.disconnect();
  });

  it("sets ttl metadata before rename and avoids post-rename utimes", async () => {
    const dir = path.join(
      TEST_ROOT,
      `keyv-atomic-ttl-${Math.random().toString(36).slice(2)}`,
    );
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
    const dir = path.join(
      TEST_ROOT,
      `keyv-namespace-isolation-${Math.random().toString(36).slice(2)}`,
    );
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
    const dir = path.join(
      TEST_ROOT,
      `keyv-bulk-methods-${Math.random().toString(36).slice(2)}`,
    );
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

  it("default deserializer returns Buffer", async () => {
    const dir = path.join(
      TEST_ROOT,
      `keyv-buffer-read-${Math.random().toString(36).slice(2)}`,
    );
    const store = new KeyvFilesystem({ path: dir, expiredCheckDelay: 60_000 });

    try {
      await store.set("k", Buffer.from("raw-bytes"));
      const asyncValue = await store.get("k");

      expect(Buffer.isBuffer(asyncValue)).toBe(true);
      expect((asyncValue as Buffer).toString()).toBe("raw-bytes");
    } finally {
      await store.disconnect();
    }
  });

  it("applies configured deserializer to read values", async () => {
    const dir = path.join(
      TEST_ROOT,
      `keyv-deserializer-read-${Math.random().toString(36).slice(2)}`,
    );
    const store = new KeyvFilesystem<Buffer, string>({
      path: dir,
      expiredCheckDelay: 60_000,
      deserialize: async (value: Readable) =>
        (await streamToBuffer(value)).toString("utf8").toUpperCase(),
    });

    try {
      await store.set("k", Buffer.from("decoded"));

      expect(await store.get("k")).toBe("DECODED");
    } finally {
      await store.disconnect();
    }
  });

  it("default serializer accepts Node Readable values", async () => {
    const dir = path.join(
      TEST_ROOT,
      `keyv-readable-write-${Math.random().toString(36).slice(2)}`,
    );
    const store = new KeyvFilesystem({ path: dir, expiredCheckDelay: 60_000 });

    try {
      const stream = Readable.from([Buffer.from("hello "), "stream"]);
      await store.set("node-readable", stream);

      expect((await store.get<Buffer>("node-readable"))?.toString()).toBe(
        "hello stream",
      );
    } finally {
      await store.disconnect();
    }
  });

  it("default serializer accepts Web ReadableStream values", async () => {
    const dir = path.join(
      TEST_ROOT,
      `keyv-webstream-write-${Math.random().toString(36).slice(2)}`,
    );
    const store = new KeyvFilesystem({ path: dir, expiredCheckDelay: 60_000 });

    try {
      const stream = new ReadableStream({
        start(controller) {
          controller.enqueue(new Uint8Array(Buffer.from("web ")));
          controller.enqueue("stream");
          controller.close();
        },
      });

      await store.set("web-readable", stream);
      expect((await store.get<Buffer>("web-readable"))?.toString()).toBe(
        "web stream",
      );
    } finally {
      await store.disconnect();
    }
  });

  it("default serializer rejects unsupported values", async () => {
    const dir = path.join(
      TEST_ROOT,
      `keyv-no-serializer-types-${Math.random().toString(36).slice(2)}`,
    );
    const store = new KeyvFilesystem({ path: dir, expiredCheckDelay: 60_000 });

    try {
      await expect(store.set("unsupported", { a: 1 })).rejects.toThrow(
        "Default serializer only accepts Buffer, Readable, or ReadableStream",
      );
    } finally {
      await store.disconnect();
    }
  });

  it("delegates input handling to serializer when configured", async () => {
    const dir = path.join(
      TEST_ROOT,
      `keyv-serializer-delegation-${Math.random().toString(36).slice(2)}`,
    );
    const serialize = vi.fn((value: unknown) => {
      if (value === "ok") {
        return Readable.from([Buffer.from("serialized-ok")]);
      }

      throw new TypeError("serializer-reject");
    });
    const store = new KeyvFilesystem({
      path: dir,
      expiredCheckDelay: 60_000,
      serialize,
      deserialize: undefined,
    });

    try {
      await store.set("ok", "ok");
      expect((await store.get<Buffer>("ok"))?.toString()).toBe("serialized-ok");

      await expect(
        store.set("stream", Readable.from(["value"])),
      ).rejects.toThrow("serializer-reject");
      expect(serialize).toHaveBeenCalled();
    } finally {
      await store.disconnect();
    }
  });
});
