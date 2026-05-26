import { describe, it, expect, vi } from "vitest";
import { Readable } from "stream";
import { ReadableStream } from "stream/web";
import { promises as fsp } from "fs";
import path from "path";
import Keyv from "keyv";
import { randomTestPath, streamToBuffer } from "./helpers.js";
import KeyvFilesystem from "../src/index.ts";

describe("Serialization behavior", () => {
  it("default deserializer returns Buffer", async () => {
    const dir = randomTestPath("buffer-read");
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
    const dir = randomTestPath("deserializer-read");
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
    const dir = randomTestPath("readable-write");
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
    const dir = randomTestPath("webstream-write");
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
    const dir = randomTestPath("no-serializer-types");
    const store = new KeyvFilesystem({ path: dir, expiredCheckDelay: 60_000 });

    try {
      await expect(store.set("unsupported", { a: 1 })).rejects.toThrow(
        "Default serializer only accepts Buffer, Readable, or ReadableStream",
      );
    } finally {
      await store.disconnect();
    }
  });

  it("stores Readable payload bytes when Keyv serialization is disabled", async () => {
    const dir = randomTestPath("keyv-readable-raw");
    const store = new KeyvFilesystem({ path: dir, expiredCheckDelay: 60_000 });
    const keyv = new Keyv<Buffer>({
      store,
      serialize: undefined,
      deserialize: undefined,
    });

    try {
      await keyv.set("stream", Readable.from([Buffer.from("hello "), "keyv"]));

      const value = await keyv.get("stream");
      expect(Buffer.isBuffer(value)).toBe(true);
      expect((value as Buffer).toString("utf8")).toBe("hello keyv");

      const files = await fsp.readdir(dir);
      const entryFiles = files.filter((fileName) => fileName.endsWith(".bin"));
      expect(entryFiles.length).toBe(1);
      const stored = await fsp.readFile(path.join(dir, entryFiles[0]!));
      expect(stored.toString("utf8")).toBe("hello keyv");
    } finally {
      await keyv.disconnect();
      await store.disconnect();
    }
  });

  it("delegates input handling to serializer when configured", async () => {
    const dir = randomTestPath("serializer-delegation");
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
