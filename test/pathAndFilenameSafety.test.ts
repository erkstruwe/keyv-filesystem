import { expect, it, describe } from "vitest";
import { promises as fsp } from "fs";
import path from "path";
import KeyvFilesystem from "../lib/index.js";
import { randomTestPath } from "./helpers.js";

describe("Path and filename safety", () => {
  it("requires a non-empty path option", () => {
    expect(() => new KeyvFilesystem({ path: "" })).toThrow(
      "KeyvFilesystem requires a non-empty options.path",
    );
    expect(() => new KeyvFilesystem({} as any)).toThrow(
      "KeyvFilesystem requires a non-empty options.path",
    );
  });

  it("supports ASCII special characters and empty key", async () => {
    const dir = randomTestPath("filename-safety");
    const store = new KeyvFilesystem({ path: dir, expiredCheckDelay: 60_000 });

    const weirdKey = 'test/\\:*?"<>|file';
    await store.set(weirdKey, Buffer.from("ok"));
    await store.set("", Buffer.from("empty"));

    expect((await store.get<Buffer>(weirdKey))?.toString()).toBe("ok");
    expect((await store.get<Buffer>(""))?.toString()).toBe("empty");

    await store.disconnect();
  });

  it("expires encoded keys during explicit sweep", async () => {
    const dir = randomTestPath("sweep");
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
    const dir = randomTestPath("empty-iterate-clear");
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
});