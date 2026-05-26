import { readFile } from "fs/promises";
import Keyv from "keyv";
import { KeyvFilesystem } from "../src/index.ts";

const cache = new Keyv({
  store: new KeyvFilesystem({
    path: "./node_modules/.cache/keyv-filesystem-demo",
  }),
  serialize: undefined,
  deserialize: undefined,
});

const fileBuffer = await readFile("./demo/demo.webp");
await cache.set("as-buffer", fileBuffer);

const cacheWithIndex = new Keyv({
  store: new KeyvFilesystem({
    path: "./node_modules/.cache/keyv-filesystem-demo2",
    useIndexFile: true,
  }),
  serialize: undefined,
  deserialize: undefined,
});

await cacheWithIndex.set("as-buffer", fileBuffer);
