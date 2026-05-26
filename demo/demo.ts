import { createReadStream } from "fs";
import { readFile } from "fs/promises";
import Keyv from "keyv";
import { resolve } from "path";
import { KeyvFilesystem } from "../src/index.ts";

const filePath = resolve("./demo/demo.webp");

// Buffer
const cache = new Keyv({
  store: new KeyvFilesystem({
    path: "./node_modules/.cache/keyv-filesystem-demo",
  }),
  serialize: undefined,
  deserialize: undefined,
});

const fileBuffer = await readFile(filePath);
await cache.set("as-buffer", fileBuffer, 1000 * 60 * 60);
const cachedBuffer = await cache.get<Buffer>("as-buffer");
console.log(`Cached buffer length: ${cachedBuffer?.length}`);

// Readable stream
const fileStream = createReadStream(filePath);
await cache.set("as-stream", fileStream);
const cachedStreamBuffer = await cache.get<Buffer>("as-stream");
console.log(`Cached stream buffer length: ${cachedStreamBuffer?.length}`);
