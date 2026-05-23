import Keyv from "keyv";
import KeyvFilesystem from "../lib/index.js";
import keyvTestSuite, { keyvIteratorTests } from "@keyv/test-suite";
import { defaultDeserialize, defaultSerialize } from "@keyv/serialize";
import * as test from "vitest";
import path from "path";
import { Readable } from "stream";

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

const store = () =>
  new KeyvFilesystem({
    path: path.join(TEST_ROOT, `test1-${Math.random().toString(36).slice(2)}`),
    expiredCheckDelay: 60_000,
    serialize: keyvSerialize,
    deserialize: keyvDeserialize,
  });

const store2 = () =>
  new KeyvFilesystem({
    path: path.join(TEST_ROOT, `test2-${Math.random().toString(36).slice(2)}`),
    expiredCheckDelay: 60_000,
    serialize: keyvSerialize,
    deserialize: keyvDeserialize,
  });

const store3 = () =>
  new KeyvFilesystem({
    path: path.join(TEST_ROOT, `test3-${Math.random().toString(36).slice(2)}`),
    expiredCheckDelay: 60_000,
    serialize: keyvSerialize,
    deserialize: keyvDeserialize,
  });

keyvTestSuite(test, Keyv, store);
keyvIteratorTests(test, Keyv, store);

keyvTestSuite(test, Keyv, store2);
keyvIteratorTests(test, Keyv, store2);

keyvTestSuite(test, Keyv, store3);
keyvIteratorTests(test, Keyv, store3);
