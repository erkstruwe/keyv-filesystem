import { defaultDeserialize, defaultSerialize } from "@keyv/serialize";
import path from "path";
import { Readable } from "stream";

export const TEST_ROOT = path.join(
  "node_modules",
  ".cache",
  "keyv-filesystem-tests",
);

export function randomTestPath(prefix: string): string {
  return path.join(
    TEST_ROOT,
    `${prefix}-${Math.random().toString(36).slice(2)}`,
  );
}

export async function streamToBuffer(stream: Readable): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  return Buffer.concat(chunks);
}

export const keyvSerialize = (value: unknown) => {
  const serialized = defaultSerialize(value);
  return Readable.from([
    Buffer.isBuffer(serialized) ? serialized : Buffer.from(serialized),
  ]);
};

export const keyvDeserialize = async (stream: Readable) =>
  defaultDeserialize((await streamToBuffer(stream)).toString());
