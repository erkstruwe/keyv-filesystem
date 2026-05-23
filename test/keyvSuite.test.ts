import keyvTestSuite, { keyvIteratorTests } from "@keyv/test-suite";
import Keyv from "keyv";
import * as test from "vitest";
import KeyvFilesystem from "../lib/index.js";
import { keyvDeserialize, keyvSerialize, randomTestPath } from "./helpers.js";

const storeOne = () =>
  new KeyvFilesystem({
    path: randomTestPath("keyv-suite-one"),
    expiredCheckDelay: 60_000,
    serialize: keyvSerialize,
    deserialize: keyvDeserialize,
  });

const storeTwo = () =>
  new KeyvFilesystem({
    path: randomTestPath("keyv-suite-two"),
    expiredCheckDelay: 60_000,
    serialize: keyvSerialize,
    deserialize: keyvDeserialize,
  });

const storeThree = () =>
  new KeyvFilesystem({
    path: randomTestPath("keyv-suite-three"),
    expiredCheckDelay: 60_000,
    serialize: keyvSerialize,
    deserialize: keyvDeserialize,
  });

keyvTestSuite(test, Keyv, storeOne);
keyvIteratorTests(test, Keyv, storeOne);

keyvTestSuite(test, Keyv, storeTwo);
keyvIteratorTests(test, Keyv, storeTwo);

keyvTestSuite(test, Keyv, storeThree);
keyvIteratorTests(test, Keyv, storeThree);