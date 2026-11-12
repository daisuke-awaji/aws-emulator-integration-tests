/**
 * エミュレータの Node.js ランタイムが実 Lambda と「違う」ところを固定する特性テスト (characterization test)。
 * aws-emulator 1.0.322 時点の観測結果。エミュレータが改善されるとここが落ちるので、そのとき前提を見直す。
 */
import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";
import { InvokeCommand } from "@aws-sdk/client-lambda";
import { bundleHandler, Cleanup, type Clients, deployFunction, setupClients } from "./helpers/emulator.ts";
import { zipFiles } from "./helpers/zip.ts";

const HANDLER = new URL("../src/order-consumer.ts", import.meta.url).pathname;

async function invoke(c: Clients, fn: string, payload: unknown = {}) {
  const r = await c.lambda.send(new InvokeCommand({ FunctionName: fn, Payload: JSON.stringify(payload) }));
  return { functionError: r.FunctionError, body: JSON.parse(new TextDecoder().decode(r.Payload)) };
}

describe("エミュレータ固有の Lambda ランタイム挙動 (実 Lambda とは異なる)", { concurrency: true }, () => {
  let c: Clients;
  const cl = new Cleanup();
  before(async () => {
    c = await setupClients();
  });
  after(async () => {
    await cl.run();
    c.destroy();
  });

  it("本物の Node.js ではない: process.versions は node だけ、structuredClone / node:http / node:zlib がない", async () => {
    const code = `
      export const handler = async () => {
        const has = async (m) => { try { await import(m); return true; } catch { return false; } };
        return {
          version: process.version, versionKeys: Object.keys(process.versions),
          structuredClone: typeof structuredClone, fetch: typeof fetch,
          http: await has("node:http"), https: await has("node:https"), zlib: await has("node:zlib"),
          crypto: await has("node:crypto"), fs: await has("node:fs"), endpoint: process.env.AWS_ENDPOINT_URL ?? null,
        };
      };`;
    const fn = await deployFunction(c, { zip: zipFiles({ "index.mjs": code }), prefix: "rt" });
    cl.fn(c, fn);
    const { functionError, body } = await invoke(c, fn);
    assert.equal(functionError, undefined);
    assert.deepEqual(body, {
      version: "v22.0.0",
      versionKeys: ["node"],
      structuredClone: "undefined",
      fetch: "undefined",
      http: false,
      https: false,
      zlib: false,
      crypto: true,
      fs: true,
      endpoint: null, // 関数内の SDK は endpoint 設定なしでエミュレータへつながる（同梱 SDK がエミュレータ実装のため）
    });
  });

  it("本物の AWS SDK をバンドルに含めると node:zlib が無く起動できない → @aws-sdk/* は external にする", async () => {
    const fn = await deployFunction(c, { zip: await bundleHandler(HANDLER, "inline-sdk"), prefix: "inline" });
    cl.fn(c, fn);
    const { functionError, body } = await invoke(c, fn, { Records: [] });
    assert.equal(functionError, "Unhandled");
    assert.equal(body.errorType, "Runtime.ImportModuleError");
    assert.match(body.errorMessage, /Cannot find module 'node:zlib'/);
  });

  it("同梱 SDK は一部のクライアントしかコマンドを持たない (STS / SNS / SSM は空)", async () => {
    const code = `
      export const handler = async () => {
        const out = {};
        for (const m of ["client-dynamodb", "lib-dynamodb", "client-s3", "client-sqs", "client-eventbridge", "client-secrets-manager", "client-sts", "client-sns", "client-ssm"]) {
          const x = await import("@aws-sdk/" + m);
          out[m] = Object.keys(x).some((k) => k.endsWith("Command"));
        }
        return out;
      };`;
    const fn = await deployFunction(c, { zip: zipFiles({ "index.mjs": code }), prefix: "sdk" });
    cl.fn(c, fn);
    const { body } = await invoke(c, fn);
    assert.deepEqual(body, {
      "client-dynamodb": true,
      "lib-dynamodb": true,
      "client-s3": true,
      "client-sqs": true,
      "client-eventbridge": true,
      "client-secrets-manager": true,
      "client-sts": false,
      "client-sns": false,
      "client-ssm": false,
    });
  });
});
