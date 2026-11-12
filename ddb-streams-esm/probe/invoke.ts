import { CreateFunctionCommand, InvokeCommand, LambdaClient, waitUntilFunctionActiveV2 } from "@aws-sdk/client-lambda";
import { zipFiles } from "../test/helpers/zip.ts";

const lambda = new LambdaClient({});
const code = `
export const handler = async (event, context) => {
  const probe = {};
  const t = (k, f) => { try { probe[k] = f(); } catch (e) { probe[k] = "ERR " + e.message; } };
  t("processVersion", () => typeof process !== "undefined" ? process.version : "no process");
  t("versions", () => process.versions);
  t("typeofRequire", () => typeof require);
  t("typeofFetch", () => typeof fetch);
  t("envKeys", () => Object.keys(process.env).sort());
  t("awsEndpoint", () => process.env.AWS_ENDPOINT_URL ?? null);
  t("ctx", () => ({ fn: context.functionName, rid: context.awsRequestId, rem: context.getRemainingTimeInMillis?.() }));
  console.log("probe log line", JSON.stringify(event));
  return { probe, event };
};`;
const name = "probe-" + Date.now();
await lambda.send(new CreateFunctionCommand({
  FunctionName: name, Runtime: "nodejs22.x", Handler: "index.handler",
  Role: "arn:aws:iam::000000000000:role/lambda-role",
  Code: { ZipFile: zipFiles({ "index.mjs": code }) },
}));
await waitUntilFunctionActiveV2({ client: lambda, maxWaitTime: 30 }, { FunctionName: name });
const r = await lambda.send(new InvokeCommand({ FunctionName: name, Payload: JSON.stringify({ hello: "world" }), LogType: "Tail" }));
console.log("StatusCode", r.StatusCode, "FunctionError", r.FunctionError);
console.log(new TextDecoder().decode(r.Payload));
console.log("LogResult:\n" + (r.LogResult ? Buffer.from(r.LogResult, "base64").toString() : "(none)"));
lambda.destroy();
