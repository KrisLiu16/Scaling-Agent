// Organization tools and hooks for the pi harness. Loaded with `pi -e`; configured by the adapter
// through SA_ORG_* environment variables. stdout belongs to pi's RPC protocol: log to stderr only.
//
// * Every tool of the coordination server's MCP endpoint is registered under its own name, taken
//   from `tools/list`, so the server stays the single source of tool names and schemas.
// * tool_call denies pushing to the main branch from the shell (defense in depth; branch protection
//   enforces it server-side).
// * tool_result appends pending updates (DMs, board entries) after every other tool, so urgent
//   messages reach a worker within one tool call. Org tool results already carry them.
import { writeFileSync } from "node:fs";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { pushesProtected } from "./guard.ts";

interface McpTool {
  name: string;
  description?: string;
  inputSchema: Record<string, unknown>;
}

interface McpContent {
  type: string;
  text?: string;
}

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is not set`);
  return value;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export default async function (pi: ExtensionAPI) {
  const coord = required("SA_ORG_COORD_URL").replace(/\/+$/, "");
  const auth = { Authorization: `Bearer ${required("SA_ORG_TOKEN")}` };
  const mainBranch = process.env.SA_ORG_MAIN_BRANCH || "main";
  const denied = process.env.SA_ORG_PUSH_DENIED_REASON || `Do not push to ${mainBranch} or force-push.`;
  const toolTimeoutMs = Number(process.env.SA_ORG_TOOL_TIMEOUT_MS || 300_000);
  const bashTimeoutS = Number(process.env.SA_ORG_BASH_TIMEOUT_S || 600);

  let nextId = 1;
  async function mcp(method: string, params: Record<string, unknown>, signal?: AbortSignal): Promise<any> {
    const timeout = AbortSignal.timeout(toolTimeoutMs);
    const res = await fetch(`${coord}/mcp`, {
      method: "POST",
      headers: { ...auth, "Content-Type": "application/json", Accept: "application/json, text/event-stream" },
      body: JSON.stringify({ jsonrpc: "2.0", id: nextId++, method, params }),
      signal: signal ? AbortSignal.any([signal, timeout]) : timeout,
    });
    const raw = await res.text();
    if (!res.ok) throw new Error(`coordination server ${method}: HTTP ${res.status} ${raw.slice(0, 300)}`);
    const payload = res.headers.get("content-type")?.includes("text/event-stream")
      ? raw.split("\n").filter((l) => l.startsWith("data:")).map((l) => l.slice(5).trim()).at(-1) ?? "{}"
      : raw;
    const body = JSON.parse(payload);
    if (body.error) throw new Error(`coordination server ${method}: ${body.error.message ?? JSON.stringify(body.error)}`);
    return body.result;
  }

  let tools: McpTool[] = [];
  for (let attempt = 1; ; attempt++) {
    try {
      tools = (await mcp("tools/list", {})).tools;
      break;
    } catch (e) {
      if (attempt >= 4) throw e;
      await sleep(1000 * attempt);
    }
  }
  const orgTools = new Set(tools.map((t) => t.name));

  for (const tool of tools) {
    const description = tool.description ?? tool.name;
    pi.registerTool({
      name: tool.name,
      label: tool.name,
      description,
      promptSnippet: description.split("\n")[0],
      parameters: tool.inputSchema as any,
      async execute(_id, params, signal) {
        const result = await mcp("tools/call", { name: tool.name, arguments: params }, signal);
        const content = ((result.content ?? []) as McpContent[]).filter((c) => c.type === "text" && c.text);
        const text = content.map((c) => c.text).join("\n");
        if (result.isError) throw new Error(text || `${tool.name} failed`);
        return { content: [{ type: "text" as const, text }], details: {} };
      },
    });
  }

  pi.on("tool_call", async (event) => {
    if (event.toolName !== "bash") return;
    const input = event.input as { command?: unknown; timeout?: number };
    if (pushesProtected(String(input?.command ?? ""), mainBranch)) return { block: true, reason: denied };
    // pi's bash tool has no default timeout: one hung foreground command would hold the worker for the whole turn.
    if (input.timeout === undefined) input.timeout = bashTimeoutS;
  });

  pi.on("tool_result", async (event) => {
    if (orgTools.has(event.toolName)) return;
    let text = "";
    try {
      const res = await fetch(`${coord}/api/drain`, {
        method: "POST",
        headers: { ...auth, "Content-Type": "application/json" },
        body: "{}",
        signal: AbortSignal.timeout(10_000),
      });
      if (res.ok) text = ((await res.json()) as { text?: string }).text ?? "";
    } catch (e) {
      console.error(`drain failed: ${e}`); // a missed update must not break the agent loop
    }
    if (text) return { content: [...event.content, { type: "text" as const, text }] };
  });

  const ready = process.env.SA_ORG_READY_FILE;
  if (ready) writeFileSync(ready, JSON.stringify({ tools: [...orgTools] }));
}
