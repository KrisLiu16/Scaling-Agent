// Drives pi_extension/org.ts with a stand-in for pi's extension API against a live coordination
// server, and prints what happened as JSON. Run by tests/test_pi_extension.py.
import org from "../src/scaling_agent/runtime/adapters/pi_extension/org.ts";

const coord = process.env.SA_ORG_COORD_URL;
const peerToken = process.env.PROBE_PEER_TOKEN;
const tools = new Map();
const handlers = {};
const pi = { registerTool: (t) => tools.set(t.name, t), on: (event, h) => (handlers[event] = h) };

const out = {};
try {
  await org(pi);
  out.tools = [...tools.keys()].sort();
  out.snippet = tools.get("claim").promptSnippet;
  out.schemaRequired = tools.get("board_write").parameters.required;
  const written = await tools.get("board_write").execute("1", { type: "FACT", text: "from node" }, undefined);
  out.boardWrite = written.content[0].text;

  await fetch(`${coord}/mcp`, {
    method: "POST",
    headers: { Authorization: `Bearer ${peerToken}`, "Content-Type": "application/json", Accept: "application/json, text/event-stream" },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "send_dm", arguments: { to: "w1", text: "urgent from peer" } } }),
  });
  const bash = await handlers.tool_result({ toolName: "bash", content: [{ type: "text", text: "ls output" }] });
  out.bashResult = bash?.content.map((c) => c.text);
  out.orgResult = await handlers.tool_result({ toolName: "claim", content: [] }) ?? null;
  const blocked = await handlers.tool_call({ toolName: "bash", input: { command: "git push origin main" } });
  out.blocked = blocked;
  out.allowed = await handlers.tool_call({ toolName: "bash", input: { command: "git push origin w1" } }) ?? null;
  out.otherTool = await handlers.tool_call({ toolName: "write", input: { command: "git push origin main" } }) ?? null;
  const plain = { toolName: "bash", input: { command: "make test" } };
  await handlers.tool_call(plain);
  const explicit = { toolName: "bash", input: { command: "make test", timeout: 1800 } };
  await handlers.tool_call(explicit);
  const notBash = { toolName: "read", input: { path: "x" } };
  await handlers.tool_call(notBash);
  out.timeouts = { plain: plain.input.timeout, explicit: explicit.input.timeout, notBash: notBash.input.timeout ?? null };
} catch (e) {
  out.error = String(e);
}
console.log(JSON.stringify(out));
