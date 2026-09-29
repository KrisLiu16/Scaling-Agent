// Port of adapters/guards.py: the same cases (tests/fixtures/push_guard_cases.json) run against both.

const FORCE_FLAGS = new Set(["-f", "--force", "--force-with-lease", "--mirror", "--delete", "-d"]);

// Python's shlex.split (POSIX mode); null where shlex raises ValueError.
function shlexSplit(text: string): string[] | null {
  const tokens: string[] = [];
  let current = "";
  let inWord = false;
  let quote: string | null = null;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (quote === "'") {
      if (c === "'") quote = null;
      else current += c;
    } else if (quote === '"') {
      if (c === '"') {
        quote = null;
      } else if (c === "\\") {
        if (i + 1 >= text.length) return null;
        const next = text[++i];
        current += next === '"' || next === "\\" ? next : `\\${next}`;
      } else {
        current += c;
      }
    } else if (/\s/.test(c)) {
      if (inWord) tokens.push(current);
      current = "";
      inWord = false;
    } else if (c === "\\") {
      if (i + 1 >= text.length) return null;
      current += text[++i];
      inWord = true;
    } else if (c === "'" || c === '"') {
      quote = c;
      inWord = true;
    } else {
      current += c;
      inWord = true;
    }
  }
  if (quote !== null) return null;
  if (inWord) tokens.push(current);
  return tokens;
}

export function pushesProtected(command: string, protectedBranch = "main"): boolean {
  for (const segment of command.split(/&&|\|\||;|\||\n/)) {
    const tokens = shlexSplit(segment) ?? segment.split(/\s+/).filter(Boolean);
    const push = tokens.indexOf("push");
    if (!tokens.includes("git") || push < 0) continue;
    for (const arg of tokens.slice(push + 1)) {
      if (FORCE_FLAGS.has(arg) || arg.startsWith("--force")) return true;
      const dest = arg.replace(/^\++/, "").split(":").at(-1);
      if (dest === protectedBranch || dest === `refs/heads/${protectedBranch}` || arg.startsWith("+")) return true;
    }
  }
  return false;
}
