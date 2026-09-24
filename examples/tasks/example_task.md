Build, from scratch in this repository, a command-line Markdown-to-HTML converter in Python that
matches the reference behaviour described below as closely as possible.

Deliverable
- `./convert` (executable) reads Markdown on stdin and writes HTML on stdout.
- `bash run_tests.sh` runs the test suite and exits non-zero on failure.

Reference behaviour
- CommonMark block structure: paragraphs, ATX/setext headings, block quotes, lists, code blocks,
  thematic breaks, HTML blocks.
- Inline structure: emphasis, code spans, links, images, autolinks, raw HTML, hard breaks.

Acceptance criteria
- Every feature ships with tests under `tests/` that pin its output.
- `bash run_tests.sh` passes on `main` after every merge.

Structure hints
- Keep `convert` a thin entry point. Each block type and each inline construct lives in its own
  module and registers itself with a small registry, so features can be added in parallel without
  editing a shared dispatch table.
