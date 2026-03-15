## Workflow Orchestration

### 1. Plan Node Default
- Enter plan mode for ANY non-trivial task (3+ steps or architectural decisions)
- If something goes sideways, STOP and re-plan immediately — don't keep pushing
- Use plan mode for verification steps, not just building
- Write detailed specs upfront to reduce ambiguity

### 2. Subagent Strategy
- Use subagents liberally to keep main context window clean
- Offload research, exploration, and parallel analysis to subagents
- For complex problems, throw more compute at it via subagents
- One task per subagent for focused execution

### 3. Self-Improvement Loop
- After ANY correction from the user: update `tasks/lessons.md` with the pattern
- Write rules for yourself that prevent the same mistake
- Ruthlessly iterate on these lessons until mistake rate drops
- Review lessons at session start for relevant project
- If a file is deleted, add or update a lesson entry that records the deleted file and the reason for deletion, and keep that lesson entry for future sessions

### 4. Verification Before Done
- Never mark a task complete without proving it works
- Diff behavior between main and your changes when relevant
- Ask yourself: "Would a staff engineer approve this?"
- Run tests, check logs, demonstrate correctness

### 5. Demand Elegance (Balanced)
- For non-trivial changes: pause and ask "is there a more elegant way?"
- If a fix feels hacky: "Knowing everything I know now, implement the elegant solution"
- Skip this for simple, obvious fixes — don't over-engineer
- Challenge your own work before presenting it

### 6. Autonomous Bug Fixing
- When given a bug report: just fix it. Don't ask for hand-holding
- Point at logs, errors, failing tests — then resolve them
- Zero context switching required from the user
- Go fix failing CI tests without being told how


## Task Management

1. **Plan First**: Write plan to `tasks/todo.md` with checkable items
2. **Verify Plan**: Check in before starting implementation
3. **Track Progress**: Mark items complete as you go
4. **Explain Changes**: High-level summary at each step
5. **Document Results**: Add review section to `tasks/todo.md`
6. **Capture Lessons**: Update `tasks/lessons.md` after corrections
7. **Deletion Cleanup in Todo**: If a file is deleted, remove only the `tasks/todo.md` items that specifically mention that deleted file


## Core Principles

- **Simplicity First**: Make every change as simple as possible. Impact minimal code.
- **No Laziness**: Find root causes. No temporary fixes. Senior developer standards.
- **Minimal Impact**: Changes should only touch what's necessary. Avoid introducing bugs.

## Commit Messages

Follow the Conventional Commits 1.0.0 specification for every commit:
- Reference: `https://www.conventionalcommits.org/en/v1.0.0/`
- Required format:
  - `<type>[optional scope][!]: <description>`
- Examples:
  - `feat: add GDELT backfill CLI`
  - `fix(parser): handle large GKG fields`
  - `feat!: change Neo4j publish schema`

### Required structure
- `type` is required and must be a noun-like token such as:
  - `feat`: a new feature
  - `fix`: a bug fix
- `scope` is optional and should be a short subsystem name when it adds clarity:
  - examples: `pipeline`, `parser`, `neo4j`, `cli`
- `!` is optional and marks a breaking change when placed after the type or scope.
- `description` is required and must be a concise summary after the colon and space.

### Body and footer rules
- A longer body is optional and should be added after one blank line when context helps.
- The body should explain what changed and why, especially for non-trivial commits.
- One or more footers are optional and should be added after one blank line following the body.
- Footer tokens should use the conventional trailer format such as:
  - `BREAKING CHANGE: <details>`
  - `Refs: <ticket-or-link>`

### Breaking change rules
- A breaking change must be indicated by either:
  - `!` after the type or scope in the header, or
  - a `BREAKING CHANGE:` footer
- If possible, include both when the change is significant and the migration needs explanation.

### SemVer mapping
- `fix` corresponds to a PATCH-level change.
- `feat` corresponds to a MINOR-level change.
- `BREAKING CHANGE` corresponds to a MAJOR-level change.

### Repository expectations
- Use Conventional Commits for the first commit as well as every later commit.
- Prefer one logical change per commit.
- Keep the subject line short, specific, and in imperative style.
- Use the most precise scope available when it improves scanability.
- Recommended additional types for this repo:
  - `build`, `chore`, `ci`, `docs`, `perf`, `refactor`, `revert`, `style`, `test`
- Do not use vague subjects like `updates`, `misc fixes`, or `work in progress`.

## Run Commands

Use the local project virtual environment for all commands:
- `source .venv/bin/activate`

Primary CLI commands:
- `python -m gdelt_risk_graph --help`
- `python -m gdelt_risk_graph backfill --days 1 --workers 4`
- `python -m gdelt_risk_graph poll-once`
- `python -m gdelt_risk_graph process-batch --timestamp 20260313000000 --export-url http://data.gdeltproject.org/gdeltv2/20260313000000.export.CSV.zip --mentions-url http://data.gdeltproject.org/gdeltv2/20260313000000.mentions.CSV.zip --gkg-url http://data.gdeltproject.org/gdeltv2/20260313000000.gkg.csv.zip`
- `python -m gdelt_risk_graph rebuild-day --date 2026-03-13`
- `python -m gdelt_risk_graph publish-neo4j --since 2026-03-13`

Environment setup for Neo4j publishing:
- `export NEO4J_URI=bolt://localhost:7687`
- `export NEO4J_USERNAME=neo4j`
- `export NEO4J_PASSWORD=password`
- `export NEO4J_DATABASE=neo4j`

Recommended verification commands:
- `./.venv/bin/python -m unittest discover -s tests -v`
- `./.venv/bin/python -m gdelt_risk_graph --help`

Recommended local workflow:
1. `source .venv/bin/activate`
2. `python -m gdelt_risk_graph backfill --days 1 --workers 4`
3. `python -m gdelt_risk_graph publish-neo4j --since 2026-03-13`
4. Open Neo4j Browser at `http://localhost:7474`
