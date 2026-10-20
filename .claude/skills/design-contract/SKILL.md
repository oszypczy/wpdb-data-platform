---
name: design-contract
description: How to use docs/design/*.md as the contract and definition of done for a component - which doc to read, what the "Testy" and "Do weryfikacji" sections mean, repo layout and naming conventions shared by all agents.
user-invocable: false
---
# Design docs are the contract

Every component has one design doc in `docs/design/` (Polish). It is the source of
truth: the orchestrator, the component owner and QA all read the same file.

## Before you change anything

1. Read `docs/design/00-overview.md` (decisions, data flow, work plan).
2. Read the doc(s) for your component in full. Sections "Część podstawowa" are the
   graded minimum, "Rozszerzenie" sections are the bonus extras. Both are in scope.
3. Read the docs of your neighbours only for the interface you share
   (e.g. Spark reads 03 §1 envelope, 03 §4 keys, 04 §2 topics).

## Definition of done

- **"Testy" table** = acceptance criteria. Each row becomes a pytest test at the
  stated level (`unit` / `integration` / `e2e`). The component is done when all its
  rows pass and `qa-engineer` confirms it independently.
- **"Do weryfikacji przy implementacji"** = questions the design left open. Resolve
  each one during implementation and write the answer into the doc (replace the
  bullet with the decision and the evidence, e.g. a doc link or a test id).
- If implementation proves the design wrong, **update the doc first**, then the
  code, and say so in your report. Silent deviations are defects.
- Design docs are in Polish; keep that. Code, comments, commit messages and
  CLAUDE.md are in English.

## Repo layout (agreed, create on first use)

```
simulator/      business simulator (Python)            owner: business-simulator
postgres/       image config + init SQL                owner: database-cdc
connect/        Debezium Connect image + connector     owner: database-cdc
kafka/          topics as code + AVRO consumer         owner: database-cdc
spark/          streaming jobs, pure transforms        owner: streaming-lakehouse
minio/          init scripts, policies                 owner: streaming-lakehouse (+devops)
dashboard/      Streamlit app, query layer             owner: analytics-dashboard
tests/          unit/ integration/ e2e/ + conftest     owner: qa-engineer
docker-compose.yml, Dockerfiles, Makefile, .env.example   owner: platform-devops
docs/design/08-operations.md                           owner: platform-devops (+qa)
```

Python: one `pyproject.toml` per component container plus one at the root for tests
and shared tooling; `pytest` markers `unit`, `integration`, `e2e`.

## Non-negotiables from CLAUDE.md (reminders, not repeats)

- Each component in its own container; downstream components never connect to
  Postgres; message key = table PK; AVRO via Schema Registry; Delta, not Parquet.
- Tutorials in `docs/tutorials/` contain deliberate bugs: context only.
- Keep the data rate low; defaults must not overload the stack.
- No Claude/AI attribution in commits.
