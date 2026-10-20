---
name: run-tests
description: Run the project's pytest suites by level (unit without Docker, integration with a subset of containers, e2e on the full stack) and read the results honestly. Use after any change and before reporting work as done.
argument-hint: "[unit|integration|e2e|<test id>]"
---
# Running tests

Three levels, selected by pytest markers (defined in the root `pyproject.toml`):

| Level | Marker | Needs | Target |
|---|---|---|---|
| unit | `unit` | nothing (local Spark + Delta on filesystem is allowed) | `make test-unit` |
| integration | `integration` | the containers the test declares (compose profile or testcontainers), started by the fixture | `make test-integration` |
| e2e | `e2e` | the full stack, healthy, fresh (`make reset` first) | `make test-e2e` |

```bash
make test-unit                                   # fast, run on every change
make test-integration                            # starts what it needs
make test-e2e                                    # full stack, minutes; includes fault injection
pytest tests/integration/test_debezium.py -k add_column -x -ra   # one test, stop on first failure
pytest -m "unit and not slow" -q
```

## Rules

- Test ids follow the design: `tests/<level>/test_<component>.py::test_<claim>`,
  and the docstring quotes the row of the "Testy" table it covers.
- No `sleep` for synchronization: poll a condition with a timeout
  (`tests/helpers/wait.py: wait_until(pred, timeout, every)`).
- Exact assertions: counts and values from the design table, equality for
  reconciliation, not tolerances.
- A skipped test is reported as skipped with its reason; a flaky test is a bug.
- Reading the result: report `passed/failed/skipped/error` counts and the full
  assertion message of each failure. Never summarize a run you did not execute.
- Unit tests must stay runnable without Docker; if one needs a container, it is an
  integration test and must be moved.
