---
name: olist-data-quirks
description: Non-obvious facts about the Olist data that shaped simulator defaults and tests (empty Jan 1-4 2017, midnight reviews, date-vs-timestamp anomaly counts)
metadata:
  type: project
---

- Olist has no purchases on 2017-01-01..04 (first one 2017-01-05 11:56); a window starting
  2017-01-01 at x1440 shows nothing for 4.5 wall minutes. Default `SIM_START` is therefore
  2017-01-05.
- `review_creation_date` is always at 00:00:00, so every simulated midnight is a burst of
  dozens of review INSERTs that the rate limiter spreads out; anomaly counts differ by
  granularity (reviews before purchase: 74 by timestamp, 64 by date; before delivery:
  8 320 vs 5 126). The design doc now states both.
- Full 2017-01-01..2018-08-31 window = 807 648 row changes incl. customer inserts (the
  original ~706k figure omitted customers); 8 `delivered` orders have no delivery date and
  need a final status UPDATE.

**Why:** these were discovered only by running the generator on the real files; the
design numbers were measured differently (DuckDB, date-level).

**How to apply:** when a doc number and a test disagree, recompute with
`simulator.events.summarize` on `data/olist` before changing either.
