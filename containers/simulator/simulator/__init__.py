"""Business simulator: replays the Olist data set into PostgreSQL.

Contract: docs/design/01-business-system.md.

Pure modules (no database): csvio, schema, events, render, clock, config.
Database access: db (writer) and engine (phases, pacing, resume).
"""
