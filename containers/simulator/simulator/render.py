"""Events → SQL statements (pure). The writer executes them inside one transaction per
event (02 §6: one purchase = one transaction)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from simulator import events as ev
from simulator.schema import TableSchema, qualified, quote_ident

Statement = tuple[str, tuple[object, ...]]


def sql_value(v: str) -> str | None:
    return None if v == "" else v


def sql_row(row: Sequence[str]) -> tuple[str | None, ...]:
    return tuple(sql_value(v) for v in row)


class Renderer:
    def __init__(self, schemas: Mapping[str, TableSchema], ds: ev.Dataset, pg_schema: str = "shop"):
        self.ds = ds
        self._insert = {name: self._insert_sql(pg_schema, s) for name, s in schemas.items()}
        self._insert["customers"] += " ON CONFLICT DO NOTHING"
        orders = qualified(pg_schema, "orders")
        status = quote_ident(ev.STATUS_COLUMN)
        oid = quote_ident(ev.ORDER_ID)
        self._update_step = {
            col: f"UPDATE {orders} SET {status} = %s, {quote_ident(col)} = %s WHERE {oid} = %s"
            for col in ev.FUTURE_COLUMNS
        }
        self._update_final = f"UPDATE {orders} SET {status} = %s WHERE {oid} = %s"
        self._delete_items = f"DELETE FROM {qualified(pg_schema, 'order_items')} WHERE {oid} = %s"
        self._future = [ds.o_lifecycle[c] for c in ev.FUTURE_COLUMNS]
        channel = quote_ident(ev.DELIVERY_CHANNEL_COLUMN)
        # nullable on purpose: BACKWARD-compatible AVRO schema (03 §5); IF NOT EXISTS keeps a
        # replay of the event harmless (the cursor already makes it run exactly once)
        self._add_channel = f"ALTER TABLE {orders} ADD COLUMN IF NOT EXISTS {channel} TEXT NULL"
        cols = ", ".join(quote_ident(c) for c in schemas["orders"].column_names)
        marks = ", ".join(["%s"] * (len(schemas["orders"].columns) + 1))
        self._insert_order_channel = f"INSERT INTO {orders} ({cols}, {channel}) VALUES ({marks})"

    @staticmethod
    def _insert_sql(pg_schema: str, schema: TableSchema) -> str:
        cols = ", ".join(quote_ident(c) for c in schema.column_names)
        marks = ", ".join(["%s"] * len(schema.columns))
        return f"INSERT INTO {qualified(pg_schema, schema.name)} ({cols}) VALUES ({marks})"

    def insert(self, table: str, row: Sequence[str]) -> Statement:
        return (self._insert[table], sql_row(row))

    def statements(self, e: ev.Event) -> list[Statement]:
        if e.kind == "purchase":
            od = e.order
            assert od is not None
            out: list[Statement] = []
            if od.customer is not None:
                out.append(self.insert("customers", od.customer))
            order = list(od.order)
            order[self.ds.o_status] = ev.INITIAL_STATUS
            for i in self._future:
                order[i] = ""  # not known yet: filled by the later UPDATEs
            if e.channel is None:
                out.append(self.insert("orders", order))
            else:
                out.append((self._insert_order_channel, (*sql_row(order), e.channel)))
            out += [self.insert("order_items", r) for r in od.items]
            out += [self.insert("order_payments", r) for r in od.payments]
            return out
        if e.kind == "status":
            assert e.column is not None
            return [(self._update_step[e.column], (e.status, e.value, e.order_id))]
        if e.kind == "final":
            out = [(self._update_final, (e.status, e.order_id))]
            if e.deletes:
                out.append((self._delete_items, (e.order_id,)))
            return out
        if e.kind == "review":
            assert e.row is not None
            return [self.insert("order_reviews", e.row)]
        if e.kind == "schema":
            return [(self._add_channel, ())]
        raise ValueError(f"unknown event kind {e.kind!r}")
