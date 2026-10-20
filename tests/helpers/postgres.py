"""PostgreSQL test fixtures shared by the integration and e2e suites.

- `apply_sql_file`: run a repo SQL file (e.g. containers/postgres/sql/truth-views.sql)
  on a connection, the way the simulator applies it (as `shop_app`).
- `create_olist_fixture_tables` / `drop_olist_fixture_tables`: minimal Olist-shaped
  tables (CSV column names, PKs as detected in 01 §3) with a known data set, for
  tests that run before the simulator exists.
- `catalog_snapshot`: the state created by initdb (roles, schemas, default
  privileges, publication, event trigger, server settings) as sorted text lines,
  independent of the simulator's tables.
- `InitdbProbe`: a throw-away `postgres` container started from the same image,
  initdb scripts and config as the stack, for "initdb twice → identical state".
"""

from __future__ import annotations

import subprocess
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import psycopg
import yaml

from tests.helpers.wait import wait_until

TRUTH_VIEWS_SQL = Path("containers/postgres/sql/truth-views.sql")

# tables referenced by truth-views.sql, in dependency order (parents first)
OLIST_FIXTURE_TABLES = ("customers", "orders", "order_items", "order_payments", "order_reviews")

OLIST_FIXTURE_DDL = """
CREATE TABLE shop.customers (
    customer_id text PRIMARY KEY,
    customer_unique_id text NOT NULL,
    customer_zip_code_prefix text,
    customer_city text,
    customer_state text
);
CREATE TABLE shop.orders (
    order_id text PRIMARY KEY,
    customer_id text NOT NULL REFERENCES shop.customers (customer_id),
    order_status text NOT NULL,
    order_purchase_timestamp timestamp NOT NULL,
    order_approved_at timestamp,
    order_delivered_carrier_date timestamp,
    order_delivered_customer_date timestamp,
    order_estimated_delivery_date timestamp
);
CREATE TABLE shop.order_items (
    order_id text NOT NULL REFERENCES shop.orders (order_id),
    order_item_id integer NOT NULL,
    product_id text NOT NULL,
    seller_id text NOT NULL,
    shipping_limit_date timestamp,
    price numeric(10,2) NOT NULL,
    freight_value numeric(10,2) NOT NULL,
    PRIMARY KEY (order_id, order_item_id)
);
CREATE TABLE shop.order_payments (
    order_id text NOT NULL REFERENCES shop.orders (order_id),
    payment_sequential integer NOT NULL,
    payment_type text NOT NULL,
    payment_installments integer,
    payment_value numeric(10,2) NOT NULL,
    PRIMARY KEY (order_id, payment_sequential)
);
CREATE TABLE shop.order_reviews (
    review_id text NOT NULL,
    order_id text NOT NULL REFERENCES shop.orders (order_id),
    review_score integer,
    review_comment_title text,
    review_comment_message text,
    review_creation_date timestamp,
    review_answer_timestamp timestamp,
    PRIMARY KEY (review_id, order_id)
);
-- FK index policy (02 §7, containers/postgres/README.md): every FK column leads an
-- index. order_items.order_id and order_payments.order_id are covered by their PK;
-- these two are not.
CREATE INDEX orders_customer_id_idx ON shop.orders (customer_id);
CREATE INDEX order_reviews_order_id_idx ON shop.order_reviews (order_id);
"""

# Known data set: 3 customers (SP, SP, RJ), 4 orders, 4 items, 3 payments, 1 review.
#   delivered o1 (SP): items 58.90 + 21.10 (freight 13.29 + 5.00), approved/shipped/delivered, late
#   shipped   o2 (SP): item 100.00 (freight 10.00), approved/shipped
#   canceled  o3 (RJ): items deleted (none), approved
#   created   o4 (RJ): item 5.50 (freight 1.50), no milestones
OLIST_FIXTURE_DATA = """
INSERT INTO shop.customers VALUES
  ('c1', 'u1', '01037', 'sao paulo', 'SP'),
  ('c2', 'u2', '01038', 'sao paulo', 'SP'),
  ('c3', 'u1', '20000', 'rio de janeiro', 'RJ');
INSERT INTO shop.orders VALUES
  ('o1', 'c1', 'delivered', '2017-10-02 10:56:33', '2017-10-02 11:07:15',
   '2017-10-04 19:55:00', '2017-10-10 21:25:13', '2017-10-09 00:00:00'),
  ('o2', 'c2', 'shipped',   '2017-10-03 09:00:00', '2017-10-03 09:30:00',
   '2017-10-05 08:00:00', NULL, '2017-10-20 00:00:00'),
  ('o3', 'c3', 'canceled',  '2017-10-04 12:00:00', '2017-10-04 12:10:00',
   NULL, NULL, '2017-10-25 00:00:00'),
  ('o4', 'c3', 'created',   '2017-10-05 15:00:00', NULL, NULL, NULL, '2017-10-30 00:00:00');
INSERT INTO shop.order_items VALUES
  ('o1', 1, 'p1', 's1', '2017-10-06 11:07:15', 58.90, 13.29),
  ('o1', 2, 'p2', 's1', '2017-10-06 11:07:15', 21.10, 5.00),
  ('o2', 1, 'p3', 's2', '2017-10-07 09:30:00', 100.00, 10.00),
  ('o4', 1, 'p1', 's1', '2017-10-09 15:00:00', 5.50, 1.50);
INSERT INTO shop.order_payments VALUES
  ('o1', 1, 'credit_card', 3, 98.29),
  ('o2', 1, 'boleto', 1, 110.00),
  ('o4', 1, 'voucher', 1, 7.00);
INSERT INTO shop.order_reviews VALUES
  ('r1', 'o1', 5, NULL, 'muito bom', '2017-10-11 00:00:00', '2017-10-12 10:00:00');
"""

# what the views must return for OLIST_FIXTURE_DATA (exact; see run-tests skill)
OLIST_FIXTURE_EXPECTED = {
    "v_truth_row_counts": {
        "customers": 3,
        "orders": 4,
        "order_items": 4,
        "order_payments": 3,
        "order_reviews": 1,
    },
    "v_truth_orders": {
        # order_status: (orders, items, revenue, freight)
        "delivered": (1, 2, "80.00", "18.29"),
        "shipped": (1, 1, "100.00", "10.00"),
        "canceled": (1, 0, "0.00", "0.00"),
        "created": (1, 1, "5.50", "1.50"),
    },
    "v_truth_totals": (4, 3, 4, 3, 1, "185.50", "29.79", "215.29"),
    "v_truth_revenue_by_state": {
        # customer_state: (orders, items, revenue)
        "SP": (2, 3, "180.00"),
        "RJ": (2, 1, "5.50"),
    },
    "v_truth_payments": {
        "credit_card": (1, "98.29"),
        "boleto": (1, "110.00"),
        "voucher": (1, "7.00"),
    },
    "v_truth_order_lifecycle": (4, 3, 2, 1, 1),
}

TRUTH_VIEW_COLUMNS = {
    "v_truth_row_counts": ["table_name", "row_count"],
    "v_truth_orders": ["order_status", "orders", "items", "revenue", "freight"],
    "v_truth_totals": [
        "orders",
        "customers",
        "order_items",
        "order_payments",
        "order_reviews",
        "revenue",
        "freight",
        "payments",
    ],
    "v_truth_revenue_by_state": ["customer_state", "orders", "items", "revenue"],
    "v_truth_payments": ["payment_type", "payments", "value"],
    "v_truth_order_lifecycle": ["orders", "approved", "shipped", "delivered", "late"],
}


def apply_sql_file(conn: psycopg.Connection, path: Path) -> None:
    """Execute a whole SQL file as one batch (what the simulator does with truth-views.sql)."""
    conn.execute(path.read_text())


def table_exists(conn: psycopg.Connection, schema: str, table: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM pg_tables WHERE schemaname = %s AND tablename = %s", (schema, table)
        ).fetchone()
    )


def create_olist_fixture_tables(app: psycopg.Connection) -> None:
    """Minimal Olist tables + known data, as shop_app. Fails if any already exists."""
    for t in OLIST_FIXTURE_TABLES:
        if table_exists(app, "shop", t):
            raise RuntimeError(f"shop.{t} already exists; refusing to overwrite simulator data")
    app.execute(OLIST_FIXTURE_DDL)
    app.execute(OLIST_FIXTURE_DATA)


def drop_olist_fixture_tables(app: psycopg.Connection) -> None:
    """Drop the fixture tables and every view that depends on them (CASCADE)."""
    for t in reversed(OLIST_FIXTURE_TABLES):
        app.execute(f"DROP TABLE IF EXISTS shop.{t} CASCADE")
    app.execute("DROP FUNCTION IF EXISTS sim.truth_row_counts() CASCADE")


def view_columns(conn: psycopg.Connection, schema: str, view: str) -> list[str]:
    return [
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
            (schema, view),
        ).fetchall()
    ]


# --- initdb state -------------------------------------------------------------
CATALOG_SNAPSHOT_SQL = """
SELECT 'role ' || rolname || ' login=' || rolcanlogin || ' repl=' || rolreplication
       || ' super=' || rolsuper || ' createdb=' || rolcreatedb || ' createrole=' || rolcreaterole
FROM pg_roles WHERE rolname NOT LIKE 'pg\\_%'
UNION ALL
SELECT 'database ' || datname || ' acl=' || coalesce(datacl::text, '-')
FROM pg_database WHERE datname = current_database()
UNION ALL
SELECT 'schema ' || nspname || ' owner=' || pg_get_userbyid(nspowner)
       || ' acl=' || coalesce(nspacl::text, '-')
FROM pg_namespace WHERE nspname IN ('shop', 'sim', 'public')
UNION ALL
SELECT 'defacl role=' || pg_get_userbyid(d.defaclrole) || ' schema=' || coalesce(n.nspname, '-')
       || ' type=' || d.defaclobjtype::text || ' acl=' || d.defaclacl::text
FROM pg_default_acl d LEFT JOIN pg_namespace n ON n.oid = d.defaclnamespace
UNION ALL
SELECT 'publication ' || pubname || ' alltables=' || puballtables || ' ops='
       || pubinsert || pubupdate || pubdelete || pubtruncate
FROM pg_publication
UNION ALL
SELECT 'pubschema ' || p.pubname || ' ' || n.nspname
FROM pg_publication_namespace pn
JOIN pg_publication p ON p.oid = pn.pnpubid
JOIN pg_namespace n ON n.oid = pn.pnnspid
UNION ALL
SELECT 'eventtrigger ' || e.evtname || ' event=' || e.evtevent || ' enabled=' || e.evtenabled::text
       || ' tags=' || coalesce(array_to_string(e.evttags, ','), '-')
       || ' fn=' || e.evtfoid::regproc || ' src=' || md5(p.prosrc)
FROM pg_event_trigger e JOIN pg_proc p ON p.oid = e.evtfoid
UNION ALL
SELECT 'setting ' || name || '=' || setting
FROM pg_settings
WHERE name IN ('wal_level', 'max_replication_slots', 'max_wal_senders',
               'max_slot_wal_keep_size', 'wal_compression', 'shared_buffers', 'max_wal_size')
ORDER BY 1
"""


def catalog_snapshot(conn: psycopg.Connection) -> list[str]:
    return [r[0] for r in conn.execute(CATALOG_SNAPSHOT_SQL).fetchall()]


@dataclass
class InitdbProbe:
    """A throw-away `postgres` container with the stack's image, initdb/ and config.

    No published ports (the stack owns 5432); queries go through `docker exec psql`
    over the unix socket, which the image trusts for the local superuser.
    """

    root: Path
    env: dict[str, str]
    name: str = field(default_factory=lambda: f"wpdb-initdb-probe-{uuid.uuid4().hex[:8]}")

    @property
    def image(self) -> str:
        compose = yaml.safe_load((self.root / "docker-compose.yml").read_text())
        return compose["services"]["postgres"]["image"]

    def _docker(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["docker", *args], cwd=self.root, check=check, text=True, capture_output=True
        )

    def start(self, timeout: float = 120) -> None:
        initdb = self.root / "containers" / "postgres" / "initdb"
        conf = self.root / "containers" / "postgres" / "postgresql.conf"
        env_args: list[str] = []
        for key in ("SHOP_APP_PASSWORD", "DEBEZIUM_DB_PASSWORD", "READER_PASSWORD"):
            env_args += ["-e", f"{key}={self.env[key]}"]
        self._docker(
            "run",
            "-d",
            "--rm",
            "--name",
            self.name,
            "-e",
            "POSTGRES_DB=shop",
            "-e",
            "POSTGRES_USER=postgres",
            "-e",
            f"POSTGRES_PASSWORD={self.env['POSTGRES_PASSWORD']}",
            *env_args,
            "-v",
            f"{initdb}:/docker-entrypoint-initdb.d:ro",
            "-v",
            f"{conf}:/etc/postgresql/postgresql.conf:ro",
            self.image,
            "postgres",
            "-c",
            "config_file=/etc/postgresql/postgresql.conf",
        )
        # The entrypoint's temporary server (while initdb scripts run) listens on
        # the unix socket only, so a TCP pg_isready succeeds only after the final start.
        wait_until(
            lambda: (
                self._docker(
                    "exec",
                    self.name,
                    "pg_isready",
                    "-h",
                    "127.0.0.1",
                    "-U",
                    "postgres",
                    "-d",
                    "shop",
                ).returncode
                == 0
            ),
            timeout=timeout,
            every=2,
            desc=f"{self.name} initdb finished",
        )

    def psql(self, sql: str, user: str = "postgres") -> list[list[str]]:
        """Run `sql` (one batch, may hold many statements) as `user` over the unix
        socket (trusted locally by the image); rows come back as lists of strings."""
        out = self._docker(
            "exec",
            self.name,
            "psql",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            user,
            "-d",
            "shop",
            "-At",
            "-F",
            "\t",
            "-c",
            sql,
        ).stdout
        return [line.split("\t") for line in out.splitlines() if line]

    def catalog_snapshot(self) -> list[str]:
        return [row[0] for row in self.psql(CATALOG_SNAPSHOT_SQL)]

    def logs(self) -> str:
        return self._docker("logs", self.name, check=False).stderr

    def stop(self) -> None:
        self._docker("rm", "-f", self.name, check=False)

    def __enter__(self) -> InitdbProbe:
        try:
            self.start()
        except Exception:
            self.stop()
            raise
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop()
