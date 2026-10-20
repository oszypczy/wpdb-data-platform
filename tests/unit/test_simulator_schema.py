"""Business simulator, schema inference (01 §3, §8 unit rows): type, PK and FK detection,
BOM handling, DDL and the FK index policy (02 §7). No Docker."""

from __future__ import annotations

from pathlib import Path

import pytest

from simulator import events as ev
from simulator.csvio import CsvError, CsvTable, load_csv, load_dir, table_name
from simulator.schema import (
    create_index_sql,
    create_table_sql,
    creation_order,
    detect_foreign_keys,
    detect_primary_key,
    detect_type,
    infer_schemas,
)
from tests.helpers import olist_mini

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data" / "olist"


# --- types -----------------------------------------------------------------------
def test_zip_code_with_leading_zero_is_text():
    """01 §8: zip `01037` → TEXT (a digit string with a leading zero is not a number)."""
    assert detect_type(["14409", "01037", "09790"]).sql_type == "TEXT"
    assert detect_type(["01037"]).sql_type == "TEXT"
    assert detect_type(["14409", "9790"]).sql_type == "INTEGER"


def test_integer_column_with_empty_values_is_nullable_integer():
    """01 §8: column with empties → INTEGER NULL (product_photos_qty: 610 empties)."""
    assert detect_type(["1", "", "4", "2"]) == ("INTEGER", True)
    assert detect_type(["1", "4"]) == ("INTEGER", False)
    assert detect_type(["0", "-7"]) == ("INTEGER", False)


def test_amount_is_numeric_10_2():
    """01 §8: `58.90` → NUMERIC(10,2) (scale from the data, at least 8 integer digits)."""
    assert detect_type(["58.90", "6735.00", "13.29"]) == ("NUMERIC(10,2)", False)
    assert detect_type(["5", "5.5", ""]) == ("NUMERIC(9,1)", True)  # ints mixed with decimals
    assert detect_type(["123456789012.5"]) == ("NUMERIC(13,1)", False)


def test_timestamp_bigint_mixed_and_empty_columns():
    assert detect_type(["2017-10-02 10:56:33", "", "2018-07-24 20:41:37"]) == ("TIMESTAMP", True)
    assert detect_type(["3000000000", "1"]) == ("BIGINT", False)
    assert detect_type(["2017-10-02 10:56:33", "12"]) == ("TEXT", False)  # mixed kinds
    assert detect_type(["2017-10-02", "2017-10-03"]) == (
        "TEXT",
        False,
    )  # date-only is not TIMESTAMP
    assert detect_type(["", ""]) == ("TEXT", True)
    assert detect_type([]) == ("TEXT", True)


# --- CSV input --------------------------------------------------------------------
def test_bom_does_not_break_the_header(tmp_path: Path):
    """01 §8: product_category_name_translation.csv starts with a BOM; read as utf-8-sig."""
    path = tmp_path / "product_category_name_translation.csv"
    path.write_bytes(
        b"\xef\xbb\xbfproduct_category_name,product_category_name_english\n"
        b"beleza_saude,health_beauty\n"
    )
    table = load_csv(path)
    assert table.name == "product_category_name_translation"
    assert table.columns == ("product_category_name", "product_category_name_english")
    assert table.rows == [("beleza_saude", "health_beauty")]


def test_table_names_from_file_names():
    assert table_name("olist_order_items_dataset.csv") == "order_items"
    assert table_name(Path("/data/olist/olist_customers_dataset.csv")) == "customers"
    assert (
        table_name("product_category_name_translation.csv") == "product_category_name_translation"
    )


def test_multiline_quoted_fields_and_width_errors(tmp_path: Path):
    path = tmp_path / "olist_order_reviews_dataset.csv"
    path.write_text(
        '"review_id","order_id","review_comment_message"\n"r1","o1","line one\nline two"\n'
    )
    assert load_csv(path).rows == [("r1", "o1", "line one\nline two")]
    bad = tmp_path / "olist_bad_dataset.csv"
    bad.write_text("a,b\n1,2,3\n")
    with pytest.raises(CsvError, match="3 fields, header has 2"):
        load_csv(bad)
    with pytest.raises(CsvError, match="no CSV files"):
        load_dir(tmp_path / "empty")


# --- primary keys ------------------------------------------------------------------
def test_pk_orders_prefers_order_id_over_unique_customer_id():
    """01 §8: orders → order_id (not customer_id, although both are unique)."""
    tables = olist_mini.tables()
    assert detect_primary_key(tables["orders"]) == ("order_id",)
    assert detect_primary_key(tables["customers"]) == ("customer_id",)
    assert detect_primary_key(tables["sellers"]) == ("seller_id",)


def test_pk_reviews_is_pair_because_review_id_is_not_unique():
    """01 §8: order_reviews → (review_id, order_id)."""
    tables = olist_mini.tables()
    reviews = tables["order_reviews"]
    assert len({r[0] for r in reviews.rows}) < len(reviews.rows), "fixture must duplicate review_id"
    assert detect_primary_key(reviews) == ("review_id", "order_id")


def test_pk_items_and_payments_are_pairs():
    """01 §8: order_items → (order_id, order_item_id);
    order_payments → (order_id, payment_sequential)."""
    tables = olist_mini.tables()
    assert detect_primary_key(tables["order_items"]) == ("order_id", "order_item_id")
    assert detect_primary_key(tables["order_payments"]) == ("order_id", "payment_sequential")


def test_pk_without_id_column_falls_back_to_first_unique_column():
    tables = olist_mini.tables()
    assert detect_primary_key(tables["product_category_name_translation"]) == (
        "product_category_name",
    )
    nothing = CsvTable("x", ("a", "b"), [("1", "1"), ("1", "1")])
    with pytest.raises(Exception, match="no unique column"):
        detect_primary_key(nothing)


# --- foreign keys ------------------------------------------------------------------
def test_fk_accepts_orders_customer_id_and_rejects_products_translation():
    """01 §8: orders.customer_id → customers detected; products → translation rejected
    (2 categories without a translation)."""
    tables = olist_mini.tables()
    pks = {name: detect_primary_key(t) for name, t in tables.items()}
    accepted, rejected = detect_foreign_keys(tables, pks)
    as_pairs = {name: {(fk.column, fk.ref_table) for fk in fks} for name, fks in accepted.items()}
    assert as_pairs["orders"] == {("customer_id", "customers")}
    assert as_pairs["order_items"] == {
        ("order_id", "orders"),
        ("product_id", "products"),
        ("seller_id", "sellers"),
    }
    assert as_pairs["order_payments"] == {("order_id", "orders")}
    assert as_pairs["order_reviews"] == {("order_id", "orders")}
    assert as_pairs["products"] == set()
    assert [(r.table, r.column, r.ref_table, r.missing) for r in rejected] == [
        ("products", "product_category_name", "product_category_name_translation", 2)
    ]
    assert set(rejected[0].examples) == {
        "pc_gamer",
        "portateis_cozinha_e_preparadores_de_alimentos",
    }


# --- DDL ---------------------------------------------------------------------------
def test_ddl_has_detected_types_keys_and_fk_indexes():
    """02 §7: every FK column leads an index; the PK covers order_items/order_payments
    (order_id); extra indexes: order_reviews(order_id), orders(customer_id),
    order_items(product_id, seller_id)."""
    schemas, _ = infer_schemas(olist_mini.tables(), force_nullable={"orders": ev.FUTURE_COLUMNS})
    items = create_table_sql(schemas["order_items"])
    assert 'CREATE TABLE "shop"."order_items"' in items
    assert '"price" NUMERIC(10,2) NOT NULL' in items
    assert 'CONSTRAINT "order_items_pkey" PRIMARY KEY ("order_id", "order_item_id")' in items
    assert (
        'CONSTRAINT "order_items_order_id_fkey" FOREIGN KEY ("order_id") '
        'REFERENCES "shop"."orders" ("order_id")'
    ) in items
    assert '"customer_zip_code_prefix" TEXT NOT NULL' in create_table_sql(schemas["customers"])
    orders = create_table_sql(schemas["orders"])
    assert '"order_approved_at" TIMESTAMP,' in orders  # future column: nullable whatever the data
    assert '"order_purchase_timestamp" TIMESTAMP NOT NULL' in orders
    indexes = {name: create_index_sql(s) for name, s in schemas.items()}
    assert indexes["order_items"] == [
        'CREATE INDEX "order_items_product_id_idx" ON "shop"."order_items" ("product_id")',
        'CREATE INDEX "order_items_seller_id_idx" ON "shop"."order_items" ("seller_id")',
    ]
    assert indexes["order_reviews"] == [
        'CREATE INDEX "order_reviews_order_id_idx" ON "shop"."order_reviews" ("order_id")'
    ]
    assert indexes["orders"] == [
        'CREATE INDEX "orders_customer_id_idx" ON "shop"."orders" ("customer_id")'
    ]
    assert indexes["order_payments"] == [] and indexes["customers"] == []


def test_creation_order_puts_parents_first():
    schemas, _ = infer_schemas(olist_mini.tables())
    order = creation_order(schemas)
    assert set(order) == set(schemas)
    assert order.index("customers") < order.index("orders") < order.index("order_items")
    assert order.index("products") < order.index("order_items")
    assert order.index("sellers") < order.index("order_items")
    assert order.index("orders") < order.index("order_payments")
    assert order.index("orders") < order.index("order_reviews")


# --- the real data set (01 §3 table) ------------------------------------------------
@pytest.mark.slow
def test_real_data_gives_the_schema_of_the_design_doc():
    """01 §3: PKs, FKs and the pitfalls, measured on data/olist (skipped without `make data`)."""
    if not (DATA / "olist_orders_dataset.csv").exists():
        pytest.skip("data/olist missing: run `make data`")
    tables = load_dir(DATA)
    assert set(tables) == {*ev.CATALOG_TABLES, *ev.LIFECYCLE_TABLES}
    assert {n: len(t) for n, t in tables.items()} == {
        "customers": 99441,
        "orders": 99441,
        "order_items": 112650,
        "order_payments": 103886,
        "order_reviews": 99224,
        "products": 32951,
        "sellers": 3095,
        "product_category_name_translation": 71,
    }
    schemas, rejected = infer_schemas(tables, force_nullable={"orders": ev.FUTURE_COLUMNS})
    assert {n: s.primary_key for n, s in schemas.items()} == {
        "customers": ("customer_id",),
        "sellers": ("seller_id",),
        "products": ("product_id",),
        "orders": ("order_id",),
        "order_items": ("order_id", "order_item_id"),
        "order_payments": ("order_id", "payment_sequential"),
        "order_reviews": ("review_id", "order_id"),
        "product_category_name_translation": ("product_category_name",),
    }
    fks = {(n, fk.column, fk.ref_table) for n, s in schemas.items() for fk in s.foreign_keys}
    assert fks == {
        ("orders", "customer_id", "customers"),
        ("order_items", "order_id", "orders"),
        ("order_items", "product_id", "products"),
        ("order_items", "seller_id", "sellers"),
        ("order_payments", "order_id", "orders"),
        ("order_reviews", "order_id", "orders"),
    }
    assert [(r.table, r.ref_table, r.missing) for r in rejected] == [
        ("products", "product_category_name_translation", 2)
    ]
    col = lambda t, c: schemas[t].column(c)  # noqa: E731
    assert col("customers", "customer_zip_code_prefix").sql_type == "TEXT"
    assert col("sellers", "seller_zip_code_prefix").sql_type == "TEXT"
    assert col("products", "product_photos_qty") == ("product_photos_qty", "INTEGER", True)
    assert col("products", "product_weight_g") == ("product_weight_g", "INTEGER", True)
    assert col("products", "product_name_lenght").sql_type == "INTEGER"  # header typo kept 1:1
    assert col("order_items", "price") == ("price", "NUMERIC(10,2)", False)
    assert col("order_items", "freight_value").sql_type == "NUMERIC(10,2)"
    assert col("order_payments", "payment_value").sql_type == "NUMERIC(10,2)"
    assert col("orders", "order_purchase_timestamp") == (
        "order_purchase_timestamp",
        "TIMESTAMP",
        False,
    )
    assert col("orders", "order_approved_at") == ("order_approved_at", "TIMESTAMP", True)
    assert col("order_reviews", "review_creation_date").sql_type == "TIMESTAMP"
    assert {n: s.index_columns for n, s in schemas.items() if s.index_columns} == {
        "orders": ["customer_id"],
        "order_items": ["product_id", "seller_id"],
        "order_reviews": ["order_id"],
    }
