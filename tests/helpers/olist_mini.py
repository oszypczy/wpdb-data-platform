"""A miniature Olist data set with the real CSV headers, for the simulator's unit tests.

It reproduces the detection pitfalls of 01 §3 on a few rows: zip codes with a leading
zero, empty numeric values, duplicated review_id, two product categories without a
translation, orders whose customer_id is as unique as order_id.
"""

from __future__ import annotations

from simulator.csvio import CsvTable

CUSTOMERS = (
    "customer_id",
    "customer_unique_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state",
)
ORDERS = (
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
)
ITEMS = (
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    "shipping_limit_date",
    "price",
    "freight_value",
)
PAYMENTS = (
    "order_id",
    "payment_sequential",
    "payment_type",
    "payment_installments",
    "payment_value",
)
REVIEWS = (
    "review_id",
    "order_id",
    "review_score",
    "review_comment_title",
    "review_comment_message",
    "review_creation_date",
    "review_answer_timestamp",
)
PRODUCTS = (
    "product_id",
    "product_category_name",
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
)
SELLERS = ("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state")
TRANSLATION = ("product_category_name", "product_category_name_english")


def tables() -> dict[str, CsvTable]:
    return {
        "customers": CsvTable(
            "customers",
            CUSTOMERS,
            [
                ("c1", "u1", "01037", "sao paulo", "SP"),
                ("c2", "u2", "14409", "franca", "SP"),
                ("c3", "u1", "20000", "rio de janeiro", "RJ"),
                ("c4", "u4", "30000", "belo horizonte", "MG"),
            ],
        ),
        "orders": CsvTable(
            "orders",
            ORDERS,
            [
                # delivered, full chain, shipped before approval (anomaly)
                (
                    "o1",
                    "c1",
                    "delivered",
                    "2017-10-02 10:56:33",
                    "2017-10-02 11:07:15",
                    "2017-10-02 09:00:00",
                    "2017-10-10 21:25:13",
                    "2017-10-18 00:00:00",
                ),
                # shipped: no delivery date
                (
                    "o2",
                    "c2",
                    "shipped",
                    "2017-10-03 09:00:00",
                    "2017-10-03 09:30:00",
                    "2017-10-05 08:00:00",
                    "",
                    "2017-10-20 00:00:00",
                ),
                # canceled after approval, has items
                (
                    "o3",
                    "c3",
                    "canceled",
                    "2017-10-04 12:00:00",
                    "2017-10-04 12:10:00",
                    "",
                    "",
                    "2017-10-25 00:00:00",
                ),
                # created: no dates at all
                ("o4", "c4", "created", "2017-10-05 15:00:00", "", "", "", "2017-10-30 00:00:00"),
            ],
        ),  # fmt: skip
        "order_items": CsvTable(
            "order_items",
            ITEMS,
            [
                ("o1", "1", "p1", "s1", "2017-10-06 11:07:15", "58.90", "13.29"),
                ("o1", "2", "p2", "s1", "2017-10-06 11:07:15", "21.10", "5.00"),
                ("o2", "1", "p3", "s2", "2017-10-07 09:30:00", "100.00", "10.00"),
                ("o3", "1", "p1", "s1", "2017-10-07 09:30:00", "6735.00", "5.00"),
                ("o4", "1", "p1", "s1", "2017-10-06 11:07:15", "58.90", "13.29"),
            ],
        ),
        "order_payments": CsvTable(
            "order_payments",
            PAYMENTS,
            [
                ("o1", "1", "credit_card", "3", "98.29"),
                ("o1", "2", "voucher", "1", "10.00"),
                ("o2", "1", "boleto", "1", "110.00"),
                ("o3", "1", "voucher", "1", "7.00"),
                ("o4", "1", "voucher", "1", "7.00"),
            ],
        ),
        "order_reviews": CsvTable(
            "order_reviews",
            REVIEWS,
            [
                # r1 is used for two orders (review_id is not unique in Olist) and o1 has two
                # reviews (order_id is not unique either): only the pair identifies a row
                ("r1", "o1", "5", "", "muito bom", "2017-10-11 00:00:00", "2017-10-12 10:00:00"),
                ("r1", "o2", "5", "", "", "2017-10-06 00:00:00", "2017-10-07 10:00:00"),
                # review dated before the purchase (anomaly)
                (
                    "r2",
                    "o3",
                    "1",
                    "ruim",
                    "cancelado",
                    "2017-10-01 00:00:00",
                    "2017-10-05 10:00:00",
                ),
                ("r3", "o1", "1", "", "", "2017-10-11 00:00:00", "2017-10-12 10:00:00"),
            ],
        ),
        "products": CsvTable(
            "products",
            PRODUCTS,
            [
                ("p1", "perfumaria", "40", "287", "1", "225", "16", "10", "14"),
                ("p2", "pc_gamer", "44", "276", "", "1000", "30", "18", "20"),
                ("p3", "portateis_cozinha_e_preparadores_de_alimentos", "", "", "", "", "", "", ""),
                ("p4", "", "10", "20", "2", "", "1", "1", "1"),
            ],
        ),
        "sellers": CsvTable(
            "sellers",
            SELLERS,
            [("s1", "13023", "campinas", "SP"), ("s2", "01037", "sao paulo", "SP")],
        ),
        "product_category_name_translation": CsvTable(
            "product_category_name_translation",
            TRANSLATION,
            [("perfumaria", "perfumery"), ("beleza_saude", "health_beauty")],
        ),
    }


def synthetic_purchase(
    product_id: str, seller_id: str, tag: str, *, duplicate_payment: bool = False
):
    """A purchase event (plus its Renderer and ids) for an order that is not in the data set.

    Used by the interrupted-purchase tests against the live database: the ids are
    `test-half-<tag>-*`, the item references real catalog rows (FKs), and with
    `duplicate_payment` the last INSERT of the purchase violates the payments PK.
    Returns (event, renderer, order_id, customer_id).
    """
    from simulator import events as ev
    from simulator.render import Renderer
    from simulator.schema import infer_schemas

    data = tables()
    oid, cid = f"test-half-{tag}-order", f"test-half-{tag}-customer"
    data["customers"] = CsvTable("customers", CUSTOMERS, [(cid, "u", "01037", "x", "SP")])
    data["orders"] = CsvTable(
        "orders",
        ORDERS,
        [
            (oid, cid, "delivered", "2017-10-02 10:56:33", "2017-10-02 11:07:15",
             "2017-10-04 19:55:00", "2017-10-10 21:25:13", "2017-10-18 00:00:00")
        ],
    )  # fmt: skip
    data["order_items"] = CsvTable(
        "order_items",
        ITEMS,
        [(oid, "1", product_id, seller_id, "2017-10-06 11:07:15", "58.90", "13.29")],
    )
    payments = [(oid, "1", "credit_card", "3", "98.29")]
    if duplicate_payment:
        payments.append((oid, "1", "voucher", "1", "1.00"))  # same PK → the last INSERT fails
    data["order_payments"] = CsvTable("order_payments", PAYMENTS, payments)
    # order_reviews keeps the mini rows (none for `oid`): the schema needs rows to find its PK
    ds = ev.Dataset(data)
    schemas, _ = infer_schemas(data, force_nullable={"orders": ev.FUTURE_COLUMNS})
    purchase = ev.order_events(ds.build_orders()[0], ds)[0]
    return purchase, Renderer(schemas, ds), oid, cid
