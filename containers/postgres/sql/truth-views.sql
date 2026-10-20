-- Reconciliation ("truth") views in schema sim (docs/design/02-postgresql.md §8).
--
-- Contract:
--   * Applied by the simulator, as shop_app (owner of schema sim), right after
--     it has created the shop tables; e.g. `psql -f` or `conn.execute(path.read_text())`
--     in one statement batch. Not part of initdb: the tables do not exist yet then.
--   * Re-runnable: only CREATE OR REPLACE. Every output column has an explicit
--     type, so a re-run after a change of the inferred column types still matches
--     (CREATE OR REPLACE VIEW cannot change the type of an existing column).
--   * Readable by `reader` through the default privileges on sim
--     (01-schemas.sql); test code and the e2e reconciliation (03/05/07 "Testy")
--     compare these views with the Delta silver/gold layers.
--   * Requires tables shop.customers, shop.orders, shop.order_items,
--     shop.order_payments and shop.order_reviews with the Olist CSV column names
--     (01-business-system.md §1). The catalogue tables (products, sellers,
--     category translation) are only counted, by name, in v_truth_row_counts.
--
-- Views and columns (the interface for tests/e2e and the dashboard):
--   sim.v_truth_row_counts   (table_name text, row_count bigint)            one row per table in shop
--   sim.v_truth_orders       (order_status text, orders bigint, items bigint,
--                             revenue numeric(14,2), freight numeric(14,2))  per status; orders without items count with 0
--   sim.v_truth_totals       (orders, customers, order_items, order_payments, order_reviews bigint,
--                             revenue, freight, payments numeric(14,2))      single row
--   sim.v_truth_revenue_by_state (customer_state text, orders bigint, items bigint, revenue numeric(14,2))
--   sim.v_truth_payments     (payment_type text, payments bigint, value numeric(14,2))
--   sim.v_truth_order_lifecycle (orders, approved, shipped, delivered, late bigint) single row
-- Rows are not ordered; consumers sort by the key column.

-- Exact row counts of every table in shop, discovered from the catalogue so the
-- view needs no change when the simulator adds a table. count(*) on purpose:
-- pg_stat_user_tables.n_live_tup is an estimate and reconciliation needs equality.
CREATE OR REPLACE FUNCTION sim.truth_row_counts()
RETURNS TABLE (table_name text, row_count bigint)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    t record;
BEGIN
    FOR t IN
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'shop' AND c.relkind = 'r'
        ORDER BY c.relname
    LOOP
        table_name := t.relname;
        EXECUTE format('SELECT count(*) FROM shop.%I', t.relname) INTO row_count;
        RETURN NEXT;
    END LOOP;
END;
$$;

CREATE OR REPLACE VIEW sim.v_truth_row_counts AS
SELECT table_name, row_count
FROM sim.truth_row_counts();

-- Orders per status with revenue from their items (02 §8). LEFT JOIN: canceled
-- and unavailable orders usually have no items and must still be counted.
CREATE OR REPLACE VIEW sim.v_truth_orders AS
SELECT o.order_status::text                           AS order_status,
       count(DISTINCT o.order_id)::bigint             AS orders,
       count(i.order_id)::bigint                      AS items,
       coalesce(sum(i.price), 0)::numeric(14, 2)      AS revenue,
       coalesce(sum(i.freight_value), 0)::numeric(14, 2) AS freight
FROM shop.orders o
LEFT JOIN shop.order_items i USING (order_id)
GROUP BY o.order_status;

-- One-row KPI summary: compared with the dashboard's query layer (07 §7).
CREATE OR REPLACE VIEW sim.v_truth_totals AS
SELECT (SELECT count(*) FROM shop.orders)::bigint                                        AS orders,
       (SELECT count(*) FROM shop.customers)::bigint                                     AS customers,
       (SELECT count(*) FROM shop.order_items)::bigint                                   AS order_items,
       (SELECT count(*) FROM shop.order_payments)::bigint                                AS order_payments,
       (SELECT count(*) FROM shop.order_reviews)::bigint                                 AS order_reviews,
       (SELECT coalesce(sum(price), 0) FROM shop.order_items)::numeric(14, 2)            AS revenue,
       (SELECT coalesce(sum(freight_value), 0) FROM shop.order_items)::numeric(14, 2)    AS freight,
       (SELECT coalesce(sum(payment_value), 0) FROM shop.order_payments)::numeric(14, 2) AS payments;

-- Revenue per customer state: the gold dim_geography join (05 §5) checked
-- against the source.
CREATE OR REPLACE VIEW sim.v_truth_revenue_by_state AS
SELECT c.customer_state::text                         AS customer_state,
       count(DISTINCT o.order_id)::bigint             AS orders,
       count(i.order_id)::bigint                      AS items,
       coalesce(sum(i.price), 0)::numeric(14, 2)      AS revenue
FROM shop.orders o
JOIN shop.customers c USING (customer_id)
LEFT JOIN shop.order_items i USING (order_id)
GROUP BY c.customer_state;

-- Payments per type (01 §6: credit_card, boleto, voucher, debit_card, not_defined).
CREATE OR REPLACE VIEW sim.v_truth_payments AS
SELECT p.payment_type::text                           AS payment_type,
       count(*)::bigint                               AS payments,
       coalesce(sum(p.payment_value), 0)::numeric(14, 2) AS value
FROM shop.order_payments p
GROUP BY p.payment_type;

-- Lifecycle milestones: every UPDATE of the simulator fills one of these dates,
-- so this view proves that updates (not only inserts) reached silver/gold
-- (fact_orders_lifecycle, 05 §5). `late` = delivered after the estimate (01 §6).
CREATE OR REPLACE VIEW sim.v_truth_order_lifecycle AS
SELECT count(*)::bigint                                                     AS orders,
       count(order_approved_at)::bigint                                     AS approved,
       count(order_delivered_carrier_date)::bigint                          AS shipped,
       count(order_delivered_customer_date)::bigint                         AS delivered,
       count(*) FILTER (WHERE order_delivered_customer_date
                              > order_estimated_delivery_date)::bigint      AS late
FROM shop.orders;
