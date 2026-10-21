# PostgreSQL: baza operacyjna sklepu

PostgreSQL to **baza operacyjna sklepu i jedyne źródło prawdy**. Business System ([01-business-system.md](01-business-system.md)) do niej pisze, a Debezium czyta jej dziennik zmian (WAL) przez replikację logiczną (plugin `pgoutput`). Wszystko dalej w potoku jest pochodną tego, co trafi do WAL, więc konfiguracja Postgresa decyduje o tym, jak bogate są zdarzenia na końcu.

## 1. Część podstawowa

```yaml
postgres:
  image: postgres:16
  command: postgres -c wal_level=logical -c max_replication_slots=4 -c max_wal_senders=4
  volumes: [pg_data:/var/lib/postgresql/data]
  healthcheck: pg_isready
```

- **`wal_level=logical`:** WAL zawiera dość informacji, żeby odtworzyć zmiany wierszy.
- **Slot replikacyjny:** pamięta, do którego miejsca w WAL konektor już doczytał.
- **Wolumen** przechowuje dane między restartami, a **healthcheck** pozwala innym usługom czekać na `service_healthy`.

Weryfikacja: `SHOW wal_level;` → `logical`.

## 2. Rozszerzenie: `REPLICA IDENTITY FULL`

Domyślnie przy UPDATE i DELETE WAL zawiera tylko PK starego wiersza. Z `REPLICA IDENTITY FULL` zdarzenie niesie cały stary wiersz.

Przykład: anulacja zamówienia i usunięcie pozycji (`op=d`).

| | `before` w zdarzeniu |
|---|---|
| domyślnie | `{"order_id": "e481f5…", "order_item_id": 1, "product_id": null, "price": null, …}` |
| FULL | `{"order_id": "e481f5…", "order_item_id": 1, "product_id": "4244733e…", "price": 58.90, "freight_value": 13.29}` |

Co to daje:
- **Przy DELETE** warstwa gold może od razu odjąć przychód właściwej kategorii i sprzedawcy.
- **Przy UPDATE** zamówienia widać przejście `before.status → after.status` (np. `approved → shipped`). Z tego Spark liczy lejek statusów i czasy etapów bez sięgania do historii.

**Automatyzacja dla dynamicznych tabel:** event trigger `shop_replica_identity_full` (`ON ddl_command_end`, tagi `CREATE TABLE`, `CREATE TABLE AS`, `SELECT INTO`; `containers/postgres/initdb/10-event-trigger.sql`) wykonuje `ALTER TABLE … REPLICA IDENTITY FULL` dla każdej nowej tabeli w schemacie `shop`. Funkcja działa z uprawnieniami roli, która wydała DDL (`shop_app` jest właścicielem tabeli), więc nie jest `SECURITY DEFINER`; tabele w `sim` zachowują identyfikację domyślną, bo nie są publikowane. Test: `test_postgres.py::test_new_table_in_shop_gets_replica_identity_full`.

**Kompromis:** większy WAL. Przy ruchu rzędu dziesiątek zdarzeń na sekundę jest to pomijalne.

## 3. Rozszerzenie: publikacja dla całego schematu

```sql
CREATE PUBLICATION dbz_publication FOR TABLES IN SCHEMA shop;   -- PostgreSQL 15+
```

Publikację tworzy skrypt startowy, a Debezium tylko z niej korzysta (`publication.autocreate.mode=disabled`). Każda nowa tabela w `shop` automatycznie trafia do CDC. Zgadza się to z dynamicznym tworzeniem tabel i z demo ewolucji schematu (`ALTER TABLE ADD COLUMN`).

**Wyjątki (etap 2):** do publikacji należy dodatkowo pojedyncza tabela `sim.heartbeat` (§5; sam schemat `sim` nie), a tabele `shop._*` (sondy testowe tworzone i usuwane w milisekundach) są w publikacji, ale konektor ich nie przechwytuje (`table.exclude.list`, [03-debezium.md](03-debezium.md) §13: tabela usunięta przed zapisem offsetu konektora zostawia zdarzenia bez klucza, które kompaktowany temat odrzuca).

## 4. Rozszerzenie: schematy i role z minimalnymi uprawnieniami

| Schemat | Zawartość | W CDC |
|---|---|---|
| `shop` | tabele biznesowe (customers, orders, order_items, …) | tak |
| `sim` | `sim_state` (zegar symulacji), widoki uzgadniania, tabela heartbeat | nie; wyjątek: tabela `sim.heartbeat` jest w publikacji, ale konektor ją odfiltrowuje (§5) |

| Rola | Uprawnienia |
|---|---|
| `shop_app` (symulator) | DDL i DML w `shop`, zapis w `sim` |
| `debezium` | `REPLICATION` + `SELECT` na `shop`; `USAGE` na `sim` i `SELECT, INSERT, UPDATE` tylko na `sim.heartbeat` (§5); `sim.sim_state` i widoki niewidoczne |
| `reader` | tylko `SELECT` na `shop` i `sim`: odczyt uzgadniający w testach i podgląd dla agentów (MCP `postgres`, [09-agents.md](09-agents.md) §3) |
| `postgres` | tylko skrypty startowe |

## 5. Rozszerzenie: bezpieczeństwo slotu replikacyjnego

Slot zatrzymuje WAL, dopóki Debezium go nie odczyta. Jeśli Connect leży, a symulator pisze, WAL rośnie bez ograniczeń i może zapchać dysk Dockera.

- **`max_slot_wal_keep_size=2GB`:** po przekroczeniu limitu slot zostaje unieważniony zamiast zapychać dysk. Debezium musi wtedy wykonać ponowny snapshot, co trzeba uwzględnić w konfiguracji `snapshot.mode`.
- **Heartbeat (rozstrzygnięte, etap 2):** Debezium co 10 s wykonuje upsert jednego wiersza `sim.heartbeat (id = 1, ts)` (`heartbeat.action.query`), żeby slot przesuwał się także w okresach ciszy. **Tabela musi być w publikacji** (dokumentacja Debezium 3.6, „WAL disk space consumption”): konektor dostaje zmianę, odfiltrowuje ją (`schema.include.list=shop`), a odfiltrowane zdarzenie wymusza zapis offsetu, co przesuwa `confirmed_flush_lsn`. Realizacja w `containers/postgres/initdb/20-heartbeat.sql`: tabela (właściciel `postgres`, nie `shop_app`: właściciel tabeli może dodać trigger, a trigger odpalony przez upsert konektora wykonałby się z uprawnieniami roli `debezium` z `REPLICATION`; nikt poza `postgres` nie ma `TRIGGER`; `CHECK (id = 1)`), `ALTER PUBLICATION dbz_publication ADD TABLE sim.heartbeat`, `GRANT USAGE ON SCHEMA sim TO debezium`, `GRANT SELECT, INSERT, UPDATE ON sim.heartbeat TO debezium`, `GRANT SELECT … TO reader` (tabelę tworzy `postgres`, więc domyślne uprawnienia `shop_app` jej nie obejmują). Testy: `test_postgres.py::test_heartbeat_table_grants_and_publication`, `test_debezium.py::test_heartbeat_advances_the_slot_while_shop_is_idle`.
- **Unieważnienie slotu:** przy `snapshot.mode=initial` konektor nie robi sam ponownego snapshotu; procedura ręczna w [03-debezium.md](03-debezium.md) §13. Przy domyślnym oknie (≈ 40 MB WAL) limit 2 GB jest nieosiągalny.
- **Monitoring opóźnienia** (kandydat na wskaźnik w dashboardzie):
  ```sql
  SELECT slot_name, active,
         pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag
  FROM pg_replication_slots;
  ```

## 6. Rozszerzenie: transakcja = jeden zakup

Symulator zapisuje cały zakup w jednej transakcji, razem z zegarem symulacji:

```sql
BEGIN;
  INSERT INTO shop.customers ... ON CONFLICT DO NOTHING;
  INSERT INTO shop.orders ...;
  INSERT INTO shop.order_items ...;
  INSERT INTO shop.order_payments ...;
  UPDATE sim.sim_state SET sim_clock = '2017-10-02 10:56:33';
COMMIT;
```

- **Brak połowicznych zamówień** w WAL i w Kafce.
- **Atomowe wznowienie po restarcie** symulatora: zegar zapisuje się razem z danymi.
- **Metadane transakcji:** Debezium może je emitować (`provide.transaction.metadata`), co pozwala Sparkowi grupować zdarzenia jednego zakupu.

## 7. Rozszerzenie: indeksy i ograniczenia z detekcji

- Indeks na każdej wykrytej kolumnie FK, np. `order_items(order_id)`. Przy anulacji `DELETE … WHERE order_id=…` i sprawdzanie FK nie skanują całej tabeli.
- **Reguła (sprawdzana testem):** kolumny każdego FK muszą być **wiodącymi** kolumnami jakiegoś indeksu tabeli podrzędnej. Indeks PK lub unikalny wystarcza, jeśli zaczyna się od tych kolumn. Dla FK z [01-business-system.md](01-business-system.md) §3:

  | FK | Pokrycie |
  |---|---|
  | `order_items.order_id` | PK `(order_id, order_item_id)`: bez dodatkowego indeksu |
  | `order_payments.order_id` | PK `(order_id, payment_sequential)`: bez dodatkowego indeksu |
  | `order_reviews.order_id` | PK `(review_id, order_id)` **nie** pokrywa (druga kolumna): potrzebny `order_reviews(order_id)` |
  | `orders.customer_id` | potrzebny `orders(customer_id)` (unikalny w danych, ale symulator nie musi tego zakładać) |
  | `order_items.product_id`, `order_items.seller_id` | potrzebne `order_items(product_id)`, `order_items(seller_id)` |

  Testy: `test_postgres.py::test_every_fk_column_has_a_leading_index` (reguła dla wszystkich FK w `shop`) i `::test_delete_order_items_by_order_id_uses_index` (plan `DELETE` z `EXPLAIN`, z `enable_seqscan=off`, bo na małej tabeli planer wybrałby skan sekwencyjny mimo indeksu).
- Opcjonalnie `CHECK` dla kolumn o małej liczbie wartości (np. `order_status`), wykrytych z danych.

## 8. Rozszerzenie: uzgadnianie danych (reconciliation)

Widoki z „prawdą” biznesową w schemacie `sim`, np.:

```sql
CREATE VIEW sim.v_truth_orders AS
SELECT o.order_status, count(DISTINCT o.order_id) AS orders, sum(i.price) AS revenue
FROM shop.orders o LEFT JOIN shop.order_items i USING (order_id)
GROUP BY o.order_status;
```

Test e2e (§10) porównuje je z tabelami silver w Delta. Zgodność dowodzi, że CDC, Kafka i `MERGE` w Sparku nie zgubiły ani nie zdublowały żadnego wiersza.

**Realizacja (etap 1):** widoki nie mogą powstać w initdb, bo tabele `shop.*` tworzy dopiero symulator. Leżą w `containers/postgres/sql/truth-views.sql` (wyłącznie `CREATE OR REPLACE`, każda kolumna z jawnym typem, więc plik jest wykonywalny wielokrotnie), a **symulator wykonuje ten plik jako `shop_app` zaraz po utworzeniu tabel** (jedna paczka SQL, np. `conn.execute(path.read_text())`). `reader` widzi je dzięki domyślnym uprawnieniom w `sim`. Wymagane tabele: `shop.customers`, `shop.orders`, `shop.order_items`, `shop.order_payments`, `shop.order_reviews` z nazwami kolumn z nagłówków CSV.

| Widok | Kolumny | Treść |
|---|---|---|
| `sim.v_truth_row_counts` | `table_name text, row_count bigint` | dokładne `count(*)` każdej tabeli w `shop` (z katalogu, więc obejmuje tabele dodane później) |
| `sim.v_truth_orders` | `order_status text, orders, items bigint, revenue, freight numeric(14,2)` | per status; zamówienia bez pozycji liczone z 0 |
| `sim.v_truth_totals` | `orders, customers, order_items, order_payments, order_reviews bigint, revenue, freight, payments numeric(14,2)` | jeden wiersz KPI |
| `sim.v_truth_revenue_by_state` | `customer_state text, orders, items bigint, revenue numeric(14,2)` | złączenie z klientem (gold `dim_geography`) |
| `sim.v_truth_payments` | `payment_type text, payments bigint, value numeric(14,2)` | per typ płatności |
| `sim.v_truth_order_lifecycle` | `orders, approved, shipped, delivered, late bigint` | liczba zamówień z wypełnionym kamieniem milowym: dowód, że `UPDATE` doszły do silver/gold |

Testy: `test_postgres.py::test_truth_views_exact_on_known_data` wykonuje plik dwa razy jako `shop_app` w jednorazowym kontenerze (ten sam obraz i initdb, `tests/helpers/postgres.py::InitdbProbe`) na znanym zestawie danych (`OLIST_FIXTURE_DATA`) i sprawdza dokładne wartości każdego widoku; `::test_truth_views_apply_and_reconcile` sprawdza na działającym stosie, że symulator utworzył widoki o ustalonych kolumnach, `reader` je czyta, a `v_truth_row_counts` równa się bezpośrednim `count(*)` w jednym snapshocie `REPEATABLE READ`.

## 9. Rozszerzenie: konfiguracja jako kod i strojenie

- **Skrypty w `/docker-entrypoint-initdb.d/`:** role, schematy, publikacja i event trigger (`containers/postgres/initdb/`). Wykonują się przy pustym wolumenie, więc `docker compose down -v && docker compose up` zawsze daje identyczny stan. Widoki uzgadniania są poza initdb (§8). Test `test_postgres.py::test_initdb_twice_gives_identical_state` uruchamia initdb w dwóch jednorazowych kontenerach z tego samego obrazu i porównuje stan katalogu (role, schematy, domyślne uprawnienia, publikacja, event trigger, ustawienia) między nimi i z działającym stosem.
- **Strojenie pod laptopa:** `shared_buffers`, `max_wal_size`, `wal_compression=on`, z uzasadnieniem w komentarzach.

## 10. Testy

| Poziom | Test | Oczekiwany wynik |
|---|---|---|
| integracyjny | konfiguracja serwera | `wal_level=logical`, ustawiony `max_slot_wal_keep_size` |
| integracyjny | nowa tabela w `shop` | automatycznie `REPLICA IDENTITY FULL` (event trigger) i obecna w `dbz_publication` |
| integracyjny | uprawnienia roli `debezium` | odczyt i replikacja działają; INSERT/UPDATE/DDL w `shop` odrzucone; w `sim` tylko upsert `heartbeat` (`test_postgres.py::test_debezium_cannot_write`, `::test_sim_schema_privileges`, `::test_heartbeat_table_grants_and_publication`) |
| integracyjny | skrypty startowe na pustym wolumenie, dwa razy | identyczny stan bazy |
| integracyjny | DELETE z `order_items` po `order_id` | plan zapytania używa indeksu FK |
| e2e | uzgadnianie | widoki `sim.v_truth_*` zgodne z tabelami silver w Delta po zakończeniu przebiegu |

## 11. Podsumowanie

| | Podstawa | Z rozszerzeniami |
|---|---|---|
| UPDATE i DELETE w zdarzeniach | tylko PK | pełny wiersz przed i po |
| Nowe tabele w CDC | ręczna konfiguracja | automatycznie (publikacja schematu + event trigger) |
| Uprawnienia | jeden superuser | osobne role, Debezium tylko odczyt + replikacja |
| Awaria Debezium | WAL rośnie bez limitu | limit, heartbeat, monitoring opóźnienia |
| Spójność | pojedyncze INSERT-y | transakcja = zakup, atomowe wznowienie |
| Dowód poprawności | brak | widoki prawdy uzgadniane z warstwą silver |

## Do weryfikacji przy implementacji

- **Rozstrzygnięte (etap 1):** zgodność wersji. Przypięte `postgres:16.15` i `quay.io/debezium/connect:3.6.3.Final`; Debezium 3.6 wspiera PostgreSQL 14–18 ([debezium.io/releases/3.6](https://debezium.io/releases/3.6/), macierz w [08-operations.md](08-operations.md) §1). `CREATE PUBLICATION … FOR TABLES IN SCHEMA` istnieje od PG 15 i wymaga superużytkownika, dlatego publikację tworzy initdb (`02-publication.sql`), a `pg_publication_namespace` jest sprawdzana testem `test_postgres.py::test_publication_covers_schema_shop`.
- **Rozstrzygnięte (etap 1):** uprawnienia Debezium przy `publication.autocreate.mode=disabled`. Według dokumentacji konektora 3.6 (sekcja „Setting up permissions” i opis `publication.autocreate.mode`) w trybie `disabled` konektor nie tworzy ani nie zmienia publikacji, a jedynie sprawdza, że istnieje (brak → wyjątek i stop). Rola potrzebuje więc tylko `LOGIN`, `REPLICATION` (slot + `pgoutput`), `CONNECT` do bazy, `USAGE` na schemacie `shop` i `SELECT` na tabelach (snapshot `initial`). `CREATE` na bazie i własność tabel są wymagane wyłącznie dla trybów `all_tables`/`filtered`/`no_tables`, których nie używamy. Dokładnie taki zestaw daje `00-roles.sh` i `01-schemas.sql` (`ALTER DEFAULT PRIVILEGES … GRANT SELECT` obejmuje tabele tworzone później przez `shop_app`); testy `test_debezium_cannot_write` i `test_sim_schema_privileges`. Pełne połączenie `pgoutput` potwierdzi etap 2.
- **Rozstrzygnięte (etap 2):** `snapshot.mode=initial`; pełne połączenie `pgoutput` z domyślnym `pg_hba.conf` obrazu działa (slot `debezium_slot` aktywny, `test_debezium.py::test_connector_registered_by_connect_init_and_running`); po unieważnieniu slotu konektor zatrzymuje się i wymaga procedury ręcznej ([03-debezium.md](03-debezium.md) §13), limit 2 GB chroni dysk, nie jest osiągany w domyślnym oknie.
