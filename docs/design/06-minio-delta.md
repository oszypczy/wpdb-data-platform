# MinIO + Delta Lake: jezioro danych

**MinIO** to magazyn obiektów zgodny z S3, czyli „dysk” jeziora danych. **Delta Lake** nadaje plikom zachowanie tabel: transakcje ACID, historię wersji i kontrolę schematu.

Podział odpowiedzialności: [05-spark.md](05-spark.md) opisuje, *co* jest liczone. Ten dokument opisuje, *gdzie i jak* jest przechowywane, jak tym zarządzać i jak to odczytać.

## 1. Część podstawowa

- **MinIO** w compose: własny obraz `wpdb/minio` zbudowany ze źródeł z przypiętego tagu (`containers/minio/Dockerfile`, patrz niżej „Dystrybucja MinIO”), wolumen `minio_data`, healthcheck `/minio/health/live`.
- **Dostęp Sparka przez S3A:** endpoint `http://minio:9000`, `fs.s3a.path.style.access=true`, `fs.s3a.connection.ssl.enabled=false`.
- **Zapis w formacie Delta.**
- **Dokumentacja odczytu danych** (§8): wymóg specyfikacji.

## 2. Rozszerzenie: kubełek na warstwę

```
bronze/        orders/  order_items/  ...  (jedna tabela na temat, także dla tabel dodanych w trakcie)
silver/        orders/  order_items/  ...  (CDF włączony)
gold/          dim_date/ dim_geography/ dim_customer/ dim_product/ dim_seller/
               fact_order_items/ fact_orders_lifecycle/ fact_payments/ fact_reviews/
               revenue_events/ status_transitions/
               agg_revenue_hourly/ agg_status_funnel/
ops/           stream_metrics/ e2e_latency/ cdc_heartbeat/ quarantine/ maintenance_log/
checkpoints/   kafka_to_bronze/ silver_<tabela>/ silver_to_gold/ gold_transitions/
               gold_agg_revenue/ gold_agg_funnel/
```
Etap 3: jedno zapytanie silver na tabelę (`silver_<tabela>`), bo zapytanie strumieniowe ma stały zbiór źródeł, a tabele mogą dochodzić w trakcie ([05-spark.md](05-spark.md) §12). Pełny katalog kolumn: §9.

Osobne kubełki zamiast prefiksów w jednym pozwalają na **uprawnienia per warstwa** (ta sama zasada co role w Postgresie):

| Konto (access key) | Uprawnienia |
|---|---|
| `spark` | odczyt i zapis we wszystkich kubełkach |
| `dashboard` | tylko odczyt `gold` i `ops` |
| `agent` | tylko odczyt wszystkich kubełków: serwer MCP `duckdb` dla agentów i odczyty w testach (DuckDB przez `httpfs` potrafi zapisywać do S3, więc nie dostaje kluczy `spark`) |
| `root` | tylko kontener startowy |

**Kontener startowy `minio-init`** (`mc`) tworzy kubełki, polityki i konta. Jest idempotentny (`mc mb --ignore-existing`).

## 3. Rozszerzenie: dziennik transakcji

Każda tabela ma katalog `_delta_log/` z kolejnymi commitami:
```
silver/orders/_delta_log/00000000000000000042.json
{"commitInfo": {"operation": "MERGE", "operationMetrics":
   {"numTargetRowsUpdated": "37", "numTargetRowsInserted": "12", "numTargetRowsDeleted": "0"}}}
{"add": {"path": "part-00001-….parquet", ...}}
{"remove": {"path": "part-00000-….parquet", ...}}
```

- **Spójny odczyt:** czytelnik (dashboard) widzi ostatnią zatwierdzoną wersję, nigdy połowę MERGE.
- **Audyt:** `DESCRIBE HISTORY` pokazuje każdą operację z metrykami.

## 4. Rozszerzenie: time travel i RESTORE

```sql
SELECT sum(price) FROM gold.fact_order_items VERSION AS OF 120;
SELECT * FROM gold.fact_orders_lifecycle TIMESTAMP AS OF '2026-10-19 14:00';
RESTORE TABLE gold.dim_customer TO VERSION AS OF 95;
```

- **Analiza:** porównanie stanu przed i po zdarzeniu (np. Black Friday).
- **Wycofanie złego wdrożenia:** `RESTORE` zamiast odbudowy z bronze.

## 5. Rozszerzenie: Change Data Feed

`delta.enableChangeDataFeed = true` na tabelach silver. Gold czyta tylko zmienione wiersze, z typem zmiany (`insert`, `update_preimage`, `update_postimage`, `delete`), zgodnie z [05-spark.md](05-spark.md) §5. W gold CDF jest wyłączony, bo nie ma dalszych odbiorców.

## 6. Rozszerzenie: schemat i ograniczenia

- **Wymuszanie schematu:** zapis z niezgodnym typem jest odrzucany. Nowa kolumna jest dozwolona tylko jawnie (`mergeSchema`).
- **Ograniczenia CHECK**, jako ostatnia linia obrony za kwarantanną z [05-spark.md](05-spark.md) §6:
  ```sql
  ALTER TABLE gold.fact_order_items ADD CONSTRAINT price_nonneg CHECK (price >= 0);
  ```
  Etap 3 (zakładane przy tworzeniu tabel przez `delta_io.ensure_table`): `fact_order_items`: `price_nonneg` (`price >= 0`), `freight_nonneg` (`freight_value >= 0`); `fact_orders_lifecycle`: `items_nonneg`; `fact_reviews`: `score_range` (`review_score BETWEEN 1 AND 5`, od 2026-10-27; `fact_payments` bez CHECK, bo jego reguły jakości mają poziom `warn`); `dim_customer`: `valid_range` (`valid_from < valid_to`); `agg_revenue_hourly`: `corrections_nonpos` (`corrections <= 0`). Silver przyjmuje nowe kolumny jawnie (`MERGE … withSchemaEvolution()`), gold i ops nie (schemat zamrożony w `lakehouse/tables.py`).

## 7. Rozszerzenie: utrzymanie tabel

Trigger co 10 s tworzy dużo drobnych plików.

- **OPTIMIZE + Z-ORDER** (np. `fact_order_items` po `customer_state, purchase_date`): łączy pliki i układa dane pod typowe filtry. Pomiar liczby plików i czasu zapytania przed i po trafia do dokumentacji.
- **VACUUM:** usuwa pliki starsze niż retencja. Retencja per warstwa (gold 7 dni, bronze dłużej) to świadomy kompromis między zasięgiem time travel a miejscem.
- **Uruchamianie:** okresowe zadanie wsadowe w kontenerze Sparka (np. co 30 min), bez nowego komponentu.
- **Antywzorzec:** reguły cyklu życia MinIO (automatyczne usuwanie obiektów) na tabelach Delta uszkodziłyby tabelę. Pliki Delta usuwa wyłącznie VACUUM.
- **Partycjonowanie świadomie minimalne:** przy setkach MB partycje tworzą głównie małe pliki (Delta rekomenduje je dopiero od około 1 GB). Zamiast tego Z-ORDER lub liquid clustering. Wyjątek: bronze partycjonowany po dacie ingestii.

**Implementacja i pomiar (etap 3).** `containers/spark/lakehouse/maintenance.py`, uruchamiane przez pętlę sterownika co `MAINTENANCE_INTERVAL_S` (1800 s) albo na żądanie (`docker compose exec spark touch /tmp/spark/run-maintenance`), w osobnym wątku obok strumieni (konflikty commitów są ponawiane po obu stronach). Gold i ops: OPTIMIZE (fakty z Z-ORDER `customer_state, purchase_date` / `customer_state, purchased_at`); bronze i silver: `delta.autoOptimize.autoCompact` przy zapisie; wszystkie warstwy: VACUUM z retencją 720 h (bronze) / 168 h (pozostałe, `RETENTION_HOURS` w `lakehouse/tables.py`). Każda operacja to wiersz w `ops/maintenance_log` (pliki i bajty przed/po, czas typowego zapytania przed/po dla faktów).

Pomiar na stosie (okno domyślne, ok. 1,5 h strumienia, 2026-10-23):

| Tabela | Operacja | Pliki przed → po | Bajty przed → po | Zapytanie przed → po |
|---|---|---|---|---|
| `ops/cdc_heartbeat` (dopisywanie co 10 s) | OPTIMIZE | 445 → 1 | 856 510 → 19 986 (43×) | – |
| `ops/stream_metrics` | OPTIMIZE | 3 → 1 | 119 987 → 110 367 | – |
| `gold/fact_order_items` (6 tys. wierszy, MERGE co 20 s) | OPTIMIZE ZORDER BY (customer_state, purchase_date) | 1 → 1 | 923 868 → 923 868 | 155 → 123 ms (`sum(price)` dla SP) |
| `gold/fact_orders_lifecycle` | OPTIMIZE ZORDER BY (customer_state, purchased_at) | 1 → 1 | 963 335 → 963 335 | 122 → 111 ms |
| `gold/fact_payments` (5,9 tys., 2026-10-27 po backfillu) | OPTIMIZE ZORDER BY (purchased_at, payment_type) | 4 → 1 | 301 715 → 290 932 | – |
| `gold/fact_reviews` (4,5 tys., 2026-10-27 po backfillu) | OPTIMIZE ZORDER BY (review_created_at, customer_state) | 4 → 1 | 435 537 → 417 110 | – |
| test `test_optimize_compacts_files_and_keeps_the_data` | OPTIMIZE ZORDER | 8 → 1 | suma kontrolna bez zmian | – |
| bronze/silver (autoCompact) | VACUUM 720 h / 168 h | 1 → 1 na tabelę | bez zmian (pliki usunięte przez autoCompact są młodsze niż retencja) | – |

Wniosek: przy tej skali tabele gold mieszczą się w jednym pliku, bo każdy MERGE przepisuje dotknięty (jedyny) plik, więc OPTIMIZE i Z-ORDER nie mają czego łączyć ani porządkować (pomiar zapytania mieści się w szumie); realny problem małych plików mają tabele tylko dopisywane (heartbeat, metryki: setki plików po kilka kB na godzinę), a bronze/silver rozwiązuje autoCompact. Cały przebieg utrzymania (30 tabel, 60 operacji) trwa ok. 50 s.

**Pułapki znalezione przy implementacji:**
- **`spark.hadoop.delta.enableFastS3AListFrom=true` niszczy tabele przy VACUUM** (Delta 3.3.3 + S3A + MinIO): VACUUM usunął pliki `_delta_log/*.json` tabeli (odtworzone na tabeli testowej: po VACUUM zostały tylko `_commits/` i `_last_vacuum_info`, odczyt kończył się `DELTA_NO_COMMITS_FOUND`). Ustawienie przyspiesza listowanie dziennika, ale jest zakazane (`tests/unit/test_spark_session.py::test_fast_s3a_list_from_stays_off`).
- **VACUUM listuje pliki w 10 000 zadaniach** (`spark.sql.sources.parallelPartitionDiscovery.parallelism`), co obok strumieni dało przebieg utrzymania > 15 min; ustawione 8.
- **`RETAIN` jest zaokrąglany do pełnych godzin**, więc krótkie retencje w testach to 0 h albo 1 h (test VACUUM pokazuje obie strony).

## 8. Rozszerzenie: checkpointy w MinIO

Checkpointy wszystkich zapytań strumieniowych są w kubełku `checkpoints`. Cały stan przetwarzania (dane i pozycje strumieni) jest w MinIO, więc **kontener Sparka jest bezstanowy**: po usunięciu i ponownym utworzeniu wznawia pracę od miejsca, w którym skończył. Koszt to wolniejsze checkpointy niż na dysku lokalnym. MinIO ma silną spójność, a poprawność sprawdza test e2e.

Etap 3: `checkpoints/<zapytanie>/` (`kafka_to_bronze`, `silver_<tabela>`, `silver_to_gold`, `gold_transitions`, `gold_agg_revenue`, `gold_agg_funnel`) plus dwa pliki projektu: `_wpdb_txn_app_id` (token `txnAppId` danego checkpointu, [05-spark.md](05-spark.md) §12) i `_wpdb_gold_versions/<paczka>.json` (wersje silver przypięte do paczki gold, ostatnie 20). Stan agregacji okienkowych (state store) też jest w checkpoincie. Koszt S3A: ok. 0,3 s na mikropaczkę poza samym przetwarzaniem (zapis `offsets/` i `commits/`), zmierzone w `ops/stream_metrics`. Restart kontenera wznawia z checkpointów bez duplikatów w bronze (`tests/integration/test_spark.py::test_restart_from_checkpoint_keeps_bronze_offsets_unique`); usunięcie i odtworzenie kontenera z uzgadnianiem: e2e w etapie 5.

## 9. Odczyt danych (wymagana dokumentacja) i odczyt bez Sparka

Wszystkie tabele są w formacie Delta w MinIO: `s3://<warstwa>/<tabela>` (Spark: `s3a://…`). Endpoint z hosta: `http://127.0.0.1:9000` (path-style, bez TLS, region `us-east-1`), z kontenera: `http://minio:9000`. Klucze kont są w `.env` (`MINIO_*`): do przeglądania wszystkiego służy konto tylko do odczytu `agent`, do warstw publicznych `dashboard` (tylko `gold` i `ops`); `spark` (zapis) zostaje w kontenerze Sparka.

**Katalog tabel (etap 3).** Schematy gold i ops są kontraktem dla dashboardu: `containers/spark/lakehouse/tables.py` (nazwy, kolumny, właściwości, CHECK), opis w [05-spark.md](05-spark.md) §12.

| Kubełek / tabela | Co zawiera | Klucz | Uwagi |
|---|---|---|---|
| `bronze/<tabela>` (8 tabel `shop` + tabele dodane w trakcie) | każde zdarzenie Debezium: `op`, `before`, `after` (struktury), `source_lsn`, `source_tx_id`, `source_ts`, `source_snapshot`, `transaction_id`, `dbz_ts`, `key`, `kafka_topic/partition/offset/timestamp`, `key_schema_id`, `value_schema_id`, `ingested_at`, `ingest_date` | unikalne `(kafka_topic, kafka_partition, kafka_offset)` | tylko dopisywanie (`delta.appendOnly`), partycje `ingest_date`, `wpdb.primaryKey` = PK ze schematu klucza |
| `silver/<tabela>` | aktualny stan tabeli źródłowej: kolumny Postgresa + `_lsn`, `_op`, `_is_deleted`, `_source_ts`, `_tx_id`, `_kafka_partition`, `_kafka_offset`, `_ingested_at`, `_updated_at` | PK z `wpdb.primaryKey` | CDF włączony; odbiorcy filtrują `NOT _is_deleted` |
| `gold/dim_date`, `dim_geography`, `dim_product`, `dim_seller` | wymiary (kalendarz 2016–2019, 27 stanów, produkty z kategorią EN, sprzedawcy) | `date_key` / `state` / `product_id` / `seller_id` | |
| `gold/dim_customer` | SCD2 po `customer_unique_id`: `customer_sk`, adres, `version`, `valid_from`, `valid_to` (9999-12-31 = bieżąca), `is_current` | `customer_sk` | CHECK `valid_from < valid_to` |
| `gold/fact_order_items` | ziarno: pozycja zamówienia; `price`, `freight_value`, status, stan klienta i sprzedawcy, `customer_sk`, `purchased_at`, `purchase_date(_key)` | `(order_id, order_item_id)` | CHECK `price >= 0`, `freight_value >= 0`; Z-ORDER `customer_state, purchase_date` |
| `gold/fact_orders_lifecycle` | akumulujący snapshot: kamienie milowe, `h_to_approve`, `d_to_ship`, `d_to_deliver`, `is_delivered`, `is_late`, pozycje, płatności | `order_id` | CHECK `items >= 0` |
| `gold/fact_payments` | ziarno: płatność; `payment_type`, `payment_installments`, `payment_value`, stan klienta, status, `purchased_at`, `purchase_date_key` | `(order_id, payment_sequential)` | Z-ORDER `purchased_at, payment_type` |
| `gold/fact_reviews` | ziarno: opinia; `review_score`, `has_comment`, `review_created_at`, `review_answered_at`, stan klienta, `purchased_at`, `is_delivered`, `is_late`, `d_to_deliver` (z `fact_orders_lifecycle`) | `(review_id, order_id)` | CHECK `review_score BETWEEN 1 AND 5`; Z-ORDER `review_created_at, customer_state` |
| `gold/revenue_events` | ruchy przychodu: `sale` (+), `correction` (− po anulacji), `adjustment` | `event_id` | tylko dopisywanie |
| `gold/status_transitions` | przejścia statusów z par `before → after` | `(order_id, source_lsn)` | |
| `gold/agg_revenue_hourly` | okna 1 h czasu symulacji × stan: `revenue`, `sales`, `corrections`, `freight`, `items_sold`, `items_canceled` | `(window_start, customer_state)` | watermark; CHECK `corrections <= 0` |
| `gold/agg_status_funnel` | okna 1 h × `from_status` → `to_status`: `transitions` (`(new)` = utworzenie) | `(window_start, from_status, to_status)` | watermark |
| `ops/stream_metrics` | postęp mikropaczek każdego zapytania | – | tylko dopisywanie |
| `ops/e2e_latency` | opóźnienie zapis − commit w Postgresie (silver, gold) | – | tylko dopisywanie |
| `ops/cdc_heartbeat` | heartbeat Debezium co 10 s | – | tylko dopisywanie |
| `ops/quarantine` | wiersze łamiące reguły jakości (`rule`, `severity`, `record_key`, `payload`) | `(rule, source_table, record_key)` | |
| `ops/maintenance_log` | przebiegi OPTIMIZE/VACUUM z pomiarem przed/po | – | tylko dopisywanie |

**DuckDB (CLI, Python albo serwer MCP `duckdb` agentów):**
```sql
INSTALL delta; LOAD delta; INSTALL httpfs; LOAD httpfs;
SET TimeZone = 'UTC';   -- znaczniki czasu Delta to UTC; bez tego DuckDB pokaże strefę hosta
CREATE SECRET minio (TYPE s3, KEY_ID 'dashboard', SECRET '<MINIO_DASHBOARD_SECRET_KEY>',
                     ENDPOINT '127.0.0.1:9000', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1');
SELECT customer_state, sum(price) FROM delta_scan('s3://gold/fact_order_items') GROUP BY 1 ORDER BY 2 DESC LIMIT 5;
SELECT order_status, count(*) FROM delta_scan('s3://gold/fact_orders_lifecycle') GROUP BY 1;
-- silver/bronze wymagają konta agent (dashboard nie ma do nich dostępu)
SELECT count(*) FROM delta_scan('s3://silver/orders') WHERE NOT _is_deleted;
SELECT op, before.order_status, after.order_status FROM delta_scan('s3://bronze/orders') LIMIT 5;
```

**Python `deltalake` (delta-rs), wersje, historia, time travel, CDF:**
```python
from deltalake import DeltaTable
opts = {"AWS_ACCESS_KEY_ID": "dashboard", "AWS_SECRET_ACCESS_KEY": "…",
        "AWS_ENDPOINT_URL": "http://127.0.0.1:9000", "AWS_ALLOW_HTTP": "true", "AWS_REGION": "us-east-1"}
dt = DeltaTable("s3://gold/fact_order_items", storage_options=opts)
dt.version(); dt.history(5); dt.schema(); dt.metadata().configuration   # CHECK, właściwości
df = dt.to_pandas()                                                      # albo dt.to_pyarrow_table()
old = DeltaTable("s3://gold/fact_order_items", version=120, storage_options=opts).to_pandas()
cdf = DeltaTable("s3://silver/orders", storage_options=agent_opts).load_cdf(starting_version=10)
```

**Spark (w kontenerze `spark`, np. `docker compose exec spark /opt/spark/bin/pyspark` z konfiguracją z `containers/spark/lakehouse/session.py`):**
```python
spark.read.format("delta").load("s3a://gold/fact_order_items")
spark.read.format("delta").option("versionAsOf", 120).load("s3a://gold/fact_order_items")
spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", 10).load("s3a://silver/orders")
spark.sql("DESCRIBE HISTORY delta.`s3a://gold/fact_order_items`").show()
```

**Konsola MinIO** (`http://127.0.0.1:9091`, w sieci compose `minio:9001`): przeglądanie kubełków i plików (`_delta_log/*.json` to commity, `part-*.parquet` dane). Od wydań z maja 2025 konsola community to tylko przeglądarka obiektów (zarządzanie kontami i politykami wyłącznie przez `mc`, co robi `minio-init`). Port hosta to 9091, bo 9001 zajmuje inny kontener na maszynie autora; API i konsola są publikowane tylko na `127.0.0.1` (patrz „Dystrybucja MinIO”).

Dashboard również czyta przez delta-rs lub DuckDB, bez Sparka. Znaczniki czasu biznesowe to czas symulacji (oryginalne wartości Olist bez strefy), zapisane jako `timestamp` UTC; należy je czytać w UTC.

**Zgodność protokołu:** niektóre funkcje Delta (np. deletion vectors, liquid clustering, column mapping, `timestampNtz`) podnoszą wersję protokołu czytelnika, której starsi czytelnicy nie odczytają. W gold i ops włączamy tylko funkcje obsługiwane przez delta-rs i DuckDB: CHECK (cecha zapisującego), `appendOnly`; `minReaderVersion = 1`, `delta.enableDeletionVectors = false`, brak column mapping, brak CDF. Sprawdza to test `tests/integration/test_delta.py::test_every_gold_and_ops_table_is_readable_by_delta_rs_and_duckdb` (wszystkie tabele gold/ops, delta-rs na koncie `dashboard` i DuckDB).

## 10. Rozszerzenie: sekrety

- Klucze `spark` i `dashboard` w `.env` (poza repozytorium).
- W repozytorium `.env.example` z opisem zmiennych.
- Hasło root wyłącznie dla `minio-init`.

## 11. Testy

Status: ✅ przechodzi, ⏳ e2e: odłożone do etapu 5. Scenariusze niszczące działają na tabelach jednorazowych `s3a://gold/_it_<hex>/…` zapisywanych przez Sparka na hoście (te same wersje Delta i S3A co obraz, konto `spark`), odczyt przez delta-rs i DuckDB; prefiks jest usuwany po teście.

| Poziom | Test | Oczekiwany wynik | Identyfikator | Status |
|---|---|---|---|---|
| integracyjny | `minio-init` uruchomiony dwa razy | kubełki, polityki i konta istnieją; brak błędów | `tests/integration/test_stack.py::test_init_container_idempotent[minio-init]` | ✅ |
| integracyjny | uprawnienia konta `dashboard` | odczyt `gold` działa; zapis do `gold` i odczyt `bronze` odrzucone | `tests/integration/test_minio.py::test_dashboard_account_restricted` | ✅ |
| integracyjny | uprawnienia konta `agent` | odczyt każdego kubełka działa; zapis i usuwanie odrzucone | `test_minio.py::test_agent_account_read_only_everywhere` | ✅ |
| integracyjny | zapis przez Sparka, odczyt przez delta-rs i DuckDB | identyczne wyniki | `tests/integration/test_delta.py::test_spark_write_reads_identically_in_delta_rs_and_duckdb`, `::test_every_gold_and_ops_table_is_readable_by_delta_rs_and_duckdb` (żywe tabele gold/ops: kolumny kontraktu, `minReaderVersion = 1`, bez DV i column mapping) | ✅ |
| integracyjny | time travel | `VERSION AS OF n` zwraca stan sprzed późniejszego MERGE | `test_delta.py::test_time_travel_returns_the_state_before_a_later_merge` (Spark i delta-rs) | ✅ |
| integracyjny | `RESTORE` | tabela wraca do wskazanej wersji | `test_delta.py::test_restore_returns_the_table_to_a_version` | ✅ |
| integracyjny | CDF po MERGE | zmiany z typami `update_preimage`/`update_postimage`/`delete` | `test_delta.py::test_change_data_feed_after_merge_has_pre_post_images_and_deletes` (CDF przez delta-rs) | ✅ |
| integracyjny | zapis łamiący `CHECK` | odrzucony | `test_delta.py::test_write_breaking_a_check_constraint_is_rejected` (i ograniczenia obecne w żywym `gold/fact_order_items`) | ✅ |
| integracyjny | niezgodny typ kolumny / nowa kolumna z `mergeSchema` | odrzucony / przyjęta | `test_delta.py::test_type_mismatch_is_rejected_and_a_new_column_needs_merge_schema` | ✅ |
| integracyjny | OPTIMIZE | mniej plików, dane identyczne (suma kontrolna) | `test_delta.py::test_optimize_compacts_files_and_keeps_the_data` (8 plików → 1, Z-ORDER jak w zadaniu utrzymania) | ✅ |
| integracyjny | VACUUM | usuwa tylko pliki starsze niż retencja; time travel w oknie retencji działa | `test_delta.py::test_vacuum_removes_only_files_older_than_the_retention` | ✅ |
| integracyjny (`slow`) | zadanie utrzymania w kontenerze | OPTIMIZE + Z-ORDER gold, VACUUM wszystkich warstw, pomiar przed/po w `ops/maintenance_log` | `test_delta.py::test_maintenance_job_runs_in_the_container_and_logs_before_after` | ✅ |
| jednostkowy | konfiguracja sesji nie włącza `enableFastS3AListFrom` (§7) | brak ustawienia | `tests/unit/test_spark_session.py::test_fast_s3a_list_from_stays_off` | ✅ |
| e2e | usunięcie i ponowne utworzenie kontenera Sparka | wznowienie z checkpointów w MinIO, brak duplikatów, uzgadnianie zgodne | etap 5 (`tests/e2e/`); restart z checkpointu na poziomie integracyjnym: [05-spark.md](05-spark.md) §10 | ⏳ e2e |
| e2e (awaria) | restart MinIO w trakcie strumienia | Spark ponawia zapis; uzgadnianie zgodne | etap 5 | ⏳ e2e |

## 12. Dystrybucja MinIO: build ze źródeł (decyzja z etapu 0)

Stan na 2026-10-20: MinIO zarchiwizowało repozytoria `minio/minio` i `minio/mc` (2026-04-24), a obrazy `minio/minio` i `minio/mc` zostały usunięte z Docker Hub i Quay (2026-09); `dl.min.io` zwraca 410. Specyfikacja wymaga MinIO, więc:

- **Obraz `wpdb/minio`** (`containers/minio/Dockerfile`) buduje serwer i klienta z archiwów źródłowych GitHub ostatnich tagów: `minio` `RELEASE.2025-10-15T17-29-55Z`, `mc` `RELEASE.2025-08-13T08-35-41Z` (builder `golang:1.24.13-bookworm`, `CGO_ENABLED=0`, te same `-ldflags` co w upstreamowym `Makefile`, runtime `debian:bookworm-slim` + `ca-certificates` + `curl`). Jeden obraz ma oba binaria, więc `minio-init` nie potrzebuje osobnego obrazu `mc`.
- **CVE-2026-40344** (obejście uwierzytelnienia w ostatnim wydaniu community, wymaga ważnego klucza z prawem zapisu) nie ma łatki w wersji otwartej. Ograniczenie ryzyka: porty 9000 i 9091 publikowane wyłącznie na `127.0.0.1`, konta `spark`/`dashboard` z minimalnymi politykami, root tylko dla `minio-init`. Stos jest środowiskiem laboratoryjnym na jednej maszynie, nie usługą sieciową.
- **Fallback** (udokumentowany, nieużywany): `bitnamilegacy/minio:2025.7.23-debian-12-r5`.
- **Konsekwencje dla czytelników Delta** (delta-rs, DuckDB; [08-operations.md](08-operations.md) §1): bez identity columns i column mapping w tabelach czytanych przez dashboard, `delta.enableDeletionVectors=false`, CDF czytany tylko przez delta-rs.

## 13. Podsumowanie

| | Podstawa | Z rozszerzeniami |
|---|---|---|
| Kubełki | ręcznie w konsoli | kubełek per warstwa, idempotentny `minio-init` |
| Dostęp | root wszędzie | konta per usługa, dashboard tylko do odczytu gold |
| Tabele | pliki Delta | ACID, audyt, time travel, RESTORE |
| Zmiany | — | CDF na silver jako źródło inkrementalnego gold |
| Jakość | — | wymuszanie schematu + ograniczenia CHECK |
| Utrzymanie | — | OPTIMIZE + Z-ORDER + VACUUM z pomiarem; minimalne partycjonowanie |
| Stan Sparka | lokalnie | checkpointy w MinIO, bezstanowy kontener |
| Odczyt | konsola | DuckDB, delta-rs (z time travel), konsola; zgodność protokołu testowana |

## Do weryfikacji przy implementacji

- ~~Dostępność obrazów MinIO community.~~ **Rozstrzygnięte (etap 0, 2026-10-20):** obrazy community zniknęły, więc budujemy MinIO ze źródeł (sekcja „Dystrybucja MinIO” powyżej, [08-operations.md](08-operations.md) §13). Test: `tests/integration/test_minio.py::test_server_version_matches_pinned_tag`.
- **Rozstrzygnięte (etap 3): liquid clustering w Delta OSS 3.3.** Dostępny (`CLUSTER BY`, cecha zapisującego `clustering` + `domainMetadata`, protokół zapisującego 7), ale nieużywany: tabele mieszczą się w jednym pliku (§7), a Z-ORDER nie zmienia protokołu. Decyzja: Z-ORDER na faktach.
- **Rozstrzygnięte (etap 3): funkcje Delta czytelne dla delta-rs 1.6.6 i DuckDB 1.5.6.** Gold i ops mają `minReaderVersion = 1` i tylko cechy zapisującego (CHECK, `appendOnly`, `invariants` nie są używane), bez DV, column mapping, CDF, `timestampNtz`; silver ma dodatkowo CDF (czytany przez delta-rs `load_cdf`, DuckDB czyta silver zwykłym `delta_scan`). Test: `tests/integration/test_delta.py::test_every_gold_and_ops_table_is_readable_by_delta_rs_and_duckdb`; znaczniki czasu Delta są w DuckDB typu `TIMESTAMP WITH TIME ZONE` (UTC), stąd `SET TimeZone='UTC'` w §9.
- **Rozstrzygnięte (etap 3): wydajność i poprawność checkpointów na S3A.** Poprawne (restart z checkpointu, test w §11), koszt ok. 0,3 s na paczkę (§8); pliki dziennika i checkpointów są małe, MinIO ma silną spójność (zapis przez `rename` S3A = kopiowanie + usunięcie).
