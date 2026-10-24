# Spark: przetwarzanie strumieniowe (bronze → silver → gold)

Spark to **mózg platformy**. Czyta zdarzenia CDC z Kafki ([04-kafka.md](04-kafka.md)), interpretuje je (AVRO, koperta Debezium, kolejność, duplikaty) i buduje trzy warstwy danych w Delta Lake. Podział odpowiedzialności:
- **ten dokument:** logika przetwarzania,
- **[06-minio-delta.md](06-minio-delta.md):** przechowywanie (kubełki, time travel, OPTIMIZE, odczyt z hosta).

```
Kafka (AVRO) ──► BRONZE ──────────► SILVER ─────────────► GOLD
                 surowe zdarzenia    aktualny stan tabel    model gwiazdy + agregaty
                 tylko dopisywane    (MERGE = lustro bazy)  (czyta dashboard)
```

## 1. Część podstawowa

- **Jeden kontener, `local[*]`** (single node).
- **Odczyt:** `readStream.format("kafka")`, `subscribePattern = olist\.shop\..*` (obejmuje też tabele dodane później), `startingOffsets = earliest`.
- **Deserializacja:** odcięcie 5-bajtowego nagłówka Confluenta + `from_avro` ze schematem z Schema Registry.
- **Transformacja:** rozpakowanie koperty, rzutowanie typów, zapis do Delta.
- **Obraz z wbudowanymi JAR-ami** (`spark-sql-kafka`, `spark-avro`, `delta-spark`, `hadoop-aws`, `aws-java-sdk-bundle`) w zgodnych wersjach, zamiast `--packages` z internetu przy każdym starcie.

## 2. Rozszerzenie: AVRO z ewolucją schematu

`from_avro` przyjmuje jeden schemat, a po `ADD COLUMN` w temacie są wiadomości v1 i v2. W każdej mikropaczce:
1. Odczyt `schema_id` z nagłówka (bajty 1–4).
2. Grupowanie po `schema_id` i dekodowanie każdej grupy jej własnym schematem (z rejestru, z pamięcią podręczną).
3. `unionByName(allowMissingColumns=True)`: stare wiersze mają nową kolumnę jako `null`.
4. Delta przyjmuje nową kolumnę (`mergeSchema` / `autoMerge`).

Tombstones (`value = null`) są odrzucane.

## 3. Rozszerzenie: bronze

Jedna tabela Delta na temat, **tylko dopisywanie**. Kolumny:
- pełna koperta: `op`, `before`, `after`,
- metadane źródła: `source_lsn`, `source_tx`, `source_ts`,
- metadane Kafki i ingestii: `kafka_partition`, `kafka_offset`, `schema_id`, `ingested_at`.

Właściwości:
- **Pełna koperta** (decyzja z [03-debezium.md](03-debezium.md) §3): `before` jest potrzebne w gold.
- **Pełna historia:** kompaktowane tematy Kafki trzymają tylko ostatnią wersję każdego wiersza, a bronze wszystkie zmiany. Z bronze można odbudować silver i gold bez Kafki.
- **Exactly-once:** zapis w `foreachBatch` z opcjami Delta `txnAppId` + `txnVersion` (= numer mikropaczki). Ponowiona po awarii paczka jest rozpoznana i pominięta, także przy wielu tabelach w jednym `foreachBatch`.

## 4. Rozszerzenie: silver (lustro bazy)

Osobne zapytanie strumieniowe czyta **bronze jako strumień Delta**. Każda warstwa ma własny checkpoint, więc zmiana logiki silver nie dotyka bronze.

**Klucze główne** Spark odczytuje ze **schematu klucza wiadomości** w Schema Registry (np. `olist.shop.order_items-key` → `order_id`, `order_item_id`), bo kluczem jest zawsze PK ([03-debezium.md](03-debezium.md) §4). Nowa tabela przynosi własny schemat klucza. **Spark nie łączy się z Postgresem.** Jedynym kontraktem ze źródłem jest strumień.

**Krok 1: deduplikacja w paczce.** Dla każdego PK zostaje zdarzenie z najwyższym `source_lsn`.

**Krok 2: MERGE ze strażnikiem LSN:**
```sql
MERGE INTO silver.orders t USING batch s ON t.order_id = s.order_id
WHEN MATCHED AND s.source_lsn > t._lsn AND s.op = 'd' THEN UPDATE SET _is_deleted = true, _lsn = s.source_lsn
WHEN MATCHED AND s.source_lsn > t._lsn                 THEN UPDATE SET *, _lsn = s.source_lsn, _is_deleted = false
WHEN NOT MATCHED                                        THEN INSERT *  -- także dla 'd' (jako usunięty)
```

Co daje strażnik `s.source_lsn > t._lsn`:
- **duplikaty** (Debezium jest at-least-once) są ignorowane, więc MERGE jest idempotentny,
- **spóźnione starsze zdarzenia** są ignorowane, więc brak kolejności między tematami nie ma znaczenia.

**Miękkie usuwanie (`_is_deleted`):** wiersz-nagrobek pamięta LSN usunięcia. Przy fizycznym DELETE spóźniony duplikat `op=c` „wskrzesiłby” wiersz. Widoki dla odbiorców filtrują `_is_deleted = false`.

## 5. Rozszerzenie: gold (model gwiazdy)

Gold jest liczony **ze stanu silver** (inkrementalnie, przez Change Data Feed Delty), a nie bezpośrednio ze strumienia. Nie ma złączeń strumień–strumień, a kolejność przychodzenia pozycji i zamówień nie ma znaczenia.

```
                    dim_date
                       │
dim_customer (SCD2) ── fact_order_items ── dim_product (+ kategoria EN)
                       │           │
                  dim_seller    dim_geography (stan)

fact_orders_lifecycle  (1 wiersz = 1 zamówienie, kamienie milowe)
```

**`fact_order_items`:** ziarno = pozycja zamówienia. Miary: `price`, `freight_value`, plus status zamówienia.

**`fact_orders_lifecycle`:** fakt z akumulującym się snapshotem (wzorzec Kimballa). Jeden wiersz na zamówienie, uzupełniany w miarę postępu:

| order_id | purchased_at | approved_at | shipped_at | delivered_at | h_to_approve | d_to_deliver | is_late | status |
|---|---|---|---|---|---|---|---|---|
| e481f5… | 2017-10-02 10:56 | 2017-10-02 11:07 | 2017-10-04 19:55 | 2017-10-10 21:25 | 0.2 | 8.4 | false | delivered |

**`dim_customer` (SCD typu 2)** po `customer_unique_id`, na prawdziwych przeprowadzkach (250 osób, [01-business-system.md](01-business-system.md) §6):

| customer_unique_id | zip | city | valid_from | valid_to | is_current |
|---|---|---|---|---|---|
| 8d50f5ea… | 06233 | osasco | 2017-03-14 | 2018-01-20 | false |
| 8d50f5ea… | 13330 | indaiatuba | 2018-01-20 | ∞ | true |

Fakt łączy się z wersją ważną w chwili zakupu.

**Agregaty okienkowe w czasie zdarzeń** (czas symulacji, nie czas Kafki), z watermarkiem (np. 2 h czasu symulacji):
- przychód na godzinę i stan (okno 1 h po `order_purchase_timestamp`),
- lejek przejść statusów z par `before → after` (np. ile zamówień przeszło `approved → shipped` w danej godzinie),
- korekty: DELETE pozycji anulowanego zamówienia daje ujemną kwotę (pełne `before` dzięki `REPLICA IDENTITY FULL`).

## 6. Rozszerzenie: jakość danych z kwarantanną

Reguły na przejściu silver → gold, np. `price ≥ 0`, istniejący produkt dla pozycji, `delivered_at ≥ purchased_at`. Wiersze łamiące reguły trafiają do tabeli `quarantine` z nazwą reguły, a strumień działa dalej. Na prawdziwych danych przykładowo: 9 płatności o wartości 0, 3 typy płatności `not_defined`.

## 7. Rozszerzenie: obserwowalność

- **`StreamingQueryListener`** zapisuje postęp każdej mikropaczki do `ops.stream_metrics`: wiersze/s na wejściu i przetworzone, czas paczki, opóźnienie offsetów.
- **Opóźnienie end-to-end** = czas zapisu w gold − `source_ts` (commit w Postgresie). Dashboard pokazuje je jako wskaźnik.
- **Heartbeat CDC:** Spark czyta też temat `__debezium-heartbeat.olist` i zapisuje ostatnie sygnały do `ops.cdc_heartbeat`. Dashboard pokazuje świeżość CDC bez czytania Kafki.
- **Opóźnienie per warstwa (etap 3):** każda mikropaczka silver i gold liczy czas zapisu − `source_ts` dla swoich zmian (bez `op=r` ze snapshotu) i zapisuje podsumowanie (min, p50, p95, max) do `ops.e2e_latency` (`layer` = `silver`/`gold`). Postęp zapytań i opóźnienia są buforowane w sterowniku i zapisywane jednym commitem co 30 s, a nie commitem na paczkę i tabelę.

## 8. Rozszerzenie: strojenie

- **`spark.sql.shuffle.partitions = 4`** zamiast domyślnych 200. Przy małych paczkach domyślne 200 tworzy setki pustych zadań i drobnych plików. Pomiar przed i po trafia do dokumentacji.
- **Trigger** `processingTime = 10 s`: kompromis między opóźnieniem a liczbą małych plików.
- **Delta `optimizeWrite`/`autoCompact`:** mniej drobnych plików.
- **Pamięć sterownika** ustawiona jawnie, zgodnie z budżetem RAM całego stosu.

## 9. Rozszerzenie: struktura kodu

Logika (deduplikacja, MERGE, SCD2, lejek, reguły jakości) jest w czystych funkcjach `DataFrame → DataFrame`, a odczyt i zapis są osobno. Większość logiki da się przetestować jednostkowo na małych DataFrame'ach, bez Kafki i MinIO.

## 10. Testy

Status: ✅ przechodzi, ⏳ e2e: odłożone do etapu 5 (decyzja z 2026-10-21, 00-overview).

| Poziom | Test | Oczekiwany wynik | Identyfikator | Status |
|---|---|---|---|---|
| jednostkowy | dekodowanie z nagłówkiem Confluenta, schematy v1 i v2 w jednej paczce | wszystkie wiersze odczytane, stare z nową kolumną = `null` | `tests/unit/test_spark_decode.py::test_decoding_confluent_header_v1_and_v2_in_one_batch` | ✅ |
| jednostkowy | tombstone w paczce | odfiltrowany | `test_spark_decode.py::test_tombstone_in_batch_is_filtered` | ✅ |
| jednostkowy | wiadomość bez nagłówka Confluenta (§6) | trafia do kwarantanny, paczka przechodzi | `test_spark_decode.py::test_undecodable_value_goes_to_quarantine_rows` | ✅ |
| jednostkowy | 3 zmiany jednego wiersza w paczce | zostaje zdarzenie z najwyższym LSN | `test_spark_decode.py::test_three_changes_of_one_row_keep_the_highest_lsn` | ✅ |
| jednostkowy (Delta lokalnie) | MERGE sekwencji c → u → d | wiersz z `_is_deleted = true` | `tests/unit/test_spark_silver.py::test_merge_c_u_d_gives_a_soft_deleted_row` | ✅ |
| jednostkowy (Delta lokalnie) | ta sama paczka dwa razy | stan identyczny (idempotencja) | `test_spark_silver.py::test_same_batch_twice_gives_the_same_state` | ✅ |
| jednostkowy (Delta lokalnie) | starsze zdarzenie po nowszym; `c` po `d` | ignorowane; brak „wskrzeszenia” | `test_spark_silver.py::test_older_event_after_newer_and_create_after_delete_are_ignored` | ✅ |
| jednostkowy (Delta lokalnie) | `ADD COLUMN` (v2) w silver; wymuszona zmiana typu (03 §5) | kolumna dodana, stare wiersze `null`; nowy typ rzutowany na istniejący, bez awarii | `test_spark_silver.py::test_add_column_reaches_silver_through_schema_evolution`, `::test_forced_type_change_is_cast_to_the_existing_column_type` | ✅ |
| jednostkowy | SCD2: klient zmienia zip | dwie wersje; fakt wskazuje wersję ważną w chwili zakupu | `tests/unit/test_spark_gold.py::test_scd2_customer_moves_and_the_fact_points_to_the_version_valid_at_purchase` | ✅ |
| jednostkowy | fakt z akumulującym snapshotem | poprawne czasy etapów i `is_late` | `test_spark_gold.py::test_accumulating_snapshot_fills_the_milestones_and_is_late` | ✅ |
| jednostkowy | okno z watermarkiem | zdarzenie spóźnione poza watermark nie zmienia zamkniętego okna | `test_spark_gold.py::test_late_event_beyond_the_watermark_does_not_change_a_closed_window` (Structured Streaming, tryb `update`, jak w kontenerze) | ✅ |
| jednostkowy | DELETE pozycji anulowanego zamówienia | ujemna korekta w agregacie przychodu | `test_spark_gold.py::test_delete_of_a_canceled_orders_item_is_a_negative_correction` | ✅ |
| jednostkowy (`slow`) | reguły jakości | `price < 0` w kwarantannie, strumień działa dalej | `test_spark_gold.py::test_quality_rules_send_bad_rows_to_quarantine_and_the_batch_goes_on` (cała paczka `GoldJob.process` na lokalnym silver: kwarantanna, fakty, wymiary, korekty po anulacji), `::test_quality_rule_predicates` | ✅ |
| jednostkowy (`slow`) | awaria paczki gold między commitem zdarzeń a commitem faktu, silver zmienia się przed powtórką | powtórka czyta przypięte wersje silver: zdarzenia bez duplikatu, fakt zastosowany, suma zdarzeń = suma faktu | `test_spark_gold.py::test_gold_batch_replayed_after_a_crash_between_events_and_fact_is_exactly_once` | ✅ |
| jednostkowy | konfiguracja sesji | brak `enableFastS3AListFrom` (06 §7), DV wyłączone, S3A path-style | `tests/unit/test_spark_session.py` | ✅ |
| jednostkowy | usługa `spark` w compose i JAR-y obrazu | domyślna usługa z healthcheckiem i `depends_on`; JAR-y z macierzy wersji, sprawdzone `.sha1` | `tests/unit/test_compose_file.py::test_spark_service_runs_by_default_after_the_connector_and_minio_init`, `::test_spark_image_pins_every_connector_jar_with_a_checksum` | ✅ |
| integracyjny (Kafka + SR + MinIO) | prawdziwe wiadomości Debezium | bronze i silver zapisane; kolumny i typy poprawne (`decimal`, `timestamp`) | `tests/integration/test_spark.py::test_real_debezium_messages_reach_bronze_and_silver_with_types` (zakup → UPDATE → DELETE sondy przez Postgres; odczyt DuckDB i delta-rs) | ✅ |
| integracyjny | `ADD COLUMN` w trakcie działania | nowa kolumna w silver bez restartu | `test_spark.py::test_add_column_reaches_silver_without_restart` (ten sam `StartedAt` kontenera) | ✅ |
| integracyjny (`slow`) | restart z checkpointu | w bronze unikalne `(topic, partition, offset)` | `test_spark.py::test_restart_from_checkpoint_keeps_bronze_offsets_unique` | ✅ |
| integracyjny | PK ze schematu klucza | PK zgodne z tabelami, także dla tabeli dodanej w trakcie; brak połączeń Sparka z Postgresem | `test_spark.py::test_primary_keys_come_from_the_key_schema`, `::test_add_column_reaches_silver_without_restart` (tabela dodana w trakcie), `::test_spark_never_connects_to_postgres` | ✅ |
| e2e | uzgadnianie | `sim.v_truth_*` = silver; sumy gold = sumy silver | etap 5 (`tests/e2e/`); jednorazowe uzgodnienie na stosie w §12 | ⏳ e2e |
| e2e (awaria) | `kill` Sparka w trakcie strumienia, restart | uzgadnianie zgodne | etap 5 | ⏳ e2e |
| e2e | opóźnienie | opóźnienie end-to-end poniżej ustalonego progu przy domyślnym tempie | etap 5; progi i pomiar w §12 | ⏳ e2e |

## 11. Podsumowanie

| | Podstawa | Z rozszerzeniami |
|---|---|---|
| Obraz | `--packages` z internetu | JAR-y wbudowane w zgodnych wersjach |
| AVRO | jeden schemat | dekodowanie per `schema_id`, ewolucja bez restartu |
| Zapis | jeden sink | bronze → silver → gold, osobne checkpointy |
| Semantyka | at-least-once | exactly-once (`txnAppId`) + idempotentny MERGE ze strażnikiem LSN |
| Usuwanie | — | miękkie, odporne na spóźnione duplikaty |
| Model | — | gwiazda, fakt z akumulującym snapshotem, SCD2 na prawdziwych danych |
| Agregaty | — | okna w czasie zdarzeń + watermark, lejek i korekty z `before` |
| Jakość | — | kwarantanna zamiast awarii strumienia |
| Obserwowalność | logi | metryki strumieni i opóźnienie end-to-end |
| Weryfikacja | — | testy jednostkowe (Delta lokalnie), integracyjne i e2e z awarią |

## 12. Ustalenia z implementacji (etap 3)

Kod: `containers/spark/lakehouse/` (opis modułów w `containers/spark/README.md`). Usługa `spark` startuje domyślnie z `make up`, po `connect-init` i `minio-init`; wersje w [08-operations.md](08-operations.md) §1.

**Zapytania strumieniowe** (każde z własnym checkpointem `checkpoints/<nazwa>`, [06-minio-delta.md](06-minio-delta.md) §8):

| Zapytanie | Źródło | Ujście | Trigger |
|---|---|---|---|
| `kafka_to_bronze` | Kafka `olist\.shop\..*\|__debezium-heartbeat\.olist`, `startingOffsets=earliest`, `failOnDataLoss=false`, `maxOffsetsPerTrigger=50000`, `kafka.metadata.max.age.ms=10000` | `bronze/<tabela>` (append + `txnAppId/txnVersion`), `ops/cdc_heartbeat`, niedekodowalne → `ops/quarantine` | 10 s |
| `silver_<tabela>` (jedno na tabelę bronze) | `bronze/<tabela>` jako strumień Delta | `silver/<tabela>` (MERGE ze strażnikiem LSN) | 10 s |
| `silver_to_gold` | CDF 7 tabel silver (`orders`, `order_items`, `order_payments`, `customers`, `products`, `sellers`, `product_category_name_translation`), unia w jednym zapytaniu | wymiary, fakty, `gold/revenue_events`, `ops/quarantine` | 20 s |
| `gold_transitions` | `bronze/orders` (każde zdarzenie, `before → after`) | `gold/status_transitions` | 20 s |
| `gold_agg_revenue`, `gold_agg_funnel` | `gold/revenue_events`, `gold/status_transitions` | `gold/agg_revenue_hourly`, `gold/agg_status_funnel` (tryb `update`, MERGE zastępujący okna) | 20 s |

**Decyzje i odstępstwa od pierwotnego projektu (dokument poprawiony przed kodem):**
- **Silver: jedno zapytanie na tabelę, nie jedno wspólne.** Zapytanie strumieniowe ma stały zbiór źródeł zapisany w checkpoincie, a tabele dochodzą w trakcie (§4). Pętla sterownika (`lakehouse/supervisor.py`) co 10 s listuje `bronze/` i uruchamia `silver_<tabela>` dla każdej nowej tabeli, której temat istnieje w Kafce (PK z `<temat>-key`). Zapytanie tabeli, której temat usunięto (sondy testów), jest zatrzymywane po 2 min bezczynności; jej silver zostaje jako ostatni stan.
- **Zmiana schematu źródła Delta zatrzymuje zapytanie.** Strumień Delta (bronze → silver, CDF silver → gold) kończy się błędem przy każdej zmianie schematu tabeli źródłowej, także addytywnej (`ADD COLUMN`). Pętla sterownika wznawia takie zapytanie z checkpointu w ciągu ok. 5 s, z nowym schematem; kontener i pozostałe zapytania działają dalej (test `test_add_column_reaches_silver_without_restart`: ten sam `StartedAt`). Zapytanie, które pada > 10 razy w 10 min, kończy proces kodem 1 (Docker restartuje kontener).
- **Wymuszona zmiana typu (03 §5, `compatibility=NONE`)** nie zatrzymuje strumienia: wiersze nowego schematu są rzutowane na typ, który tabela już ma (`schema_align.conform`; `int` → `string` bez straty), test `test_forced_type_change_is_cast_to_the_existing_column_type`.
- **Gold: przeliczanie dotkniętych kluczy.** Paczka CDF wyznacza zbiór dotkniętych zamówień i osób (`gold.affected`: zmiany `orders`/`order_items`/`order_payments`, klienci, produkty → ich pozycje, wszystkie zamówienia osoby dla SCD2); ich wiersze gold są liczone od nowa ze stanu silver i synchronizowane jednym MERGE na tabelę (`delta_io.sync_subset`: insert/update/delete w zakresie). Dzięki temu kolejność przychodzenia tabel nie ma znaczenia (pozycja bez zamówienia w silver pojawi się, gdy zamówienie dojdzie) i paczka jest idempotentna.
- **`dim_customer` (SCD2):** wersja zaczyna się przy pierwszym zamówieniu z nowym adresem (`customer_id` jest per zamówienie, 01 §1), `valid_to` bieżącej wersji = 9999-12-31 (zamiast `∞`, bo `NULL` utrudnia złączenia zakresowe), `customer_sk = xxhash64(customer_unique_id, valid_from)` (deterministyczny, bez kolumny identity, której czytelnicy nie obsługują).
- **Lejek z bronze, nie z silver.** Silver trzyma tylko najnowszy stan na paczkę (krok 1), a przy ×1440 akceptacja przychodzi zwykle w tej samej paczce co zakup; pary `before → after` są w bronze. `r` ze snapshotu to stany, nie przejścia, i są pomijane.
- **Czas zdarzeń bez własnej kolumny.** Spark nie widzi zegara symulatora. Dla przejść z kolumną (zatwierdzenie, wysyłka, dostawa) czasem jest ta kolumna; dla statusu końcowego (`canceled`, `unavailable`, …) i korekty po DELETE pozycji: najpóźniejszy znany czas biznesowy (maksimum dat cyklu życia w silver i w paczce), dokładność rzędu odstępu między zdarzeniami.
- **Korekty:** `gold/revenue_events` = różnice `fact_order_items` (nowa pozycja = `sale` w chwili zakupu, usunięta = `correction` z ujemną kwotą, zmiana ceny = `adjustment`); suma zdarzeń jest zawsze równa sumie faktu. Exactly-once: paczka gold czyta silver w wersjach przypiętych przy jej pierwszym wykonaniu (`checkpoints/silver_to_gold/_wpdb_gold_versions/<paczka>.json`), zdarzenia są zapisywane przed faktem z `txnAppId/txnVersion`; powtórka po awarii między tymi commitami liczy tę samą różnicę, pomija zdarzenia i stosuje fakt (test `test_gold_batch_replayed_after_a_crash_between_events_and_fact_is_exactly_once`). Bez przypięcia powtórka na nowszym silver gubiła zdarzenia (zmierzone, „Pomiary”).
- **Watermark 24 h czasu symulacji** (`AGG_WATERMARK`) zamiast przykładowych 2 h: przy `SIM_SPEED=1440` 1 s zegara to 24 min symulacji, a opóźnienie potoku (kilka triggerów) to dziesiątki sekund, więc 2 h odrzucałyby poprawne zdarzenia jako spóźnione. Okna są zapisywane w trybie `update` (MERGE zastępujący wartości okna, idempotentny); okno zamknięte przez watermark już się nie zmienia (test jednostkowy).
- **Jakość (§6):** reguły `price_nonneg`, `freight_nonneg`, `product_exists` (`reject`: wiersz nie trafia do gold, CHECK z 06 §6 to ostatnia linia obrony), `delivered_after_purchase`, `shipped_after_purchase`, `payment_value_positive`, `payment_type_defined` (`warn`: wiersz zostaje w gold, jest zgłoszony); `ops/quarantine` = MERGE po `(rule, source_table, record_key)` z `first_seen_at`/`last_seen_at`. Wiadomość niedekodowalna (brak nagłówka, uszkodzone AVRO) → reguła `avro_decodable`.
- **Exactly-once w bronze:** `txnAppId` = token zapisany w katalogu checkpointu (`_wpdb_txn_app_id`), `txnVersion` = numer mikropaczki; ten sam token dla wszystkich tabel bronze i `ops/cdc_heartbeat` w jednym `foreachBatch` (Delta zapamiętuje parę per tabela). Nowy checkpoint = nowy token, więc paczki od 0 nie są ignorowane.

**Strojenie (§8), zmierzone profilerem JFR w kontenerze** (13 zapytań, ok. 8 zmian/s z symulatora): mikropaczka silver trwała 15–25 s, a sterownik zużywał 5–7 rdzeni. Przyczyny i zmiany: (1) Delta odbudowuje stan snapshotu w 50 zadaniach (`spark.databricks.delta.snapshotPartitions=2`); (2) ok. 30 % CPU to generowanie i kompilacja kodu dla każdego nowego planu, także projekcji odtwarzania dziennika Delta i statystyk plików (`spark.sql.codegen.wholeStage=false`, `spark.sql.codegen.factoryMode=NO_CODEGEN`, statystyki bronze tylko dla 8 pierwszych kolumn `delta.dataSkippingNumIndexedCols=8`); (3) bufor wysyłki S3A `bytebuffer` rezerwował 64 MB pamięci bezpośredniej na strumień (OOM; teraz `disk`, części 16 MB); (4) plany paczki gold rosły tak, że ich wypisywanie (AQE/UI) kończyło się OOM (`localCheckpoint` pośrednich wyników, `spark.sql.maxPlanStringLength`); (5) zapytania czekały na siebie w kolejce FIFO (`spark.scheduler.mode=FAIR`), a zapisy tabel bronze i tabel gold w jednej paczce idą równolegle (pule wątków); (6) AQE wyłączone (dziesiątki wierszy w paczce); `spark.sql.shuffle.partitions=4`. Po zmianach: mediana paczki bronze ok. 10 s, silver ok. 7 s, gold ok. 10–13 s (kroki gold logowane jako `gold batch N: {...}`). Pułapka: `spark.hadoop.delta.enableFastS3AListFrom=true` przyspiesza listowanie dziennika, ale w Delta 3.3.3 na S3A powoduje, że VACUUM usuwa pliki `_delta_log/*.json` (odtworzone na MinIO); ustawienie jest zakazane testem (06 §7).

**Pomiary (2026-10-23, `make reset && make up`, ustawienia domyślne: okno 2017-01-05…03-31, ×1440, ≤ 20 zmian/s; 12 min strumienia bez zakłóceń, Apple Silicon, 8 CPU w VM Dockera):**

| Wielkość | Wartość |
|---|---|
| czas mikropaczki (mediana / p95) | `kafka_to_bronze` 3,2 / 5,0 s; `silver_<tabela>` 2,3 / 4,0 s; `silver_to_gold` 6,8 / 8,6 s; `gold_transitions` 3,0 s; agregaty 2,5 s; narzut poza `foreachBatch` (offsety i commit checkpointu na S3A) 0,1–0,35 s |
| opóźnienie Postgres → silver (`ops/e2e_latency`, mediana p50 paczek / mediana p95 / maks.) | **17 s / 21 s / 25 s** |
| opóźnienie Postgres → gold | **37 s / 45 s / 49 s** |
| skład opóźnienia | CDC ≈ 0,5 s (03 §13) + oczekiwanie na trigger bronze (≤ 10 s) + paczka bronze + oczekiwanie na trigger silver (≤ 10 s) + paczka silver; gold: + trigger 20 s + paczka gold |
| CPU / RAM kontenera `spark` | skoki 400–630 % w czasie paczek, 1–3 % między nimi; 2,5–2,6 GB (limit 3 GB), [08-operations.md](08-operations.md) §7 |
| progi dla testu e2e (etap 5) | silver p95 < 60 s, gold p95 < 120 s przy ustawieniach domyślnych (ok. 3× zmierzone) |

**Uzgodnienie na stosie po zakończeniu przebiegu** (symulator w fazie `done`, potok bezczynny, przebieg z wieloma restartami Sparka i pełnym zestawem testów integracyjnych w trakcie): liczby żywych wierszy silver = `sim.v_truth_row_counts` dla wszystkich 8 tabel (`customers` 5 591, `orders` 5 591, `order_items` 6 219, `order_payments` 5 919, `order_reviews` 4 458, `products` 32 951, `sellers` 3 095, `product_category_name_translation` 71); `gold/fact_order_items`: 6 219 pozycji, przychód 780 132,69, fracht 119 784,74 = silver = `sim.v_truth_totals`; `gold/fact_orders_lifecycle`: 5 591 zamówień, płatności 939 621,99, 70 spóźnionych = `sim.v_truth_order_lifecycle`; kwarantanna pusta (w tym oknie danych nie ma płatności 0 ani `not_defined`). Wykryta przy tym niezgodność `gold/revenue_events` (suma 641 294,04 zamiast 780 132,69: zdarzenia paczek przerwanych restartem między commitem faktu a commitem zdarzeń i powtórzonych na nowszym silver) doprowadziła do przypięcia wersji silver per paczka i kolejności „zdarzenia, potem fakt” (wyżej; test `test_gold_batch_replayed_after_a_crash_between_events_and_fact_is_exactly_once`); wynik po poprawce (drugi przebieg od `make reset`, z restartem Sparka i pełnym zestawem testów integracyjnych w trakcie): `sum(revenue_events.amount)` = `sum(fact_order_items.price)` = `sum(agg_revenue_hourly.revenue)` = 379 375,28 (stan w trakcie przebiegu, między paczkami gold); uzgodnienie drugiego przebiegu po fazie `done` nie zostało wykonane (przerwane na prośbę użytkownika), pozostaje dla `qa-engineer`.

## Do weryfikacji przy implementacji

- **Rozstrzygnięte (etap 3): para Spark/Delta.** Spark 3.5.9 (Scala 2.12, Java 17, Hadoop 3.3.4) + Delta 3.3.3 (linia 3.x = Spark 3.5; 4.x tylko dla Sparka 4), JAR-y z POM-ów (`spark-sql-kafka-0-10` 3.5.9 → `kafka-clients` 3.4.1, `commons-pool2` 2.11.1, `spark-token-provider-kafka-0-10` 3.5.9; `hadoop-aws` 3.3.4 → `aws-java-sdk-bundle` 1.12.262), wbudowane w obraz z kontrolą `.sha1`; [08-operations.md](08-operations.md) §1.
- **Rozstrzygnięte (etap 3): typy logiczne w `from_avro`.** `bytes` + `decimal(10,2)` → `DecimalType(10,2)`, `long` + `timestamp-millis` → `TimestampType` (wartość = naiwny `TIMESTAMP` Postgresa jako UTC; sesja Sparka i proces Pythona w UTC, `TZ=UTC` w obrazie, inaczej `collect()` przesuwa godziny o strefę). Testy `test_decoding_confluent_header_v1_and_v2_in_one_batch`, `test_real_debezium_messages_reach_bronze_and_silver_with_types` (wartości = Postgres).
- **Rozstrzygnięte (etap 3): kompaktowane tematy.** `failOnDataLoss=false`: luki w offsetach po kompaktowaniu i tematy usunięte przez testy nie są utratą danych potoku; ciągły odczyt widzi pełną historię, odczyt od zera po kompaktowaniu tylko ostatnie wersje (04 §10), silver jest i tak poprawny (ostatnia wersja + tombstone'y). Nowe tematy są wykrywane w ok. 10 s (`kafka.metadata.max.age.ms`).
- **Rozstrzygnięte (etap 3): `txnAppId`/`txnVersion` przy wielu tabelach** w jednym `foreachBatch`: działa, bo Delta zapisuje parę (`appId`, `version`) w dzienniku każdej tabeli osobno; token per checkpoint (wyżej). Unikalność `(topic, partition, offset)` po restarcie: `test_restart_from_checkpoint_keeps_bronze_offsets_unique`.
- **Rozstrzygnięte (etap 3): checkpointy na S3A.** Poprawne (restart kontenera wznawia od checkpointu, test wyżej); koszt to ok. 0,3 s na paczkę poza `addBatch` (zapis `offsets/` i `commits/` przez S3A), zmierzone w `ops/stream_metrics` (`batch_duration_ms − add_batch_ms`).
