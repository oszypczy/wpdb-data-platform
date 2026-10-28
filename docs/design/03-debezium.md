# Debezium: przechwytywanie zmian (CDC)

Debezium to **most między bazą a strumieniem**. Czyta WAL PostgreSQL ([02-postgresql.md](02-postgresql.md)) przez slot replikacyjny i każdą zmianę wiersza zamienia na zdarzenie w Kafce. Działa jako konektor wewnątrz **Kafka Connect**, w osobnym kontenerze `connect`.

Stan: **zaimplementowany (etap 2, 2026-10-20)**. Konfiguracja konektora: `containers/connect/connectors/olist-postgres.json` (rejestrowana przez `connect-init`), obraz: `containers/connect/Dockerfile`, tabela heartbeat: `containers/postgres/initdb/20-heartbeat.sql`. Ustalenia z implementacji i dokładne schematy AVRO dla Sparka: §13.

## 1. Część podstawowa

- **Kafka Connect** w trybie distributed. Stan trzymany w tematach `connect-configs`, `connect-offsets` i `connect-status`.
- **Konektor Postgres:**
  ```json
  {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "plugin.name": "pgoutput",
    "database.hostname": "postgres",
    "database.dbname": "shop",
    "database.user": "debezium",
    "database.password": "${env:DEBEZIUM_DB_PASSWORD}",
    "topic.prefix": "olist",
    "slot.name": "debezium_slot",
    "publication.name": "dbz_publication",
    "publication.autocreate.mode": "disabled",
    "schema.include.list": "shop",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081"
  }
  ```
  Pełny plik (`containers/connect/connectors/olist-postgres.json`) dodaje do tego: `tasks.max=1` (konektor Postgres i tak ma jedno zadanie), `table.exclude.list=shop\._.*` (tabele o nazwie zaczynającej się od `_` to sondy testowe, nie są przechwytywane; powód w §13), ustawienia z §2–§10 oraz grupy `topic.creation.*` ([04-kafka.md](04-kafka.md) §3).
- **Rejestracja** przez REST API (`:8083`).
- **Wynik:** jeden temat na tabelę (`olist.shop.orders`, `olist.shop.order_items`, …), a każde zdarzenie ma kopertę:
  ```json
  {
    "before": {"order_id": "e481f5…", "order_status": "approved", "order_delivered_carrier_date": null},
    "after":  {"order_id": "e481f5…", "order_status": "shipped",  "order_delivered_carrier_date": "2017-10-04T19:55:00"},
    "source": {"schema": "shop", "table": "orders", "lsn": 24023128, "txId": 771, "snapshot": "false", "ts_ms": 1760870400123},
    "op": "u",
    "ts_ms": 1760870400456
  }
  ```
  `op`: `c` (insert), `u` (update), `d` (delete), `r` (odczyt w snapshocie). Pełne `before` wynika z `REPLICA IDENTITY FULL`.

**Pułapka:** od Debezium 2.0 obraz `quay.io/debezium/connect` **nie zawiera konwertera AVRO Confluenta**. Potrzebny jest własny obraz (`Dockerfile`) z JAR-ami `kafka-connect-avro-converter` i ich zależnościami.

## 2. Rozszerzenie: snapshot + strumień

- Historia sprzed `SIM_START` jest ładowana przed startem konektora ([01-business-system.md](01-business-system.md)). **Realizacja:** `connect-init` ma `depends_on: simulator: condition: service_healthy`, a symulator jest zdrowy dopiero po preloadzie (01 §5). Rejestracja konektora następuje więc po załadowaniu całej historii sprzed `SIM_START`; test `test_compose_file.py::test_connect_init_waits_for_the_simulator_preload` pilnuje zależności w compose.
- **`snapshot.mode=initial` (decyzja):** Debezium tworzy slot, eksportuje jego spójny snapshot, czyta nim istniejące wiersze jako **`op=r`** (`source.snapshot` = `first`/`true`/`last`), a potem strumieniuje `c/u/d` od punktu spójności slotu. Nie ma dziury ani duplikatu na styku: dowód w `test_debezium.py::test_preload_rows_arrive_as_snapshot_reads_and_the_stream_continues_without_gap_or_duplicate` (wszystkie zamówienia sprzed `SIM_START` mają dokładnie jedno `r`, żaden klucz nie ma zarazem `r` i `c`, w każdej partycji wszystkie `r` poprzedzają strumień, każdy wiersz z Postgresa jest w temacie). Symulator nie czeka na konektor, więc zamówienia odtworzone między końcem preloadu a punktem spójności slotu też trafiają do snapshotu jako `r` (zamiast `c`), nadal dokładnie raz; na wolnym runnerze CI zdarza się to przy pierwszych zdarzeniach (2026-10-28), lokalnie nie. Odrzucone: `when_needed` robi pełny snapshot ponownie, gdy brak offsetu (np. po `make reset` samej Kafki), co dublowałoby `r` w bronze; `no_data` nie dałoby historii. Snapshot domyślnego okna (2 642 wiersze + katalogi 36 117 wierszy) trwa ok. 2 s; przy prezentacyjnym oknie (283 tys. wierszy) kilkanaście sekund, w budżecie `CONNECT_INIT_TIMEOUT=120`.
- **Snapshot przyrostowy na żądanie: nie zaimplementowany w etapie 2.** Dla PostgreSQL wymaga tabeli sygnałowej w przechwytywanym schemacie (`signal.data.collection`, konektor zapisuje w niej znaczniki wodne, więc rola `debezium` potrzebowałaby `INSERT` w `shop`) albo kanału Kafka plus tej samej tabeli; tabela w `shop` psułaby `sim.v_truth_row_counts` i test „8 tabel” symulatora. Zostaje jako kandydat do etapu 6 (odtworzenie silver), patrz [08-operations.md](08-operations.md) §14.

## 3. Rozszerzenie: pełna koperta (świadoma decyzja)

Transformacja `ExtractNewRecordState` (spłaszczenie do samego `after`) **nie jest włączona**. `REPLICA IDENTITY FULL` ustawiamy właśnie po to, żeby mieć `before`, a spłaszczenie by je wyrzuciło.
- **bronze** dostaje pełną kopertę,
- **silver** dostaje `after` (z `MERGE`),
- **gold** korzysta z par `before → after` (lejek statusów, korekty przychodu przy DELETE i UPDATE).

`source.lsn` służy jako znacznik kolejności przy `MERGE`.

## 4. Rozszerzenie: klucz wiadomości = PK (strumień sam się opisuje)

Kluczem wiadomości jest **PK tabeli** (domyślne zachowanie Debezium, bez `message.key.columns`). Debezium rejestruje schemat klucza w Schema Registry, np. `olist.shop.order_items-key` z polami `order_id` i `order_item_id`.

Co to daje:
- **Spark zna PK każdej tabeli ze schematu klucza.** Nie potrzebuje żadnego połączenia z Postgresem. Kafka i Schema Registry są jedynym kontraktem między źródłem a przetwarzaniem.
- **Kolejność zmian jednego wiersza jest zachowana** (ten sam klucz → ta sama partycja).
- **Kompaktowanie jest poprawne dla wszystkich tematów tabel** ([04-kafka.md](04-kafka.md) §2).

**Odrzucona alternatywa:** `message.key.columns` z kluczem `order_id` w `order_items`, `order_payments` i `order_reviews` dawałby kolejność zdarzeń całego zamówienia w partycji. Nie jest to potrzebne, bo Spark jest odporny na kolejność (MERGE ze strażnikiem LSN, gold ze stanu silver, [05-spark.md](05-spark.md)). Kosztowałby za to dwa obejścia: zakaz kompaktowania tych tematów oraz odczyt PK przez Sparka bezpośrednio z Postgresa, co łamałoby rozdzielenie komponentów w architekturze CDC.

**Między tabelami** kolejność nie jest gwarantowana (osobne tematy). Spark porządkuje zdarzenia po `source.lsn`.

## 5. Rozszerzenie: ewolucja schematu na żywo

Symulator w zadanym momencie wykonuje `ALTER TABLE shop.orders ADD COLUMN delivery_channel TEXT` (01 §7; od etapu 3 włączane `SIM_SCHEMA_EVOLUTION_AT`, domyślnie wyłączone; test etapu 2 wykonuje to samo DDL sam jako `shop_app`, więc w stosie testowym ustawienie zostaje puste).

| Krok | Gdzie | Efekt |
|---|---|---|
| 1 | Postgres | nowa kolumna; publikacja schematu obejmuje ją automatycznie |
| 2 | Debezium | nowy schemat AVRO z polem nullable |
| 3 | Schema Registry | `olist.shop.orders-value` ma wersje `[1, 2]`; zgodność BACKWARD przepuszcza zmianę |
| 4 | Spark | wiadomości ze schematem 1 i 2 czytane obok siebie; Delta przyjmuje kolumnę (`mergeSchema`) |

Zmiana niezgodna, np. typu kolumny na niekompatybilny, jest odrzucana przez Schema Registry, a konektor przechodzi w `FAILED`. Oba przypadki są sprawdzane testami (§11).

**Ustalenia z implementacji:**
- `pgoutput` wysyła nowy opis tabeli (relation message) dopiero z **następną zmianą** w tej tabeli, więc nowa wersja schematu pojawia się w rejestrze przy pierwszym `c/u/d` po `ALTER`, nie w chwili DDL. Nowa kolumna jest w AVRO unią `["null","string"]` z `default: null`, co BACKWARD przepuszcza (`POST /compatibility/...` → `is_compatible: true`).
- Rejestr nie tworzy nowej wersji dla schematu, który już istnieje pod subjectem: po `DROP COLUMN` zdarzenia wracają do **id** wersji 1, a `latest` zostaje wersją 2. Spark musi więc dekodować każdą wiadomość **jej własnym** `schema_id` z nagłówka, a nie wersją `latest` ([05-spark.md](05-spark.md) §2 już tak zakłada). Test: `test_debezium.py::test_add_nullable_column_gives_a_new_schema_version_and_the_connector_stays_running` (dodaje i usuwa `delivery_channel`, więc jest powtarzalny; subject `olist.shop.orders-value` ma po nim wersje `[1, 2]`).
- **Zmiana niezgodna** (`ALTER COLUMN v TYPE integer` na tabeli sondy `shop.zz_stage2_probe_*`): rejestr odpowiada `409` „Schema being registered is incompatible with an earlier schema”, konwerter rzuca `DataException`, zadanie przechodzi w `FAILED` (konektor zostaje `RUNNING`), slot przestaje się przesuwać. **Procedura naprawy (operator):** zdarzenie czeka w WAL i po każdym restarcie wróci, więc trzeba je przepuścić albo zmianę wycofać: `PUT /config/<subject>` z `{"compatibility":"NONE"}` **tylko dla tego subjectu**, potem `POST /connectors/olist-postgres/restart?includeTasks=true&onlyFailed=true`; zdarzenie rejestruje wersję 2 i strumień rusza; globalne BACKWARD nie zmienia się. Test: `::test_incompatible_type_change_is_rejected_and_the_connector_fails_then_recovers` (po teście tabela, temat i subjecty sondy są usuwane).

## 6. Rozszerzenie: poprawne typy w całym potoku

| Typ w Postgres | Domyślnie w AVRO | Problem | Ustawienie |
|---|---|---|---|
| `TIMESTAMP` | `long` (mikrosekundy, `io.debezium.time.MicroTimestamp`) | Spark widzi liczbę | `time.precision.mode=connect`, czyli logiczny typ `timestamp-millis` |
| `NUMERIC(10,2)` | `bytes` + logiczny `decimal` | wymaga obsługi w deserializacji | `decimal.handling.mode=precise` (dokładne kwoty) |

Cel: `58.90` dociera do gold jako `58.90`, a `2017-10-04 19:55:00` jako znacznik czasu, a nie liczba.

**Zweryfikowane na rejestrze** (`test_debezium.py::test_timestamps_and_decimals_are_logical_types`): `price` → `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2,"connect.name":"org.apache.kafka.connect.data.Decimal"}` (wartość = `unscaled` big-endian two's complement, czyli `58.90` to bajty `0x1702`), `shipping_limit_date`/`order_*` → `{"type":"long","logicalType":"timestamp-millis","connect.name":"org.apache.kafka.connect.data.Timestamp"}` (ms od epoki, czas naiwny z Postgresa traktowany jako UTC). Kolumny nullable to unie `["null", <typ>]` z `default: null`. Dla Sparka: `from_avro` daje `decimal(10,2)` i `timestamp` wprost dla tych typów logicznych (do potwierdzenia w etapie 3, 05 „Do weryfikacji”); pełne schematy w §13.

## 7. Rozszerzenie: heartbeat i metadane transakcji

- **Heartbeat** (`heartbeat.interval.ms=10000` + `heartbeat.action.query`):
  - przesuwa slot w okresach ciszy (patrz [02-postgresql.md](02-postgresql.md) §5),
  - emituje zdarzenia do tematu `__debezium-heartbeat.olist` (klucz `{"serverName":"olist"}`, wartość `{"ts_ms":…}`), więc dashboard może pokazać „ostatni sygnał CDC: 3 s temu”.
  - **Realizacja:** `heartbeat.action.query = INSERT INTO dbz.heartbeat (id, ts) VALUES (1, now()) ON CONFLICT (id) DO UPDATE SET ts = EXCLUDED.ts` (jeden wiersz, bez wzrostu tabeli). Dokumentacja Debezium 3.6 wymaga, żeby tabela heartbeat **była w publikacji** („you must explicitly add the heartbeat table to the publication”): zmiana musi dotrzeć do konektora, który ją odfiltrowuje (`schema.include.list=shop`), a odfiltrowane zdarzenie wymusza zapis offsetu i `confirmed_flush_lsn` idzie do przodu. Stąd `20-heartbeat.sql`: tabela `dbz.heartbeat`, `ALTER PUBLICATION dbz_publication ADD TABLE dbz.heartbeat` (sam schemat `sim` nadal poza publikacją), `GRANT USAGE ON SCHEMA sim` i `SELECT, INSERT, UPDATE` na tej jednej tabeli dla roli `debezium` (`sim.sim_state` pozostaje dla niej niewidoczna). Testy: `test_debezium.py::test_heartbeat_writes_the_sim_table_and_feeds_the_heartbeat_topic`, `::test_heartbeat_advances_the_slot_while_shop_is_idle` (symulator zatrzymany na czas testu), `test_postgres.py::test_heartbeat_table_grants_and_publication`.
- **Metadane transakcji** (`provide.transaction.metadata=true`): temat `olist.transaction` z BEGIN i END, a w zdarzeniach danych `transaction.id`:
  ```json
  {"status": "END", "id": "771:24023128", "event_count": 5,
   "data_collections": [{"data_collection": "shop.orders", "event_count": 1},
                        {"data_collection": "shop.order_items", "event_count": 2}]}
  ```
  Widać, że jeden zakup to jedna transakcja. Spark może tego użyć do sprawdzania kompletności, a dashboard do wskaźnika „zakupy na sekundę”.
  - **Pułapka (zmierzona):** identyfikator to `txId:lsn`, gdzie `lsn` jest **własnym LSN każdego rekordu**, więc BEGIN, END i zdarzenia danych jednej transakcji mają **różne** `id` o wspólnym prefiksie `txId` (np. BEGIN `803:33460312`, dane `803:33460600`, END `803:33462144`). Grupować trzeba po `txId` (prefiks przed `:`, równy `source.txId`), nie po całym `id`. Zdarzenia danych mają `transaction.total_order` (numer w transakcji) i `data_collection_order` (numer w tabeli); BEGIN ma `event_count` i `data_collections` równe `null`. Transakcje samego heartbeatu dają BEGIN/END z `event_count: 0`. Test: `test_debezium.py::test_purchase_transaction_metadata` (zakup = END z `event_count: 4` nad `customers/orders/order_items/order_payments`).

## 8. Rozszerzenie: automatyczna rejestracja i sekrety

- **Kontener startowy `connect-init`:** czeka na zdrową usługę `connect`, potem:
  ```bash
  curl -X PUT http://connect:8083/connectors/olist-postgres/config \
       -H "Content-Type: application/json" -d @connector.json
  ```
  `PUT` tworzy albo aktualizuje, więc jest idempotentny. `POST` z tutoriala za drugim razem zwraca 409.
- **Konfiguracja** leży w repozytorium jako poprawny JSON (tutorialowy ma niedozwolone komentarze `//`).
- **Hasło** przez `config.providers=env` i `${env:DEBEZIUM_DB_PASSWORD}`. Nie trafia do repozytorium ani do odpowiedzi `GET /connectors/…`.

## 9. Rozszerzenie: semantyka dostarczania

- **Debezium dostarcza co najmniej raz.** Po awarii część zdarzeń może przyjść ponownie. **Okno powtórek:** worker zapisuje offsety co `offset.flush.interval.ms=5000` (`OFFSET_FLUSH_INTERVAL_MS=5000` w `docker-compose.yml`, decyzja `platform-devops` z etapu 2; domyślne obrazu Debezium to 60000), więc po restarcie zadania (także po `restart` z REST, patrz §5) konektor czyta WAL ponownie od ostatniego zapisanego offsetu i powtarza do ok. 5 s zdarzeń (przy 20 zmianach/s około 100 rekordów); rejestr i tematy dostają te same rekordy drugi raz (ten sam klucz, ten sam `source.lsn`).
- **Odpowiedź systemu:** idempotentny `MERGE` w Sparku (porównanie po `source.lsn`), więc duplikat nie zmienia wyniku.
- **Exactly-once w Kafka Connect (sprawdzone, nie włączone):** konektor Postgres wspiera `exactly.once.support=required` od Debezium 2.2, ale wymaga po stronie workera `exactly.once.source.support=enabled` (producent transakcyjny, dodatkowe tematy i opóźnienie zatwierdzania), a nie usuwa duplikatów po stronie Postgresa (slot i tak wznawia od `confirmed_flush_lsn`). Przy jednym workerze i idempotentnym `MERGE` zysk jest żaden, a koszt to zmiana konfiguracji workera należącej do `platform-devops`; zostaje jako opcja, gdyby test e2e „kill connect” wykazał problem; test (etap 5, `tests/e2e/test_pipeline.py::test_kill_connect_mid_stream_resumes_from_its_offset_without_loss_or_duplicates`) problemu nie wykazał: powtórki z Connecta odrzuca strażnik LSN, uzgadnianie dokładne. Tańsze zmniejszenie okna powtórek jest włączone: `OFFSET_FLUSH_INTERVAL_MS=5000` na workerze ([08-operations.md](08-operations.md) §5, §14; test `test_compose_file.py::test_connect_worker_flushes_offsets_every_5_s`, w logu workera `offset.flush.interval.ms = 5000`).
- Odporność na awarię Connect sprawdza test e2e (§11).

## 10. Rozszerzenie: parametry tematów przy tworzeniu

`topic.creation.default.*` (i grupy per tabela) zamiast `auto.create.topics.enable=true` z tutoriala:
- liczba partycji zależna od wolumenu tabeli,
- `cleanup.policy=compact` dla tematów-lustra,
- retencja czasowa dla heartbeat i transakcji.

**Realizacja:** `topic.creation.groups=large,transaction,heartbeat` (jedna grupa nie może mieć dwóch retencji, dlatego transakcje i heartbeat są osobno), `large` = `olist\.shop\.(orders|order_items|order_payments|order_reviews|customers)` → 3 partycje, `compact`; `transaction` = `olist\.transaction` → `delete`, 1 dzień; `heartbeat` = `__debezium-heartbeat\.olist` → `delete`, 1 h; `default` (słowniki i tabele dodane później) → 1 partycja, `compact`. Tematy kompaktowane mają `segment.ms=3600000`, bo przy domyślnym segmencie 1 GB kompaktowanie nigdy by się w laboratorium nie uruchomiło (aktywny segment nie jest czyszczony). Zweryfikowane `AdminClient`em: `test_kafka.py::test_business_topics_match_the_topic_design`, `::test_new_table_gets_a_topic_from_the_default_group`. Szczegóły: [04-kafka.md](04-kafka.md).

## 11. Testy

Identyfikatory: `tests/integration/test_debezium.py` (stos z etapu 2; fixture `probe_lifecycle` w `tests/conftest.py` wykonuje jako `shop_app` cykl życia dwóch syntetycznych zamówień `test-stage2-*` i usuwa je, fixture `captured_probe_table` daje jednorazową tabelę przechwytywaną `shop.zz_stage2_probe_*`).

**Testy nie piszą do tabel biznesowych żywego stosu** (od 2026-10-26). Każda zatwierdzona zmiana `shop.orders` trafia przez Sparka do historii gold (`status_transitions`, `agg_status_funnel`), a późniejszy `DELETE` tego nie cofa: sondy z wcześniejszych uruchomień zostawiły na lejku 5 × `(new) → delivered` i 5 × `delivered → created`. Cykl życia sond pisze więc do kopii `shop.zz_probe_<hex>_{customers,orders,order_items,order_payments}` zakładanych raz na sesję przez fixture `probe_shop` (`tests/helpers/cdc.py::ProbeShop`): `CREATE TABLE … (LIKE shop.<tabela> INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)`, czyli te same kolumny, typy i PK, `REPLICA IDENTITY FULL` z event triggera, bez FK; tematy `olist.shop.zz_probe_<hex>_<tabela>` są zakładane z góry z liczbą partycji i `cleanup.policy`/`segment.ms` tematu biznesowego (3 partycje dla `orders`). Spark robi z nich bronze/silver, ale nie gold (nie ma ich w `GOLD_SOURCES`), consumer waliduje je regułami tabeli, którą odwzorowują ([04-kafka.md](04-kafka.md) §6). Na końcu sesji tabele są usuwane po zapisaniu offsetu za ich ostatnią zmianą (§13), razem z subjectami i tematami; pozostałości przerwanej sesji usuwa następna. Wyjątki, które dotykają tabel biznesowych, nie zostawiają wierszy ani przejść statusów: `::test_add_nullable_column_gives_a_new_schema_version_and_the_connector_stays_running` (twierdzenie dotyczy subjectu `olist.shop.orders-value`) robi tylko DDL i `UPDATE` bez zmiany wartości jednego istniejącego zamówienia (nowa kolumna zostaje `NULL`; Spark nie liczy takiej zmiany jako przejścia), `test_kafka.py::test_compaction_keeps_the_last_version_of_every_item` (twierdzenie o lustrze `olist.shop.order_items`) robi `UPDATE … SET price = price` trzech pozycji; testy symulatora w `test_simulator.py` piszą wyłącznie w transakcjach wycofywanych lub przerwanych (Debezium ich nie widzi).

| Poziom | Test | Oczekiwany wynik | Identyfikator |
|---|---|---|---|
| integracyjny | INSERT, UPDATE i DELETE w tabeli zamówień (kopia `shop.orders` z `probe_shop`) | zdarzenia `c`, `u`, `d`; przy `u` i `d` pełne `before` (`u.before == c.after`, `d.before == u.after`), `source.lsn` rosnące, blok `transaction`, po `d` tombstone; pozycje/płatności/klient: `c`, `d`, tombstone | `test_debezium.py::test_insert_update_delete_give_c_u_d_with_full_before` |
| integracyjny | dane istniejące przed startem konektora | wszystkie zamówienia sprzed `SIM_START` jako dokładnie jedno `op=r`; żaden klucz z `r` i `c`; w każdej partycji `r` przed strumieniem; każdy wiersz Postgresa w temacie | `::test_preload_rows_arrive_as_snapshot_reads_and_the_stream_continues_without_gap_or_duplicate`, `::test_snapshot_time_is_recorded_in_source` |
| integracyjny | typy | `price` = `bytes` `decimal(10,2)`, daty = `long` `timestamp-millis`, nullable jako unia z `default: null`; wartości z Kafki równe wartościom z Postgresa (`Decimal('58.90')`) | `::test_timestamps_and_decimals_are_logical_types` |
| integracyjny | klucze wiadomości | dla każdej z 8 tabel pola `<temat>-key` = kolumny PK z `pg_index`; `order_items-key` = `order_id: string, order_item_id: int`; klucz zdekodowany = PK z `after` | `::test_message_key_is_the_table_pk_with_its_own_subject` |
| integracyjny | kolejność zmian jednego wiersza | jeden klucz → jedna partycja, offsety i `source.lsn` rosnące, statusy rosnące w cyklu życia | `::test_changes_of_one_row_stay_in_one_partition_in_lsn_order`, `test_kafka.py::test_changes_of_one_row_stay_in_one_partition_in_order` |
| integracyjny | `ADD COLUMN` | nowe id schematu z polem `["null","string"]`, nowa wersja subjectu, `is_compatible: true`, konektor `RUNNING`; po `DROP COLUMN` powrót do starego id; tylko DDL i `UPDATE` bez zmiany wartości | `::test_add_nullable_column_gives_a_new_schema_version_and_the_connector_stays_running` |
| integracyjny (`slow`) | niezgodna zmiana typu | rejestr: `is_compatible: false`, brak nowej wersji; zadanie `FAILED` ze śladem „incompatible”; po `compatibility=NONE` dla subjectu i restarcie zadania: `RUNNING`, wersja 2, globalnie nadal BACKWARD | `::test_incompatible_type_change_is_rejected_and_the_connector_fails_then_recovers` |
| integracyjny | rejestracja dwa razy (`connect-init`) | ponowny `PUT` = 200, jeden konektor, konfiguracja równa plikowi; powtórny `connect-init` exit 0 bez zmian | `::test_register_twice_keeps_one_connector_with_the_current_config`, `test_stack.py::test_init_container_idempotent[connect-init]`, `test_connect.py::test_connect_init_registered_the_connector` |
| integracyjny | `GET /connectors/olist-postgres/config` | `database.password` = `${env:DEBEZIUM_DB_PASSWORD}`; hasło z `.env` nie występuje w żadnej odpowiedzi REST | `::test_password_is_never_exposed_by_the_rest_api` |
| integracyjny | heartbeat | `dbz.heartbeat.ts` odświeżany, rekordy w `__debezium-heartbeat.olist` co ~10 s; rola `debezium` wykonuje `heartbeat.action.query`, nie czyta `sim.sim_state` | `::test_heartbeat_writes_the_sim_table_and_feeds_the_heartbeat_topic`, `::test_debezium_role_cannot_read_sim_state_even_with_heartbeat_access`, `test_postgres.py::test_heartbeat_table_grants_and_publication` |
| integracyjny (`slow`) | heartbeat przy braku ruchu | symulator zatrzymany: `confirmed_flush_lsn` nadal rośnie, slot aktywny; symulator podniesiony z powrotem | `::test_heartbeat_advances_the_slot_while_shop_is_idle` |
| integracyjny | metadane transakcji | BEGIN/END zakupu, END z `event_count: 4` nad 4 tabelami, wspólny `txId` ze zdarzeniami danych, cykl życia sondy = 3 transakcje (4, 1, 4) | `::test_purchase_transaction_metadata` |
| integracyjny | rejestracja i slot | `connect-init` zrobił `PUT` i doczekał `RUNNING`; slot `debezium_slot` aktywny, `wal_status=reserved`; konfiguracja z `pgoutput`, `dbz_publication`, `snapshot.mode=initial` | `::test_connector_registered_by_connect_init_and_running` |
| integracyjny | opóźnienie i lag | mediana commit→Kafka < 2 s, p95 < 5 s na 200 ostatnich zdarzeniach; lag slotu < 16 MB; najnowsze zamówienie w temacie w < 30 s | `::test_cdc_latency_and_slot_lag_at_defaults`, `::test_stream_keeps_up_with_the_simulator` |
| integracyjny | sprzątanie | po cyklu życia sond tabele `probe_shop` puste i brak wierszy `test-stage2-*` w tabelach biznesowych | `::test_probe_rows_were_removed` |
| integracyjny (`slow`) | slot przed zapisanym offsetem (stos bezczynny, 08 §14) | symulator i konektor zatrzymane, WAL z samym heartbeatem (sprawdzone `pg_logical_slot_peek_binary_changes`), slot przesunięty tak, że `restart_lsn` > `lsn_proc` z `connect-offsets`; po wznowieniu zadanie `RUNNING` (z `no_validation` było `FAILED` „…no longer available on the server”), zmiana zapisana w czasie postoju dociera dokładnie raz, nic starszego nie jest wysyłane ponownie | `::test_slot_ahead_of_the_stored_offset_resumes_from_the_slot_without_loss_or_duplicates` |
| unit | plik konektora | poprawny JSON, hasło jako `${env:…}`, tryby snapshot/decimal/time, heartbeat, grupy `topic.creation`, nadpisania producenta, `table.exclude.list` | `tests/unit/test_compose_file.py::test_connector_json_is_pure_json_with_env_secret_and_topic_groups`, `::test_connect_init_waits_for_the_simulator_preload` |
| e2e (awaria), ✅ etap 5 | `kill` kontenera `connect` w trakcie strumienia, restart | wznowienie od offsetu, opóźnienie slotu wraca do zera, uzgadnianie Postgres ↔ silver zgodne (wymaga silver z etapu 3) | `tests/e2e/test_pipeline.py::test_kill_connect_mid_stream_resumes_from_its_offset_without_loss_or_duplicates` (SIGKILL, 20 zdarzeń symulatora w czasie postoju, `up -d`: konektor i zadanie `RUNNING`, brak nowego snapshotu (liczba `op=r` w bronze bez zmian), `confirmed_flush_lsn` slotu za ostatnią zmianą, wszystkie widoki prawdy i tabele = silver wiersz po wierszu, bez duplikatów w silver i bronze, sumy gold = silver) |

## 12. Podsumowanie

| | Podstawa | Z rozszerzeniami |
|---|---|---|
| Format | AVRO (własny obraz z konwerterem) | AVRO + świadome typy (daty, kwoty dziesiętne) |
| Start | strumień od teraz | snapshot `op=r` + strumień bez dziury (snapshot przyrostowy na żądanie: odłożony, §2) |
| Treść zdarzeń | koperta | pełna koperta zachowana dla `before` |
| Klucze | PK (domyślnie, nieświadomie) | PK jako świadoma decyzja: strumień sam się opisuje, Spark bez dostępu do bazy, kompaktowanie wszędzie |
| Zmiana schematu | niezdefiniowane | ewolucja zgodna przechodzi, niezgodna jest odrzucana (oba przypadki w testach) |
| Rejestracja | ręczny `POST` | `PUT` z kontenera startowego, hasło ze zmiennej środowiskowej |
| Awaria | niezdefiniowane | at-least-once + idempotentny MERGE, test e2e z awarią Connect |
| Obserwowalność | `/status` | heartbeat, metadane transakcji, opóźnienie slotu w dashboardzie |

## 13. Ustalenia z implementacji (etap 2) i kontrakt dla Sparka

**Decyzje konfiguracyjne:** `snapshot.mode=initial` (§2), `decimal.handling.mode=precise` + `time.precision.mode=connect` (§6), `tombstones.on.delete=true`, `heartbeat.interval.ms=10000` z upsertem do `dbz.heartbeat` (§7), `provide.transaction.metadata=true` (§7), `offset.mismatch.strategy=trust_greater_lsn` + `lsn.flush.mode=connector` (niżej), grupy `topic.creation` `large/transaction/heartbeat/default` (§10), `producer.override.acks=all`, `enable.idempotence=true`, `compression.type=zstd` ([04-kafka.md](04-kafka.md) §7), `table.exclude.list=shop\._.*`.

**Pomiary na domyślnych ustawieniach** (Apple Silicon, 20 zmian/s): opóźnienie commit w Postgresie → rekord w Kafce (znacznik czasu rekordu − `source.ts_ms`) mediana **≈ 0,45–0,55 s**, p95 **≈ 0,8–0,85 s**, maks. < 1 s na 200 zdarzeniach (dwa pomiary); z tego samo Debezium (`ts_ms − source.ts_ms`) mediana ≈ 0,25 s, p95 ≈ 0,46 s (konektor pobiera zmiany z `poll.interval.ms=500`). Lag slotu (`pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)`) waha się 0–2 MB, bo `confirmed_flush_lsn` przesuwa się przy zapisie offsetów (pomiar przy interwale 60 s; od zmiany na 5 s lag jest odpowiednio mniejszy), nie przy każdym zdarzeniu. Snapshot okna domyślnego: ok. 2 s. Konektor `RUNNING` ok. 10 s po `PUT`.

**Pułapka: tabela usunięta zanim konektor zapisał offset.** Debezium (`pgoutput`) odczytuje PK tabeli z katalogu w chwili dekodowania zdarzenia. Jeśli tabela z `shop` zostanie usunięta, zanim konektor przetworzy i **zatwierdzi** (offset co 5 s) jej ostatnie zmiany, to po restarcie zadania zdarzenia wracają z WAL, Debezium loguje „Primary keys are not defined for table …, defaulting to unique indices”, rekord nie ma klucza, a kompaktowany temat go odrzuca („Compacted topic cannot accept message without key”): zadanie `FAILED` i tak po każdym restarcie. Testy etapów 0–1 tworzą i usuwają tabele-sondy `shop._stage0_probe` w milisekundach, dlatego **tabele o nazwie zaczynającej się od `_` nie są przechwytywane** (`table.exclude.list`; publikacja nadal je zawiera, konektor je odfiltrowuje). Sondy etapu 2, które muszą być przechwytywane (`shop.zz_stage2_probe_*`), są usuwane dopiero, gdy ostatni offset w `connect-offsets` przekroczy ich ostatnią zmianę (`tests/helpers/cdc.py::wait_committed_past`). Reguła operacyjna: **`DROP TABLE` w `shop` tylko po tym, jak konektor zapisał offset za ostatnią zmianą tabeli**. Zmiana nazwy na `_…` tego nie zastępuje: `pgoutput` opisuje tabelę nazwą z chwili zapisu zmiany, więc po restarcie zadania Debezium szuka PK pod starą nazwą i rekord też traci klucz.

**Slot przed zapisanym offsetem (`offset.mismatch.strategy=trust_greater_lsn`, od 2026-10-26).** Objaw (08 §14): po ponad dobie bezczynności (tylko heartbeat) odtworzony kontener `connect` dawał zadanie `FAILED`: „The connector is trying to read change stream starting at … lsn=X …, but this is no longer available on the server”, przy `restart_lsn` slotu > X. Przyczyna (kod Debezium 3.6.3): przy starcie `PostgresConnection.validateLogPosition` porównuje `lsn_proc` z zapisanego offsetu z `restart_lsn` slotu i przy `snapshot.mode=initial` kończy zadanie, gdy slot jest dalej. Debezium przesuwa slot (`flushLsn`) w `commit()` na LSN ostatniego rekordu **potwierdzonego przez Kafkę**, a Connect zapisuje do `connect-offsets` stan z chwili rozpoczęcia zapisu offsetów, więc slot bywa o heartbeat lub dwa przed zapisanym offsetem; jeśli worker skończy się przed kolejnym zapisem (odtworzenie kontenera), ta różnica zostaje. Przy ruchu nie szkodzi, bo `restart_lsn` jest daleko za `confirmed_flush_lsn` (otwarte transakcje); na bezczynnym stosie `restart_lsn` = `confirmed_flush_lsn` (zmierzone: oba 0/6D55708), więc wystarczy jeden heartbeat różnicy. Pierwotnego wyścigu nie da się wywołać deterministycznie; test odtwarza jego skutek (scenariusz z dokumentacji `offset.mismatch.strategy`: slot przesunięty `pg_replication_slot_advance`) i przy dotychczasowej konfiguracji daje dokładnie ten błąd. Rozwiązanie: `offset.mismatch.strategy=trust_greater_lsn` (start od `max(offset, confirmed_flush_lsn)`; slot za offsetem jest do offsetu przesuwany jak w `trust_offset`; pomija się też walidację `validateLogPosition`) oraz jawne `lsn.flush.mode=connector` (domyślne). **Dlaczego bez utraty danych:** przy `lsn.flush.mode=connector` slot przesuwa tylko Debezium i tylko do LSN rekordów, które Kafka już potwierdziła (`acks=all`), sterownik JDBC nie potwierdza niczego sam; wszystko przed `confirmed_flush_lsn` jest więc już w Kafce, a pominięty zakres WAL to zmiany już wysłane albo (na bezczynnym stosie) same heartbeaty. Ręczne przesunięcie slotu przez operatora też zostałoby przyjęte bez błędu, ale rola z `REPLICATION` ma tylko `debezium` (02 §4). Gdy slot zostanie usunięty i założony od nowa, `trust_greater_lsn` też nie zgłosi błędu (ta sama cecha co `no_validation` przy PostgreSQL ≥ 15); procedura z akapitu niżej (ponowny snapshot) obowiązuje bez zmian. Test: `test_debezium.py::test_slot_ahead_of_the_stored_offset_resumes_from_the_slot_without_loss_or_duplicates`; konektor na żywym stosie zaktualizowany przez `connect-init` (`PUT …/config`), zadanie `FAILED` z odtworzenia przeszło po aktualizacji samo na `RUNNING` (log: „Retrieved latest position from stored offset 'LSN{0/6D55708}'”, „Starting replication stream from LSN LSN{0/6D55B38}”).

**Unieważnienie slotu (`max_slot_wal_keep_size=2GB`, 02 §5):** przy `snapshot.mode=initial` konektor po unieważnieniu slotu kończy z błędem replikacji (slot `wal_status=lost`) i nie robi sam ponownego snapshotu. Procedura: `DELETE /connectors/olist-postgres`, `SELECT pg_drop_replication_slot('debezium_slot')`, usunięcie offsetu konektora (nowy `topic.prefix` albo rekord-tombstone w `connect-offsets`), ponowna rejestracja → pełny snapshot (`r` dla wszystkich wierszy; silver przez `MERGE` zostaje spójny, bronze dostaje drugi komplet `r`). Nieprzetestowane automatycznie: wymaga wyprodukowania 2 GB WAL przy wyłączonym Connect, czyli ok. 2,5 pełnego przebiegu historii (≈ 1 KB WAL na zmianę przy `REPLICA IDENTITY FULL` i `wal_compression=on`); przy domyślnym oknie (40 tys. zmian ≈ 40 MB) limit jest nieosiągalny i chroni wyłącznie dysk.

**Kontrakt dla Sparka (etap 3):**
- Tematy: `olist.shop.customers`, `olist.shop.orders`, `olist.shop.order_items`, `olist.shop.order_payments`, `olist.shop.order_reviews` (3 partycje), `olist.shop.products`, `olist.shop.sellers`, `olist.shop.product_category_name_translation` (1 partycja), `olist.transaction` i `__debezium-heartbeat.olist` (1 partycja, retencja 1 dzień / 1 h). Subjecty: `<temat>-key`, `<temat>-value` (TopicNameStrategy).
- Wiadomość: nagłówek Confluenta (bajt `0x00` + 4 bajty id schematu, big-endian) w kluczu i w wartości; tombstone = klucz z nagłówkiem i wartość `null` (jeden po każdym `d`, te same bajty klucza). `op` ∈ `c/u/d/r`; `r` tylko ze snapshotu (`source.snapshot` ≠ `false`, `transaction` = `null`).
- Klucz `olist.shop.orders-key` (id 9 na stosie od zera; identyfikatory są przydzielane w kolejności rejestracji, więc **nie** należy ich wpisywać na sztywno):
  ```json
  {"type":"record","name":"Key","namespace":"olist.shop.orders",
   "fields":[{"name":"order_id","type":"string"}],"connect.name":"olist.shop.orders.Key"}
  ```
  `olist.shop.order_items-key`: pola `order_id: string`, `order_item_id: int`.
- Wartość `olist.shop.orders-value` (wersja 1): rekord `Envelope` (`namespace: olist.shop.orders`, `connect.name: olist.shop.orders.Envelope`) z polami w tej kolejności: `before: ["null", Value]` (default null), `after: ["null", "Value"]`, `source: io.debezium.connector.postgresql.Source`, `transaction: ["null", event.block]`, `op: string`, `ts_ms: ["null","long"]`, `ts_us`, `ts_ns`. Rekord `Value` (`connect.name: olist.shop.orders.Value`):
  ```json
  {"name":"order_id","type":"string"}
  {"name":"customer_id","type":"string"}
  {"name":"order_status","type":"string"}
  {"name":"order_purchase_timestamp","type":{"type":"long","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Timestamp","logicalType":"timestamp-millis"}}
  {"name":"order_approved_at","type":["null",{…timestamp-millis…}],"default":null}
  {"name":"order_delivered_carrier_date","type":["null",{…timestamp-millis…}],"default":null}
  {"name":"order_delivered_customer_date","type":["null",{…timestamp-millis…}],"default":null}
  {"name":"order_estimated_delivery_date","type":{…timestamp-millis…}}
  ```
  `Source`: `version, connector, name, ts_ms: long, snapshot: [Enum(string), null] default "false", db, sequence: ["null","string"], ts_us, ts_ns, schema, table, txId: ["null","long"], lsn: ["null","long"], xmin, origin, origin_lsn`. `event.block`: `id: string, total_order: long, data_collection_order: long`.
- Wartość `olist.shop.order_items-value`: `Value` z polami `order_id: string`, `order_item_id: int`, `product_id: string`, `seller_id: string`, `shipping_limit_date: timestamp-millis`, `price` i `freight_value`: `{"type":"bytes","scale":2,"precision":10,"connect.version":1,"connect.parameters":{"scale":"2","connect.decimal.precision":"10"},"connect.name":"org.apache.kafka.connect.data.Decimal","logicalType":"decimal"}`. Wszystkie kolumny `NOT NULL` z 01 §3 są w AVRO nie-nullable, pozostałe to unie z `null` na pierwszym miejscu.
- Wersja 2 `olist.shop.orders-value` (po teście ewolucji) ma dodatkowo `{"name":"delivery_channel","type":["null","string"],"default":null}`; wiadomości w temacie noszą id wersji 1 lub 2, zatem dekodowanie per `schema_id` jest obowiązkowe (§5).
- Nie ma pola `__deleted` (brak `ExtractNewRecordState`, §3): usunięcie to `op=d` z `before` i `after: null`, a po nim tombstone, którego Spark odfiltrowuje (`value IS NULL`).
- Powtórki (at-least-once, §9): ten sam rekord może wystąpić dwa razy z tym samym `source.lsn`; `MERGE` ze strażnikiem LSN jest idempotentny.

## Do weryfikacji przy implementacji

- **Rozstrzygnięte (etap 2):** Debezium `3.6.3.Final` + PostgreSQL 16.15 + Kafka 4.3 (CP 8.3.2): pełne połączenie `pgoutput`, snapshot i strumień działają (§11; macierz w [08-operations.md](08-operations.md) §1).
- **Rozstrzygnięte (etap 2):** zestaw JAR-ów konwertera AVRO 8.3.2 z Confluent Hub działa z Schema Registry 8.3.2 (rejestracja, kompatybilność, odrzucenie niezgodnej zmiany).
- **Rozstrzygnięte (etap 2):** tabela heartbeat **musi** być w publikacji (dokumentacja Debezium 3.6, potwierdzone testem przy zatrzymanym symulatorze); tabela sygnałowa nie jest używana (§2, snapshot przyrostowy odłożony).
- **Rozstrzygnięte (etap 2):** exactly-once dla konektora Postgres jest możliwe, ale nie włączone (§9; wymaga konfiguracji workera, zysk zerowy przy idempotentnym `MERGE`).
- **Przeniesione do etapu 3:** odczyt `decimal` i `timestamp-millis` z AVRO w Sparku: schematy i typy logiczne są w §13, consumer w Pythonie (`fastavro`) odczytuje je jako `Decimal` i `datetime` ([04-kafka.md](04-kafka.md) §6).
- **Rozstrzygnięte (etap 2):** zachowanie po unieważnieniu slotu i `snapshot.mode=initial`: procedura ręczna w §13; limit 2 GB jest nieosiągalny w domyślnym oknie.
