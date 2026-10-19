# Debezium: przechwytywanie zmian (CDC)

Debezium to **most między bazą a strumieniem**. Czyta WAL PostgreSQL ([02-postgresql.md](02-postgresql.md)) przez slot replikacyjny i każdą zmianę wiersza zamienia na zdarzenie w Kafce. Działa jako konektor wewnątrz **Kafka Connect**, w osobnym kontenerze `connect`.

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

- Historia sprzed `SIM_START` jest ładowana przed startem konektora ([01-business-system.md](01-business-system.md)).
- `snapshot.mode=initial`: Debezium odczytuje istniejące wiersze jako **`op=r`** w spójnym punkcie w czasie, a potem przechodzi do strumienia `c/u/d` dokładnie od tego miejsca w WAL. Nie ma dziury ani duplikatu na styku.
- **Snapshot przyrostowy na żądanie:** przez kanał sygnałów (tabela sygnałowa lub temat Kafki) zlecamy ponowny odczyt jednej tabeli, np. `products`, bez zatrzymywania strumienia. Debezium przeplata snapshot ze zdarzeniami na żywo i rozwiązuje konflikty. Zastosowanie: odtworzenie silver po awarii.

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

Symulator w zadanym momencie wykonuje `ALTER TABLE shop.orders ADD COLUMN delivery_channel TEXT`.

| Krok | Gdzie | Efekt |
|---|---|---|
| 1 | Postgres | nowa kolumna; publikacja schematu obejmuje ją automatycznie |
| 2 | Debezium | nowy schemat AVRO z polem nullable |
| 3 | Schema Registry | `olist.shop.orders-value` ma wersje `[1, 2]`; zgodność BACKWARD przepuszcza zmianę |
| 4 | Spark | wiadomości ze schematem 1 i 2 czytane obok siebie; Delta przyjmuje kolumnę (`mergeSchema`) |

Zmiana niezgodna, np. typu kolumny na niekompatybilny, jest odrzucana przez Schema Registry, a konektor przechodzi w `FAILED`. Oba przypadki są sprawdzane testami (§11).

## 6. Rozszerzenie: poprawne typy w całym potoku

| Typ w Postgres | Domyślnie w AVRO | Problem | Ustawienie |
|---|---|---|---|
| `TIMESTAMP` | `long` (mikrosekundy, `io.debezium.time.MicroTimestamp`) | Spark widzi liczbę | `time.precision.mode=connect`, czyli logiczny typ `timestamp-millis` |
| `NUMERIC(10,2)` | `bytes` + logiczny `decimal` | wymaga obsługi w deserializacji | `decimal.handling.mode=precise` (dokładne kwoty) |

Cel: `58.90` dociera do gold jako `58.90`, a `2017-10-04 19:55:00` jako znacznik czasu, a nie liczba.

## 7. Rozszerzenie: heartbeat i metadane transakcji

- **Heartbeat** (`heartbeat.interval.ms=10000` + `heartbeat.action.query`):
  - przesuwa slot w okresach ciszy (patrz [02-postgresql.md](02-postgresql.md) §5),
  - emituje zdarzenia do tematu `__debezium-heartbeat.olist`, więc dashboard może pokazać „ostatni sygnał CDC: 3 s temu”.
- **Metadane transakcji** (`provide.transaction.metadata=true`): temat `olist.transaction` z BEGIN i END, a w zdarzeniach danych `transaction.id`:
  ```json
  {"status": "END", "id": "771:24023128", "event_count": 5,
   "data_collections": [{"data_collection": "shop.orders", "event_count": 1},
                        {"data_collection": "shop.order_items", "event_count": 2}]}
  ```
  Widać, że jeden zakup to jedna transakcja. Spark może tego użyć do sprawdzania kompletności, a dashboard do wskaźnika „zakupy na sekundę”.

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

- **Debezium dostarcza co najmniej raz.** Po awarii część zdarzeń może przyjść ponownie.
- **Odpowiedź systemu:** idempotentny `MERGE` w Sparku (porównanie po `source.lsn`), więc duplikat nie zmienia wyniku. Tryb exactly-once w Kafka Connect zostanie sprawdzony jako dodatkowe zabezpieczenie.
- Odporność na awarię Connect sprawdza test e2e (§11).

## 10. Rozszerzenie: parametry tematów przy tworzeniu

`topic.creation.default.*` (i grupy per tabela) zamiast `auto.create.topics.enable=true` z tutoriala:
- liczba partycji zależna od wolumenu tabeli,
- `cleanup.policy=compact` dla tematów-lustra,
- retencja czasowa dla heartbeat i transakcji.

Szczegóły: [04-kafka.md](04-kafka.md).

## 11. Testy

| Poziom | Test | Oczekiwany wynik |
|---|---|---|
| integracyjny | INSERT, UPDATE i DELETE w `shop.orders` | zdarzenia `c`, `u`, `d`; przy `u` i `d` pełne `before` |
| integracyjny | dane istniejące przed startem konektora | zdarzenia `op=r`, potem strumień bez dziury i bez duplikatu na styku |
| integracyjny | typy | znacznik czasu jako logiczny `timestamp-millis`, `58.90` jako dokładny `decimal` |
| integracyjny | klucze wiadomości | klucz = PK tabeli; schemat klucza w rejestrze (np. `order_items-key`: `order_id`, `order_item_id`) |
| integracyjny | `ADD COLUMN` | nowa wersja schematu w rejestrze, konektor `RUNNING` |
| integracyjny | niezgodna zmiana typu | rejestr odrzuca schemat, konektor `FAILED` |
| integracyjny | rejestracja dwa razy (`connect-init`) | brak błędu, jeden konektor, konfiguracja aktualna |
| integracyjny | `GET /connectors/olist-postgres/config` | hasło nie jest jawne |
| integracyjny | heartbeat przy braku ruchu | zdarzenia w temacie heartbeat, slot przesuwa się |
| e2e (awaria) | `kill` kontenera `connect` w trakcie strumienia, restart | wznowienie od offsetu, opóźnienie slotu wraca do zera, uzgadnianie Postgres ↔ silver zgodne |

## 12. Podsumowanie

| | Podstawa | Z rozszerzeniami |
|---|---|---|
| Format | AVRO (własny obraz z konwerterem) | AVRO + świadome typy (daty, kwoty dziesiętne) |
| Start | strumień od teraz | snapshot `op=r` + strumień bez dziury; snapshot przyrostowy na żądanie |
| Treść zdarzeń | koperta | pełna koperta zachowana dla `before` |
| Klucze | PK (domyślnie, nieświadomie) | PK jako świadoma decyzja: strumień sam się opisuje, Spark bez dostępu do bazy, kompaktowanie wszędzie |
| Zmiana schematu | niezdefiniowane | ewolucja zgodna przechodzi, niezgodna jest odrzucana (oba przypadki w testach) |
| Rejestracja | ręczny `POST` | `PUT` z kontenera startowego, hasło ze zmiennej środowiskowej |
| Awaria | niezdefiniowane | at-least-once + idempotentny MERGE, test e2e z awarią Connect |
| Obserwowalność | `/status` | heartbeat, metadane transakcji, opóźnienie slotu w dashboardzie |

## Do weryfikacji przy implementacji

- Aktualna stabilna wersja Debezium 3.x: zgodność z PostgreSQL 16 i wersją Kafki.
- Zestaw JAR-ów konwertera AVRO zgodny z wersją Schema Registry.
- Czy tabela sygnałowa i tabela heartbeat muszą należeć do publikacji (wpływ na schematy `shop`/`sim`).
- Obsługa exactly-once dla konektora Postgres.
- Odczyt logicznych typów `decimal` i `timestamp-millis` z AVRO w Sparku.
- Zachowanie po unieważnieniu slotu (`max_slot_wal_keep_size`) i dobór `snapshot.mode`.
