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

## 8. Rozszerzenie: strojenie

- **`spark.sql.shuffle.partitions = 4`** zamiast domyślnych 200. Przy małych paczkach domyślne 200 tworzy setki pustych zadań i drobnych plików. Pomiar przed i po trafia do dokumentacji.
- **Trigger** `processingTime = 10 s`: kompromis między opóźnieniem a liczbą małych plików.
- **Delta `optimizeWrite`/`autoCompact`:** mniej drobnych plików.
- **Pamięć sterownika** ustawiona jawnie, zgodnie z budżetem RAM całego stosu.

## 9. Rozszerzenie: struktura kodu

Logika (deduplikacja, MERGE, SCD2, lejek, reguły jakości) jest w czystych funkcjach `DataFrame → DataFrame`, a odczyt i zapis są osobno. Większość logiki da się przetestować jednostkowo na małych DataFrame'ach, bez Kafki i MinIO.

## 10. Testy

| Poziom | Test | Oczekiwany wynik |
|---|---|---|
| jednostkowy | dekodowanie z nagłówkiem Confluenta, schematy v1 i v2 w jednej paczce | wszystkie wiersze odczytane, stare z nową kolumną = `null` |
| jednostkowy | tombstone w paczce | odfiltrowany |
| jednostkowy | 3 zmiany jednego wiersza w paczce | zostaje zdarzenie z najwyższym LSN |
| jednostkowy (Delta lokalnie) | MERGE sekwencji c → u → d | wiersz z `_is_deleted = true` |
| jednostkowy (Delta lokalnie) | ta sama paczka dwa razy | stan identyczny (idempotencja) |
| jednostkowy (Delta lokalnie) | starsze zdarzenie po nowszym; `c` po `d` | ignorowane; brak „wskrzeszenia” |
| jednostkowy | SCD2: klient zmienia zip | dwie wersje; fakt wskazuje wersję ważną w chwili zakupu |
| jednostkowy | fakt z akumulującym snapshotem | poprawne czasy etapów i `is_late` |
| jednostkowy | okno z watermarkiem | zdarzenie spóźnione poza watermark nie zmienia zamkniętego okna |
| jednostkowy | DELETE pozycji anulowanego zamówienia | ujemna korekta w agregacie przychodu |
| jednostkowy | reguły jakości | `price < 0` w kwarantannie, strumień działa dalej |
| integracyjny (Kafka + SR + MinIO) | prawdziwe wiadomości Debezium | bronze i silver zapisane; kolumny i typy poprawne (`decimal`, `timestamp`) |
| integracyjny | `ADD COLUMN` w trakcie działania | nowa kolumna w silver bez restartu |
| integracyjny | restart z checkpointu | w bronze unikalne `(topic, partition, offset)` |
| integracyjny | PK ze schematu klucza | PK zgodne z tabelami, także dla tabeli dodanej w trakcie; brak połączeń Sparka z Postgresem |
| e2e | uzgadnianie | `sim.v_truth_*` = silver; sumy gold = sumy silver |
| e2e (awaria) | `kill` Sparka w trakcie strumienia, restart | uzgadnianie zgodne |
| e2e | opóźnienie | opóźnienie end-to-end poniżej ustalonego progu przy domyślnym tempie |

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

## Do weryfikacji przy implementacji

- Para Spark/Delta (3.5.x + Delta 3.x lub 4.x) i zgodne wersje JAR-ów.
- Odczyt logicznych typów `decimal` i `timestamp-millis` przez `from_avro`.
- Źródło Kafki przy kompaktowanych tematach (`failOnDataLoss`).
- `txnAppId`/`txnVersion` w `foreachBatch` z wieloma tabelami.
- Checkpointy na S3A (MinIO), decyzja w [06-minio-delta.md](06-minio-delta.md) §8: wydajność i poprawność.
