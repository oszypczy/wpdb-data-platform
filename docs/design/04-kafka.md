# Kafka + Schema Registry: dziennik zdarzeń

Kafka to **trwały, uporządkowany dziennik zdarzeń** między Debezium ([03-debezium.md](03-debezium.md)) a Sparkiem:
- rozdziela producenta od konsumenta: Spark może być niedostępny, a zdarzenia czekają,
- pozwala odtworzyć dane przez odczyt tematu od początku.

**Schema Registry** przechowuje schematy AVRO. Wiadomość niesie tylko 4-bajtowe ID schematu, a rejestr pilnuje zgodności zmian.

Stan: **zaimplementowane (etap 2, 2026-10-20)**: tematy wewnętrzne z `containers/kafka/topics.txt` (`kafka-init`), tematy biznesowe z `topic.creation.*` konektora (`containers/connect/connectors/olist-postgres.json`), consumer w `containers/consumer/` (usługa `consumer`, profil `tools`). Ustalenia z implementacji: §10.

## 1. Część podstawowa

- **Kafka w trybie KRaft, jeden węzeł** (broker + kontroler, bez ZooKeepera).
- **Dwa listenery** (naprawia błąd tutoriala z samym `localhost:9092`):
  ```yaml
  KAFKA_LISTENERS:            INTERNAL://:29092,EXTERNAL://:9092,CONTROLLER://:9093
  KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092
  ```
  Kontenery łączą się przez `kafka:29092`, a skrypty z hosta przez `localhost:9092`.
- **Schema Registry** (`cp-schema-registry`): schematy w temacie `_schemas`, subjecty według nazw tematów (`olist.shop.orders-key`, `olist.shop.orders-value`).
- **Consumer w Pythonie** (`confluent-kafka` + `AvroDeserializer`): dekoduje wiadomości ze schematem pobranym z rejestru i je **waliduje** (wymóg specyfikacji).
- **Obrazy:** `cp-kafka` i `cp-schema-registry` w jednej przypiętej wersji, Debezium Connect w zgodnej wersji Kafki. Bez `latest`.

## 2. Rozszerzenie: projekt tematów

| Temat | Klucz | Partycje | Czyszczenie | Uzasadnienie |
|---|---|---|---|---|
| `olist.shop.orders`, `order_items`, `order_payments`, `order_reviews`, `customers` | PK | 3 | **compact**, `segment.ms=1h` | lustro tabeli: ostatni stan każdego wiersza |
| `olist.shop.products`, `sellers`, `…translation` (i każda tabela dodana później) | PK | 1 | **compact**, `segment.ms=1h` | małe tabele słownikowe, lustro |
| `olist.transaction` | `{"id": "txId:lsn"}` | 1 | delete, `retention.ms=86400000` (1 dzień) | metadane |
| `__debezium-heartbeat.olist` | `{"serverName":"olist"}` | 1 | delete, `retention.ms=3600000` (1 h) | sygnał życia |
| `connect-*`, `_schemas` | — | 1 | compact | wymagane przez Connect i rejestr (`kafka-init`) |

`segment.ms=3600000` na tematach kompaktowanych: Kafka czyści tylko zamknięte segmenty, a domyślny segment (1 GB / 7 dni) nigdy by się w laboratorium nie zamknął, więc kompaktowanie nie zaszłoby wcale. Co godzinę segment się zamyka i cleaner może zostawić ostatnią wersję każdego klucza (`min.cleanable.dirty.ratio` domyślne 0,5). Stan zweryfikowany `AdminClient`em: `test_kafka.py::test_business_topics_match_the_topic_design`.

- **Jedna reguła: klucz = PK, więc temat jest lustrem i kompaktujemy.** Kluczem jest zawsze PK tabeli ([03-debezium.md](03-debezium.md) §4). Kompaktowanie usuwa pośrednie wersje wiersza, ale nigdy cały wiersz. Pełną historię zmian przechowuje warstwa bronze w Delta.
- **3 partycje to wybór demonstracyjny:** równoległość w Sparku i kolejność per wiersz. Przy około 20 zdarzeniach/s wydajnościowo wystarczyłaby jedna.
- **Tombstones:** po DELETE Debezium wysyła wiadomość z kluczem i pustą wartością, żeby kompaktowanie usunęło klucz. **Wymaganie dla Sparka:** odfiltrować wiadomości z `value = null`.

## 3. Rozszerzenie: tematy jako kod

- **Broker:** `auto.create.topics.enable=false`. Literówka w nazwie tematu daje błąd zamiast pustego tematu.
- **Tematy tworzy Debezium** przez `topic.creation.*` z grupami według wyrażeń regularnych (`topic.creation.groups=large,transaction,heartbeat`; grupa ma jeden zestaw parametrów, więc dwie różne retencje to dwie grupy):
  - `large` (`olist\.shop\.(orders|order_items|order_payments|order_reviews|customers)`): compact, 3 partycje,
  - `transaction` (`olist\.transaction`): delete, 1 dzień; `heartbeat` (`__debezium-heartbeat\.olist`): delete, 1 h,
  - `default` (słowniki i tabele dodane dynamicznie): compact, 1 partycja; `replication.factor=1` wszędzie.
- **Jedno źródło prawdy:** parametry tematów są w konfiguracji konektora w repozytorium. Zmiana parametrów działa tylko dla tematów tworzonych od nowa (Connect nie zmienia istniejących): po edycji grup trzeba usunąć temat albo zrobić `make reset`.
- **Wyjątek:** tabele `shop._*` (sondy testowe) nie są przechwytywane (`table.exclude.list`, [03-debezium.md](03-debezium.md) §13), więc nie dostają tematów. Test: `test_kafka.py::test_new_table_gets_a_topic_from_the_default_group`.

## 4. Rozszerzenie: kolejność per wiersz

Klucz (PK) → hasz (murmur2) → zawsze ta sama partycja. Wszystkie zmiany jednego zamówienia w `orders`:
```
olist.shop.orders  p=1 off=88   key=e481f5…  op=c  created
olist.shop.orders  p=1 off=91   key=e481f5…  op=u  created  → approved
olist.shop.orders  p=1 off=140  key=e481f5…  op=u  approved → shipped
olist.shop.orders  p=1 off=255  key=e481f5…  op=u  shipped  → delivered
```

Kolejność jest gwarantowana dla zmian jednego wiersza. Między tematami i między wierszami nie jest. Spark nie wymaga tego dzięki strażnikowi LSN ([05-spark.md](05-spark.md) §4).

## 5. Rozszerzenie: zarządzanie schematami

- **Jawny tryb zgodności `BACKWARD`:** dodanie pola nullable przechodzi, niezgodna zmiana typu jest odrzucana.
- **Ewolucja schematu** (`ALTER TABLE … ADD COLUMN` z [03-debezium.md](03-debezium.md) §5) daje kolejne wersje subjectu.
- **Pomiar oszczędności** (`test_kafka.py::test_avro_messages_are_smaller_than_json_with_schema`, zdarzenia `olist.shop.orders`): wartość AVRO ma średnio **≈ 285 B** (5 B nagłówka + dane binarne), ten sam ładunek jako JSON bez schematu ≈ 960 B (**AVRO = 30 %**), a jako JSON ze schematem w każdej wiadomości (domyślny `JsonConverter` z `schemas.enable=true`) ≈ 4 050 B (**AVRO = 7 %**). Przy pełnym przebiegu historii (≈ 810 tys. zdarzeń) to ok. 230 MB zamiast 3,3 GB w tematach.
- **Jedna wiadomość = jej własny `schema_id`:** po `DROP COLUMN` rejestr nie tworzy nowej wersji (schemat identyczny z wersją 1 dostaje jej id), więc w temacie leżą obok siebie wiadomości z id wersji 1 i 2, a `latest` nie opisuje najnowszych rekordów. Konsumenci dekodują zawsze schematem o id z nagłówka ([03-debezium.md](03-debezium.md) §5).
- **Endpoint zgodności:** `POST /compatibility/subjects/<subject>/versions/latest` odpowiada `is_compatible` bez rejestrowania; `?verbose=true` podaje powód (np. `MISSING_UNION_BRANCH`). Schemat z polem `int` i `default: null` jest **niepoprawny** (422), nie tylko niezgodny. Test: `test_schema_registry.py::test_compatibility_endpoint_accepts_nullable_field_and_rejects_type_change`.

## 6. Rozszerzenie: consumer walidujący

Usługa `consumer` w compose (`containers/consumer/`, obraz `python:3.12.15-slim-bookworm` + `confluent-kafka[avro]==2.16.0`), w profilu `tools`, więc nie startuje z `docker compose up`: `docker compose run --rm consumer <opcje>`. Działa też z hosta (`cd containers/consumer && uv run python -m consumer …`, domyślnie `localhost:9092` i `http://localhost:8081`; w kontenerze `KAFKA_BOOTSTRAP=kafka:29092`). Ma własny `group.id` (`wpdb-consumer`, a z `--from-beginning` losowy i bez zapisu offsetów), więc nie przeszkadza Sparkowi, który trzyma offsety w checkpoincie. Nigdy nie łączy się z Postgresem.

| Tryb | Co robi | Przykład wyjścia |
|---|---|---|
| podgląd (domyślny) | jedna linia na zdarzenie, klucz i wartość zdekodowane przez rejestr | `[orders] u e481f5 approved → shipped lsn=24023128 tx=771:24023128 schema v1` |
| `--stats` (co `--stats-interval`, domyślnie 5 s) | zdarzenia/s, rozkład `c/u/d/r/t` (t = tombstone), lag konsumenta, widziane id schematów, naruszenia | `olist.shop.orders 4.2/s c=1210 u=3502 d=0 r=329 t=0 lag=0 schemas=10 violations=0` |
| `--validate` | reguły poniżej; naruszenie dopisane do linii zdarzenia (`!! status_transition: …`), raport końcowy | `SUMMARY events=12431 violations=0 decode_errors=0 clock=2017-01-28T23:09:31` |
| `--follow-order <order_id>` (alias `--trace`) | cały cykl życia zamówienia we wszystkich tematach, na końcu oś czasu po `source.lsn` (tombstone tuż po swoim `d`); zdarzenia `customers` są odkładane, dopóki zdarzenie `orders` nie wskaże klienta (tematy są czytane niezależnie) | klient → zamówienie → pozycje → płatność → statusy → recenzja |
| `--json` | jedna linia JSON na zdarzenie (klucz, wartość, id schematów, naruszenia) i końcowe `{"summary": …}`; używane przez testy | |
| `--from-beginning`, `--max-messages N`, `--until-end`, `--idle-timeout S`, `--topics` | tryb jednorazowy: od początku tematów, stop po N wiadomościach, po dojściu do końca każdej partycji (zdarzenie EOF partycji z librdkafka, dokładne także przy lukach po kompaktowaniu; strumień na żywo nigdy nie „milknie”, więc `--idle-timeout` służy tylko do tematów bez ruchu) albo po S sekundach ciszy; `--topics` to regex (`^olist\.shop\..*` domyślnie) lub lista nazw | |

Reguły walidacji (`containers/consumer/consumer/rules.py`, czyste funkcje; testy jednostkowe `tests/unit/test_consumer_rules.py`):
- `before_present`: `op=u` i `op=d` mają niepuste `before` (`REPLICA IDENTITY FULL` działa),
- `status_transition`: przejścia statusów `orders` są dozwolone: ranga rośnie (`created` 0 → `approved` 1 → `invoiced`/`processing`/`unavailable`/`shipped` 2 → `delivered` 3, pominięty krok dozwolony), ta sama wartość (UPDATE bez zmiany) albo `canceled` z dowolnego stanu; `delivered → created` to błąd,
- `amount_non_negative`: `price`, `freight_value`, `payment_value` ≥ 0 (0 dozwolone: 9 płatności w danych),
- `date_not_in_future`: daty zdarzeń (`order_purchase_timestamp`, `order_approved_at`, `order_delivered_*`, `review_creation_date`) nie późniejsze niż zegar symulacji; consumer nie zna zegara z `sim.sim_state` (brak dostępu do bazy), więc szacuje go jako najpóźniejszą datę zdarzenia widzianą dotąd w strumieniu z tolerancją 30 dni (przerwa między preloadem a `SIM_START` to 8–14 dni); `order_estimated_delivery_date` i `shipping_limit_date` to z natury daty przyszłe i nie podlegają regule; wiersze `op=r` (snapshot w kolejności tabeli) tylko przesuwają zegar,
- `schema_id_known`: nagłówek ma bajt magiczny `0x00`, a id schematu istnieje w rejestrze (`GET /schemas/ids/<id>`, z pamięcią podręczną).

Wynik na danych symulatora: **0 naruszeń** (`test_consumer.py::test_consumer_validate_flags_only_the_bad_probe`: jedyne naruszenia pochodzą z celowo zepsutej sondy `test-stage2-bad-*`, `delivered → created`).

## 7. Rozszerzenie: trwałość i ochrona zasobów

- **Jeden węzeł = brak replikacji** (`replication.factor=1` wszędzie, też dla tematów wewnętrznych). Trwałość zapewnia wolumen.
- **Producent w Connect:** `producer.override.acks=all`, `enable.idempotence=true`, `compression.type=zstd` w konfiguracji konektora (worker ma `connector.client.config.override.policy=All`, 08 §3). **Zmierzony współczynnik** (`test_kafka.py::test_producer_overrides_are_applied_and_zstd_compresses`): każda partia rekordów w logu ma `compresscodec: zstd` (`kafka-dump-log`), a `olist.shop.orders` zajmuje na dysku **71–96 %** bajtów wyprodukowanych (1,04–1,4×, zależnie od udziału snapshotu i rozmiaru partii: kompresowana jest partia, a przy kilku zdarzeniach/s partie mają kilka rekordów); AVRO jest już zwarte, zstd zyskuje głównie na powtarzalnych blokach `source`.
- **Limity dysku:** tematy-lustra są ograniczone rozmiarem tabeli (kompaktowanie), tematy metadanych czasem (1 dzień / 1 h); `retention.bytes` nie jest potrzebne (dla samego `compact` i tak nie działa). Pełny przebieg historii to ok. 230 MB AVRO przed kompresją i kompaktowaniem; pełną historię zmian trzyma bronze w Delta.
- **Stały `CLUSTER_ID`:** wolumen sformatowany pod znany identyfikator, więc broker zawsze wstaje na istniejących danych.

## 8. Testy

Zgodnie z zasadą z [00-overview.md](00-overview.md): każda obietnica jest sprawdzana testem automatycznym. Identyfikatory: `tests/unit/test_consumer_rules.py`, `tests/integration/test_kafka.py`, `test_schema_registry.py`, `test_consumer.py` (fixture `probe_lifecycle` jak w [03-debezium.md](03-debezium.md) §11).

| Poziom | Test | Oczekiwany wynik | Identyfikator |
|---|---|---|---|
| jednostkowy | reguły walidacji consumera na spreparowanych zdarzeniach | każda reguła wykrywa swoje naruszenie, poprawne zdarzenia przechodzą; opis zdarzeń, statystyki, śledzenie zamówienia | `test_consumer_rules.py` (27 testów: `test_update_and_delete_need_before`, `test_allowed_status_transitions[…]`, `test_forbidden_status_transitions[…]`, `test_amounts_non_negative`, `test_dates_not_in_the_future_against_the_clock`, `test_schema_id_must_exist_in_registry`, `test_validate_event_runs_every_rule_and_clean_events_pass`, `test_describe_lines_and_tombstone`, `test_stats_count_ops_schemas_and_rates`, `test_order_tracker_follows_the_order_and_its_customer`, …) |
| integracyjny | konfiguracja tematów (AdminClient) | 10 tematów `olist.*`/heartbeat z partycjami, `cleanup.policy`, `retention.ms`, `segment.ms` z tabeli w §2, RF 1; nowa tabela → temat z grupy domyślnej (1 partycja, compact) i subjecty; tabele `_*` bez tematu | `test_kafka.py::test_business_topics_match_the_topic_design`, `::test_new_table_gets_a_topic_from_the_default_group` |
| integracyjny | wysłanie do nieistniejącego tematu | błąd `UNKNOWN_TOPIC_OR_PART` (auto-tworzenie wyłączone) | `test_kafka.py::test_auto_create_disabled`, `::test_unknown_topic_raises_for_admin_describe` (etap 0) |
| integracyjny | DELETE wiersza w `shop.orders` | `op=d` z pełnym `before`, a zaraz po nim tombstone o identycznych bajtach klucza w tej samej partycji | `test_kafka.py::test_delete_gives_d_then_tombstone` |
| integracyjny | kolejne zmiany jednego wiersza | `c, u, d, tombstone` sondy w jednej partycji z rosnącymi offsetami i `source.lsn`; to samo dla zamówień symulatora | `test_kafka.py::test_changes_of_one_row_stay_in_one_partition_in_order`, `test_debezium.py::test_changes_of_one_row_stay_in_one_partition_in_lsn_order` |
| integracyjny (`slow`) | kompaktowanie tematu `order_items` | po kompaktowaniu każda pozycja z Postgresa nadal obecna z aktualną wartością, stare wersje trzech sond usunięte; kompaktowanie wymuszone na czas testu przez `segment.ms=1000` i `min.cleanable.dirty.ratio=0.01` (przywracane), dodatkowe wersje to `UPDATE … SET price = price` | `test_kafka.py::test_compaction_keeps_the_last_version_of_every_item` |
| integracyjny | zgodność schematów | pole nullable z domyślną wartością → `is_compatible: true`; zmiana typu `string → int` i pole wymagane → `false`; nic nie zostaje zarejestrowane | `test_schema_registry.py::test_compatibility_endpoint_accepts_nullable_field_and_rejects_type_change`; ewolucja na żywo: [03-debezium.md](03-debezium.md) §11 |
| integracyjny | subjecty | dokładnie `<temat>-key` i `<temat>-value` dla każdego tematu `olist.*` i heartbeatu; rekordy `Key`/`Envelope`, pola koperty w ustalonej kolejności | `test_schema_registry.py::test_subjects_follow_the_topic_name_strategy`, `::test_subjects_endpoint`, `::test_global_compatibility_backward` |
| integracyjny | listenery | połączenie działa z hosta (`localhost:9092`) i z kontenera (`kafka:29092`) | `test_kafka.py::test_reachable_from_host`, `::test_reachable_from_container` (etap 0) |
| integracyjny | producent Connect | `acks=all`, idempotencja i `zstd` przyjęte przez workera; rozmiar na dysku < bajtów wyprodukowanych | `test_kafka.py::test_producer_overrides_are_applied_and_zstd_compresses` |
| integracyjny | AVRO vs JSON | AVRO < JSON < JSON ze schematem na tych samych zdarzeniach (liczby w §5) | `test_kafka.py::test_avro_messages_are_smaller_than_json_with_schema` |
| integracyjny | consumer (kontener i host) | dekoduje klucze i wartości przez rejestr (`--json`), `--validate` = 0 naruszeń poza celową sondą, `--stats` w ustalonym formacie, `--follow-order` daje 13 zdarzeń cyklu życia sondy w kolejności LSN, uruchomienie z hosta | `test_consumer.py::test_consumer_container_decodes_keys_and_values_through_the_registry`, `::test_consumer_validate_flags_only_the_bad_probe`, `::test_consumer_stats_mode_reports_rates_ops_lag_and_schemas`, `::test_follow_order_prints_the_lifecycle_across_topics`, `::test_consumer_runs_from_the_host_too` |
| e2e (awaria), ⏳ etap 6 | `kill` brokera w trakcie strumienia, restart | Debezium i Spark wznawiają pracę; uzgadnianie Postgres ↔ silver zgodne (brak strat i duplikatów) | `tests/e2e/` (skill `fault-injection`) |

## 9. Podsumowanie

| | Podstawa | Z rozszerzeniami |
|---|---|---|
| Sieć | jeden listener (błąd w tutorialu) | wewnętrzny + zewnętrzny + kontroler |
| Tematy | auto-tworzone, domyślne | projekt per temat jako kod |
| Kompaktowanie | — | wszystkie tematy tabel (klucz = PK → lustro), historia w bronze |
| Schematy | rejestr działa | jawna zgodność BACKWARD, ewolucja, pomiar AVRO vs JSON |
| Consumer | drukuje zdarzenia | podgląd, statystyki, walidacja reguł, śledzenie zamówienia |
| Trwałość | domyślna | acks=all, idempotencja, zstd, limity dysku, stały CLUSTER_ID |
| Weryfikacja | ręczna | testy jednostkowe, integracyjne i e2e z awarią brokera |

## 10. Ustalenia z implementacji (etap 2)

- **Compose:** usługa `consumer` (profil `tools`, `restart: "no"`, bez healthchecku, zależna tylko od `kafka` i `schema-registry`); obraz budowany przez `docker compose --profile tools build consumer`. Pułapka obrazu: `cryptography` ≥ 47 (zależność `authlib` klienta rejestru w `confluent-kafka`) pada z `SIGILL` przy imporcie w maszynie wirtualnej Docker Desktop na Apple Silicon (linuxkit 6.10), dlatego `containers/consumer/pyproject.toml` przypina `cryptography==46.0.7` ([08-operations.md](08-operations.md) §1).
- **Klient rejestru w `confluent-kafka` 2.16:** po tym, jak `AvroDeserializer` pobrał schemat po id, ten sam `SchemaRegistryClient` zwraca z pamięci podręcznej `get_version()` z `schema_id=None`; consumer używa osobnego klienta do metadanych (`consumer/cli.py::Registry`).
- **Tematy i subjecty sond testowych** są usuwane przez testy (`permanent=true` dla subjectów), a tabela sondy dopiero po zapisaniu offsetu konektora ([03-debezium.md](03-debezium.md) §13).
- **Kompaktowanie działa naprawdę po godzinie** (zaobserwowane na stosie starszym niż `segment.ms=1h`): cleaner usuwa wszystkie poza ostatnią wersją klucza w zamkniętych segmentach, więc historia klucza w temacie może zaczynać się od `u` albo składać się z samego tombstone'a, a puste partie (`count: 0`, kodek `none`) zostają, żeby zachować numery sekwencji idempotentnego producenta. Testy, które czytają tematy po fakcie, sprawdzają więc tylko własności zachowywane przez kompaktowanie (żaden klucz z preloadu nie ma `c`, żaden klucz ze strumienia nie ma `r`, co najwyżej jedno `r`/`c` i tylko jako pierwsze zdarzenie klucza, `r` przed strumieniem w każdej partycji, każdy wiersz Postgresa obecny), a testy consumera piszą własną, świeżą sondę (aktywny segment nie jest czyszczony). **Dla Sparka:** pełną historię zmian daje tylko ciągły odczyt; checkpoint starszy niż godzina + kompaktowanie = brak pośrednich wersji w bronze.
- **Zdarzenia/s przy domyślnych ustawieniach:** `olist.shop.orders` ≈ 2–5/s, cały strumień ≈ 8 zmian/s średnio (01 §4), lag consumera przy odczycie na żywo 0.

## Do weryfikacji przy implementacji

- **Rozstrzygnięte (etap 2):** `cp-kafka` 8.3.2, `cp-schema-registry` 8.3.2, konwerter 8.3.2 i Debezium Connect 3.6.3.Final (Kafka 4.3.0) współpracują: snapshot, strumień, rejestracja schematów, kompaktowanie, consumer ([08-operations.md](08-operations.md) §1).
- **Rozstrzygnięte (etap 2):** grupy `topic.creation` nadają różne polityki (§2, §3; test `test_business_topics_match_the_topic_design`); jedna grupa = jeden zestaw parametrów, stąd osobne grupy `transaction` i `heartbeat`; parametry dotyczą tylko tematów tworzonych od nowa.
- **Przeniesione do etapu 3:** zachowanie Sparka przy tombstones (`value IS NULL` do odfiltrowania) i odczycie kompaktowanych tematów od początku (po kompaktowaniu klucz ma tylko ostatnią wersję, więc bronze zbudowane z Kafki po fakcie nie jest pełną historią; pełna historia powstaje tylko przez ciągły odczyt). Kontrakt w [03-debezium.md](03-debezium.md) §13.
