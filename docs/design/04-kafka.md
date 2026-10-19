# Kafka + Schema Registry: dziennik zdarzeń

Kafka to **trwały, uporządkowany dziennik zdarzeń** między Debezium ([03-debezium.md](03-debezium.md)) a Sparkiem:
- rozdziela producenta od konsumenta: Spark może być niedostępny, a zdarzenia czekają,
- pozwala odtworzyć dane przez odczyt tematu od początku.

**Schema Registry** przechowuje schematy AVRO. Wiadomość niesie tylko 4-bajtowe ID schematu, a rejestr pilnuje zgodności zmian.

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
| `olist.shop.orders`, `order_items`, `order_payments`, `order_reviews`, `customers` | PK | 3 | **compact** | lustro tabeli: ostatni stan każdego wiersza |
| `olist.shop.products`, `sellers`, `…translation` | PK | 1 | **compact** | małe tabele słownikowe, lustro |
| `olist.transaction` | id transakcji | 1 | delete, 1 dzień | metadane |
| `__debezium-heartbeat.olist` | — | 1 | delete, 1 h | sygnał życia |
| `connect-*`, `_schemas` | — | 1 | compact | wymagane przez Connect i rejestr |

- **Jedna reguła: klucz = PK, więc temat jest lustrem i kompaktujemy.** Kluczem jest zawsze PK tabeli ([03-debezium.md](03-debezium.md) §4). Kompaktowanie usuwa pośrednie wersje wiersza, ale nigdy cały wiersz. Pełną historię zmian przechowuje warstwa bronze w Delta.
- **3 partycje to wybór demonstracyjny:** równoległość w Sparku i kolejność per wiersz. Przy około 20 zdarzeniach/s wydajnościowo wystarczyłaby jedna.
- **Tombstones:** po DELETE Debezium wysyła wiadomość z kluczem i pustą wartością, żeby kompaktowanie usunęło klucz. **Wymaganie dla Sparka:** odfiltrować wiadomości z `value = null`.

## 3. Rozszerzenie: tematy jako kod

- **Broker:** `auto.create.topics.enable=false`. Literówka w nazwie tematu daje błąd zamiast pustego tematu.
- **Tematy tworzy Debezium** przez `topic.creation.*` z grupami według wyrażeń regularnych:
  - grupa `tables-large` (orders, order_items, order_payments, order_reviews, customers): compact, 3 partycje,
  - grupa `meta` (transakcje, heartbeat): delete z krótką retencją,
  - domyślnie (słowniki i tabele dodane dynamicznie): compact, 1 partycja.
- **Jedno źródło prawdy:** parametry tematów są w konfiguracji konektora w repozytorium.

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
- **Pomiar oszczędności:** AVRO (5 bajtów nagłówka) vs JSON ze schematem w każdej wiadomości, na tych samych danych. Wynik trafia do dokumentacji.

## 6. Rozszerzenie: consumer walidujący

Usługa `consumer` w compose, uruchamiana na żądanie (profil compose, nie startuje z `docker compose up`), np. `docker compose run --rm consumer --stats`. Działa też z hosta. Ma własny `group.id`, więc nie przeszkadza Sparkowi, który trzyma offsety w checkpoincie.

| Tryb | Co robi | Przykład wyjścia |
|---|---|---|
| podgląd | czytelny opis zdarzeń | `[orders] u e481f5 approved → shipped lsn=24023128 tx=771 schema v1` |
| `--stats` | co 5 s: zdarzenia/s, rozkład `c/u/d/r`, lag per temat, widziane wersje schematów | `olist.shop.orders 4.2/s c=1210 u=3502 d=0 r=329 lag=0` |
| `--validate` | reguły biznesowe, licznik naruszeń, raport końcowy | `12431 zdarzeń, 0 naruszeń` |
| `--trace <order_id>` | cały cykl życia zamówienia we wszystkich tematach | zakup → pozycje → płatność → statusy → recenzja |

Reguły walidacji:
- `op=u` na `orders` ma niepuste `before` (`REPLICA IDENTITY FULL` działa),
- przejścia statusów są dozwolone (np. `delivered → created` to błąd),
- kwoty ≥ 0, daty nie późniejsze niż zegar symulacji,
- ID schematu istnieje w rejestrze.

## 7. Rozszerzenie: trwałość i ochrona zasobów

- **Jeden węzeł = brak replikacji** (`replication.factor=1` wszędzie, też dla tematów wewnętrznych). Trwałość zapewnia wolumen.
- **Producent w Connect:** `acks=all`, idempotencja (brak duplikatów przy ponowieniach), kompresja `zstd` ze zmierzonym współczynnikiem.
- **Limity dysku:** `retention.bytes` na partycję, `segment.bytes`. Pełny przebieg symulacji to szacunkowo kilkaset MB.
- **Stały `CLUSTER_ID`:** wolumen sformatowany pod znany identyfikator, więc broker zawsze wstaje na istniejących danych.

## 8. Testy

Zgodnie z zasadą z [00-overview.md](00-overview.md): każda obietnica jest sprawdzana testem automatycznym.

| Poziom | Test | Oczekiwany wynik |
|---|---|---|
| jednostkowy | reguły walidacji consumera na spreparowanych zdarzeniach | każda reguła wykrywa swoje naruszenie, poprawne zdarzenia przechodzą |
| integracyjny | konfiguracja tematów (AdminClient) | partycje, `cleanup.policy` i retencja zgodne z tabelą w §2 |
| integracyjny | wysłanie do nieistniejącego tematu | błąd (auto-tworzenie wyłączone) |
| integracyjny | DELETE wiersza w `shop.orders` | w temacie zdarzenie `op=d`, a po nim tombstone |
| integracyjny | kolejne zmiany jednego wiersza | ta sama partycja, kolejność zachowana |
| integracyjny | kompaktowanie tematu `order_items` | po kompaktowaniu każda pozycja zamówienia nadal obecna (ostatnia wersja) |
| integracyjny | zgodność schematów | dodanie pola nullable → nowa wersja; niezgodna zmiana → odrzucona (`is_compatible: false`) |
| integracyjny | listenery | połączenie działa z hosta (`localhost:9092`) i z kontenera (`kafka:29092`) |
| e2e (awaria) | `kill` brokera w trakcie strumienia, restart | Debezium i Spark wznawiają pracę; uzgadnianie Postgres ↔ silver zgodne (brak strat i duplikatów) |

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

## Do weryfikacji przy implementacji

- Spójna wersja `cp-kafka`, `cp-schema-registry` i Debezium Connect.
- Czy grupy `topic.creation` w Debezium poprawnie nadają różne polityki tematom.
- Zachowanie Sparka przy tombstones i odczycie kompaktowanych tematów od początku.
