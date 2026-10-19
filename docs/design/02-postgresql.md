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

**Automatyzacja dla dynamicznych tabel:** event trigger (`ON ddl_command_end`) wykonuje `ALTER TABLE … REPLICA IDENTITY FULL` dla każdej nowej tabeli w schemacie `shop`.

**Kompromis:** większy WAL. Przy ruchu rzędu dziesiątek zdarzeń na sekundę jest to pomijalne.

## 3. Rozszerzenie: publikacja dla całego schematu

```sql
CREATE PUBLICATION dbz_publication FOR TABLES IN SCHEMA shop;   -- PostgreSQL 15+
```

Publikację tworzy skrypt startowy, a Debezium tylko z niej korzysta (`publication.autocreate.mode=disabled`). Każda nowa tabela w `shop` automatycznie trafia do CDC. Zgadza się to z dynamicznym tworzeniem tabel i z demo ewolucji schematu (`ALTER TABLE ADD COLUMN`).

## 4. Rozszerzenie: schematy i role z minimalnymi uprawnieniami

| Schemat | Zawartość | W CDC |
|---|---|---|
| `shop` | tabele biznesowe (customers, orders, order_items, …) | tak |
| `sim` | `sim_state` (zegar symulacji), widoki uzgadniania, tabela heartbeat | nie (heartbeat: patrz §5) |

| Rola | Uprawnienia |
|---|---|
| `shop_app` (symulator) | DDL i DML w `shop`, zapis w `sim` |
| `debezium` | `REPLICATION` + `SELECT` na `shop` || `postgres` | tylko skrypty startowe |

## 5. Rozszerzenie: bezpieczeństwo slotu replikacyjnego

Slot zatrzymuje WAL, dopóki Debezium go nie odczyta. Jeśli Connect leży, a symulator pisze, WAL rośnie bez ograniczeń i może zapchać dysk Dockera.

- **`max_slot_wal_keep_size=2GB`:** po przekroczeniu limitu slot zostaje unieważniony zamiast zapychać dysk. Debezium musi wtedy wykonać ponowny snapshot, co trzeba uwzględnić w konfiguracji `snapshot.mode`.
- **Heartbeat:** Debezium cyklicznie wykonuje mały UPDATE na tabeli heartbeat (`heartbeat.action.query`), żeby slot przesuwał się także w okresach ciszy. Szczegóły, w tym czy tabela musi być w publikacji, zostaną ustalone przy Debezium.
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

## 9. Rozszerzenie: konfiguracja jako kod i strojenie

- **Skrypty w `/docker-entrypoint-initdb.d/`:** schematy, role, publikacja, event trigger i widoki. Wykonują się przy pustym wolumenie, więc `docker compose down -v && docker compose up` zawsze daje identyczny stan.
- **Strojenie pod laptopa:** `shared_buffers`, `max_wal_size`, `wal_compression=on`, z uzasadnieniem w komentarzach.

## 10. Testy

| Poziom | Test | Oczekiwany wynik |
|---|---|---|
| integracyjny | konfiguracja serwera | `wal_level=logical`, ustawiony `max_slot_wal_keep_size` |
| integracyjny | nowa tabela w `shop` | automatycznie `REPLICA IDENTITY FULL` (event trigger) i obecna w `dbz_publication` |
| integracyjny | uprawnienia roli `debezium` | odczyt i replikacja działają; INSERT/UPDATE/DDL w `shop` odrzucone |
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

- Macierz zgodności wybranej wersji Debezium z PostgreSQL 16. `FOR TABLES IN SCHEMA` wymaga PG 15 lub nowszego.
- Uprawnienia wymagane przez Debezium przy publikacji utworzonej wcześniej (`publication.autocreate.mode=disabled`) i przy snapshocie.
- Zachowanie Debezium po unieważnieniu slotu (`max_slot_wal_keep_size`) i dobór `snapshot.mode`.
