# MinIO + Delta Lake: jezioro danych

**MinIO** to magazyn obiektów zgodny z S3, czyli „dysk” jeziora danych. **Delta Lake** nadaje plikom zachowanie tabel: transakcje ACID, historię wersji i kontrolę schematu.

Podział odpowiedzialności: [05-spark.md](05-spark.md) opisuje, *co* jest liczone. Ten dokument opisuje, *gdzie i jak* jest przechowywane, jak tym zarządzać i jak to odczytać.

## 1. Część podstawowa

- **MinIO** w compose: przypięta wersja obrazu, wolumen, healthcheck `/minio/health/live`.
- **Dostęp Sparka przez S3A:** endpoint `http://minio:9000`, `fs.s3a.path.style.access=true`, `fs.s3a.connection.ssl.enabled=false`.
- **Zapis w formacie Delta.**
- **Dokumentacja odczytu danych** (§8): wymóg specyfikacji.

## 2. Rozszerzenie: kubełek na warstwę

```
bronze/        orders/  order_items/  ...
silver/        orders/  order_items/  ...            (CDF włączony)
gold/          fact_order_items/  fact_orders_lifecycle/  dim_customer/  agg_revenue_hourly/ ...
ops/           stream_metrics/  quarantine/
checkpoints/   kafka_to_bronze/  bronze_to_silver/  silver_to_gold/
```

Osobne kubełki zamiast prefiksów w jednym pozwalają na **uprawnienia per warstwa** (ta sama zasada co role w Postgresie):

| Konto (access key) | Uprawnienia |
|---|---|
| `spark` | odczyt i zapis we wszystkich kubełkach |
| `dashboard` | tylko odczyt `gold` i `ops` |
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

## 7. Rozszerzenie: utrzymanie tabel

Trigger co 10 s tworzy dużo drobnych plików.

- **OPTIMIZE + Z-ORDER** (np. `fact_order_items` po `customer_state, purchase_date`): łączy pliki i układa dane pod typowe filtry. Pomiar liczby plików i czasu zapytania przed i po trafia do dokumentacji.
- **VACUUM:** usuwa pliki starsze niż retencja. Retencja per warstwa (gold 7 dni, bronze dłużej) to świadomy kompromis między zasięgiem time travel a miejscem.
- **Uruchamianie:** okresowe zadanie wsadowe w kontenerze Sparka (np. co 30 min), bez nowego komponentu.
- **Antywzorzec:** reguły cyklu życia MinIO (automatyczne usuwanie obiektów) na tabelach Delta uszkodziłyby tabelę. Pliki Delta usuwa wyłącznie VACUUM.
- **Partycjonowanie świadomie minimalne:** przy setkach MB partycje tworzą głównie małe pliki (Delta rekomenduje je dopiero od około 1 GB). Zamiast tego Z-ORDER lub liquid clustering. Wyjątek: bronze partycjonowany po dacie ingestii.

## 8. Rozszerzenie: checkpointy w MinIO

Checkpointy wszystkich zapytań strumieniowych są w kubełku `checkpoints`. Cały stan przetwarzania (dane i pozycje strumieni) jest w MinIO, więc **kontener Sparka jest bezstanowy**: po usunięciu i ponownym utworzeniu wznawia pracę od miejsca, w którym skończył. Koszt to wolniejsze checkpointy niż na dysku lokalnym. MinIO ma silną spójność, a poprawność sprawdza test e2e.

## 9. Rozszerzenie: odczyt danych bez Sparka

**DuckDB z hosta:**
```sql
INSTALL delta; LOAD delta;
CREATE SECRET (TYPE s3, KEY_ID 'dashboard', SECRET '…', ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL false);
SELECT customer_state, sum(price) FROM delta_scan('s3://gold/fact_order_items') GROUP BY 1 ORDER BY 2 DESC LIMIT 5;
```

**Python `deltalake` (delta-rs), z time travel:**
```python
DeltaTable("s3://gold/fact_order_items", version=120, storage_options={...}).to_pandas()
```

**Konsola MinIO** (`localhost:9001`): przeglądanie kubełków i plików.

Dashboard również czyta przez delta-rs lub DuckDB, bez Sparka.

**Zgodność protokołu:** niektóre funkcje Delta (np. deletion vectors, liquid clustering) podnoszą wersję protokołu tabeli, której starsi czytelnicy nie odczytają. W gold i ops włączamy tylko funkcje obsługiwane przez delta-rs i DuckDB. Sprawdza to test.

## 10. Rozszerzenie: sekrety

- Klucze `spark` i `dashboard` w `.env` (poza repozytorium).
- W repozytorium `.env.example` z opisem zmiennych.
- Hasło root wyłącznie dla `minio-init`.

## 11. Testy

| Poziom | Test | Oczekiwany wynik |
|---|---|---|
| integracyjny | `minio-init` uruchomiony dwa razy | kubełki, polityki i konta istnieją; brak błędów |
| integracyjny | uprawnienia konta `dashboard` | odczyt `gold` działa; zapis do `gold` i odczyt `bronze` odrzucone |
| integracyjny | zapis przez Sparka, odczyt przez delta-rs i DuckDB | identyczne wyniki |
| integracyjny | time travel | `VERSION AS OF n` zwraca stan sprzed późniejszego MERGE |
| integracyjny | `RESTORE` | tabela wraca do wskazanej wersji |
| integracyjny | CDF po MERGE | zmiany z typami `update_preimage`/`update_postimage`/`delete` |
| integracyjny | zapis łamiący `CHECK` | odrzucony |
| integracyjny | niezgodny typ kolumny / nowa kolumna z `mergeSchema` | odrzucony / przyjęta |
| integracyjny | OPTIMIZE | mniej plików, dane identyczne (suma kontrolna) |
| integracyjny | VACUUM | usuwa tylko pliki starsze niż retencja; time travel w oknie retencji działa |
| e2e | usunięcie i ponowne utworzenie kontenera Sparka | wznowienie z checkpointów w MinIO, brak duplikatów, uzgadnianie zgodne |
| e2e (awaria) | restart MinIO w trakcie strumienia | Spark ponawia zapis; uzgadnianie zgodne |

## 12. Podsumowanie

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

- **Dostępność obrazów MinIO community.** W 2025 roku MinIO zmieniło zasady dystrybucji i ograniczyło konsolę. Specyfikacja wymaga MinIO, więc priorytetem jest przypięta, dostępna wersja obrazu z potrzebnymi funkcjami. Zamiennik zgodny z S3 tylko w ostateczności i po konsultacji z prowadzącym.
- Liquid clustering w wybranej wersji Delta OSS.
- Funkcje Delta obsługiwane przez delta-rs i DuckDB (wersje protokołu).
- Wydajność i poprawność checkpointów na S3A.
