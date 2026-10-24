# Przegląd systemu: Mini Data Platform (analityka e-commerce)

Projekt oddawany jako **jeden działający ekosystem**:
- **Ocena podstawowa:** system działa end-to-end po `docker compose up`.
- **Punkty dodatkowe:** rozszerzenia **w ramach istniejących komponentów**, a nie nowe komponenty.

## Decyzje (2026-10-18)

| Obszar | Decyzja |
|---|---|
| Dane | **Olist Brazilian E-Commerce** (prawdziwe dane, CC BY-NC-SA 4.0), odtwarzane w przyspieszonym czasie |
| Serializacja | **AVRO + Confluent Schema Registry** |
| Spark | jeden kontener, `local[*]` (single node), architektura medalionowa |
| Wizualizacja | **Dashboard Streamlit** (kontener czytający warstwę gold z MinIO) |

## Przepływ

```
Olist CSV ─► Business System ─► PostgreSQL ─► Debezium ─► Kafka + Schema Registry ─► Spark ─► MinIO (Delta) ─► Streamlit
             replay cyklu        logical       CDC          tematy per tabela         bronze → silver → gold
             życia zamówień      replication
```

## Komponenty

Diagram C4 na poziomie kontenerów: [architecture.drawio](architecture.drawio) (PNG: [architecture.png](architecture.png)); diagram ogólny z wymagań: `../requirements/ArchitekturaProjekt.drawio.pdf`.


Szczegóły każdego komponentu znajdują się w osobnym dokumencie. Tutaj są tylko najważniejsze punkty.

| # | Komponent | Podstawa (wymagane) | Najważniejsze rozszerzenia |
|---|---|---|---|
| 1 | [Business System](01-business-system.md) | 3 CSV → dynamiczne tabele → INSERT | detekcja typów/PK/FK; replay cyklu życia zamówień (`c/u/d`); kontrola tempa z backpressure; wznawianie po restarcie |
| 2 | [PostgreSQL](02-postgresql.md) | `wal_level=logical` | `REPLICA IDENTITY FULL` przez event trigger; publikacja schematu; role z minimalnymi uprawnieniami; ochrona slotu; transakcja = zakup; widoki uzgadniania |
| 3 | [Debezium](03-debezium.md) | konektor Postgres, AVRO (własny obraz z konwerterem) | snapshot (`op=r`) + strumień; pełna koperta dla `before`; klucz = PK (strumień sam się opisuje); ewolucja schematu na żywo; typy daty i kwot; heartbeat i metadane transakcji; idempotentna rejestracja (`PUT`) |
| 4 | [Kafka + Schema Registry](04-kafka.md) | KRaft single node, dwa listenery, consumer AVRO z walidacją | projekt tematów jako kod (wszystkie tematy tabel kompaktowane jako lustra); kolejność per wiersz; zgodność BACKWARD; consumer: statystyki, reguły, śledzenie zamówienia; acks=all + idempotencja + zstd |
| 5 | [Spark](05-spark.md) | odczyt z Kafki, deserializacja AVRO, transformacja | dekodowanie per `schema_id` (ewolucja); **bronze** (exactly-once, `txnAppId`) → **silver** (MERGE ze strażnikiem LSN, miękkie usuwanie, PK ze schematu klucza) → **gold** (gwiazda, fakt z akumulującym snapshotem, SCD2, okna z watermarkiem, lejek i korekty); kwarantanna; metryki i opóźnienie e2e |
| 6 | [MinIO + Delta](06-minio-delta.md) | zapis w formacie Delta, dokumentacja odczytu | kubełek per warstwa + konta per usługa (idempotentny `minio-init`); audyt, time travel, RESTORE; CDF na silver; CHECK; OPTIMIZE/Z-ORDER/VACUUM z pomiarem; checkpointy w MinIO (bezstanowy Spark); odczyt przez DuckDB i delta-rs |
| 7 | [Dashboard (Streamlit)](07-dashboard.md) | odczyt gold, wykresy, odświeżanie | 9 stron (biznes, mapa, lejek, logistyka, klienci SCD2, zdrowie potoku, podróż w czasie); tylko Delta przez delta-rs/DuckDB; odczyt tylko po zmianie wersji tabeli; Grafana rozważona i odrzucona (brak źródła Delta) |
| 8 | Operacje i testy | jeden `docker compose up` | healthchecki + `service_healthy`; kontenery startowe; zestaw testów automatycznych (patrz niżej) |

## Zasada: weryfikacja = testy automatyczne

Każda obietnica z dokumentów projektowych, np. „po awarii nic nie ginie”, „zmiana schematu przechodzi bez przestoju” czy „brak duplikatów”, jest sprawdzana **testem automatycznym**, a nie scenariuszem uruchamianym ręcznie na prezentacji. Każdy dokument komponentu ma sekcję **Testy** z podziałem na poziomy:

| Poziom | Zakres | Środowisko |
|---|---|---|
| jednostkowy | logika bez infrastruktury (detekcja typów, generowanie zdarzeń, transformacje Sparka na małych DataFrame, reguły walidacji) | bez Dockera, szybkie |
| integracyjny | współpraca kilku komponentów (np. Postgres + Debezium + Kafka + Schema Registry) | wybrane kontenery |
| e2e | cały system, w tym wstrzykiwanie awarii (`kill` kontenera w trakcie strumienia) i uzgadnianie Postgres ↔ silver | pełny `docker compose` |

Narzędzia i sposób uruchamiania (`pytest` z markerami `unit`/`integration`/`e2e`, cele `make test-*`) są opisane w [08-operations.md](08-operations.md) §9.

## Plan prac

Po każdym etapie system działa i jest zweryfikowany.

| Etap | Zakres | Projekt | Implementacja |
|---|---|---|---|
| 0 | Szkielet platformy: `docker-compose.yml` z infrastrukturą (Postgres, Kafka KRaft, Schema Registry, Connect z konwerterem AVRO, MinIO), healthchecki, kontenery startowe, `.env.example`, harness `pytest`, `Makefile`, CI, macierz wersji ([08](08-operations.md)) | ✅ | ✅ |
| 1 | Dane + Business System + PostgreSQL | ✅ | ✅ (2026-10-20; zweryfikowane przez `qa-engineer`: 40 testów jednostkowych, 49 integracyjnych) |
| 2 | Debezium + Kafka + Schema Registry (+ consumer) | ✅ | ✅ (2026-10-21; zweryfikowane przez `qa-engineer`: 69 testów jednostkowych, 82 integracyjne; `containers/connect/connectors/olist-postgres.json`, `containers/consumer/`, `20-heartbeat.sql`; kontrakt dla Sparka w [03](03-debezium.md) §13) |
| 3 | Spark + MinIO/Delta: bronze → silver → gold (dawne etapy 3 i 4 scalone 2026-10-21, jedna weryfikacja QA) | ✅ | ✅ (2026-10-23, zbudowane przez `streaming-lakehouse`, weryfikacja `qa-engineer` razem z całością na końcu (decyzja 2026-10-24); `containers/spark/`, ustalenia w [05](05-spark.md) §12 i [06](06-minio-delta.md) §7–§9; e2e w etapie 5) |
| 4 | Dashboard Streamlit | ✅ | ✅ (2026-10-24, zbudowany przez `analytics-dashboard`, weryfikacja `qa-engineer` razem z etapami 3–5 na końcu; `containers/dashboard/`: 9 stron, warstwa zapytań delta-rs + DuckDB, odczyt tylko po zmianie wersji tabeli, mapa z GeoJSON Natural Earth; 41 testów jednostkowych (w tym 2 testy compose), 14 integracyjnych; e2e w etapie 5) |
| 5 | Testy e2e z awariami i uzgadnianiem Postgres ↔ Delta na docelowej wersji platformy, jedna pełna weryfikacja `qa-engineer` etapów 3–5, dokumentacja wdrożenia | ⏳ | ⏳ |

Decyzja 2026-10-21: testy e2e (wiersze „e2e” w sekcjach Testy, dziś ⏳) powstają dopiero na końcu, gdy cała platforma jest w wersji docelowej.

## Otwarte kwestie

- Dashboard to dodatkowy kontener poza diagramem z `requirements/`. Trzeba potwierdzić z prowadzącym, że nie liczy się jako „nowy komponent”.
- Plik geolocation (58 MB) jest poza repozytorium (`.gitignore`), bo mapa w dashboardzie korzysta z GeoJSON stanów (etap 4: Natural Earth, domena publiczna, [07-dashboard.md](07-dashboard.md) §4). Wraca tylko, jeśli okaże się potrzebny.
- **Rozstrzygnięte (etap 1):** domyślne `SIM_SPEED=1440`, `MAX_EVENTS_PER_SEC=20`, okno 2017-01-05 → 2017-03-31; zestaw na prezentację (Black Friday) w [01-business-system.md](01-business-system.md) §4 i `.env.example`.
