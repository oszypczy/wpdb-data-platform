# Dokumentacja

## Struktura

| Katalog | Zawartość |
|---|---|
| [`design/`](design/) | **Projekt systemu (nasze dokumenty).** Zacznij od [`00-overview.md`](design/00-overview.md). |
| [`requirements/`](requirements/) | Materiały źródłowe: specyfikacja projektu, notatki autora, diagram architektury |
| `tutorials/` | Tutoriale z laboratoriów, **tylko lokalnie** (poza repozytorium). **Zawierają celowe błędy**, więc służą tylko jako kontekst. |

## Projekt systemu (`design/`)

Numeracja odpowiada kolejności w przepływie danych.

| Dokument | Komponent | Status |
|---|---|---|
| [00-overview.md](design/00-overview.md) | Całość: decyzje, przepływ, plan prac | ✅ |
| [architecture.drawio](design/architecture.drawio) / [.png](design/architecture.png) | Diagram C4 (poziom kontenerów): wszystkie kontenery, protokoły i kierunki przepływu | ✅ |
| [01-business-system.md](design/01-business-system.md) | Symulator sklepu (Python), zweryfikowany na danych Olist | ✅ (zaimplementowany, etap 1: `containers/simulator/`) |
| [02-postgresql.md](design/02-postgresql.md) | PostgreSQL | ✅ (zaimplementowany, etapy 1–2: `containers/postgres/`: event trigger `REPLICA IDENTITY FULL`, widoki prawdy `sim.v_truth_*`, polityka indeksów FK, tabela heartbeat `dbz.heartbeat` w publikacji) |
| [03-debezium.md](design/03-debezium.md) | Debezium (Kafka Connect) | ✅ (zaimplementowany, etap 2: `containers/connect/connectors/olist-postgres.json`: snapshot + strumień, pełna koperta, klucz = PK, typy logiczne, heartbeat, metadane transakcji, `topic.creation`; kontrakt dla Sparka w §13) |
| [04-kafka.md](design/04-kafka.md) | Kafka + Schema Registry | ✅ (zaimplementowany, etap 2: tematy jako kod, BACKWARD, pomiar AVRO vs JSON i zstd, consumer walidujący `containers/consumer/`) |
| [05-spark.md](design/05-spark.md) | Spark Structured Streaming | ✅ (zaimplementowany, etap 3: `containers/spark/`: dekodowanie per `schema_id`, bronze exactly-once, silver MERGE ze strażnikiem LSN i miękkim usuwaniem, gold: gwiazda, SCD2, akumulujący snapshot, okna z watermarkiem, lejek i korekty, kwarantanna, metryki i opóźnienie; ustalenia w §12; e2e w etapie 5) |
| [06-minio-delta.md](design/06-minio-delta.md) | MinIO + Delta Lake | ✅ (zaimplementowany, etap 3: kubełek na warstwę, CDF na silver, CHECK, time travel/RESTORE, OPTIMIZE/Z-ORDER/VACUUM z pomiarem, checkpointy w MinIO, **odczyt danych: §9**) |
| [07-dashboard.md](design/07-dashboard.md) | Dashboard Streamlit | ✅ (zaimplementowany, etap 4: `containers/dashboard/`: 9 stron, warstwa zapytań delta-rs + DuckDB na Arrow, odczyt tylko po zmianie wersji Delta, `st.fragment(run_every)`, kartogram stanów z GeoJSON Natural Earth; ustalenia w §5; QA i e2e w etapie 5) |
| [08-operations.md](design/08-operations.md) | Automatyzacja, niezawodność, strategia i zestaw testów (macierz wersji, start, awarie, reset, testy, CI) | ✅ (etapy 0–4; sekcje e2e uzupełnia etap 5) |
| [09-agents.md](design/09-agents.md) | Zespół agentów budujących system: role, skille, MCP, orkiestracja | ✅ |

## Materiały źródłowe (`requirements/`)

- `ProjectDescription.docx`: oficjalna specyfikacja (6 zadań i wymagane rezultaty)
- `notes.txt`: wymagania autora (osobne kontenery, celowe błędy w tutorialach, umiarkowane tempo danych, kolejność budowy)
- `ArchitekturaProjekt.drawio.pdf`: diagram architektury

## Dane

Surowe pliki CSV Olist: [`../data/olist/`](../data/olist/) (źródło i licencja w README).
