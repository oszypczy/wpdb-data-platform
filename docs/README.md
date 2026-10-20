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
| [01-business-system.md](design/01-business-system.md) | Symulator sklepu (Python), zweryfikowany na danych Olist | ✅ |
| [02-postgresql.md](design/02-postgresql.md) | PostgreSQL | ✅ |
| [03-debezium.md](design/03-debezium.md) | Debezium (Kafka Connect) | ✅ |
| [04-kafka.md](design/04-kafka.md) | Kafka + Schema Registry | ✅ |
| [05-spark.md](design/05-spark.md) | Spark Structured Streaming | ✅ |
| [06-minio-delta.md](design/06-minio-delta.md) | MinIO + Delta Lake | ✅ |
| [07-dashboard.md](design/07-dashboard.md) | Dashboard Streamlit | ✅ |
| [08-operations.md](design/08-operations.md) | Automatyzacja, niezawodność, strategia i zestaw testów (macierz wersji, start, awarie, reset, testy, CI) | ✅ (etap 0; sekcje e2e uzupełnia etap 6) |
| [09-agents.md](design/09-agents.md) | Zespół agentów budujących system: role, skille, MCP, orkiestracja | ✅ |

## Materiały źródłowe (`requirements/`)

- `ProjectDescription.docx`: oficjalna specyfikacja (6 zadań i wymagane rezultaty)
- `notes.txt`: wymagania autora (osobne kontenery, celowe błędy w tutorialach, umiarkowane tempo danych, kolejność budowy)
- `ArchitekturaProjekt.drawio.pdf`: diagram architektury

## Dane

Surowe pliki CSV Olist: [`../data/olist/`](../data/olist/) (źródło i licencja w README).
