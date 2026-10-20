# Zespół agentów: kto buduje system i jak

Wymaganie z `requirements/notes.txt`: system powstaje z użyciem kilku agentów. Ten
dokument opisuje zespół, jego narzędzia i sposób pracy. Definicje są kodem w
repozytorium (`.claude/`), więc każdy może uruchomić ten sam zespół.

| Plik | Co zawiera |
|---|---|
| `.claude/agents/*.md` | 6 agentów: rola, narzędzia, model, wstępnie ładowane skille, pamięć |
| `.claude/skills/*/SKILL.md` | 8 skilli: procedury wspólne dla wielu agentów |
| `.mcp.json` + `.claude/mcp/` + `tools/kafka-mcp/` | 4 serwery MCP (narzędzia zewnętrzne agentów) |
| `.claude/workflows/build-stage.js` | orkiestracja jednego etapu: budowa → niezależna weryfikacja → poprawki |

## 1. Zespół

Podział odpowiada przepływowi danych i etapom z [00-overview.md](00-overview.md).
Każdy komponent ma dokładnie jednego właściciela, a każdy właściciel ma inną
specjalizację, więc agenci nie wchodzą sobie w kod.

| Agent | Rola | Komponenty (dokumenty) | Etap | MCP |
|---|---|---|---|---|
| `business-simulator` | specjalista biznesowy | symulator sklepu ([01](01-business-system.md)) | 1 | `postgres` |
| `database-cdc` | specjalista od baz danych i CDC | PostgreSQL, Debezium, Kafka + Schema Registry ([02](02-postgresql.md), [03](03-debezium.md), [04](04-kafka.md)) | 1–2 | `postgres`, `kafka`, `context7` |
| `streaming-lakehouse` | inżynier danych (Spark, Delta) | Spark bronze → silver → gold, MinIO/Delta ([05](05-spark.md), [06](06-minio-delta.md)) | 3–4 | `kafka`, `duckdb`, `context7` |
| `analytics-dashboard` | inżynier analityki | dashboard Streamlit ([07](07-dashboard.md)) | 5 | `duckdb`, `context7` |
| `platform-devops` | integrator / DevOps | `docker-compose.yml`, obrazy, kontenery startowe, healthchecki, macierz wersji, `08-operations.md` | 0 i 6 | `drawio` | `@drawio/mcp@1.5.0` (oficjalny, stdio) | diagramy architektury: otwieranie XML w edytorze, `search_shapes`, odczyt i zapis stron w plikach `.drawio` (`get_page`/`set_page`) | źródło diagramu w repo: `docs/design/architecture.drawio`; eksport PNG lokalnym CLI `drawio -x` |
| `context7` |
| `qa-engineer` | niezależny QA | `tests/` na trzech poziomach, wstrzykiwanie awarii, uzgadnianie Postgres ↔ Delta, macierz pokrycia | każdy | `postgres`, `kafka`, `duckdb` |

Decyzje:
- **„Data science” to u nas inżynieria danych.** W projekcie nie ma uczenia
  maszynowego; jest przetwarzanie strumieniowe, lakehouse i analityka. Stąd dwa
  agenty: `streaming-lakehouse` (Spark + Delta) i `analytics-dashboard` (KPI, UI).
- **Kafka należy do specjalisty od CDC**, nie do Sparka: tematy, klucze i schematy
  są wynikiem konfiguracji Debezium, a etap 2 planu obejmuje je razem.
- **QA jest osobnym agentem i nie poprawia kodu komponentów.** Właściciel nie
  ocenia własnej pracy: zgłasza „gotowe”, a `qa-engineer` zamienia tabelę „Testy”
  z dokumentu w testy `pytest`, uruchamia je i raportuje z dowodami. To realizuje
  zasadę z [00-overview.md](00-overview.md): weryfikacja = testy automatyczne.
- **DevOps idzie pierwszy**: bez szkieletu `docker-compose.yml`, healthchecków i
  harnessu testów właściciele komponentów nie mają gdzie pracować.
- Orkiestratorem jest **główna sesja Claude Code** (tech lead): przydziela etapy,
  przekazuje raporty QA właścicielom, scala wyniki, pilnuje kontraktów.

## 2. Skille (wspólna wiedza proceduralna)

| Skill | Treść | Kto ładuje na starcie |
|---|---|---|
| `design-contract` | dokument projektowy jako kontrakt i definicja ukończenia; układ repozytorium; zasady nienegocjowalne | wszyscy |
| `stack-ops` | nazwy usług, porty, adresy wewnętrzne (kontrakt), start, `wait-healthy.sh`, logi, reset | wszyscy |
| `version-matrix` | procedura sprawdzania zgodności wersji (Confluent, Debezium, Spark/Delta/hadoop-aws, czytelnicy Delta, MinIO) przed przypięciem | `database-cdc`, `streaming-lakehouse`, `platform-devops` |
| `inspect-topic` | podgląd tematów, wiadomości AVRO z kluczami i tombstones, rejestr schematów | `database-cdc`, `streaming-lakehouse`, `qa-engineer` |
| `debezium-connector` | rejestracja `PUT`, status, restart, slot i publikacja po stronie Postgresa, typowe awarie | `database-cdc` |
| `read-delta` | odczyt Delta z MinIO przez DuckDB i delta-rs, time travel, CDF, fixture'y | `streaming-lakehouse`, `analytics-dashboard`, `qa-engineer` |
| `run-tests` | poziomy testów, markery, cele `make`, zasady (brak `sleep`, dokładne asercje, uczciwy raport) | `platform-devops`, `qa-engineer` |
| `fault-injection` | szablon testu e2e: `docker kill` w trakcie strumienia → uzgadnianie z dokładną równością | `qa-engineer` |

Skill wstępnie załadowany trafia do promptu agenta na starcie; pozostałe skille
każdy agent może wywołać sam, gdy ich potrzebuje.

## 3. Serwery MCP (`.mcp.json`)

| Nazwa | Pakiet | Po co | Zasady |
|---|---|---|---|
| `postgres` | `@bytebase/dbhub@1.5.0` (stdio, konfiguracja `.claude/mcp/dbhub.toml`: `[[tools]] execute_sql readonly=true max_rows=500`, źródło `lazy=true`) | sprawdzanie tabel, slotu, publikacji, widoków prawdy | DSN roli `reader` (tylko odczyt `shop` i `sim`), hasło z `READER_PASSWORD`; nigdy rola `shop_app`. Flaga `--readonly` została usunięta z DBHub, tryb tylko do odczytu jest w TOML |
| `kafka` | własny serwer `tools/kafka-mcp/` (Python, `mcp` 2.3.0 + `confluent-kafka` 2.16.0; start `uv run --project tools/kafka-mcp kafka-mcp`) | `list_topics`, `describe_topic`, `consume` (AVRO dekodowane przez Schema Registry, klucz i wartość, tombstone → `null`), `list_subjects`, `get_schema` | tylko podgląd (grupa konsumenta jednorazowa, bez commitów); tematy tworzy `kafka-init`. Zastępuje `@confluentinc/mcp-confluent`, którego natywna zależność `@confluentinc/kafka-javascript` nie ma binarki dla Node 26/arm64 |
| `duckdb` | `mcp-server-motherduck==1.1.0` (`--db-path :memory: --read-write --init-sql .claude/mcp/duckdb-init.sql`) | zapytania do tabel Delta w MinIO (`delta_scan`) | baza w pamięci, więc `--read-write` niczego nie ryzykuje (serwer odmawia `:memory:` bez tej flagi); `init-sql` ładuje `delta`, `httpfs`, `aws` i tworzy sekret S3 `minio` z łańcucha `env` (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` = konto `agent` tylko do odczytu na wszystkich kubełkach, bo DuckDB przez `httpfs` potrafi też zapisywać do S3, ustawiane w `.mcp.json` z domyślnymi z `.env.example`) |
| `context7` | `@upstash/context7-mcp` | aktualna dokumentacja bibliotek przy ustalaniu wersji i API | wersje zawsze z dokumentacji, nie z pamięci modelu |

Celowo **bez** MCP do Dockera i GitHuba: `docker compose` i `gh` przez Bash są
prostsze i pewniejsze, a każdy agent ma Bash. Dostęp do Postgresa dla agentów nie
łamie zasady „komponenty downstream nie łączą się z Postgresem”: to ta sama
ścieżka co odczyt uzgadniający w testach, przez rolę tylko do odczytu.
Konsekwencja dla [02-postgresql.md](02-postgresql.md) §4: dochodzi rola `reader`
(SELECT na `shop` i `sim`) dla testów i agentów.

Potwierdzone przy pierwszym uruchomieniu (etap 0, 2026-10-20): DBHub nie ma już flagi
`--readonly` (tryb tylko do odczytu w `dbhub.toml`), `mcp-server-motherduck` wymaga
`--read-write` dla `:memory:` i traci sekret między połączeniami (stąd `--init-sql`
zamiast `PERSISTENT SECRET`), a `mcp-confluent` nie startuje na Node 26/arm64 (stąd
własny serwer `tools/kafka-mcp/`).

## 4. Sposób pracy: kręgosłup sekwencyjny, krawędzie równoległe

Rozważane warianty:

| Wariant | Ocena |
|---|---|
| wszyscy naraz, każdy swoją część | ❌ etapy zależą od siebie (kolejność budowy jest wymaganiem), jest jeden stos Dockera na maszynę (stałe porty), a integracja odkłada się na koniec, gdzie błędy są najdroższe |
| ściśle sekwencyjnie, etap po etapie | ✅ bezpiecznie, ❌ wolno; marnuje to, że logika Sparka i dashboardu zależy tylko od kontraktów w dokumentach, nie od działającego strumienia |
| **hybryda** (wybrana) | integracja idzie sekwencyjnie po etapach; równolegle do niej powstaje czysta logika kolejnych etapów na fixture'ach |

Przebieg:

1. **Etap 0, `platform-devops`:** szkielet: `docker-compose.yml` z usługami
   infrastruktury (Postgres, Kafka, Schema Registry, Connect, MinIO), healthchecki,
   kontenery startowe, `.env.example`, harness `pytest` z markerami, `Makefile`,
   pierwsza macierz wersji w `08-operations.md`. Kryterium: `make up` → wszystko
   zdrowe; `make test-unit` działa (na pusto).
2. **Etapy 1–5 po kolei na kręgosłupie integracyjnym.** Każdy etap to jedno
   uruchomienie `build-stage`: właściciel buduje → `qa-engineer` weryfikuje tabelę
   „Testy” → do dwóch rund poprawek → tech lead scala. Następny etap startuje
   dopiero, gdy poprzedni ma zielone testy jednostkowe i integracyjne.
3. **Równolegle, w osobnych worktree'ach git**, powstaje to, co zależy tylko od
   kontraktu: czyste transformacje Sparka (dekodowanie Confluent AVRO, MERGE, SCD2,
   okna) na lokalnej Delcie; warstwa zapytań dashboardu na fixture'ach Delta;
   reguły walidacji consumera. Te worktree'e nie uruchamiają `docker compose`.
4. **Etap 6, `platform-devops` + `qa-engineer`:** testy e2e z awariami,
   dokumentacja wdrożenia, CI.

Kontrakty między agentami to dokumenty projektowe: schemat tabel (01/02), koperta
Debezium, nazwy tematów i klucz = PK (03/04), układ i schematy tabel Delta (05/06),
nazwy usług i porty (skill `stack-ops`). Zmiana kontraktu = najpierw zmiana
dokumentu, potem powiadomienie właścicieli przez tech leada.

## 5. Uruchamianie

```bash
# w głównej sesji Claude Code, w katalogu projektu
/build-stage        # workflow z args, np. {"stage":1,"owner":"business-simulator",
                    #   "docs":["docs/design/01-business-system.md","docs/design/02-postgresql.md"],
                    #   "scope":"symulator + init SQL Postgresa"}
```

Bez workflowów (plan bez tej funkcji) tech lead wywołuje agentów ręcznie przez
narzędzie Agent w tej samej kolejności: właściciel → `qa-engineer` → poprawki.
Pamięć agentów (`memory: project`) trzyma w `.claude/agent-memory/<agent>/`
ustalenia między sesjami, np. przypięte wersje i powody.

## 6. Testy tego dokumentu

Zespół agentów nie ma zachowań do testowania automatycznie; sprawdzalne są jego
produkty. Dowodem, że podział zadziałał, jest macierz pokrycia w
`08-operations.md`: każda obietnica z dokumentów ma identyfikator testu i autora
(właściciel pisze testy jednostkowe czystej logiki, QA resztę).
