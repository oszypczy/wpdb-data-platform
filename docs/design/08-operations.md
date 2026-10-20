# Operacje: automatyzacja, niezawodność i testy

Ten dokument opisuje, jak cały system wstaje jednym `docker compose up`, jak się zachowuje przy awariach, jak go resetować i jak uruchamiać testy. Właściciel: `platform-devops` (razem z `qa-engineer` dla sekcji o testach). Kontrakt nazw usług i portów jest w skillu `stack-ops` (`.claude/skills/stack-ops/SKILL.md`); ten dokument i skill zmieniają się razem.

Stan: **etap 0 ukończony 2026-10-20** (infrastruktura: Postgres, Kafka, Schema Registry, Connect, MinIO, kontenery startowe, harness testów, CI). Sekcje dotyczące potoku (symulator, Spark, dashboard, testy e2e z awariami) są uzupełniane w kolejnych etapach.

## 1. Macierz wersji

Procedura: skill `version-matrix`. Kotwica linii Kafki to Debezium: `connect:3.6.3.Final` zawiera Kafkę 4.3.0, więc broker, rejestr i konwerter AVRO pochodzą z Confluent Platform 8.3.x, jedynej linii CP opartej na Kafce 4.3. Kotwica Sparka (etap 3) to 3.5.9 (Scala 2.12, Hadoop 3.3.4), stąd Delta 3.3.3 (Delta 4.x jest tylko dla Sparka 4). Wszystkie obrazy mają manifest `arm64` i `amd64` (sprawdzone `docker manifest inspect` 2026-10-20).

| Komponent | Obraz / biblioteka | Wersja | Zgodne z | Dowód |
|---|---|---|---|---|
| PostgreSQL | `postgres` | `16.15` | Debezium 3.6 wspiera PG 14–18; `FOR TABLES IN SCHEMA` wymaga ≥ 15 | [debezium.io/releases/3.6](https://debezium.io/releases/3.6/) „Compatibility” |
| Debezium Connect | `quay.io/debezium/connect` | `3.6.3.Final` | Kafka 4.3.0 w obrazie (`/kafka/libs/kafka-clients-4.3.0.jar`), Java 25, użytkownik `kafka` (uid 1001) | sprawdzone `docker run … ls /kafka/libs`; test `test_connect.py::test_worker_kafka_version_matches_broker_line` |
| Konwerter AVRO | Confluent Hub `confluentinc/kafka-connect-avro-converter` | `8.3.2` (zip, 38 JAR-ów łącznie z `kafka-schema-registry-client-8.3.2`) | Schema Registry 8.3.2; klasa `io.confluent.connect.avro.AvroConverter`; obraz Debezium ≥ 2.0 nie ma konwertera, więc `containers/connect/Dockerfile` rozpakowuje zip do `/kafka/connect/confluent-avro-converter/` | [hub-downloads.confluent.io](https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-avro-converter/versions/8.3.2/confluentinc-kafka-connect-avro-converter-8.3.2.zip) (HTTP 200); test `test_connect.py::test_plugins_include_avro_converter` |
| Kafka | `confluentinc/cp-kafka` | `8.3.2` | Kafka 4.3.x, KRaft combined (`KAFKA_PROCESS_ROLES=broker,controller`), CLI `kafka-topics` na `PATH` | [docs.confluent.io: CP 8.3 ↔ Kafka 4.3](https://docs.confluent.io/platform/current/installation/versions-interoperability.html); `cp-all-in-one` v8.3.2 |
| Schema Registry | `confluentinc/cp-schema-registry` | `8.3.2` | ta sama linia CP co broker i konwerter | j.w. |
| Kafka Connect: sekrety | `org.apache.kafka.common.config.provider.EnvVarConfigProvider` | w Kafce 4.3 | `${env:DEBEZIUM_DB_PASSWORD}` w JSON konektora | [Kafka KIP-887](https://cwiki.apache.org/confluence/display/KAFKA/KIP-887) |
| MinIO + mc | własny `wpdb/minio` ze źródeł | `minio` `RELEASE.2025-10-15T17-29-55Z`, `mc` `RELEASE.2025-08-13T08-35-41Z` | builder `golang:1.24.13-bookworm` (`go.mod` minio: `go 1.24.0`, mc: `go 1.23.0`), runtime `debian:bookworm-slim` | §13; test `test_minio.py::test_server_version_matches_pinned_tag` |
| curl (connect-init) | `curlimages/curl` | `8.17.0` | busybox `sh`, bez `jq` (skrypt używa `grep`) | `docker run … curl --version` |
| Python w kontenerach | `python` | `3.12.15-slim-bookworm` | PySpark 3.5 nie wspiera 3.13; `kafka-python==2.0.2` pada na ≥ 3.12, więc `confluent-kafka` | [spark.apache.org/docs/3.5.9](https://spark.apache.org/docs/3.5.9/) „Python 3.8+”; `pyproject.toml` |
| Python na hoście | uv-managed CPython | `3.12.x` (`.python-version`) | jak wyżej; host 3.13/3.14 nie jest używany do venv projektu | `uv python install 3.12` |
| Biblioteki testów (root `pyproject.toml`) | `pytest` `9.1.1`, `pytest-timeout` `2.4.0`, `psycopg[binary]` `3.3.6`, `confluent-kafka[avro]` `2.16.0`, `requests` `2.34.2`, `boto3` `1.43.115`, `python-dotenv` `1.2.4`, `pyyaml` `6.0.3`, `ruff` `0.17.1` | | koła `cp312` `macosx_arm64`/`manylinux` na PyPI | `uv.lock` |
| MCP `kafka` (`tools/kafka-mcp`) | `mcp` `2.3.0`, `confluent-kafka[avro]` `2.16.0` | | | `tools/kafka-mcp/uv.lock` |
| MCP `postgres` / `duckdb` | `@bytebase/dbhub` `1.5.0`, `mcp-server-motherduck` `1.1.0` | | [09-agents.md](09-agents.md) §3 | `.mcp.json` |
| Spark (etap 3, **zaplanowane**) | `apache/spark:3.5.9-scala2.12-java17-python3-ubuntu` + `spark-sql-kafka-0-10_2.12:3.5.9`, `spark-avro_2.12:3.5.9`, `delta-spark_2.12:3.3.3`, `delta-storage:3.3.3`, `hadoop-aws:3.3.4`, `aws-java-sdk-bundle:1.12.262`; pip `pyspark==3.5.9`, `delta-spark==3.3.3` | | Hadoop 3.3.4 w Sparku 3.5.x; `hadoop-aws` POM → `aws-java-sdk-bundle 1.12.262` | artefakty 200 na Maven Central; właściciel `streaming-lakehouse` potwierdza przy etapie 3 |
| Czytelnicy Delta (etapy 4–5, **zaplanowane**) | `deltalake` `1.6.6`, `duckdb` `1.5.6` (+ rozszerzenie `delta`), `streamlit` `1.66.0` | | bez identity columns, column mapping ani deletion vectors w tabelach czytanych bez Sparka; CDF tylko przez delta-rs | [06-minio-delta.md](06-minio-delta.md) §12 |

Plan awaryjny, gdyby linia CP 8.3 sprawiła kłopoty: Debezium `3.2.0.Final` (Kafka 4.0) + CP `8.0.8`.

Zasady: żadnego `latest` (test `test_compose_file.py::test_no_latest_tags`); wersje obrazów gotowych są w `docker-compose.yml`, wersje obrazów budowanych w `ARG` ich Dockerfile'i (`containers/minio/Dockerfile`, `containers/connect/Dockerfile`); `.env` zawiera tylko sekrety i parametry, nigdy wersje.

## 2. Kolejność startu

`docker compose up -d --build` uruchamia wszystko naraz; kolejność wymusza `depends_on` z warunkiem `service_healthy` (usługi) lub `service_completed_successfully` (kontenery startowe). Nigdzie nie ma `sleep`.

```
postgres ──────────────────────────────────────────────┐
kafka ──► kafka-init ──► schema-registry ──────────────┼──► connect ──► connect-init
minio ──► minio-init                                   │
                              (profil pipeline, etapy 1–5)
postgres ──► simulator        kafka + schema-registry + minio-init ──► spark        minio-init ──► dashboard
```

| Krok | Usługa | Czeka na | Healthy, gdy |
|---|---|---|---|
| 1 | `postgres`, `kafka`, `minio` | nic | `pg_isready`; `kafka-topics --list`; `GET /minio/health/live` |
| 2 | `kafka-init` | `kafka` healthy | exit 0 po `kafka-topics --create --if-not-exists` z `containers/kafka/topics.txt` |
| 2 | `minio-init` | `minio` healthy | exit 0 po kubełkach, politykach, kontach |
| 3 | `schema-registry` | `kafka` healthy, `kafka-init` exit 0 | `GET /subjects` |
| 4 | `connect` | `kafka`, `schema-registry`, `postgres` healthy; `kafka-init` exit 0 | `GET /connectors` |
| 5 | `connect-init` | `connect` healthy | exit 0 (bez plików w `containers/connect/connectors/`: „no connectors to register”) |

**Zmierzony czas do „all services healthy”** (Apple Silicon, 8 CPU, 9.7 GB RAM dla Dockera, obrazy już zbudowane i pobrane): patrz §2.1. Budżet w teście `test_stack.py::test_fresh_stack_healthy_within_budget`: ≤ 180 s od `down -v`.

### 2.1 Pomiary (2026-10-20)

| Scenariusz | Czas |
|---|---|
| pierwszy build obrazów `wpdb/minio` (Go ze źródeł, cache BuildKit) i `wpdb/connect` | 1 min 54 s |
| `make reset && make up` (pobrane obrazy, zbudowane obrazy, puste wolumeny) | **47 s** do „all services healthy” (initdb Postgresa, format KRaft, 3 kontenery startowe); test `test_fresh_stack_healthy_within_budget` przeszedł z budżetem 180 s |
| `make down && make up` (istniejące wolumeny) | **47 s**: czas zdominowany przez `start_period` i interwały healthchecków (10 s), nie przez pracę kontenerów |
| `make test-integration` na działającym stosie | 32 s (31 testów; 1 pomijany bez `WPDB_FRESH=1`) |
| `WPDB_FRESH=1 make test-integration` (`down -v` + start + 31 testów) | 69 s |

## 3. Healthchecki

Tabela jest kopią kontraktu ze skillu `stack-ops`; wszystkie healthchecki mają `interval: 10s`, `timeout: 5s`, `retries: 12` i `start_period` 30–60 s.

| Usługa | Wewnątrz sieci | Port hosta | Healthcheck |
|---|---|---|---|
| `postgres` | `postgres:5432` | `127.0.0.1:5432` | `pg_isready -U postgres -d shop` |
| `kafka` | `kafka:29092` | `127.0.0.1:9092` (advertised `localhost:9092`) | `kafka-topics --bootstrap-server kafka:29092 --list` |
| `schema-registry` | `schema-registry:8081` | `127.0.0.1:8081` | `GET :8081/subjects` przez `python3 -c "urllib.request.urlopen(...)"` (obraz `cp-schema-registry` nie ma `curl`; `urlopen` rzuca wyjątek przy HTTP ≥ 400, jak `curl -f`) |
| `connect` | `connect:8083` | `127.0.0.1:8083` | `curl -sf :8083/connectors` |
| `minio` | `minio:9000` (API), `minio:9001` (konsola) | `127.0.0.1:9000`, `127.0.0.1:9091` | `curl -sf :9000/minio/health/live` |
| `spark` (etap 3) | – | `127.0.0.1:4040` | ustala etap 3 |
| `dashboard` (etap 5) | – | `127.0.0.1:8501` | `curl -f :8501/_stcore/health` |
| `kafka-init`, `minio-init`, `connect-init` | – | – | kod wyjścia 0 |

Skrypt `.claude/skills/stack-ops/scripts/wait-healthy.sh [timeout]` blokuje, aż każda usługa jest `healthy` i każdy kontener startowy wyszedł z kodem 0; `tests/helpers/compose.py::ComposeStack.wait_healthy` to jego port w Pythonie.

**Wszystkie porty hosta są publikowane tylko na `127.0.0.1`** (wynik przeglądu bezpieczeństwa etapu 0). Stos jest laboratorium na jednej maszynie; bez tego ograniczenia każdy w sieci lokalnej mógłby przez REST Connecta (`:8083`) zarejestrować konektor i, dzięki `config.providers=env`, odczytać `DEBEZIUM_DB_PASSWORD`, a przez `:5432`/`:9092` czytać bazę i tematy. Listener `EXTERNAL` Kafki i tak ogłasza `localhost:9092`, więc nic poza adresem wiązania się nie zmienia. Test: `test_compose_file.py::test_all_published_ports_bound_to_loopback`.

**`connector.client.config.override.policy=All`** zostaje świadomie: [04-kafka.md](04-kafka.md) §7 wymaga w konektorze `producer.override.acks=all`, `producer.override.enable.idempotence=true` i `producer.override.compression.type=zstd`; polityka `Principal` dopuszcza tylko nadpisania związane z uwierzytelnianiem (`sasl.*`, `security.protocol`), więc blokowałaby te ustawienia. Ryzyko (nadpisania klienta przez osobę rejestrującą konektor) jest zneutralizowane przez wiązanie REST do loopback.

Dwa listenery Kafki (naprawa błędu z tutoriali): `INTERNAL://kafka:29092` dla kontenerów, `EXTERNAL://localhost:9092` dla hosta, `CONTROLLER://kafka:9093` dla KRaft. Test: `test_kafka.py::test_reachable_from_host`, `::test_reachable_from_container`.

## 4. Kontenery startowe

| Kontener | Obraz | Co robi | Jak jest idempotentny |
|---|---|---|---|
| `kafka-init` | `cp-kafka` (ten sam, co broker) | tematy wewnętrzne z `containers/kafka/topics.txt` (`connect-configs/offsets/status`, `_schemas`; 1 partycja, compact) | `--create --if-not-exists`; istniejący temat o innych parametrach jest tylko opisany, nie zmieniany |
| `minio-init` | `wpdb/minio` (ma `mc`) | kubełki `bronze silver gold ops checkpoints`, polityki `spark`/`dashboard` z `containers/minio/policies/*.json`, konta `spark` (RW wszędzie) i `dashboard` (RO `gold`, `ops`) | `mc mb --ignore-existing`; `mc admin policy create` i `mc admin user add` nadpisują; `mc admin policy attach` jest strzeżone przez `mc admin user info --json` (ponowne `attach` zwróciłoby błąd) |
| `connect-init` | `curlimages/curl` | `PUT /connectors/<nazwa>/config` dla każdego `containers/connect/connectors/*.json`, czeka na `RUNNING` (timeout 120 s) | `PUT` tworzy lub aktualizuje (tutorialowy `POST` dałby 409); bez plików loguje „no connectors to register” i kończy się kodem 0 |

Ponowne uruchomienie jednego kontenera startowego: `docker compose up -d --force-recreate --no-deps <nazwa>`; w testach `ComposeStack.rerun_init(nazwa)`. Test: `test_stack.py::test_init_container_idempotent[kafka-init|minio-init|connect-init]` (stan przed i po jest identyczny, kod 0).

## 5. Awarie per kontener

Usługi długotrwałe mają `restart: unless-stopped`, więc po `docker kill` Docker sam je podnosi; kontenery startowe mają `restart: "no"`. Zasada systemu: każdy stan jest w wolumenie lub w Kafce, a nie w kontenerze.

| Umiera | Co się dzieje | Kto wznawia | Test (etap) |
|---|---|---|---|
| `postgres` | symulator traci połączenie, Debezium traci slot; WAL i slot są w `pg_data` | Docker restartuje; symulator wznawia od `sim.sim_state` (01 §6), Debezium łączy się ponownie i czyta od `confirmed_flush_lsn` | e2e, etap 6 |
| `kafka` | Connect i Spark dostają błędy producenta/konsumenta i ponawiają; log jest w `kafka_data`, `CLUSTER_ID` stały, więc broker wstaje na tych samych danych | Docker restartuje; Debezium wznawia z `connect-offsets`, Spark z checkpointu | e2e „kill brokera” (04 §8), etap 6 |
| `schema-registry` | rejestr jest bezstanowy: schematy są w temacie `_schemas` | Docker restartuje; klienci (konwerter, Spark) ponawiają żądania HTTP | e2e, etap 6 |
| `connect` | zdarzenia nie płyną; slot w Postgresie trzyma WAL (limit `max_slot_wal_keep_size=2GB`) | Docker restartuje; Debezium wznawia od offsetu z `connect-offsets`, slot się domyka; dostarczanie co najmniej raz + idempotentny MERGE w Sparku | e2e „kill connect” (03 §11), etap 6 |
| `minio` | Spark nie może zapisać; ponawia (S3A retry); dane w `minio_data` | Docker restartuje | e2e „restart MinIO” (06 §11), etap 6 |
| `spark` | przetwarzanie staje; checkpointy w kubełku `checkpoints`, więc kontener jest bezstanowy | Docker restartuje; strumienie wznawiają z checkpointów | e2e (06 §11), etap 6 |
| `simulator` | brak nowych zdarzeń; zegar w `sim.sim_state` zapisany w tej samej transakcji co dane | Docker restartuje; wznowienie bez duplikatów (01 §6) | e2e, etap 1/6 |
| kontener startowy (kod ≠ 0) | `depends_on … service_completed_successfully` blokuje zależne usługi; `wait-healthy.sh` kończy się `FAILED: <nazwa> exited <kod>` | człowiek: `docker compose logs <nazwa>`, poprawka, `--force-recreate --no-deps` | integracyjny (idempotencja) |

Ograniczenia zasobów (`deploy.resources.limits.memory`) chronią maszynę przed jednym kontenerem, który zjada całą pamięć; po przekroczeniu limitu kontener dostaje OOM-kill i `restart` go podnosi.

## 6. Reset

| Komenda | Skutek | Kiedy |
|---|---|---|
| `make down` (`docker compose down --remove-orphans`) | zatrzymuje kontenery, **zachowuje** wolumeny: dane Postgresa, log Kafki, offsety Connecta, obiekty i checkpointy w MinIO | zwykłe zatrzymanie; po `make up` wszystko wznawia się od miejsca, w którym stanęło |
| `make reset` (`docker compose down -v --remove-orphans`) | jak wyżej plus **usuwa wolumeny**; następny `make up` wykonuje initdb Postgresa, formatuje KRaft i uruchamia kontenery startowe od zera | zmiana skryptów `initdb/`, zmiana `CLUSTER_ID`, uszkodzony checkpoint, zmiana schematu lub trybu wyjścia sinku Sparka |
| `docker compose up -d --force-recreate --no-deps <init>` | powtarza jeden kontener startowy | nowy konektor w `containers/connect/connectors/`, nowy kubełek, nowy temat w `topics.txt` |

Reset sinku Sparka (etap 3): checkpoint **i** katalog tabeli czyścimy razem, nigdy osobno (skill `stack-ops`).

## 7. Budżet RAM

Limity z `docker-compose.yml` (Docker Desktop ma 9.7 GB):

| Usługa | Limit | Uwagi |
|---|---|---|
| `postgres` | 1 GB | `shared_buffers=256MB` |
| `kafka` | 1.5 GB | `KAFKA_HEAP_OPTS=-Xms512m -Xmx1g` |
| `schema-registry` | 512 MB | heap 256 MB |
| `connect` | 1.5 GB | heap 1 GB |
| `minio` | 512 MB | |
| kontenery startowe | 128–512 MB | krótkotrwałe |
| **etap 0 razem** | **≈ 5.8 GB** (5.0 GB usługi + 0.77 GB kontenery startowe, które żyją sekundy) | |
| `simulator` | 512 MB | etap 1 |
| `spark` | 3 GB | etap 3; `local[*]` |
| `dashboard` | 512 MB | etap 5 |
| **pełny stos** | **≈ 9.8 GB** (≈ 9.0 GB bez kontenerów startowych) | na granicy 9.7 GB; w etapie 3 limit Sparka i heap Connecta do przycięcia po pomiarze |

## 8. Zmienne środowiskowe

Wszystkie zmienne są w `.env.example` z komentarzem; `make up` kopiuje go do `.env` (gitignored), gdy pliku nie ma. Compose wymaga sekretów składnią `${VAR:?set it in .env}`, więc brak zmiennej zatrzymuje start z czytelnym komunikatem. Test `test_compose_file.py::test_env_example_covers_compose_variables` pilnuje, że każda zmienna z compose jest w `.env.example`.

| Grupa | Zmienne | Kto używa |
|---|---|---|
| Postgres | `POSTGRES_PASSWORD`, `SHOP_APP_PASSWORD`, `DEBEZIUM_DB_PASSWORD`, `READER_PASSWORD` | initdb (role), symulator, Connect (`${env:DEBEZIUM_DB_PASSWORD}`), testy i MCP (`reader`) |
| MinIO | `MINIO_ROOT_USER/PASSWORD`, `MINIO_SPARK_ACCESS_KEY/SECRET_KEY`, `MINIO_DASHBOARD_ACCESS_KEY/SECRET_KEY`, `MINIO_AGENT_ACCESS_KEY/SECRET_KEY` (tylko odczyt, MCP `duckdb` i testy) | `minio`, `minio-init`, Spark, dashboard, testy, MCP `duckdb` |
| Kafka | `KAFKA_CLUSTER_ID` | formatowanie wolumenu KRaft (stałe ID = broker wstaje na istniejących danych) |
| Symulator | `SIM_SPEED`, `MAX_EVENTS_PER_SEC`, `SIM_START`, `SIM_END` | etap 1 |

Pułapka compose: w `command:`/`entrypoint:` `${VAR}` jest podstawiane z `.env` hosta przy parsowaniu; wartość z kontenera wymaga `$$VAR`. Test `test_compose_file.py::test_commands_use_double_dollar`.

## 9. Testy

Trzy poziomy, markery `pytest` w `pyproject.toml` (`--strict-markers`), skill `run-tests`:

| Poziom | Marker | Potrzebuje | Komenda | Czas |
|---|---|---|---|---|
| jednostkowy | `unit` | nic (bez Dockera) | `make test-unit` | sekundy |
| integracyjny | `integration` | kontenery, które fixture `stack` uruchamia (`docker compose up -d` + wait-healthy; **nie** zatrzymuje stosu po testach) | `make test-integration`; `make test-integration-fresh` = to samo od pustych wolumenów (`WPDB_FRESH=1`, włącza test budżetu startu) | minuty |
| e2e | `e2e` (+ `slow`) | pełny, świeży stos (`WPDB_FRESH=1` → `down -v` przed startem), wstrzykiwanie awarii | `make test-e2e` | dziesiątki minut |

Narzędzia na hoście: `uv` (Python 3.12 z `.python-version`, `make sync`), Docker Desktop. `psql` i `mc` nie są potrzebne: testy używają `psycopg`, `confluent-kafka`, `boto3`, `requests` i `docker compose exec`.

Fixture'y (`tests/conftest.py`): `settings` (`.env` przez `python-dotenv`), `compose`/`stack` (`tests/helpers/compose.py::ComposeStack`: `up`, `down`, `wait_healthy`, `rerun_init`, `exec`, `kill`, `logs`), `pg_reader`/`pg_app`/`pg_debezium` (psycopg, autocommit), `kafka_admin`, `schema_registry`/`connect` (`Http` nad `requests`), `minio(konto)` (boto3, path-style, `localhost:9000`). Synchronizacja wyłącznie przez `tests/helpers/wait.py::wait_until(pred, timeout, every, desc)`, nigdy `sleep`.

Zasady: identyfikator testu = `tests/<poziom>/test_<komponent>.py::test_<obietnica>`, docstring cytuje wiersz tabeli „Testy”; asercje dokładne (liczby i równość, nie tolerancje); test pominięty ma powód; test niestabilny to błąd. Jedna instancja stosu na maszynę (stałe porty i nazwy kontenerów), więc testy integracyjne i e2e nie są uruchamiane równolegle z pracą w innych worktree.

## 10. Macierz pokrycia

Status: ✅ przechodzi, ⏳ zaplanowany (etap). Wiersze etapu 0 z planu `qa-engineer` weryfikuje niezależnie.

| Dokument § | Obietnica | Poziom | Test | Status |
|---|---|---|---|---|
| 08 §8 | każda `${VAR}` z compose jest w `.env.example` | unit | `tests/unit/test_compose_file.py::test_env_example_covers_compose_variables` | ✅ |
| 08 §8 | `command:`/`entrypoint:`/healthcheck używają `$$` | unit | `test_compose_file.py::test_commands_use_double_dollar` | ✅ |
| 08 §3, §5 | każda usługa ma healthcheck i `restart: unless-stopped`; init `restart: "no"`; `depends_on` tylko z warunkiem | unit | `test_compose_file.py::test_every_long_running_service_has_healthcheck_and_restart_policy` | ✅ |
| 08 §1 | brak `latest`, każdy obraz z tagiem (compose) | unit | `test_compose_file.py::test_no_latest_tags` | ✅ |
| 08 §1 | `FROM` w `containers/connect/Dockerfile` i `containers/minio/Dockerfile` z dokładnym tagiem po podstawieniu `ARG` | unit | `test_compose_file.py::test_dockerfiles_pin_base_images` | ✅ |
| 08 §3 | każdy `ports:` publikowany na `127.0.0.1` | unit | `test_compose_file.py::test_all_published_ports_bound_to_loopback` | ✅ |
| 08 §2 | `docker compose config` poprawny (domyślnie i `--profile pipeline`) | integracyjny | `tests/integration/test_stack.py::test_compose_config_valid` | ✅ |
| 08 §2, §3 | wszystkie usługi healthy, kontenery startowe exit 0 | integracyjny | `test_stack.py::test_all_services_healthy` | ✅ |
| 08 §2 | healthy w ≤ 180 s od `down -v` (obrazy zbudowane) | integracyjny, `slow` | `test_stack.py::test_fresh_stack_healthy_within_budget`: uruchamiany przez `make test-integration-fresh` (lokalnie i w jobie CI `integration`); w zwykłym `make test-integration` jest pomijany z powodem | ✅ |
| 08 §4, 02 §10, 06 §11 | kontenery startowe idempotentne (stan przed = po: tematy; kubełki, użytkownicy, polityki i przypięcia polityk; lista konektorów) | integracyjny | `test_stack.py::test_init_container_idempotent[kafka-init\|minio-init\|connect-init]` | ✅ |
| 02 §1, §5, §10 | `wal_level=logical`, `max_slot_wal_keep_size=2GB`, limity slotów | integracyjny | `tests/integration/test_postgres.py::test_wal_level_logical_and_slot_limits` | ✅ |
| 02 §4 | schematy `shop`/`sim` własnością `shop_app`; role; `debezium` ma REPLICATION | integracyjny | `test_postgres.py::test_schemas_and_roles_exist` | ✅ |
| 02 §3 | publikacja `FOR TABLES IN SCHEMA shop` obejmuje nową tabelę | integracyjny | `test_postgres.py::test_publication_covers_schema_shop` | ✅ |
| 02 §4 | `reader` widzi tabele `shop_app`; INSERT/UPDATE/DELETE/TRUNCATE/CREATE odrzucone | integracyjny | `test_postgres.py::test_reader_is_read_only` | ✅ |
| 02 §10 | `debezium` czyta i replikuje; INSERT/UPDATE/DELETE/TRUNCATE/DDL odrzucone | integracyjny | `test_postgres.py::test_debezium_cannot_write` | ✅ |
| 02 §4 | schemat `sim`: `reader` tylko SELECT, `debezium` bez dostępu | integracyjny | `test_postgres.py::test_sim_schema_privileges` | ✅ |
| 04 §1, §8 | Kafka z hosta `localhost:9092` | integracyjny | `tests/integration/test_kafka.py::test_reachable_from_host` | ✅ |
| 04 §1, §8 | Kafka z kontenera `kafka:29092` | integracyjny | `test_kafka.py::test_reachable_from_container` | ✅ |
| 04 §3, §8 | auto-tworzenie tematów wyłączone (broker config + produkcja do nieistniejącego tematu) | integracyjny | `test_kafka.py::test_auto_create_disabled`, `::test_unknown_topic_raises_for_admin_describe` | ✅ |
| 04 §3 | tematy wewnętrzne = `containers/kafka/topics.txt` (partycje, `cleanup.policy`) | integracyjny | `test_kafka.py::test_internal_topics_match_topics_txt` | ✅ |
| 04 §1 | produkcja z hosta do jawnie utworzonego tematu jest potwierdzana (`acks=all`) | integracyjny | `test_kafka.py::test_producer_to_explicitly_created_topic_is_acked` | ✅ |
| 04 §1 | `/subjects` odpowiada; rejestr w `_schemas`; tryb READWRITE | integracyjny | `tests/integration/test_schema_registry.py::test_subjects_endpoint`, `::test_registry_stores_schemas_in__schemas_topic`, `::test_mode_is_readwrite` | ✅ |
| 04 §5 | zgodność globalna BACKWARD | integracyjny | `test_schema_registry.py::test_global_compatibility_backward` | ✅ |
| 03 §1 | Connect ma konwerter AVRO (`/connector-plugins?connectorsOnly=false`) i konektor Debezium Postgres 3.6.3 | integracyjny | `tests/integration/test_connect.py::test_plugins_include_avro_converter`, `::test_plugins_include_debezium_postgres` | ✅ |
| 03 §8 | `connect-init` bez konektora: exit 0, `/connectors == []` | integracyjny | `test_connect.py::test_no_connectors_registered_in_stage0` | ✅ |
| 03 §8 | worker ma `config.providers=env`, politykę override `All`, URL rejestru dla konwerterów | integracyjny | `test_connect.py::test_worker_uses_env_config_provider` | ✅ |
| 08 §1 | Connect działa na linii Kafka 4.3 (= CP 8.3) | integracyjny | `test_connect.py::test_worker_kafka_version_matches_broker_line` | ✅ |
| 06 §2 | kubełki `bronze silver gold ops checkpoints` istnieją (i żadne inne) | integracyjny | `tests/integration/test_minio.py::test_buckets_exist` | ✅ |
| 06 §2 | konto `spark` RW we wszystkich kubełkach | integracyjny | `test_minio.py::test_spark_account_rw_all_buckets` | ✅ |
| 06 §11 | konto `dashboard` czyta `gold`/`ops`; zapis do `gold` i odczyt `bronze`/`silver` odrzucone | integracyjny | `test_minio.py::test_dashboard_account_restricted` | ✅ |
| 06 §2 / 09 §3 | konto `agent` (MCP `duckdb`, testy) czyta każdy kubełek, nie zapisuje ani nie usuwa | integracyjny | `test_minio.py::test_agent_account_read_only_everywhere` | ✅ |
| 06 §12, 08 §13 | MinIO i mc zbudowane z przypiętych tagów (`--version`) | integracyjny | `test_minio.py::test_server_version_matches_pinned_tag` | ✅ |
| 08 §13 | porty MinIO tylko na `127.0.0.1` | integracyjny | `test_minio.py::test_api_bound_to_localhost_only` | ✅ |
| 02 §2, §10 | nowa tabela w `shop` → `REPLICA IDENTITY FULL` (event trigger) | integracyjny | etap 1 (`database-cdc`) | ⏳ |
| 03 §11, 04 §8 | zdarzenia `c/u/d/r`, klucze = PK, ewolucja schematu, heartbeat, tombstones | integracyjny | etap 2 | ⏳ |
| 05, 06 §11 | bronze/silver/gold, time travel, CDF, OPTIMIZE/VACUUM | integracyjny | etapy 3–4 | ⏳ |
| 02–06 (awarie) | `kill` `connect`/`kafka`/`minio`/`spark`/`simulator` → uzgadnianie Postgres ↔ silver zgodne | e2e | etap 6 (skill `fault-injection`) | ⏳ |

## 11. CI

`.github/workflows/ci.yml`:

| Job | Kiedy | Co robi |
|---|---|---|
| `lint-unit` | każdy push i PR | `astral-sh/setup-uv` (uv 0.9.18) → `uv python install` (3.12 z `.python-version`) → `uv sync --frozen` → `ruff check`, `ruff format --check` → `pytest -m unit` → `docker compose config -q` (domyślnie i z profilem `pipeline`) |
| `integration` | ręcznie (`workflow_dispatch`, wejście `integration=true`) | `docker compose build minio connect` → `docker compose up -d` → `wait-healthy.sh 300` → `pytest -m integration` → `make test-integration-fresh` (`down -v`, ponowny start, test budżetu 180 s) → przy błędzie `ps -a` + logi → zawsze `down -v` |

Runnery GitHub są `amd64`: obrazy gotowe są multi-arch, `wpdb/minio` i `wpdb/connect` budują się natywnie (bez emulacji). Otwarte: cache warstw Go w GHA (`docker/bake-action` z `cache-to=type=gha`) skróciłby job o ok. 2 min; limit pobrań Docker Hub bez logowania (100/h na IP) może wymagać `docker/login-action` z sekretem.

## 12. Wdrożenie krok po kroku (README)

Wymagania: Docker Desktop (Compose v2, BuildKit), `uv`, `make`, ~10 GB RAM dla Dockera, dostęp do Docker Hub, quay.io, Confluent Hub i GitHub (źródła MinIO).

```bash
git clone https://github.com/oszypczy/wpdb-data-platform && cd wpdb-data-platform
make data                 # CSV Olist do data/olist/ (etap 1 i dalsze)
make up                   # kopiuje .env.example → .env, docker compose up -d --build, czeka na healthy
make ps                   # stan i zdrowie kontenerów
make test-unit            # bez Dockera
make test-integration     # na działającym stosie (uruchamia go, gdy leży)
make test-integration-fresh  # od pustych wolumenów, z testem budżetu startu (180 s)
make logs S=connect       # logi jednej usługi
make down                 # stop, dane zostają
make reset                # stop + wolumeny usunięte (pełny reset)
```

Adresy z hosta (wszystkie tylko z tej maszyny, porty wiązane do `127.0.0.1`): Postgres `localhost:5432` (`shop`, role z `.env`), Kafka `localhost:9092`, Schema Registry `http://localhost:8081`, Kafka Connect `http://localhost:8083`, MinIO API `http://127.0.0.1:9000`, konsola MinIO `http://127.0.0.1:9091` (login root z `.env`), Spark UI `localhost:4040` (etap 3), dashboard `http://localhost:8501` (etap 5).

Pierwsze uruchomienie buduje `wpdb/minio` ze źródeł (ok. 2 min, cache BuildKit) i pobiera 6 obrazów. Docker Hub bez logowania limituje pobrania do 100/h na adres IP; przy `429 Too Many Requests` wystarczy `docker login` albo odczekanie.

## 13. MinIO: dystrybucja, build ze źródeł, CVE-2026-40344

Stan na 2026-10-20 (decyzja użytkownika z etapu 0, szczegóły w [06-minio-delta.md](06-minio-delta.md) §12):

- **Co się stało.** MinIO zarchiwizowało repozytoria `minio/minio` i `minio/mc` (2026-04-24); obrazy community zniknęły z Docker Hub i Quay (2026-09); `dl.min.io` zwraca 410 dla binariów. Tutoriale z laboratoriów zakładają `minio/minio:latest`, który już nie istnieje. Specyfikacja projektu wymaga MinIO, więc zamiast zamiennika zgodnego z S3 budujemy MinIO sami.
- **Jak.** `containers/minio/Dockerfile`: etap `build` na `golang:1.24.13-bookworm` pobiera archiwa źródłowe tagów `RELEASE.2025-10-15T17-29-55Z` (`minio`) i `RELEASE.2025-08-13T08-35-41Z` (`mc`) z `codeload.github.com`, buduje `CGO_ENABLED=0 go build -trimpath` z `-ldflags` odpowiadającymi `buildscripts/gen-ldflags.go` (`cmd.ReleaseTag`, `cmd.Version` w RFC 3339, `cmd.CopyrightYear`; `CommitID` zostaje domyślny, bo archiwum nie ma metadanych git). Cache modułów i kompilacji Go to `--mount=type=cache`, więc ponowny build po zmianie tagu kompiluje tylko różnicę. Etap runtime: `debian:bookworm-slim` + `ca-certificates` + `curl` (healthcheck) + oba binaria w `/usr/local/bin`. `minio --version` zwraca dokładnie tag, co sprawdza `test_minio.py::test_server_version_matches_pinned_tag`. Pierwszy build: 1 min 54 s na Apple M-series; obraz 338 MB.
- **Konsola.** Od wydań z maja 2025 konsola community to tylko przeglądarka obiektów; konta i polityki zarządza `mc` w `minio-init`. Konsola jest na porcie hosta **9091** (w sieci `minio:9001`), bo 9001 zajmuje inny kontener na maszynie autora.
- **CVE-2026-40344.** Ostatnie wydanie community ma niezałataną lukę obejścia uwierzytelnienia (wymaga ważnego klucza z prawem zapisu). Ograniczenia: API i konsola publikowane tylko na `127.0.0.1` (test `test_minio.py::test_api_bound_to_localhost_only`; od przeglądu bezpieczeństwa etapu 0 ta sama zasada obejmuje wszystkie porty stosu, §3), root używany wyłącznie przez `minio-init`, konta `spark`/`dashboard` z minimalnymi politykami, stos to laboratorium na jednej maszynie. Ryzyko akceptowane i opisane, bo alternatywą byłaby zmiana komponentu wymaganego przez specyfikację.
- **Fallback** (nieużywany): `bitnamilegacy/minio:2025.7.23-debian-12-r5`.
- **Licencja.** MinIO jest na AGPL-3.0; obraz nie jest publikowany, a Dockerfile wskazuje repozytorium źródłowe (`org.opencontainers.image.source`).

## 14. Do weryfikacji w kolejnych etapach

- **Snapshot przed startem konektora** (01 §4, 03 §2): `simulator` jest w profilu `pipeline`, więc dziś `connect-init` nie czeka na preload. Etap 2 decyduje: `connect-init` czeka na marker preloadu w `sim.sim_state`, albo symulator wychodzi z profilu i `connect-init` dostaje `depends_on: simulator` z warunkiem.
- **Widoki `sim.v_truth_*`** (02 §8) nie mogą powstać w initdb przed tabelami symulatora: etap 1 tworzy je `CREATE OR REPLACE` z symulatora albo z event triggera (`containers/postgres/README.md`).
- **`pg_hba.conf`** domyślne z obrazu (`host all all all scram-sha-256`) obejmuje replikację; test `test_debezium_cannot_write` tworzy tymczasowy slot, pełne połączenie `pgoutput` sprawdzi etap 2.
- **Heartbeat i tabela sygnałowa Debezium** (03 §7): jeśli mają leżeć w `sim`, rola `debezium` potrzebuje tam `INSERT/UPDATE`; dziś ma tylko `SELECT` na `shop`.
- **RAM pełnego stosu** ≈ 9.5 GB przy 9.7 GB dostępnych: po etapie 3 zmierzyć i przyciąć limity Sparka i Connecta.
- **Tematy `connect-offsets`/`connect-status` z 1 partycją**: wystarczające dla jednego workera; Connect przyjmuje istniejące tematy bez zmian.
- **CI:** cache Go w GHA i logowanie do Docker Hub (§11).
- **MCP `kafka`:** serwer nie ma jeszcze żadnych wiadomości do dekodowania (etap 2 to zweryfikuje na tematach `olist.*`).
