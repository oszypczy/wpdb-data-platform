# Operacje: automatyzacja, niezawodność i testy

Ten dokument opisuje, jak cały system wstaje jednym `docker compose up`, jak się zachowuje przy awariach, jak go resetować i jak uruchamiać testy. Właściciel: `platform-devops` (razem z `qa-engineer` dla sekcji o testach). Kontrakt nazw usług i portów jest w skillu `stack-ops` (`.claude/skills/stack-ops/SKILL.md`); ten dokument i skill zmieniają się razem.

Stan: **etap 0 ukończony 2026-10-20** (infrastruktura: Postgres, Kafka, Schema Registry, Connect, MinIO, kontenery startowe, harness testów, CI); **etap 1 (symulator + PostgreSQL) zaimplementowany 2026-10-20**, usługa `simulator` startuje domyślnie; **etap 2 (Debezium + Kafka + Schema Registry + consumer) zaimplementowany 2026-10-20**: konektor `olist-postgres` rejestrowany po preloadzie, tematy `olist.*` z `topic.creation`, usługa `consumer` w profilu `tools`. **Etap 3 (Spark + MinIO/Delta) zaimplementowany 2026-10-23**: usługa `spark` startuje domyślnie po `connect-init` i `minio-init`. Sekcje dotyczące dalszego potoku (Spark, dashboard, testy e2e z awariami) są uzupełniane w kolejnych etapach.

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
| Consumer (etap 2) | `wpdb/consumer` z `python:3.12.15-slim-bookworm` + `uv` `0.9.18`; `confluent-kafka[avro]` `2.16.0` (librdkafka 2.16.0), `cryptography` **`46.0.7`** (przypięta jawnie) | | `cryptography` ≥ 47.0.0 (zależność `authlib` ← klient Schema Registry) pada z `SIGILL` przy imporcie w VM Docker Desktop na Apple Silicon (linuxkit 6.10.11, aarch64); sprawdzone 46.0.5 ✅, 46.0.7 ✅, 47.0.0–50.0.2 ❌; na hoście (macOS) problem nie występuje | `containers/consumer/pyproject.toml`, `uv.lock`; test `test_consumer.py::test_consumer_container_decodes_keys_and_values_through_the_registry` |
| Debezium ↔ konwerter ↔ rejestr (etap 2) | konektor `io.debezium.connector.postgresql.PostgresConnector` `3.6.3.Final`, `kafka-connect-avro-converter` `8.3.2`, `cp-schema-registry` `8.3.2` | | snapshot, strumień, rejestracja subjectów, ewolucja i odrzucenie niezgodnej zmiany, `topic.creation`, `producer.override` z `zstd` działają razem | `tests/integration/test_debezium.py`, `test_kafka.py`, `test_schema_registry.py` (§10) |
| Python w kontenerach | `python` | `3.12.15-slim-bookworm` | PySpark 3.5 nie wspiera 3.13; `kafka-python==2.0.2` pada na ≥ 3.12, więc `confluent-kafka` | [spark.apache.org/docs/3.5.9](https://spark.apache.org/docs/3.5.9/) „Python 3.8+”; `pyproject.toml` |
| Python na hoście | uv-managed CPython | `3.12.x` (`.python-version`) | jak wyżej; host 3.13/3.14 nie jest używany do venv projektu | `uv python install 3.12` |
| Biblioteki testów (root `pyproject.toml`) | `pytest` `9.1.1`, `pytest-timeout` `2.4.0`, `psycopg[binary]` `3.3.6`, `confluent-kafka[avro]` `2.16.0`, `requests` `2.34.2`, `boto3` `1.43.115`, `python-dotenv` `1.2.4`, `pyyaml` `6.0.3`, `ruff` `0.17.1` | | koła `cp312` `macosx_arm64`/`manylinux` na PyPI | `uv.lock` |
| MCP `kafka` (`tools/kafka-mcp`) | `mcp` `2.3.0`, `confluent-kafka[avro]` `2.16.0` | | | `tools/kafka-mcp/uv.lock` |
| MCP `postgres` / `duckdb` | `@bytebase/dbhub` `1.5.0`, `mcp-server-motherduck` `1.1.0` | | [09-agents.md](09-agents.md) §3 | `.mcp.json` |
| Spark (etap 3) | obraz bazowy `apache/spark:3.5.9-scala2.12-java17-python3-ubuntu` (Ubuntu 22.04, Java 17.0.19, Python 3.10.12, Hadoop 3.3.4 w `/opt/spark/jars`) | `3.5.9` | Scala 2.12; manifest `arm64` + `amd64` | [hub.docker.com/r/apache/spark/tags](https://hub.docker.com/r/apache/spark/tags) (tag z 2026-07-24); `containers/spark/Dockerfile` (`ARG SPARK_IMAGE`), test `test_compose_file.py::test_dockerfiles_pin_base_images` |
| Spark: Kafka | `spark-sql-kafka-0-10_2.12` + `spark-token-provider-kafka-0-10_2.12` `3.5.9`, `kafka-clients` `3.4.1`, `commons-pool2` `2.11.1` | | ta sama wersja co Spark; `kafka-clients`/`commons-pool2` = zależności z POM-u `spark-sql-kafka-0-10_2.12-3.5.9` (`spark-parent` 3.5.9: `kafka.version` 3.4.1); klient 3.4 rozmawia z brokerem 4.3 (KIP-896 wymaga klientów ≥ 2.1); `zstd-jni` 1.5.5-4 jest w obrazie (tematy `zstd`) | [Maven Central POM](https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.9/spark-sql-kafka-0-10_2.12-3.5.9.pom); test `test_spark.py::test_real_debezium_messages_reach_bronze_and_silver_with_types` |
| Spark: AVRO | `spark-avro_2.12` `3.5.9` | | `avro` 1.11.5 i `xz` 1.9 już w `/opt/spark/jars` (zależności z POM-u) | [POM](https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.5.9/spark-avro_2.12-3.5.9.pom); `test_spark_decode.py` |
| Delta (Spark) | `delta-spark_2.12` + `delta-storage` `3.3.3`; Python `delta-spark==3.3.3` (`--no-deps`; PySpark z obrazu, w buildzie `assert pyspark.__version__ == '3.5.9'`) | | linia Delta 3.x = Spark 3.5 (POM: `spark-sql_2.12` 3.5.6 `provided`); Delta 4.x tylko dla Sparka 4 | [POM delta-spark 3.3.3](https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.3.3/delta-spark_2.12-3.3.3.pom), [PyPI](https://pypi.org/project/delta-spark/3.3.3/) (`pyspark<3.6.0,>=3.5.3`) |
| S3A | `hadoop-aws` `3.3.4` + `aws-java-sdk-bundle` `1.12.262` | | = Hadoop w obrazie (`hadoop-client-api-3.3.4.jar`); bundle z `hadoop-project` 3.3.4 (`aws-java-sdk.version`) | [hadoop-project 3.3.4 POM](https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-project/3.3.4/hadoop-project-3.3.4.pom); wszystkie JAR-y pobierane w buildzie z kontrolą `.sha1` (test `test_compose_file.py::test_spark_image_pins_every_connector_jar_with_a_checksum`) |
| Spark w testach na hoście | `pyspark==3.5.9`, `delta-spark==3.3.3` (root `pyproject.toml`), JAR-y przez Ivy: `io.delta:delta-spark_2.12:3.3.3`, `org.apache.spark:spark-avro_2.12:3.5.9`, `org.apache.hadoop:hadoop-aws:3.3.4`; JDK 17 (`tests/helpers/spark.py` wybiera 17 przez `java_home -v 17` / `JAVA_HOME_17_*`) | | Spark 3.5 nie działa na Javie 23 hosta | `tests/unit/conftest.py` |
| Czytelnicy Delta (testy, etap 4) | `deltalake` `1.6.6`, `duckdb` `1.5.6` (+ rozszerzenia `delta`, `httpfs`), `pyarrow` `25.0.1`, `pytz` `2026.5` (DuckDB zwraca `TIMESTAMPTZ`) | | bez identity columns, column mapping, deletion vectors i `timestampNtz` w tabelach czytanych bez Sparka; CDF czytany przez delta-rs (`load_cdf`) | test `test_delta.py::test_every_gold_and_ops_table_is_readable_by_delta_rs_and_duckdb`; [06-minio-delta.md](06-minio-delta.md) §12 |

Plan awaryjny, gdyby linia CP 8.3 sprawiła kłopoty: Debezium `3.2.0.Final` (Kafka 4.0) + CP `8.0.8`.

Zasady: żadnego `latest` (test `test_compose_file.py::test_no_latest_tags`); wersje obrazów gotowych są w `docker-compose.yml`, wersje obrazów budowanych w `ARG` ich Dockerfile'i (`containers/minio/Dockerfile`, `containers/connect/Dockerfile`); `.env` zawiera tylko sekrety i parametry, nigdy wersje.

## 2. Kolejność startu

`docker compose up -d --build` uruchamia wszystko naraz; kolejność wymusza `depends_on` z warunkiem `service_healthy` (usługi) lub `service_completed_successfully` (kontenery startowe). Nigdzie nie ma `sleep`.

```
postgres ──► simulator (healthy = preload zakończony) ─────────────┐
postgres ──────────────────────────────────────────────┐           │
kafka ──► kafka-init ──► schema-registry ──────────────┼──► connect ──► connect-init
minio ──► minio-init                                   │
                              (profil tools, na żądanie)   kafka + schema-registry ──► consumer
                              (domyślnie od etapu 3)        (profil pipeline, etap 4)
kafka + schema-registry + minio-init + connect-init ──► spark        minio-init ──► dashboard
```

| Krok | Usługa | Czeka na | Healthy, gdy |
|---|---|---|---|
| 1 | `postgres`, `kafka`, `minio` | nic | `pg_isready`; `kafka-topics --list`; `GET /minio/health/live` |
| 2 | `kafka-init` | `kafka` healthy | exit 0 po `kafka-topics --create --if-not-exists` z `containers/kafka/topics.txt` |
| 2 | `minio-init` | `minio` healthy | exit 0 po kubełkach, politykach, kontach |
| 3 | `schema-registry` | `kafka` healthy, `kafka-init` exit 0 | `GET /subjects` |
| 4 | `connect` | `kafka`, `schema-registry`, `postgres` healthy; `kafka-init` exit 0 | `GET /connectors` |
| 5 | `connect-init` | `connect` **i `simulator`** healthy (etap 2: snapshot dopiero po preloadzie, 03 §2) | exit 0 po `PUT /connectors/olist-postgres/config` i `RUNNING` konektora oraz zadania (timeout 120 s) |
| 2 | `simulator` | `postgres` healthy; `data/olist/*.csv` na hoście (`make up` wywołuje `make data`) | `healthcheck.py`: plik heartbeat < 30 s **i** faza `replay`/`done` (po preloadzie; `start_period` 120 s) |
| – | `consumer` (profil `tools`) | `kafka`, `schema-registry` healthy | nie startuje z `make up`; `docker compose run --rm consumer …` (04 §6) |
| 6 | `spark` (etap 3, domyślnie) | `kafka`, `schema-registry` healthy; `minio-init` i `connect-init` exit 0 | `healthcheck.py` (§3): zdrowy ok. 30 s po starcie kontenera (`kafka_to_bronze` aktywne); kolejne zapytania (silver per tabela, gold) dochodzą w ciągu ok. 1 min |

**Zmierzony czas do „all services healthy”** (Apple Silicon, 8 CPU, 9.7 GB RAM dla Dockera, obrazy już zbudowane i pobrane): patrz §2.1. Budżet w teście `test_stack.py::test_fresh_stack_healthy_within_budget`: ≤ 180 s od `down -v`.

### 2.1 Pomiary (2026-10-21)

| Scenariusz | Czas |
|---|---|
| pierwszy build obrazów `wpdb/minio` (Go ze źródeł, cache BuildKit) i `wpdb/connect` | 1 min 54 s |
| `make reset && make up` (pobrane obrazy, zbudowane obrazy, puste wolumeny) | **47 s** do „all services healthy” (initdb Postgresa, format KRaft, 3 kontenery startowe); test `test_fresh_stack_healthy_within_budget` przeszedł z budżetem 180 s. **Z symulatorem (etap 1): 48 s** (wczytanie CSV + detekcja + budowa zdarzeń ≈ 10 s, init jednej transakcji 0,2 s, preload 2016 r. 0,07 s) |
| `make down && make up` (istniejące wolumeny) | **47 s**: czas zdominowany przez `start_period` i interwały healthchecków (10 s), nie przez pracę kontenerów |
| `make test-integration` (zestaw szybki, `integration and not slow`) na świeżo postawionym stosie | etap 2: **72 testy, 1 min 51 s** (96 odfiltrowanych: 12 `slow` + 84 jednostkowe) |
| `make test-integration-full` (wszystkie integracyjne, także 12 `slow`) na działającym stosie | etap 2: **83 testy + 1 pominięty (budżet startu, wymaga `WPDB_FRESH=1`), 6 min 19 s** z `OFFSET_FLUSH_INTERVAL_MS=5000` (wcześniej przy 60 s: 82 + 1, 6,5–9,5 min); testy `slow`: kompaktowanie ≈ 1–2 min, heartbeat przy zatrzymanym symulatorze, niezgodna zmiana typu, restarty symulatora; etap 1: ok. 2 min 50 s / 50 testów; etap 0: 32 s / 31 testy |
| `make test-integration-fresh` (`down -v` + start + wszystkie testy) | etap 1: ok. 3 min 30 s (etap 0: 69 s) |
| `make reset && make up` z konektorem (etap 2) | **46 s** (2026-10-21, `dbz.heartbeat` z initdb, snapshot od zera; wcześniej 56 s) do „all services healthy” (connect-init czeka na preload symulatora, rejestracja + snapshot okna domyślnego ≈ 12 s; budżet 180 s testu `test_fresh_stack_healthy_within_budget` zachowany) |
| opóźnienie CDC (commit → rekord w Kafce) | mediana ≈ 0,45 s, p95 ≈ 0,8 s (03 §13) |
| `make reset && make up` ze Sparkiem (etap 3, 2026-10-23) | **70 s** do „all services healthy” (z buildem obrazu `wpdb/spark` z cache); `spark` zdrowy ok. 25 s po starcie kontenera, wszystkie 13 zapytań aktywne ok. 1 min później |
| `make test-unit` (etap 3) | **104 testy, 2 min 34 s** (w tym 22 testy Sparka na lokalnej sesji; start JVM i Ivy ok. 10 s) |
| `make test-integration` (etap 3, szybki) | **85 testów, 11 min 16 s** na świeżym stosie; z tego 388 s to `test_consumer.py::test_consumer_stats_mode_reports_rates_ops_lag_and_schemas` (czeka na 2000 wiadomości `order_items` albo 5 s ciszy, a na świeżym stosie temat ma mniej wiadomości i nowe dopływają co kilka sekund; do `database-cdc`); testy Sparka i Delta razem ok. 3 min |
| `make test-integration-full` (etap 3) | **98 testów + 1 pominięty (budżet startu), 16 min 42 s** |

## 3. Healthchecki

Tabela jest kopią kontraktu ze skillu `stack-ops`; wszystkie healthchecki mają `interval: 10s`, `timeout: 5s`, `retries: 12` i `start_period` 30–60 s.

| Usługa | Wewnątrz sieci | Port hosta | Healthcheck |
|---|---|---|---|
| `postgres` | `postgres:5432` | `127.0.0.1:5432` | `pg_isready -U postgres -d shop` |
| `kafka` | `kafka:29092` | `127.0.0.1:9092` (advertised `localhost:9092`) | `kafka-topics --bootstrap-server kafka:29092 --list` |
| `schema-registry` | `schema-registry:8081` | `127.0.0.1:8081` | `GET :8081/subjects` przez `python3 -c "urllib.request.urlopen(...)"` (obraz `cp-schema-registry` nie ma `curl`; `urlopen` rzuca wyjątek przy HTTP ≥ 400, jak `curl -f`) |
| `connect` | `connect:8083` | `127.0.0.1:8083` | `curl -sf :8083/connectors` |
| `minio` | `minio:9000` (API), `minio:9001` (konsola) | `127.0.0.1:9000`, `127.0.0.1:9091` | `curl -sf :9000/minio/health/live` |
| `simulator` | – | – | `python /app/healthcheck.py`: heartbeat `/tmp/simulator/heartbeat` < 30 s i faza `replay`/`done` (01 §5); `start_period` 120 s |
| `spark` (etap 3) | – | `127.0.0.1:4040` (Spark UI) | `python3 /app/healthcheck.py`: plik `/tmp/spark/health.json` pisany przez pętlę sterownika ma < 60 s i status `ok` (zapytanie `kafka_to_bronze` i każde już uruchomione zapytanie aktywne); `start_period` 120 s |
| `dashboard` (etap 5) | – | `127.0.0.1:8501` | `curl -f :8501/_stcore/health` |
| `kafka-init`, `minio-init`, `connect-init` | – | – | kod wyjścia 0 |

Skrypt `.claude/skills/stack-ops/scripts/wait-healthy.sh [timeout]` blokuje, aż każda usługa jest `healthy` i każdy kontener startowy wyszedł z kodem 0; `tests/helpers/compose.py::ComposeStack.wait_healthy` to jego port w Pythonie. **Poprawka etapu 2:** działający kontener `*-init` (bez healthchecku) nie jest już traktowany jak gotowy: `docker compose up -d` uruchamia kontenery startowe ponownie, a `connect-init` od etapu 2 trwa kilka sekund (czeka na `RUNNING`), więc testy startowały, zanim skończył (`test_connect.py::test_connect_init_registered_the_connector` widział stan `running`).

**Wszystkie porty hosta są publikowane tylko na `127.0.0.1`** (wynik przeglądu bezpieczeństwa etapu 0). Stos jest laboratorium na jednej maszynie; bez tego ograniczenia każdy w sieci lokalnej mógłby przez REST Connecta (`:8083`) zarejestrować konektor i, dzięki `config.providers=env`, odczytać `DEBEZIUM_DB_PASSWORD`, a przez `:5432`/`:9092` czytać bazę i tematy. Listener `EXTERNAL` Kafki i tak ogłasza `localhost:9092`, więc nic poza adresem wiązania się nie zmienia. Test: `test_compose_file.py::test_all_published_ports_bound_to_loopback`.

**`connector.client.config.override.policy=All`** zostaje świadomie: [04-kafka.md](04-kafka.md) §7 wymaga w konektorze `producer.override.acks=all`, `producer.override.enable.idempotence=true` i `producer.override.compression.type=zstd`; polityka `Principal` dopuszcza tylko nadpisania związane z uwierzytelnianiem (`sasl.*`, `security.protocol`), więc blokowałaby te ustawienia. Ryzyko (nadpisania klienta przez osobę rejestrującą konektor) jest zneutralizowane przez wiązanie REST do loopback.

Dwa listenery Kafki (naprawa błędu z tutoriali): `INTERNAL://kafka:29092` dla kontenerów, `EXTERNAL://localhost:9092` dla hosta, `CONTROLLER://kafka:9093` dla KRaft. Test: `test_kafka.py::test_reachable_from_host`, `::test_reachable_from_container`.

## 4. Kontenery startowe

| Kontener | Obraz | Co robi | Jak jest idempotentny |
|---|---|---|---|
| `kafka-init` | `cp-kafka` (ten sam, co broker) | tematy wewnętrzne z `containers/kafka/topics.txt` (`connect-configs/offsets/status`, `_schemas`; 1 partycja, compact) | `--create --if-not-exists`; istniejący temat o innych parametrach jest tylko opisany, nie zmieniany |
| `minio-init` | `wpdb/minio` (ma `mc`) | kubełki `bronze silver gold ops checkpoints`, polityki `spark`/`dashboard` z `containers/minio/policies/*.json`, konta `spark` (RW wszędzie) i `dashboard` (RO `gold`, `ops`) | `mc mb --ignore-existing`; `mc admin policy create` i `mc admin user add` nadpisują; `mc admin policy attach` jest strzeżone przez `mc admin user info --json` (ponowne `attach` zwróciłoby błąd) |
| `connect-init` | `curlimages/curl` | `PUT /connectors/<nazwa>/config` dla każdego `containers/connect/connectors/*.json` (etap 2: `olist-postgres`), czeka na `RUNNING` (timeout 120 s); startuje dopiero po zdrowym `simulator` (preload) | `PUT` tworzy lub aktualizuje (tutorialowy `POST` dałby 409); ta sama konfiguracja = brak restartu zadań; zmieniona = Connect przeładowuje konektor; bez plików loguje „no connectors to register” i kończy się kodem 0 |

Ponowne uruchomienie jednego kontenera startowego: `docker compose up -d --force-recreate --no-deps <nazwa>`; w testach `ComposeStack.rerun_init(nazwa)`. Test: `test_stack.py::test_init_container_idempotent[kafka-init|minio-init|connect-init]` (stan przed i po jest identyczny, kod 0).

## 5. Awarie per kontener

Usługi długotrwałe mają `restart: unless-stopped`; kontenery startowe mają `restart: "no"`. Zasada systemu: każdy stan jest w wolumenie lub w Kafce, a nie w kontenerze. **Uwaga (zmierzone w etapie 1):** po `docker kill` (SIGKILL) Docker traktuje kontener z `unless-stopped` jak zatrzymany ręcznie i **nie** restartuje go; polityka działa przy OOM, błędzie procesu i restarcie demona. Testy awarii robią więc `docker compose kill <usługa>` + `docker compose up -d <usługa>` (skill `fault-injection`).

| Umiera | Co się dzieje | Kto wznawia | Test (etap) |
|---|---|---|---|
| `postgres` | symulator traci połączenie, Debezium traci slot; WAL i slot są w `pg_data` | Docker restartuje; symulator wznawia od `sim.sim_state` (01 §6), Debezium łączy się ponownie i czyta od `confirmed_flush_lsn` | e2e, etap 6 |
| `kafka` | Connect i Spark dostają błędy producenta/konsumenta i ponawiają; log jest w `kafka_data`, `CLUSTER_ID` stały, więc broker wstaje na tych samych danych | Docker restartuje; Debezium wznawia z `connect-offsets`, Spark z checkpointu | e2e „kill brokera” (04 §8), etap 6 |
| `schema-registry` | rejestr jest bezstanowy: schematy są w temacie `_schemas` | Docker restartuje; klienci (konwerter, Spark) ponawiają żądania HTTP | e2e, etap 6 |
| `connect` | zdarzenia nie płyną; slot w Postgresie trzyma WAL (limit `max_slot_wal_keep_size=2GB`) | Docker restartuje; Debezium wznawia od offsetu z `connect-offsets` (zapis co 5 s, `OFFSET_FLUSH_INTERVAL_MS=5000`, więc do ok. 5 s zdarzeń przychodzi ponownie), slot się domyka; dostarczanie co najmniej raz + idempotentny MERGE w Sparku | e2e „kill connect” (03 §11), etap 6; restart samego zadania po `FAILED`: integracyjny `test_debezium.py::test_incompatible_type_change_is_rejected_and_the_connector_fails_then_recovers` ✅ |
| `minio` | Spark nie może zapisać; ponawia (S3A retry); dane w `minio_data` | Docker restartuje | e2e „restart MinIO” (06 §11), etap 6 |
| `spark` | przetwarzanie staje; checkpointy w kubełku `checkpoints`, więc kontener jest bezstanowy | Docker restartuje; strumienie wznawiają z checkpointów; pojedyncze zapytanie, które padło (np. zmiana schematu źródła Delta), wznawia pętla sterownika bez restartu kontenera (05 §12) | integracyjny: `test_spark.py::test_restart_from_checkpoint_keeps_bronze_offsets_unique` ✅ (restart), `::test_add_column_reaches_silver_without_restart` ✅; e2e z `kill` i uzgadnianiem: etap 5 |
| `simulator` | brak nowych zdarzeń; kursor w `sim.sim_state` zapisany w tej samej transakcji co dane | `docker compose up -d simulator`; wznowienie od kursora bez duplikatów i strat (01 §5) | integracyjny: `test_simulator.py::test_kill_and_restart_resumes_from_sim_state_without_duplicates` ✅; e2e z uzgadnianiem silver: etap 6 |
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

Limity z `docker-compose.yml` (Docker Desktop ma 9.7 GB) i zużycie zmierzone `docker stats` na pełnym stosie w trakcie strumienia (etap 3, 2026-10-23, okno domyślne, 13 zapytań Sparka; próbki co 20 s, wartości min–max):

| Usługa | Limit | Zmierzone (etap 3) | Uwagi |
|---|---|---|---|
| `postgres` | 1 GB | 55–67 MB | `shared_buffers=256MB` |
| `kafka` | 1.5 GB | 520–730 MB | `KAFKA_HEAP_OPTS=-Xms512m -Xmx1g` |
| `schema-registry` | 512 MB | 290–330 MB | heap 256 MB |
| `connect` | 1.5 GB | 0.67–1.13 GB | heap 1 GB (do 1.13 GB po restartach zadań w testach) |
| `minio` | **1 GB** (było 512 MB) | 300–493 MB | etap 3: przy 512 MB i obciążeniu Sparka zmierzone 493 MB, czyli tuż pod limitem; podniesione |
| `simulator` | 512 MB | 240 MB | |
| `spark` | **3 GB** | 2.5–2.6 GB w trakcie strumienia, 1.7 GB bezczynnie | `SPARK_DRIVER_MEMORY=1536m` (tryb lokalny: sterownik = wykonawca), metaspace do 384 MB, pamięć bezpośrednia do 256 MB, proces Pythona `foreachBatch` ok. 40 MB; przy limicie 2.5 GB zużycie dochodziło do 2.47 GB, stąd 3 GB; CPU: skoki 400–630 % w czasie mikropaczek, 1–3 % między nimi |
| kontenery startowe | 128–512 MB | krótkotrwałe | |
| `dashboard` | 512 MB | – | etap 4 |
| **pełny stos (limity)** | **≈ 9.5 GB** (bez kontenerów startowych) | **≈ 5.3–5.8 GB** zajęte w trakcie strumienia | limity to sufity, nie rezerwacje; zapas do 9.7 GB ok. 4 GB. Na maszynie autora działają też obce kontenery (`qdrant` 1–2 GB), które zjadają ten zapas |

## 8. Zmienne środowiskowe

Wszystkie zmienne są w `.env.example` z komentarzem; `make up` kopiuje go do `.env` (gitignored), gdy pliku nie ma. Compose wymaga sekretów składnią `${VAR:?set it in .env}`, więc brak zmiennej zatrzymuje start z czytelnym komunikatem. Test `test_compose_file.py::test_env_example_covers_compose_variables` pilnuje, że każda zmienna z compose jest w `.env.example`.

| Grupa | Zmienne | Kto używa |
|---|---|---|
| Postgres | `POSTGRES_PASSWORD`, `SHOP_APP_PASSWORD`, `DEBEZIUM_DB_PASSWORD`, `READER_PASSWORD` | initdb (role), symulator, Connect (`${env:DEBEZIUM_DB_PASSWORD}`), testy i MCP (`reader`) |
| MinIO | `MINIO_ROOT_USER/PASSWORD`, `MINIO_SPARK_ACCESS_KEY/SECRET_KEY`, `MINIO_DASHBOARD_ACCESS_KEY/SECRET_KEY`, `MINIO_AGENT_ACCESS_KEY/SECRET_KEY` (tylko odczyt, MCP `duckdb` i testy) | `minio`, `minio-init`, Spark, dashboard, testy, MCP `duckdb` |
| Kafka | `KAFKA_CLUSTER_ID` | formatowanie wolumenu KRaft (stałe ID = broker wstaje na istniejących danych) |
| Symulator | `SIM_SPEED`, `MAX_EVENTS_PER_SEC`, `SIM_START`, `SIM_END` (domyślnie 1440 / 20 / 2017-01-05 / 2017-03-31, 01 §4) | `simulator`; e2e nadpisują je przy starcie stosu |
| Consumer | `KAFKA_BOOTSTRAP`, `SCHEMA_REGISTRY_URL` (w compose `kafka:29092`, `http://schema-registry:8081`; z hosta domyślnie `localhost:9092`, `http://localhost:8081`) | `consumer` (profil `tools`) |

Pułapka compose: w `command:`/`entrypoint:` `${VAR}` jest podstawiane z `.env` hosta przy parsowaniu; wartość z kontenera wymaga `$$VAR`. Test `test_compose_file.py::test_commands_use_double_dollar`.

## 9. Testy

Trzy poziomy, markery `pytest` w `pyproject.toml` (`--strict-markers`), skill `run-tests`:

| Poziom | Marker | Potrzebuje | Komenda | Czas |
|---|---|---|---|---|
| jednostkowy | `unit` | nic (bez Dockera) | `make test-unit` | sekundy |
| integracyjny | `integration` | kontenery, które fixture `stack` uruchamia (`docker compose up -d` + wait-healthy; **nie** zatrzymuje stosu po testach) | `make test-integration` = zestaw szybki (`-m "integration and not slow"`, na działającym stosie, przy każdej zmianie); `make test-integration-full` = wszystkie testy integracyjne (także `slow`) na działającym stosie; `make test-integration-fresh` = wszystkie od pustych wolumenów (`WPDB_FRESH=1`, włącza test budżetu startu) | etap 3: ok. 5–11 min (szybki, zależnie od wieku stosu, §2.1) / ok. 17 min (pełny) |
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
| 08 §2 | healthy w ≤ 180 s od `down -v` (obrazy zbudowane) | integracyjny, `slow` | `test_stack.py::test_fresh_stack_healthy_within_budget`: uruchamiany przez `make test-integration-fresh` (lokalnie i w jobie CI `integration`); w `make test-integration` odfiltrowany markerem `slow`, w `make test-integration-full` pomijany z powodem (stos nie jest świeży) | ✅ |
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
| 03 §8 | worker ma `config.providers=env`, politykę override `All`, URL rejestru dla konwerterów | integracyjny | `test_connect.py::test_worker_uses_env_config_provider` | ✅ |
| 08 §1 | Connect działa na linii Kafka 4.3 (= CP 8.3) | integracyjny | `test_connect.py::test_worker_kafka_version_matches_broker_line` | ✅ |
| 06 §2 | kubełki `bronze silver gold ops checkpoints` istnieją (i żadne inne) | integracyjny | `tests/integration/test_minio.py::test_buckets_exist` | ✅ |
| 06 §2 | konto `spark` RW we wszystkich kubełkach | integracyjny | `test_minio.py::test_spark_account_rw_all_buckets` | ✅ |
| 06 §11 | konto `dashboard` czyta `gold`/`ops`; zapis do `gold` i odczyt `bronze`/`silver` odrzucone | integracyjny | `test_minio.py::test_dashboard_account_restricted` | ✅ |
| 06 §2 / 09 §3 | konto `agent` (MCP `duckdb`, testy) czyta każdy kubełek, nie zapisuje ani nie usuwa | integracyjny | `test_minio.py::test_agent_account_read_only_everywhere` | ✅ |
| 06 §12, 08 §13 | MinIO i mc zbudowane z przypiętych tagów (`--version`) | integracyjny | `test_minio.py::test_server_version_matches_pinned_tag` | ✅ |
| 08 §13 | porty MinIO tylko na `127.0.0.1` | integracyjny | `test_minio.py::test_api_bound_to_localhost_only` | ✅ |
| 02 §2, §10 | nowa tabela w `shop` (`CREATE TABLE`, `CREATE TABLE AS`) → `REPLICA IDENTITY FULL` (event trigger), obecna w `dbz_publication`; tabela w `sim` zachowuje identyfikację domyślną | integracyjny | `test_postgres.py::test_new_table_in_shop_gets_replica_identity_full` | ✅ |
| 02 §9, §10 | skrypty initdb na pustym wolumenie, dwa razy → identyczny stan (role, schematy, domyślne uprawnienia, publikacja, event trigger, ustawienia), równy stanowi stosu | integracyjny | `test_postgres.py::test_initdb_twice_gives_identical_state` (dwa jednorazowe kontenery `postgres:16.15`, `tests/helpers/postgres.py::InitdbProbe`) | ✅ |
| 02 §8 | `sql/truth-views.sql` wykonywalny wielokrotnie jako `shop_app`; widoki `sim.v_truth_*` dokładne na znanym zestawie danych; event trigger obejmuje tabele z initdb | integracyjny | `test_postgres.py::test_truth_views_exact_on_known_data` (jednorazowy kontener `InitdbProbe`) | ✅ |
| 02 §8 | na stosie symulator utworzył widoki `sim.v_truth_*` o ustalonych kolumnach, czytelne dla `reader`, zgodne z `count(*)` tabel w jednym snapshocie | integracyjny | `test_postgres.py::test_truth_views_apply_and_reconcile` | ✅ |
| 02 §7 | kolumny każdego FK w `shop` są wiodącymi kolumnami indeksu | integracyjny | `test_postgres.py::test_every_fk_column_has_a_leading_index` | ✅ |
| 02 §7, §10 | `DELETE FROM order_items WHERE order_id=…` używa indeksu | integracyjny | `test_postgres.py::test_delete_order_items_by_order_id_uses_index` | ✅ |
| 01 §3, §8 | detekcja typów (zip → TEXT, puste → NULL, kwoty → NUMERIC(10,2), BOM, TIMESTAMP/BIGINT/mieszane) | unit | `tests/unit/test_simulator_schema.py::test_zip_code_with_leading_zero_is_text`, `::test_integer_column_with_empty_values_is_nullable_integer`, `::test_amount_is_numeric_10_2`, `::test_timestamp_bigint_mixed_and_empty_columns`, `::test_bom_does_not_break_the_header`, `::test_multiline_quoted_fields_and_width_errors`, `::test_table_names_from_file_names` | ✅ |
| 01 §3, §8 | detekcja PK (`orders` → `order_id`; pary dla items/payments/reviews; tabela bez `*_id`) | unit | `test_simulator_schema.py::test_pk_orders_prefers_order_id_over_unique_customer_id`, `::test_pk_reviews_is_pair_because_review_id_is_not_unique`, `::test_pk_items_and_payments_are_pairs`, `::test_pk_without_id_column_falls_back_to_first_unique_column` | ✅ |
| 01 §3, §8 | detekcja FK: 6 przyjętych, `products → translation` odrzucony (2 brakujące) | unit | `test_simulator_schema.py::test_fk_accepts_orders_customer_id_and_rejects_products_translation` | ✅ |
| 01 §3, 02 §7 | DDL z typami/PK/FK; dokładnie 4 dodatkowe indeksy FK; rodzice przed dziećmi | unit | `test_simulator_schema.py::test_ddl_has_detected_types_keys_and_fk_indexes`, `::test_creation_order_puts_parents_first` | ✅ |
| 01 §3 | cała tabela §3 na prawdziwych danych (`data/olist`) | unit, `slow` | `test_simulator_schema.py::test_real_data_gives_the_schema_of_the_design_doc` (pomijany bez `make data`) | ✅ |
| 01 §4, §8 | zamówienie → c, u, u, u, c(review); daty przyszłe NULL w INSERT | unit | `tests/unit/test_simulator_events.py::test_delivered_order_gives_create_update_update_update_review` | ✅ |
| 01 §4, §8 | normalizacja czasu (wysyłka przed akceptacją, brak daty, recenzja przed zakupem) | unit | `test_simulator_events.py::test_shipment_before_approval_is_moved_to_the_approval_time`, `::test_missing_step_date_skips_the_step`, `::test_review_before_purchase_is_moved_to_the_purchase_time`, `::test_review_after_purchase_keeps_its_time` | ✅ |
| 01 §4, §8 | statusy końcowe (`canceled` + DELETE, `invoiced`, `canceled` po dostawie, `delivered` bez daty, `shipped`/`created`/`approved`), opóźnienie deterministyczne 1–3 dni | unit | `test_simulator_events.py::test_canceled_order_updates_status_and_deletes_its_items`, `::test_invoiced_order_stays_invoiced_without_delete`, `::test_canceled_after_delivery_and_delivered_without_delivery_date`, `::test_shipped_and_created_orders_end_at_their_last_step`, `::test_final_status_delay_is_deterministic_and_bounded` | ✅ |
| 01 §4, §5 | porządek `(czas, order_id, krok)`, kursor wznawiania, podział preload/replay, czysty model stanu | unit | `test_simulator_events.py::test_event_stream_is_sorted_and_resumes_strictly_after_the_cursor`, `::test_split_window_preload_before_start_and_replay_inside`, `::test_summarize_is_the_state_an_uninterrupted_run_produces` | ✅ |
| 01 §4, §8 | limit tempa: zegar zwalnia w szczycie, ≤ `MAX_EVENTS_PER_SEC`, nic nie ginie; zegar ×SIM_SPEED; limiter | unit | `tests/unit/test_simulator_clock.py::test_backpressure_caps_the_rate_holds_the_clock_back_and_loses_nothing`, `::test_sim_clock_runs_speed_times_faster_than_wall_time`, `::test_rate_limiter_paces_row_changes`, `::test_pacer_waits_for_the_simulated_clock_when_ahead_of_schedule` | ✅ |
| 01 §4, §9 | domyślne okno i tempo małe; parsowanie granic; błędne wartości odrzucone | unit | `test_simulator_clock.py::test_config_defaults_are_a_short_window_at_a_low_rate`, `::test_config_parses_bounds_and_rejects_bad_values` | ✅ |
| 08 §1 | `containers/simulator/Dockerfile` z dokładnym tagiem; usługa `simulator` ma healthcheck | unit | `test_compose_file.py::test_dockerfiles_pin_base_images`, `::test_every_long_running_service_has_healthcheck_and_restart_policy` | ✅ |
| 01 §3, §8; 02 §2, §7, §8 | 8 tabel z dokładnymi kolumnami/typami, PK, FK, 4 indeksy FK, `REPLICA IDENTITY FULL`, katalogi 32 951/3 095/71, widoki `sim.v_truth_*`, wiersz `sim.sim_state` | integracyjny | `tests/integration/test_simulator.py::test_tables_created_with_detected_types_pks_fks_and_indexes` | ✅ |
| 01 §5 | kontener zdrowy, `healthcheck.py` = 0, heartbeat niesie fazę | integracyjny | `test_simulator.py::test_healthcheck_reports_the_phase` | ✅ |
| 01 §8, 02 §6 | przerwanie zakupu: właściwy writer (`Database.apply`) zabity `SIGKILL` na ostatniej instrukcji transakcji (blokada wiersza `sim_state` trzymana przez test) i błąd na ostatnim INSERT → brak połowicznego zamówienia, kursor bez zmian | integracyjny | `test_simulator.py::test_writer_killed_before_its_last_statement_leaves_no_half_order`, `::test_purchase_failing_on_its_last_statement_is_rolled_back_entirely` | ✅ |
| 01 §2 | tabele `shop.*` bez wiersza `sim.sim_state` → odmowa startu, nic nie tworzone | integracyjny | `test_simulator.py::test_refuses_to_start_when_tables_exist_without_a_state_row` | ✅ |
| 01 §5 | `SIGTERM` (`compose stop`) → kod 0 po bieżącej transakcji, kursor spójny, `up -d` wznawia bez duplikatów | integracyjny, `slow` | `test_simulator.py::test_sigterm_stops_after_the_current_transaction_and_restart_resumes` | ✅ |
| 01 §5 | nieświeży heartbeat (proces `SIGSTOP`) → `healthcheck.py` = 1 i nieudane sprawdzenia Dockera; `SIGCONT` → zdrowy | integracyjny | `test_simulator.py::test_stale_heartbeat_fails_the_healthcheck` | ✅ |
| 01 §4, §9 | zmieniony `SIM_START` przy restarcie ignorowany z ostrzeżeniem, okno w `sim_state` bez zmian | integracyjny, `slow` | `test_simulator.py::test_changed_sim_start_on_restart_is_ignored_with_a_warning` | ✅ |
| 01 §7 | ewolucja schematu w czystym strumieniu: wyłączona domyślnie, zdarzenie w swojej chwili, kanał deterministyczny dla późniejszych zakupów, SQL nullable, model dokładny, reguły zmiany ustawienia przy restarcie | unit | `tests/unit/test_simulator_evolution.py::test_off_leaves_the_stream_unchanged`, `::test_schema_event_sits_at_its_moment_and_only_later_purchases_get_a_channel`, `::test_same_second_purchase_comes_after_the_schema_event`, `::test_delivery_channel_is_deterministic_and_covers_all_values`, `::test_rendered_sql_adds_a_nullable_column_and_fills_it`, `::test_fold_counts_the_evolution_and_the_channels_exactly`, `::test_config_parses_the_evolution_moment`, `::test_changed_setting_on_restart_follows_the_resume_rules[…]` (7 przypadków) | ✅ |
| 01 §7 | writer stosuje ewolucję (kolumna `text` nullable, `REPLICA IDENTITY FULL`, wiersz z kanałem, kursor) w transakcji wycofywanej; stos bez zmian | integracyjny | `tests/integration/test_simulator.py::test_schema_evolution_event_adds_a_nullable_column_and_fills_it_rolled_back` | ✅ |
| 01 §7, 03 §5 | ewolucja na żywo z `SIM_SCHEMA_EVOLUTION_AT` w oknie: DDL raz (także po `kill`), kanały = czysty model, nowa wersja schematu przechodzi przez Debezium/Spark | e2e (własny stos) | etap 6, `tests/e2e/` | ⏳ |
| 01 §4, §9 | po `SIM_END` faza `done`: kontener bezczynny i zdrowy; późniejszy `SIM_END` + restart wznawia od kursora | e2e (własny stos, okno kilku dni) | etap 6, `tests/e2e/` | ⏳ |
| 01 §5 | stan bazy = preload + zdarzenia do kursora w czystym modelu (liczby, c/u/d, status × przychód) w jednym snapshocie | integracyjny | `test_simulator.py::test_database_state_equals_the_pure_fold_of_events_up_to_the_cursor`, `::test_sim_clock_is_a_naive_timestamp_in_the_window` | ✅ |
| 01 §5, §8 | `kill` + restart symulatora: wznowienie od `sim_state`, bez ponownej inicjalizacji, bez duplikatów, liczby jak bez przerwy | integracyjny, `slow` | `test_simulator.py::test_kill_and_restart_resumes_from_sim_state_without_duplicates` | ✅ |
| 03 §8, 08 §14 | `connect-init` zależy od zdrowego symulatora; plik konektora to czysty JSON z `${env:…}`, trybami, heartbeatem, grupami `topic.creation`, nadpisaniami producenta i `table.exclude.list` | unit | `test_compose_file.py::test_connect_init_waits_for_the_simulator_preload`, `::test_connector_json_is_pure_json_with_env_secret_and_topic_groups` | ✅ |
| 04 §6, §8 | reguły walidacji consumera, opis zdarzeń, statystyki, śledzenie zamówienia | unit | `tests/unit/test_consumer_rules.py` (27 testów) | ✅ |
| 03 §8, §11 | konektor zarejestrowany przez `connect-init`, `RUNNING`, slot aktywny; ponowny `PUT` bez zmian; hasło nigdy w REST | integracyjny | `tests/integration/test_debezium.py::test_connector_registered_by_connect_init_and_running`, `::test_register_twice_keeps_one_connector_with_the_current_config`, `::test_password_is_never_exposed_by_the_rest_api`, `test_connect.py::test_connect_init_registered_the_connector` | ✅ |
| 03 §1, §3, §11 | INSERT/UPDATE/DELETE → `c/u/d` z pełnym `before`, `source.lsn`, blok `transaction`, tombstone; sondy usunięte po teście | integracyjny | `test_debezium.py::test_insert_update_delete_give_c_u_d_with_full_before`, `::test_probe_rows_were_removed` | ✅ |
| 03 §2, §11 | preload jako `op=r` (dokładnie raz), strumień bez dziury i duplikatu, `r` przed strumieniem, jeden snapshot | integracyjny | `test_debezium.py::test_preload_rows_arrive_as_snapshot_reads_and_the_stream_continues_without_gap_or_duplicate`, `::test_snapshot_time_is_recorded_in_source` | ✅ |
| 03 §6, §11 | `decimal(10,2)` i `timestamp-millis` w rejestrze; wartości z Kafki = Postgres | integracyjny | `test_debezium.py::test_timestamps_and_decimals_are_logical_types` | ✅ |
| 03 §4, §11 | klucz = PK każdej z 8 tabel, subject `<temat>-key`, `order_items-key` = `(order_id, order_item_id)` | integracyjny | `test_debezium.py::test_message_key_is_the_table_pk_with_its_own_subject` | ✅ |
| 03 §4, 04 §4, §8 | zmiany jednego wiersza w jednej partycji, w kolejności LSN | integracyjny | `test_debezium.py::test_changes_of_one_row_stay_in_one_partition_in_lsn_order`, `test_kafka.py::test_changes_of_one_row_stay_in_one_partition_in_order` | ✅ |
| 03 §5, §11; 04 §5 | `ADD COLUMN` → nowa wersja, `is_compatible: true`, `RUNNING`; po `DROP COLUMN` stary id | integracyjny | `test_debezium.py::test_add_nullable_column_gives_a_new_schema_version_and_the_connector_stays_running` | ✅ |
| 03 §5, §11; 04 §8 | niezgodna zmiana typu → rejestr odrzuca, zadanie `FAILED`; naprawa przez `compatibility=NONE` dla subjectu + restart | integracyjny, `slow` | `test_debezium.py::test_incompatible_type_change_is_rejected_and_the_connector_fails_then_recovers` | ✅ |
| 03 §7, §11; 02 §5 | heartbeat: upsert `dbz.heartbeat`, temat heartbeat, slot przesuwa się bez ruchu w `shop`; uprawnienia roli `debezium` w `sim` | integracyjny (+`slow`) | `test_debezium.py::test_heartbeat_writes_the_sim_table_and_feeds_the_heartbeat_topic`, `::test_heartbeat_advances_the_slot_while_shop_is_idle`, `::test_debezium_role_cannot_read_sim_state_even_with_heartbeat_access`, `test_postgres.py::test_heartbeat_table_grants_and_publication` | ✅ |
| 03 §7 | metadane transakcji: BEGIN/END, `event_count`, `data_collections`, wspólny `txId` | integracyjny | `test_debezium.py::test_purchase_transaction_metadata` | ✅ |
| 03 §9, §13 | opóźnienie CDC i lag slotu na domyślnych ustawieniach; strumień nadąża za symulatorem | integracyjny | `test_debezium.py::test_cdc_latency_and_slot_lag_at_defaults`, `::test_stream_keeps_up_with_the_simulator` | ✅ |
| 04 §2, §3, §8 | tematy `olist.*` z partycjami/`cleanup.policy`/retencją z projektu; nowa tabela → grupa domyślna; `_*` bez tematu | integracyjny | `test_kafka.py::test_business_topics_match_the_topic_design`, `::test_new_table_gets_a_topic_from_the_default_group` | ✅ |
| 04 §2, §8 | DELETE → `d` + tombstone (te same bajty klucza) | integracyjny | `test_kafka.py::test_delete_gives_d_then_tombstone` | ✅ |
| 04 §2, §8 | kompaktowanie `order_items`: każda pozycja z ostatnią wersją | integracyjny, `slow` | `test_kafka.py::test_compaction_keeps_the_last_version_of_every_item` | ✅ |
| 04 §5, §7 | AVRO < JSON < JSON+schemat; `acks=all`/idempotencja/`zstd` przyjęte, współczynnik kompresji | integracyjny | `test_kafka.py::test_avro_messages_are_smaller_than_json_with_schema`, `::test_producer_overrides_are_applied_and_zstd_compresses` | ✅ |
| 04 §1, §5, §8 | subjecty `<temat>-key/-value` i tylko takie; endpoint zgodności: pole nullable ✅, zmiana typu ❌, pole wymagane ❌ | integracyjny | `test_schema_registry.py::test_subjects_follow_the_topic_name_strategy`, `::test_compatibility_endpoint_accepts_nullable_field_and_rejects_type_change` | ✅ |
| 04 §6, §8 | consumer: dekodowanie przez rejestr, `--validate` (0 naruszeń poza celową sondą), `--stats`, `--follow-order`, z hosta | integracyjny | `tests/integration/test_consumer.py::test_consumer_container_decodes_keys_and_values_through_the_registry`, `::test_consumer_validate_flags_only_the_bad_probe`, `::test_consumer_stats_mode_reports_rates_ops_lag_and_schemas`, `::test_follow_order_prints_the_lifecycle_across_topics`, `::test_consumer_runs_from_the_host_too` | ✅ |
| 02 §9, §10 | initdb dwa razy → identyczny stan, teraz łącznie z tabelą `dbz.heartbeat` w publikacji i jej uprawnieniami | integracyjny | `test_postgres.py::test_initdb_twice_gives_identical_state` (snapshot katalogu rozszerzony o `pubtable` i `grant`) | ✅ |
| 03 §11 | `kill connect` w trakcie strumienia → wznowienie od offsetu, lag slotu do zera, uzgadnianie Postgres ↔ silver | e2e | etap 6 (wymaga silver z etapu 3; skill `fault-injection`) | ⏳ e2e |
| 04 §8 | `kill kafka` w trakcie strumienia → Debezium i Spark wznawiają, uzgadnianie zgodne | e2e | etap 6 | ⏳ e2e |
| 05 §2, §3, §10 | dekodowanie Confluent AVRO per `schema_id` (v1+v2 w paczce), tombstone odfiltrowany, wiadomość bez nagłówka → kwarantanna, najwyższy LSN w paczce | unit | `tests/unit/test_spark_decode.py` (4 testy) | ✅ |
| 05 §4, §10 | MERGE c→u→d (miękkie usuwanie), ta sama paczka dwa razy, starsze po nowszym i `c` po `d`, `ADD COLUMN` w silver, wymuszona zmiana typu rzutowana | unit (Delta lokalnie) | `tests/unit/test_spark_silver.py` (5 testów) | ✅ |
| 05 §5, §6, §10 | SCD2 + fakt z wersją z chwili zakupu, akumulujący snapshot i `is_late`, watermark (Structured Streaming), ujemna korekta, reguły jakości (cała paczka gold), exactly-once zdarzeń przychodu po awarii między commitami | unit (Delta lokalnie; 2 × `slow`) | `tests/unit/test_spark_gold.py` (7 testów) | ✅ |
| 05 §8, 06 §7, §9 | konfiguracja sesji: bez `enableFastS3AListFrom`, DV wyłączone, S3A path-style | unit | `tests/unit/test_spark_session.py` (2 testy) | ✅ |
| 05 §1, 08 §1, §2 | `spark` domyślny, healthcheck, `depends_on`; JAR-y obrazu z macierzy z `.sha1`; `FROM` przypięty | unit | `test_compose_file.py::test_spark_service_runs_by_default_after_the_connector_and_minio_init`, `::test_spark_image_pins_every_connector_jar_with_a_checksum`, `::test_dockerfiles_pin_base_images` | ✅ |
| 05 §10 | prawdziwe wiadomości Debezium → bronze i silver, typy `decimal`/`timestamp` = Postgres, UPDATE i DELETE (miękkie) | integracyjny | `tests/integration/test_spark.py::test_real_debezium_messages_reach_bronze_and_silver_with_types` | ✅ |
| 05 §4, §10 | PK ze schematu klucza (8 tabel, bronze i silver) i dla tabeli dodanej w trakcie; Spark bez połączeń z Postgresem | integracyjny | `test_spark.py::test_primary_keys_come_from_the_key_schema`, `::test_add_column_reaches_silver_without_restart`, `::test_spark_never_connects_to_postgres` | ✅ |
| 05 §2, §10 | `ADD COLUMN` w trakcie działania → silver bez restartu kontenera | integracyjny | `test_spark.py::test_add_column_reaches_silver_without_restart` | ✅ |
| 05 §3, §10 | restart z checkpointu → unikalne `(topic, partition, offset)` w bronze | integracyjny, `slow` | `test_spark.py::test_restart_from_checkpoint_keeps_bronze_offsets_unique` | ✅ |
| 06 §9, §11, §12 | każda tabela gold/ops czytelna dla delta-rs (konto `dashboard`) i DuckDB, `minReaderVersion = 1`, bez DV i column mapping; zapis Sparka = odczyt delta-rs = odczyt DuckDB | integracyjny | `tests/integration/test_delta.py::test_every_gold_and_ops_table_is_readable_by_delta_rs_and_duckdb`, `::test_spark_write_reads_identically_in_delta_rs_and_duckdb` | ✅ |
| 06 §4, §5, §6, §11 | time travel, RESTORE, CDF (pre/post/delete), CHECK odrzuca, niezgodny typ odrzucony / `mergeSchema` przyjęty | integracyjny | `test_delta.py::test_time_travel_returns_the_state_before_a_later_merge`, `::test_restore_returns_the_table_to_a_version`, `::test_change_data_feed_after_merge_has_pre_post_images_and_deletes`, `::test_write_breaking_a_check_constraint_is_rejected`, `::test_type_mismatch_is_rejected_and_a_new_column_needs_merge_schema` | ✅ |
| 06 §7, §11 | OPTIMIZE (mniej plików, ta sama suma), VACUUM (retencja, time travel w oknie), zadanie utrzymania w kontenerze z pomiarem w `ops/maintenance_log` | integracyjny (+`slow`) | `test_delta.py::test_optimize_compacts_files_and_keeps_the_data`, `::test_vacuum_removes_only_files_older_than_the_retention`, `::test_maintenance_job_runs_in_the_container_and_logs_before_after` | ✅ |
| 05 §10, 07 | uzgadnianie `sim.v_truth_*` = silver; sumy gold = sumy silver (jednorazowo na stosie: [05-spark.md](05-spark.md) §12 „Pomiary”) | e2e | etap 5 | ⏳ e2e |
| 05 §10 | `kill` Sparka w trakcie strumienia, restart → uzgadnianie zgodne | e2e (awaria) | etap 5 | ⏳ e2e |
| 05 §10 | opóźnienie end-to-end poniżej progu przy domyślnym tempie (progi w 05 §12) | e2e | etap 5 | ⏳ e2e |
| 06 §11 | usunięcie i odtworzenie kontenera Sparka → wznowienie z checkpointów w MinIO, bez duplikatów, uzgadnianie | e2e | etap 5 | ⏳ e2e |
| 06 §11 | restart MinIO w trakcie strumienia → Spark ponawia, uzgadnianie zgodne | e2e (awaria) | etap 5 | ⏳ e2e |
| 02–06 (awarie) | `kill` `connect`/`kafka`/`minio`/`spark`/`simulator` → uzgadnianie Postgres ↔ silver zgodne | e2e | etap 6 (skill `fault-injection`) | ⏳ |

## 11. CI

`.github/workflows/ci.yml`:

| Job | Kiedy | Co robi |
|---|---|---|
| `lint-unit` | każdy push i PR | `astral-sh/setup-uv` (uv 0.9.18) → `uv python install` (3.12 z `.python-version`) → `uv sync --frozen` → `ruff check`, `ruff format --check` → `pytest -m unit` → `docker compose config -q` (domyślnie i z profilem `pipeline`) |
| `integration` | ręcznie (`workflow_dispatch`, wejście `integration=true`) | `docker compose build minio connect` → `make data` → `docker compose up -d` → `wait-healthy.sh 300` → `make test-integration-fresh` (pełny zestaw z `slow`: `down -v`, ponowny start, test budżetu 180 s, wszystkie testy integracyjne) → przy błędzie `ps -a` + logi → zawsze `down -v` |

Runnery GitHub są `amd64`: obrazy gotowe są multi-arch, `wpdb/minio` i `wpdb/connect` budują się natywnie (bez emulacji). Etap 3: testy jednostkowe Sparka potrzebują Javy 17 (domyślna na `ubuntu-24.04`, `tests/helpers/spark.py` korzysta też z `JAVA_HOME_17_X64`) i przy pierwszym uruchomieniu pobierają przez Ivy JAR-y Delta, spark-avro i hadoop-aws (ok. 290 MB, warto dodać cache `~/.ivy2`); obraz `wpdb/spark` buduje `docker compose up -d` (pobranie JAR-ów z Maven Central). Do decyzji `platform-devops`: `timeout-minutes: 40` jobu `integration` jest na granicy, bo pełny zestaw z etapem 3 trwa ok. 25 min (§2.1). Otwarte: cache warstw Go w GHA (`docker/bake-action` z `cache-to=type=gha`) skróciłby job o ok. 2 min; limit pobrań Docker Hub bez logowania (100/h na IP) może wymagać `docker/login-action` z sekretem.

## 12. Wdrożenie krok po kroku (README)

Wymagania: Docker Desktop (Compose v2, BuildKit), `uv`, `make`, ~10 GB RAM dla Dockera, dostęp do Docker Hub, quay.io, Confluent Hub i GitHub (źródła MinIO).

```bash
git clone https://github.com/oszypczy/wpdb-data-platform && cd wpdb-data-platform
make data                 # CSV Olist do data/olist/ (etap 1 i dalsze)
make up                   # kopiuje .env.example → .env, docker compose up -d --build, czeka na healthy
make ps                   # stan i zdrowie kontenerów
make test-unit            # bez Dockera
make test-integration     # zestaw szybki (bez `slow`) na działającym stosie (uruchamia go, gdy leży)
make test-integration-full   # wszystkie testy integracyjne, także `slow`, na działającym stosie
make test-integration-fresh  # wszystkie od pustych wolumenów, z testem budżetu startu (180 s)
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

- **Rozstrzygnięte (etap 2): snapshot przed startem konektora** (01 §4, 03 §2): `connect-init` ma `depends_on: simulator: condition: service_healthy`; test `test_debezium.py::test_preload_rows_arrive_as_snapshot_reads_and_the_stream_continues_without_gap_or_duplicate` dowodzi `op=r` dla historii sprzed `SIM_START` i strumienia bez dziury i duplikatu.
- **Rozstrzygnięte (etap 1): widoki `sim.v_truth_*`** leżą w `containers/postgres/sql/truth-views.sql` (zamontowany do kontenera symulatora jako `/app/sql/truth-views.sql`) i są wykonywane przez symulator jako `shop_app` w transakcji inicjalizacji; indeksy FK według 02 §7 zakłada symulator. Oba wcześniej pomijane testy przechodzą.
- **`make up` zależy od `make data`** (symulator bez CSV nie staje się zdrowy); job CI `integration` pobiera dane przed startem stosu (`make data` z Kaggle bez logowania: do potwierdzenia na runnerze GitHub).
- **Rozstrzygnięte (etap 3): zdarzenia syntetyczne symulatora** (01 §7): ewolucja schematu zaimplementowana (`SIM_SCHEMA_EVOLUTION_AT`, domyślnie pusta = wyłączona, bo `test_debezium.py` sam dodaje i usuwa `delivery_channel`); korekty cen świadomie nie zaimplementowane. Otwarte: e2e ewolucji na żywo na własnym stosie (etap 6).
- **Rozstrzygnięte (etap 2): `pg_hba.conf`** domyślne z obrazu obejmuje replikację: pełne połączenie `pgoutput` roli `debezium` działa (slot `debezium_slot` aktywny, `test_debezium.py::test_connector_registered_by_connect_init_and_running`).
- **Rozstrzygnięte (etap 2): heartbeat** w `dbz.heartbeat` (02 §5, 03 §7): tabela w publikacji, rola `debezium` ma `USAGE` na `dbz` i `SELECT/INSERT/UPDATE` tylko na tej tabeli (`20-heartbeat.sql`). Tabela sygnałowa (snapshot przyrostowy) nie jest używana: odłożona (03 §2).
- **Rozstrzygnięte (etap 2): `OFFSET_FLUSH_INTERVAL_MS=5000`** na workerze Connect (domyślne obrazu 60000; w logu workera `offset.flush.interval.ms = 5000`, test `test_compose_file.py::test_connect_worker_flushes_offsets_every_5_s`): po restarcie zadania konektor powtarza do ok. 5 s zdarzeń zamiast 60 s (03 §9), a testy, które usuwają przechwytywane tabele, czekają na zapis offsetu krócej (`tests/helpers/cdc.py::wait_committed_past`, limit 60 s). Exactly-once w Connect sprawdzone i świadomie nie włączone (03 §9).
- **Snapshot przyrostowy na żądanie** (03 §2): wymaga tabeli sygnałowej w `shop` i uprawnień zapisu dla `debezium`; kandydat do etapu 6 (odtworzenie silver po awarii).
- **Rozstrzygnięte (etap 3): kontrakt dla Sparka** (03 §13): dekodowanie per `schema_id` (`lakehouse/avro.py`), tombstones odfiltrowane, powtórki z tym samym `source.lsn` ignorowane przez strażnika LSN w `MERGE`; testy w §10.
- **Otwarte (dla `database-cdc`, zaobserwowane w etapie 3):** na stosie bezczynnym przez ponad dobę (symulator w fazie `done`, tylko heartbeat) odtworzenie kontenera `connect` (`docker compose up -d --build`) skończyło się zadaniem `FAILED`: „The connector is trying to read change stream starting at … lsn=0/50B8408 …, but this is no longer available on the server”, podczas gdy slot miał już `restart_lsn` 0/50BBB58 (offset w `connect-offsets` = LSN ostatniej zmiany danych sprzed doby, a heartbeat przesuwał slot). Do sprawdzenia, czy offset heartbeatu jest zapisywany i czy `offset.mismatch.strategy`/`slot` wymaga zmiany; obejście: `make reset`.
- **Etap 3 → testy:** testy etapów 1–2 modyfikują współdzielony stos, który Spark teraz przetwarza (sondy `zz_stage2_probe_*` dostają tabele bronze/silver; zapytanie silver tabeli usuniętej jest wygaszane po 2 min; `ADD COLUMN delivery_channel` na `orders` restartuje zapytania `silver_orders` i `silver_to_gold`, co jest obsługiwane automatycznie).
- **Rozstrzygnięte (etap 3): RAM pełnego stosu** zmierzony (§7): ≈ 5.3–5.8 GB zajęte w trakcie strumienia przy limitach ≈ 9.5 GB; Spark 3 GB (2.6 GB zmierzone), MinIO podniesiony do 1 GB (493 MB zmierzone przy 512 MB). Heap Connecta zostaje (1.13 GB maks.).
- **Tematy `connect-offsets`/`connect-status` z 1 partycją**: wystarczające dla jednego workera; Connect przyjmuje istniejące tematy bez zmian.
- **CI:** cache Go w GHA i logowanie do Docker Hub (§11).
- **Rozstrzygnięte (etap 2): MCP `kafka`** dekoduje tematy `olist.*` (klucze, wartości, `Decimal` i daty jako tekst); `describe_topic` zgłaszał błąd dla tematów z 3 partycjami w trakcie etapu 2 (do sprawdzenia przez `platform-devops`; CLI `kafka-topics --describe` działa).
