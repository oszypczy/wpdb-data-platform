# Jak budowałem platformę z agentami AI

Platformę zbudował zespół agentów Claude Code, który zaprojektowałem tak, jak podzieliłbym pracę w prawdziwym zespole danych ([szczegóły](design/09-agents.md)). Każdy agent ma swój komponent i odpowiada za kod, testy i dokumentację tej części: 
- `business-simulator` (symulator)
- `database-cdc` (Postgres, Debezium, Kafka i Schema Registry)
- `streaming-lakehouse` (Spark i Delta w MinIO)
- `analytics-dashboard` (dashboard) 
- `platform-devops` (Docker Compose, CI, wersje)
- `qa-engineer`, jest niezależnym testerem

Wspólne procedury agenci dostają jako **skille**. Z działającym systemem łączą się przez **serwery MCP**
- Postgres tylko do odczytu
- mój własny serwer MCP dla Kafki (dekoduje AVRO przez Schema Registry)
- DuckDB z dostępem tylko do odczytu do tabel Delta w MinIO
- Context7 z aktualną dokumentacją bibliotek 
- draw.io do diagramów

Pracę prowadziłem etapami według planu: najpierw szkielet platformy, potem kolejne komponenty w kolejności przepływu danych. Właściciel etapu buduje, a QA weryfikuje.
