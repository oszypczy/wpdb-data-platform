# Mini Data Platform: analityka e-commerce w czasie rzeczywistym

Projekt semestralny (WPDB). Platforma danych w kontenerach Docker, która odtwarza historię prawdziwego sklepu internetowego (zbiór Olist) i przetwarza zmiany w bazie na bieżąco:

```
Olist CSV ─► Business System ─► PostgreSQL ─► Debezium ─► Kafka + Schema Registry ─► Spark ─► MinIO (Delta) ─► Streamlit
```

**Status:** projekt systemu gotowy, implementacja w toku.

## Dokumentacja

- [`docs/README.md`](docs/README.md): indeks dokumentacji i status komponentów
- [`docs/design/00-overview.md`](docs/design/00-overview.md): przegląd systemu, decyzje i plan prac

## Dane

Olist Brazilian E-Commerce (CC BY-NC-SA 4.0). Dane nie są w repozytorium. Pobranie:

```bash
make data
```

Szczegóły: [`data/olist/README.md`](data/olist/README.md).
