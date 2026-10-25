# Dashboard: Streamlit

Dashboard to **końcowy odbiorca platformy**: aplikacja Streamlit w osobnym kontenerze, która czyta warstwy `gold` i `ops` z MinIO ([06-minio-delta.md](06-minio-delta.md)) i odświeża się na żywo w trakcie symulacji.

Zgodnie z zasadą architektury dashboard **nie łączy się z Postgresem ani z Kafką** i nie używa Sparka. Czyta wyłącznie tabele Delta, kontem `dashboard` tylko do odczytu.

## 1. Część podstawowa

- **Kontener** `dashboard` (port `127.0.0.1:8501`), healthcheck `/_stcore/health`, start po `minio-init` (kubełki i konta istnieją), bez zależności od Sparka.
- **Odczyt** tabel gold przez `deltalake` (delta-rs) do Arrow, a zapytania SQL przez DuckDB na Arrow. Bez Sparka i bez JVM.
- **Wykresy** w Plotly (w tym mapa), **automatyczne odświeżanie** co 10 s.
- **Dane dostępowe** z `.env` (konto `dashboard`).

## 2. Strony

| Strona | Źródło (gold/ops) | Zawartość |
|---|---|---|
| **Przegląd** | `fact_order_items`, `fact_orders_lifecycle` | kafelki: przychód, liczba zamówień, średnia wartość koszyka, % spóźnień, mediana dni dostawy; **zegar symulacji** („stan na 2017-11-24 14:00”) |
| **Sprzedaż w czasie** | `agg_revenue_hourly` | przychód na godzinę i dzień, z ujemnymi korektami z anulacji; widoczny szczyt Black Friday |
| **Mapa** | `agg_revenue_hourly`, `fact_orders_lifecycle`, `dim_geography` | kartogram stanów Brazylii; przełącznik metryki: przychód / zamówienia / % spóźnień (zamówienia i spóźnienia z faktu cyklu życia, bo agregat godzinowy ma tylko kwoty) |
| **Lejek statusów** | `agg_status_funnel` (agregat przejść `before → after`) | główna ścieżka created → approved → shipped → delivered (lejek), wyjścia ze ścieżki (anulacje, `unavailable`, …), przejścia w czasie, wszystkie pary |
| **Logistyka** | `fact_orders_lifecycle` | rozkład czasu dostawy, % spóźnień per stan, mediana czasu do akceptacji (kafelek) |
| **Produkty** | `fact_order_items`, `dim_product` | top kategorie (nazwy angielskie), przychód i fracht |
| **Klienci** | `dim_customer` (SCD2) | klienci powracający, przeprowadzki (historia wersji klienta z prawdziwych danych) |
| **Zdrowie potoku** | `ops.stream_metrics`, `ops.e2e_latency`, `ops.cdc_heartbeat`, `ops.quarantine`, `ops.maintenance_log` | opóźnienie end-to-end (p95 per warstwa), zdarzenia/s i czas mikropaczek per zapytanie, świeżość CDC (ostatni heartbeat), liczba wierszy w kwarantannie per reguła, ostatnie OPTIMIZE/VACUUM; kafelki ze statusem (ikona + etykieta) |
| **Podróż w czasie** | dowolna tabela gold | wybór wersji tabeli (time travel delta-rs) i porównanie wskaźników „teraz” vs „wersja N”; historia operacji (`DESCRIBE HISTORY`) |

Uwagi:
- **Świeżość CDC:** Spark zapisuje zdarzenia z tematu heartbeat Debezium do `ops.cdc_heartbeat` ([05-spark.md](05-spark.md) §7). Dashboard nie musi czytać Kafki.
- **Opóźnienie slotu replikacyjnego** nie jest pokazywane, bo wymagałoby połączenia z Postgresem. Jest sprawdzane w testach ([02-postgresql.md](02-postgresql.md) §5).

## 3. Rozszerzenie: wydajny odczyt na żywo

- **Odczyt tylko po zmianie:** przy każdym odświeżeniu sprawdzana jest wersja tabeli Delta (tani odczyt dziennika). Dane są wczytywane ponownie tylko, gdy wersja się zmieniła. Pamięć podręczna jest kluczowana wersją tabeli. *Implementacja (etap 4):* zamiast `st.cache_data` własna klasa `VersionedReader` (`dashboard/lake.py`), trzymana raz na proces w `st.cache_resource` i wspólna dla wszystkich sesji przeglądarki: wersja = `DeltaTable.update_incremental()` (listowanie tylko nowych wpisów `_delta_log`, 2–4 ms na tabelę z MinIO), dane tabeli w Arrow pod kluczem (tabela, wersja), a wyniki zapytań (KPI, agregaty) pod kluczem (nazwa, wersje tabel wejściowych, parametry), więc przeliczane są też tylko po zmianie wersji. Powód: logika pamięci podręcznej jest testowalna jednostkowo bez Streamlit (licznik odczytów), a `st.cache_data` kopiowałoby (pickle) tabele przy każdym trafieniu.
- **Odświeżanie fragmentów:** odświeża się tylko część strony z danymi (`st.fragment` z `run_every`), a nie cała aplikacja. Filtry użytkownika nie są resetowane (widżety filtrów są poza fragmentem; test `test_filters_are_outside_the_live_fragment`).
- **Puste stany:** tuż po starcie systemu gold jest pusty. Dashboard pokazuje komunikat „czekam na dane” zamiast błędu. Tabela, której Spark jeszcze nie utworzył, jest czytana jako pusta tabela ze schematem kontraktu (`dashboard/contract.py`), więc każde zapytanie działa bez przypadków szczególnych. Błąd odczytu (np. restart MinIO) jest pokazywany w miejscu danych, a następne odświeżenie ponawia odczyt.

## 4. Rozszerzenie: mapa

Kartogram 27 stanów z plikiem GeoJSON granic stanów Brazylii, który leży w repozytorium (bez pobierania przy starcie). Kolor oznacza wybraną metrykę, a podpowiedź pokazuje szczegóły stanu.

**Plik (etap 4):** `containers/dashboard/assets/brazil-states.geojson` (111 KB), wycięty z **Natural Earth 1:50m „Admin 1 – States, Provinces” v5.1.2** (`geojson/ne_50m_admin_1_states_provinces.geojson` z [nvkelso/natural-earth-vector](https://github.com/nvkelso/natural-earth-vector)): 27 obiektów z `adm0_a3 = BRA`, `id` = dwuliterowy kod stanu (= `customer_state` w Olist), współrzędne zaokrąglone do 4 miejsc. **Licencja: domena publiczna** („All versions of Natural Earth raster + vector map data … are in the public domain”, [naturalearthdata.com/about/terms-of-use](https://www.naturalearthdata.com/about/terms-of-use/)); atrybucja niewymagana. Pochodzenie, suma SHA-256 pobranego pliku i kod wycięcia: `containers/dashboard/assets/README.md`.

## 5. Rozszerzenie: struktura kodu

- **Warstwa zapytań:** czyste funkcje `tabela(e) → DataFrame` (KPI, agregaty, lejek), niezależne od Streamlit.
- **Warstwa UI:** tylko układ i wykresy.

**Implementacja (etap 4, `containers/dashboard/`):**

| Plik | Rola |
|---|---|
| `dashboard/contract.py` | schematy Arrow tabel gold/ops, które czyta dashboard (podzbiór `containers/spark/lakehouse/tables.py`; zgodność typów sprawdza test), projekcja na kontrakt (brakujące kolumny = `null`, nadmiarowe pomijane) |
| `dashboard/lake.py` | `Lake` (delta-rs: wersja, odczyt wersji N, historia; brak tabeli = pusta tabela) i `VersionedReader` (pamięć podręczna §3) |
| `dashboard/queries.py` | warstwa zapytań: DuckDB w pamięci na Arrow (bez S3 i rozszerzeń), definicje KPI w docstringu |
| `dashboard/charts.py`, `dashboard/ui.py` | jeden styl wykresów Plotly i wspólna obsługa stron (fragment `run_every`, puste stany, formaty liczb) |
| `app.py`, `views/*.py` | `st.navigation` z 9 stronami (grupy „Biznes” i „Platforma”) |

**Definicje KPI** (strona „Przegląd”, porównywalne z `sim.v_truth_*`):
- **przychód netto** = suma `fact_order_items.price` (pozycje, które istnieją; pozycje anulowanego zamówienia są usunięte, więc korekty są już odjęte) = `v_truth_totals.revenue`; obok sprzedaż brutto i korekty z `agg_revenue_hourly` (brutto + korekty = netto),
- **zamówienia** = wiersze `fact_orders_lifecycle` = `v_truth_totals.orders`,
- **średni koszyk** = przychód netto / zamówienia z co najmniej jedną pozycją,
- **% spóźnień** = `is_late` / `is_delivered` (= `late` / `delivered` z `v_truth_order_lifecycle`),
- **mediana dostawy** = mediana `d_to_deliver` zamówień dostarczonych,
- **zegar symulacji** = najpóźniejszy czas biznesowy w `fact_orders_lifecycle` (zakup, akceptacja, wysyłka, dostawa).

**Styl wykresów:** kategorie w stałej kolejności (ten sam kolor dla tej samej serii niezależnie od filtra), jedna oś Y na wykres, sprzedaż i korekty jako para niebieski/czerwony (korekty poniżej zera), mapa w jednej skali sekwencyjnej (niebieski), kolory statusu tylko w kafelkach zdrowia potoku (zawsze kolorowa kropka z etykietą; w interfejsie nie ma emoji). Tło przezroczyste i `theme="streamlit"`, więc te same wykresy działają w trybie jasnym i ciemnym (sprawdzone zrzutami ekranu obu trybów). Linie w SVG (bez WebGL).

**Waluta:** jezioro trzyma kwoty Olist w BRL (tak jak źródło, żeby uzgadniać je z widokami prawdy w Postgresie), a dashboard pokazuje je w USD po jednym stałym kursie `DASHBOARD_BRL_USD` (domyślnie 0,3135 = 1 / 3,19, średni kurs USD/BRL z 2017 r., z którego pochodzi większość danych). Przeliczenie jest tylko w warstwie widoku (`ui.usd`, `ui.usd_fields`, `ui.usd_frame`); funkcje w `queries` zwracają BRL. Wyjątek: „Podróż w czasie” pokazuje surowe sumy kolumn, więc w BRL. Kafelek przychodu netto jest w pełnych dolarach, żeby kwota mieściła się w kolumnie; dokładna kwota, sprzedaż brutto, korekty i fracht są w podpowiedzi.

**Ustalenia z implementacji:**
- **Znaczniki czasu INT96.** Spark zapisuje `timestamp` w Parquet jako INT96 (domyślne `outputTimestampType`), a pyarrow czyta INT96 w nanosekundach, co przepełnia się dla `dim_customer.valid_to = 9999-12-31` (`ArrowInvalid: Casting from timestamp[ns] … would lose data`). Czytnik używa `ParquetReadOptions(coerce_int96_timestamp_unit="us")` w `to_pyarrow_dataset` (typ Delta i tak ma mikrosekundy); kod Sparka bez zmian. Test `test_dashboard_account_reads_gold_and_ops_but_not_bronze_or_silver` (odczyt `valid_to` = rok 9999 na żywym gold).
- **`TIMESTAMPTZ` z DuckDB wymaga `pytz`.** Wynik typu `TIMESTAMP WITH TIME ZONE` pobrany do Pythona importuje `pytz`, którego obraz nie ma (na hoście jest, więc błąd był widoczny dopiero w kontenerze). Każdy czas wychodzący z DuckDB jest rzutowany na `TIMESTAMP` (UTC bez strefy, jak w źródle); testy jednostkowe zapytań blokują `pytz`, a test integracyjny renderuje wszystkie strony wewnątrz obrazu (`test_every_page_renders_inside_the_container_image`).
- **Szeregi czasowe bez dziur:** przychód na godzinę/dzień/tydzień ma okresy bez zdarzeń jako zera (`generate_series`), bo linia przychodu netto łączyła okres preloadu (2016) z oknem symulacji.

Logikę wskaźników testuje się jednostkowo, a UI testem aplikacji (`streamlit.testing`).

## 6. Rozważona alternatywa: Grafana

Grafana byłaby naturalna dla szeregów czasowych, z dashboardami jako kod i alertami. **Nie ma jednak wbudowanego źródła danych dla Delta Lake.** Wymagałaby niepodpisanej wtyczki społecznościowej DuckDB z rozszerzeniem `delta` wbudowanym w obraz albo dodatkowej warstwy (Trino, własne API), czyli nowego komponentu. Zapis gold do Postgresa łamałby zasadę architektury.

Streamlit czyta Delta natywnie, obsługuje time travel w interfejsie i nie wymaga obejść.

## 7. Testy

Status: ✅ przechodzi, ⏳ e2e: odłożone do etapu 5 (decyzja z 2026-10-21, 00-overview). Fikstury: małe tabele Delta zapisane przez `deltalake` w `tests/helpers/dashboard_fixtures.py` (zestaw policzalny ręcznie, wartości oczekiwane w `EXPECTED`; `DASHBOARD_LAKE_ROOT=<katalog>` uruchamia na nich całą aplikację).

| Poziom | Test | Oczekiwany wynik | Identyfikator | Status |
|---|---|---|---|---|
| jednostkowy | KPI na małych tabelach Delta (fixture zapisane przez `deltalake`) | poprawny przychód, liczba zamówień, średni koszyk, % spóźnień, mediana dostawy | `tests/unit/test_dashboard_queries.py::test_kpis_on_small_delta_tables` (także zegar symulacji); strona: `tests/unit/test_dashboard_app.py::test_overview_shows_the_kpis_and_the_simulation_clock` | ✅ |
| jednostkowy | przychód z korektą z anulacji | ujemna korekta pomniejsza przychód | `test_dashboard_queries.py::test_negative_correction_reduces_revenue` (zdarzenia przychodu, agregat godzinowy i dzienny, filtr stanu, KPI: brutto + korekty = netto) | ✅ |
| jednostkowy | lejek | liczby przejść zgodne z danymi wejściowymi | `test_dashboard_queries.py::test_funnel_counts_match_the_input` | ✅ |
| jednostkowy | puste tabele | puste wyniki, brak wyjątków | `test_dashboard_queries.py::test_every_query_on_empty_tables_returns_empty_results`, `tests/unit/test_dashboard_lake.py::test_missing_table_reads_as_empty_and_appears_later`, `test_dashboard_app.py::test_page_waits_for_data_on_an_empty_lake[<9 stron>]` („czekam na dane”) | ✅ |
| jednostkowy | odczyt wersji N | zwraca stan tabeli z wersji N | `test_dashboard_lake.py::test_read_at_version_n_returns_that_state` (wersje 0/1/2, porównanie „teraz” vs N, historia) | ✅ |
| jednostkowy | pamięć podręczna | brak ponownego odczytu, gdy wersja tabeli się nie zmieniła | `test_dashboard_lake.py::test_no_reread_when_the_version_did_not_change` (5 odświeżeń = 3 odczyty i 1 obliczenie KPI; nowa wersja jednej tabeli = 1 odczyt tej tabeli) | ✅ |
| jednostkowy (dodatkowe) | pozostałe zapytania stron; kontrakt kolumn; strony na fiksturach; konfiguracja compose i obrazu | mapa (27 stanów), logistyka, produkty, klienci SCD2, zdrowie potoku, podróż w czasie; kolumny = `lakehouse/tables.py`, brak importów Sparka/Kafki/Postgresa; każda strona bez wyjątku; usługa domyślna po `minio-init`, healthcheck, przypięte zależności, GeoJSON 27 stanów | `test_dashboard_queries.py` (pozostałe testy), `tests/unit/test_dashboard_contract.py`, `test_dashboard_app.py::test_page_renders_on_small_delta_tables[<9 stron>]`, `::test_filters_are_outside_the_live_fragment`, `tests/unit/test_compose_file.py::test_dashboard_service_runs_by_default_after_minio_init_only`, `::test_dashboard_image_pins_its_dependencies_and_vendors_the_geojson` | ✅ |
| integracyjny (MinIO) | wszystkie strony w `streamlit.testing.AppTest` na danych w MinIO | każda strona renderuje się bez wyjątku | `tests/integration/test_dashboard.py::test_every_page_renders_on_the_live_minio_data[<9 stron>]` (z hosta, konto `dashboard`), `::test_every_page_renders_inside_the_container_image` (zależności obrazu, `minio:9000`), `::test_query_layer_on_live_gold_equals_duckdb_delta_scan` | ✅ |
| integracyjny | uprawnienia | konto `dashboard` czyta `gold` i `ops`, nie ma dostępu do `bronze`/`silver` | `test_dashboard.py::test_dashboard_account_reads_gold_and_ops_but_not_bronze_or_silver` (delta-rs i DuckDB, zapis odrzucony); na poziomie S3: `test_minio.py::test_dashboard_account_restricted` | ✅ |
| integracyjny | kontener | healthcheck zdrowy po starcie, także przy pustym gold | `test_dashboard.py::test_dashboard_container_is_healthy_and_serves_the_app`, `::test_dashboard_container_is_healthy_with_an_empty_gold` (`slow`: drugi kontener obrazu z pustym jeziorem, zdrowy, każda strona „czekam na dane”) | ✅ |
| e2e | zgodność z prawdą | KPI z warstwy zapytań = widoki uzgadniania w Postgresie (`sim.v_truth_*`) po zakończeniu przebiegu | etap 5 (`tests/e2e/`); jednorazowo na stosie 2026-10-24: przychód 780 132,69, zamówienia 5 591, spóźnione 70 = [05-spark.md](05-spark.md) §12 „Uzgodnienie” | ⏳ e2e |
| e2e | świeżość | podczas symulacji świeżość CDC i opóźnienie end-to-end poniżej ustalonych progów | etap 5; progi: świeżość CDC < 30 s, p95 e2e silver < 60 s, gold < 120 s (05 §12; te same progi kolorują kafelki strony „Zdrowie potoku”) | ⏳ e2e |

**Pomiary (2026-10-24, stos po przebiegu domyślnym, Apple Silicon):** zdjęcie pamięci kontenera: 77–80 MB bezczynnie, do 276 MB przy jednej sesji przechodzącej przez 9 stron, do 333 MB przy trzech równoległych sesjach (limit 512 MB, [08-operations.md](08-operations.md) §7); obraz `wpdb/dashboard` 1,08 GB (pyarrow 138 MB, deltalake 123 MB, DuckDB 53 MB). Sprawdzenie wersji 16 tabel gold/ops: ok. 3 ms na tabelę (`update_incremental`), pierwsze otwarcie tabeli ok. 150 ms, odczyt największych tabel gold (6–33 tys. wierszy) 5–20 ms; strona z danymi w pamięci podręcznej renderuje się w 30–300 ms (AppTest), strona „Zdrowie potoku” ok. 1 s. Świeżość widoczna na dashboardzie = opóźnienie potoku (Postgres → gold p95 ok. 45–52 s, 05 §12) + co najwyżej jeden okres odświeżania (10 s) + odczyt zmienionych tabel (< 0,1 s); świeżość CDC na stronie „Zdrowie potoku” mieściła się w 9,5–11,3 s (heartbeat co 10 s).

## 8. Podsumowanie

| | Podstawa | Z rozszerzeniami |
|---|---|---|
| Źródło | gold | gold + ops (zdrowie potoku), tylko Delta, konto tylko do odczytu |
| Zakres | kilka wykresów | 9 stron: biznes, logistyka, klienci (SCD2), zdrowie potoku, podróż w czasie |
| Odświeżanie | pełne przeładowanie | odczyt tylko po zmianie wersji tabeli, odświeżanie fragmentów |
| Mapa | — | kartogram stanów z lokalnym GeoJSON |
| Weryfikacja | ręczna | testy warstwy zapytań, testy aplikacji, zgodność KPI z Postgresem (e2e) |

## Do weryfikacji przy implementacji

- **Rozstrzygnięte (etap 4): wersja Streamlit z `st.fragment(run_every=…)`.** `streamlit==1.66.1` (2026-10-21, poprawka linii 1.66; `st.fragment` z `run_every` jest stabilne od 1.37, `st.navigation`/`st.Page` od 1.36, `width="stretch"` w `st.plotly_chart`/`st.dataframe`); `plotly==7.1.0`, `pandas==3.0.6`, `numpy==2.5.3`, `pyarrow==25.0.1` (Streamlit wyklucza tylko 25.0.0). Wersje w [08-operations.md](08-operations.md) §1, `containers/dashboard/pyproject.toml` + `uv.lock`; testy AppTest wszystkich stron (§7).
- **Rozstrzygnięte (etap 4): zgodność delta-rs z tabelami gold.** `deltalake==1.6.6` czyta wszystkie 16 tabel gold/ops (protokół czytelnika 1, zapisującego 2–3, bez DV, column mapping i CDF); jedyna pułapka to znaczniki czasu INT96 (§5 „Ustalenia”), rozwiązana po stronie czytelnika. DuckDB `1.5.6` służy tylko do SQL na Arrow (bez rozszerzeń `delta`/`httpfs` w kontenerze). Testy: `test_every_page_renders_on_the_live_minio_data`, `test_dashboard_account_reads_gold_and_ops_but_not_bronze_or_silver`, [06-minio-delta.md](06-minio-delta.md) §9.
- **Rozstrzygnięte (etap 4): źródło i licencja GeoJSON stanów Brazylii.** Natural Earth 1:50m admin-1 v5.1.2, domena publiczna (§4); plik w repozytorium, test `test_dashboard_image_pins_its_dependencies_and_vendors_the_geojson`.
