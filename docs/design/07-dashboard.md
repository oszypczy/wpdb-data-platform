# Dashboard: Streamlit

Dashboard to **końcowy odbiorca platformy**: aplikacja Streamlit w osobnym kontenerze, która czyta warstwy `gold` i `ops` z MinIO ([06-minio-delta.md](06-minio-delta.md)) i odświeża się na żywo w trakcie symulacji.

Zgodnie z zasadą architektury dashboard **nie łączy się z Postgresem ani z Kafką** i nie używa Sparka. Czyta wyłącznie tabele Delta, kontem `dashboard` tylko do odczytu.

## 1. Część podstawowa

- **Kontener** `dashboard` (port 8501), healthcheck `/_stcore/health`, start po zdrowym MinIO.
- **Odczyt** tabel gold przez `deltalake` (delta-rs) do Arrow, a zapytania SQL przez DuckDB na Arrow. Bez Sparka i bez JVM.
- **Wykresy** w Plotly (w tym mapa), **automatyczne odświeżanie** co 10 s.
- **Dane dostępowe** z `.env` (konto `dashboard`).

## 2. Strony

| Strona | Źródło (gold/ops) | Zawartość |
|---|---|---|
| **Przegląd** | `fact_order_items`, `fact_orders_lifecycle` | kafelki: przychód, liczba zamówień, średnia wartość koszyka, % spóźnień, mediana dni dostawy; **zegar symulacji** („stan na 2017-11-24 14:00”) |
| **Sprzedaż w czasie** | `agg_revenue_hourly` | przychód na godzinę i dzień, z ujemnymi korektami z anulacji; widoczny szczyt Black Friday |
| **Mapa** | `agg_revenue_hourly`, `dim_geography` | kartogram stanów Brazylii; przełącznik metryki: przychód / zamówienia / % spóźnień |
| **Lejek statusów** | agregat przejść `before → after` | liczba przejść między statusami w czasie, anulacje i `unavailable` |
| **Logistyka** | `fact_orders_lifecycle` | rozkład czasu dostawy, % spóźnień per stan, czas do akceptacji |
| **Produkty** | `fact_order_items`, `dim_product` | top kategorie (nazwy angielskie), przychód i fracht |
| **Klienci** | `dim_customer` (SCD2) | klienci powracający, przeprowadzki (historia wersji klienta z prawdziwych danych) |
| **Zdrowie potoku** | `ops.stream_metrics`, `ops.cdc_heartbeat`, `quarantine` | opóźnienie end-to-end, zdarzenia/s i czas mikropaczek per zapytanie, świeżość CDC (ostatni heartbeat), liczba wierszy w kwarantannie per reguła |
| **Podróż w czasie** | dowolna tabela gold | wybór wersji tabeli (time travel delta-rs) i porównanie wskaźników „teraz” vs „wersja N”; historia operacji (`DESCRIBE HISTORY`) |

Uwagi:
- **Świeżość CDC:** Spark zapisuje zdarzenia z tematu heartbeat Debezium do `ops.cdc_heartbeat` ([05-spark.md](05-spark.md) §7). Dashboard nie musi czytać Kafki.
- **Opóźnienie slotu replikacyjnego** nie jest pokazywane, bo wymagałoby połączenia z Postgresem. Jest sprawdzane w testach ([02-postgresql.md](02-postgresql.md) §5).

## 3. Rozszerzenie: wydajny odczyt na żywo

- **Odczyt tylko po zmianie:** przy każdym odświeżeniu sprawdzana jest wersja tabeli Delta (tani odczyt dziennika). Dane są wczytywane ponownie tylko, gdy wersja się zmieniła. Pamięć podręczna (`st.cache_data`) jest kluczowana wersją tabeli.
- **Odświeżanie fragmentów:** odświeża się tylko część strony z danymi (`st.fragment` z `run_every`), a nie cała aplikacja. Filtry użytkownika nie są resetowane.
- **Puste stany:** tuż po starcie systemu gold jest pusty. Dashboard pokazuje komunikat „czekam na dane” zamiast błędu.

## 4. Rozszerzenie: mapa

Kartogram 27 stanów z plikiem GeoJSON granic stanów Brazylii, który leży w repozytorium (bez pobierania przy starcie). Licencję pliku sprawdzamy przy implementacji. Kolor oznacza wybraną metrykę, a podpowiedź pokazuje szczegóły stanu.

## 5. Rozszerzenie: struktura kodu

- **Warstwa zapytań:** czyste funkcje `tabela(e) → DataFrame` (KPI, agregaty, lejek), niezależne od Streamlit.
- **Warstwa UI:** tylko układ i wykresy.

Logikę wskaźników testuje się jednostkowo, a UI testem aplikacji (`streamlit.testing`).

## 6. Rozważona alternatywa: Grafana

Grafana byłaby naturalna dla szeregów czasowych, z dashboardami jako kod i alertami. **Nie ma jednak wbudowanego źródła danych dla Delta Lake.** Wymagałaby niepodpisanej wtyczki społecznościowej DuckDB z rozszerzeniem `delta` wbudowanym w obraz albo dodatkowej warstwy (Trino, własne API), czyli nowego komponentu. Zapis gold do Postgresa łamałby zasadę architektury.

Streamlit czyta Delta natywnie, obsługuje time travel w interfejsie i nie wymaga obejść.

## 7. Testy

| Poziom | Test | Oczekiwany wynik |
|---|---|---|
| jednostkowy | KPI na małych tabelach Delta (fixture zapisane przez `deltalake`) | poprawny przychód, liczba zamówień, średni koszyk, % spóźnień, mediana dostawy |
| jednostkowy | przychód z korektą z anulacji | ujemna korekta pomniejsza przychód |
| jednostkowy | lejek | liczby przejść zgodne z danymi wejściowymi |
| jednostkowy | puste tabele | puste wyniki, brak wyjątków |
| jednostkowy | odczyt wersji N | zwraca stan tabeli z wersji N |
| jednostkowy | pamięć podręczna | brak ponownego odczytu, gdy wersja tabeli się nie zmieniła |
| integracyjny (MinIO) | wszystkie strony w `streamlit.testing.AppTest` na danych w MinIO | każda strona renderuje się bez wyjątku |
| integracyjny | uprawnienia | konto `dashboard` czyta `gold` i `ops`, nie ma dostępu do `bronze`/`silver` |
| integracyjny | kontener | healthcheck zdrowy po starcie, także przy pustym gold |
| e2e | zgodność z prawdą | KPI z warstwy zapytań = widoki uzgadniania w Postgresie (`sim.v_truth_*`) po zakończeniu przebiegu |
| e2e | świeżość | podczas symulacji świeżość CDC i opóźnienie end-to-end poniżej ustalonych progów |

## 8. Podsumowanie

| | Podstawa | Z rozszerzeniami |
|---|---|---|
| Źródło | gold | gold + ops (zdrowie potoku), tylko Delta, konto tylko do odczytu |
| Zakres | kilka wykresów | 9 stron: biznes, logistyka, klienci (SCD2), zdrowie potoku, podróż w czasie |
| Odświeżanie | pełne przeładowanie | odczyt tylko po zmianie wersji tabeli, odświeżanie fragmentów |
| Mapa | — | kartogram stanów z lokalnym GeoJSON |
| Weryfikacja | ręczna | testy warstwy zapytań, testy aplikacji, zgodność KPI z Postgresem (e2e) |

## Do weryfikacji przy implementacji

- Wersja Streamlit z `st.fragment(run_every=…)`.
- Zgodność delta-rs z funkcjami tabel gold (wersja protokołu, [06-minio-delta.md](06-minio-delta.md) §9).
- Źródło i licencja GeoJSON stanów Brazylii.
