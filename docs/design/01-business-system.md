# Business System: symulator sklepu internetowego

Kontener z Pythonem, który udaje działający sklep internetowy na prawdziwych danych **Olist Brazilian E-Commerce**. Pisze wyłącznie do PostgreSQL. Debezium przechwytuje te zmiany i przekazuje dalej. Wartości liczbowe w tym dokumencie zostały sprawdzone na pełnym zbiorze danych (`data/olist/`, pobranym 2026-10-18) i ponownie przy implementacji (2026-10-20, liczby policzone przez sam symulator).

Stan: **zaimplementowany (etap 1)** w `containers/simulator/` (opis modułów w `containers/simulator/README.md`). Usługa `simulator` startuje domyślnie z `make up`.

## 1. Dane wejściowe

| Plik | Wiersze | Rola w systemie |
|---|---|---|
| `olist_sellers_dataset.csv` | 3 095 | katalog, ładowany na starcie |
| `olist_products_dataset.csv` | 32 951 | katalog, ładowany na starcie |
| `product_category_name_translation.csv` | 71 | katalog, ładowany na starcie |
| `olist_customers_dataset.csv` | 99 441 | klient pojawia się razem ze swoim zamówieniem |
| `olist_orders_dataset.csv` | 99 441 | źródło zdarzeń cyklu życia zamówienia |
| `olist_order_items_dataset.csv` | 112 650 | wstawiane razem z zamówieniem, usuwane przy anulacji |
| `olist_order_payments_dataset.csv` | 103 886 | wstawiane razem z zamówieniem |
| `olist_order_reviews_dataset.csv` | 99 224 | zdarzenie po dostawie (komentarze wieloliniowe w cudzysłowach: `wc -l` pokazuje 104 719) |
| `olist_geolocation_dataset.csv` | 1 000 163 | **nieużywany** (poza repozytorium); mapa w dashboardzie korzysta z GeoJSON stanów |

Wymóg „3 CSV + dynamiczne tabele” spełniają pliki katalogowe (wstawiane hurtem przez `COPY` zaraz po utworzeniu tabel). Ten sam mechanizm tworzy jednak tabele dla wszystkich plików. Nazwa tabeli = nazwa pliku bez `olist_` i `_dataset` (`olist_order_items_dataset.csv` → `shop.order_items`, `product_category_name_translation.csv` → `shop.product_category_name_translation`).

`customer_id` w Olist jest **unikalny dla każdego zamówienia** (99 441 = liczba zamówień). Prawdziwą osobę identyfikuje `customer_unique_id` (96 096 osób). Dlatego klient jest wstawiany w chwili swojego zamówienia, a nie wszyscy naraz na starcie.

## 2. Część podstawowa

1. Wczytanie plików CSV (nagłówki → kolumny; `simulator/csvio.py`).
2. Dynamiczne `CREATE TABLE` bez schematu zapisanego na sztywno w kodzie (`simulator/schema.py` generuje DDL z wykrytych typów i kluczy; `simulator/db.py` wykonuje je jako `shop_app`).
3. Wstawianie danych w paczkach: katalogi przez `COPY`, historia sprzed `SIM_START` w paczkach po 1 000 zdarzeń w trybie pipeline (§4).

Tworzenie tabel, indeksów, ładowanie katalogów, założenie widoków `sim.v_truth_*` ([02-postgresql.md](02-postgresql.md) §8, plik `containers/postgres/sql/truth-views.sql` zamontowany do kontenera) i wiersz stanu w `sim.sim_state` to **jedna transakcja**: przerwanie w trakcie nie zostawia nic i następny start zaczyna od nowa. Jeśli tabele `shop.*` istnieją, a `sim.sim_state` nie ma wiersza, symulator odmawia startu z czytelnym komunikatem (`make reset`).

## 3. Rozszerzenie: rozpoznawanie typów i kluczy

Dla każdej kolumny wybierany jest najwęższy pasujący typ: `INTEGER/BIGINT` → `NUMERIC(p,s)` → `TIMESTAMP` → `TEXT`. Puste wartości oznaczają kolumnę nullable. `NUMERIC(p,s)`: `s` = największa liczba miejsc po przecinku w danych, `p = s + max(8, liczba cyfr części całkowitej)`, więc kwoty dostają `NUMERIC(10,2)` z zapasem na wzrost. Kolumny dat cyklu życia zamówienia (`order_approved_at`, `order_delivered_carrier_date`, `order_delivered_customer_date`) są nullable niezależnie od danych, bo przy `INSERT` są puste. `TIMESTAMP` wymaga pełnego `YYYY-MM-DD HH:MM:SS`; kolumna mieszana (np. liczby i daty) to `TEXT`.

Reguły potwierdzone na danych:

| Pułapka | Przykład z danych | Reguła |
|---|---|---|
| Kody pocztowe wyglądają na liczby | `customer_zip_code_prefix = "01037"`: 23 995 klientów ma zero wiodące | wartość cyfrowa z zerem wiodącym → `TEXT` |
| Puste wartości w kolumnach liczbowych | `product_photos_qty`: 610 pustych, `product_weight_g`: 2 puste | `INTEGER NULL` |
| Kwoty | `price` ma max. 2 miejsca po przecinku, max 6 735,00 | `NUMERIC(10,2)` |
| Czas bez strefy | lokalny czas Brazylii; 85 recenzji ma godzinę `01:00` z powodu zmiany czasu (DST 2017-10-15) | `TIMESTAMP` (bez tz) |
| BOM w pliku | `product_category_name_translation.csv` zaczyna się od `﻿` | odczyt jako `utf-8-sig` |
| Literówki w nagłówkach | `product_name_lenght` | nazwy kolumn przenoszone 1:1 z nagłówka |

**Klucze główne (PK):** kolumna unikalna i nigdy pusta; gdy kandydatów jest kilku, wygrywa `<nazwa tabeli w l. poj.>_id` (`orders` → `order_id`), potem pierwsza kolumna `*_id`, potem pierwsza w nagłówku (tak `product_category_name_translation` dostaje PK `product_category_name`). W `orders` unikalne są zarówno `order_id`, jak i `customer_id`. Jeśli nie ma PK jednokolumnowego, szukana jest pierwsza w kolejności nagłówka para kolumn nigdy pustych i unikalna:

| Tabela | PK wykryty na danych |
|---|---|
| customers / sellers / products / orders | `customer_id` / `seller_id` / `product_id` / `order_id` |
| order_items | `(order_id, order_item_id)` |
| order_payments | `(order_id, payment_sequential)` |
| order_reviews | `(review_id, order_id)`: **`review_id` nie jest unikalny** (98 410 różnych id na 99 224 wierszy, czyli 814 wierszy „nadmiarowych”; 789 id powtarza się), a `order_id` też nie (zamówienie może mieć kilka recenzji) |
| product_category_name_translation | `product_category_name` |

**Klucze obce (FK):** kolumna o nazwie jednokolumnowego PK innej tabeli **oraz** sprawdzenie, że wszystkie niepuste wartości występują w tabeli nadrzędnej. Na danych w 100% spełnione są:
`orders.customer_id → customers`, `order_items.order_id → orders`, `order_items.product_id → products`, `order_items.seller_id → sellers`, `order_payments.order_id → orders`, `order_reviews.order_id → orders`.

Samo dopasowanie nazw nie wystarcza, co widać w dwóch przypadkach:
- **`products.product_category_name → translation`:** 2 kategorie bez tłumaczenia (`pc_gamer`, `portateis_cozinha_e_preparadores_de_alimentos`), więc FK nie jest zakładany.
- **`*_zip_code_prefix → geolocation`:** zip nie jest unikalny w geolocation, a 157 kodów klientów w niej nie występuje (plik i tak nie jest ładowany).

**Indeksy** (reguła z [02-postgresql.md](02-postgresql.md) §7): każda kolumna FK musi być wiodącą kolumną jakiegoś indeksu; PK pokrywa `order_items(order_id)` i `order_payments(order_id)`, a symulator dokłada `orders(customer_id)`, `order_items(product_id)`, `order_items(seller_id)` i `order_reviews(order_id)`. Tabele dostają `REPLICA IDENTITY FULL` automatycznie (event trigger, 02 §2). Kolejność tworzenia: rodzice przed dziećmi.

Schemat utworzony na danych (interfejs publiczny dla Debezium): `shop.customers(customer_id TEXT PK, customer_unique_id TEXT, customer_zip_code_prefix TEXT, customer_city TEXT, customer_state TEXT)`, `shop.orders(order_id TEXT PK, customer_id TEXT FK, order_status TEXT, order_purchase_timestamp TIMESTAMP, order_approved_at TIMESTAMP NULL, order_delivered_carrier_date TIMESTAMP NULL, order_delivered_customer_date TIMESTAMP NULL, order_estimated_delivery_date TIMESTAMP)`, `shop.order_items(order_id TEXT, order_item_id INTEGER, product_id TEXT FK, seller_id TEXT FK, shipping_limit_date TIMESTAMP, price NUMERIC(10,2), freight_value NUMERIC(10,2))`, `shop.order_payments(order_id TEXT, payment_sequential INTEGER, payment_type TEXT, payment_installments INTEGER, payment_value NUMERIC(10,2))`, `shop.order_reviews(review_id TEXT, order_id TEXT FK, review_score INTEGER, review_comment_title TEXT NULL, review_comment_message TEXT NULL, review_creation_date TIMESTAMP, review_answer_timestamp TIMESTAMP)`, `shop.products(product_id TEXT PK, product_category_name TEXT NULL, 7 × INTEGER NULL)`, `shop.sellers(seller_id TEXT PK, seller_zip_code_prefix TEXT, seller_city TEXT, seller_state TEXT)`, `shop.product_category_name_translation(product_category_name TEXT PK, product_category_name_english TEXT)`. Wszystkie kolumny bez `NULL` są `NOT NULL`.

## 4. Rozszerzenie: odtwarzanie historii sklepu

### Okno czasowe
Zakres danych to 2016-09-04 → 2018-10-17, ale krawędzie są bezużyteczne:
- **2016:** 329 zamówień, brak listopada; w 2017 pierwsze zakupy są dopiero **2017-01-05** (1–4 stycznia: jedna recenzja).
- **2018-09/10:** 20 zamówień, z czego 19 anulowanych.

Pełna historia **2017-01-01 → 2018-08-31** to 99 092 zamówienia przez 608 dni i **807 648 zmian wierszy** (c=512 863, u=294 258, d=527; wliczając `INSERT customers`). Okno dotyczy **zdarzeń**, nie zamówień: zdarzenie o czasie `< SIM_START` trafia do preloadu, zdarzenie w `[SIM_START, SIM_END]` jest odtwarzane na żywo, późniejsze są pomijane. Zamówienie złożone przed `SIM_START` i dostarczone po nim daje więc `UPDATE` na żywo od pierwszych minut, a stan bazy w chwili `SIM_END` jest spójnym „stanem sklepu na ten dzień”.

Wszystko sprzed `SIM_START` jest ładowane hurtem (faza `preload`, paczki po 1 000 zdarzeń w trybie pipeline psycopg, bez tempa). Debezium widzi to jako **snapshot (`op=r`)**, a dalej jako strumień `c/u/d`. Ten sam silnik wykonuje „przewinięcie” historii i odtwarzanie na żywo.

**Ustawienia (decyzja etapu 1):**

| Zestaw | `SIM_START` | `SIM_END` | `SIM_SPEED` | `MAX_EVENTS_PER_SEC` | Preload | Replay | Czas | Średnio |
|---|---|---|---|---|---|---|---|---|
| **domyślny** (`.env.example`, rozwój i testy) | 2017-01-05 | 2017-03-31 | 1440 | 20 | 1 582 zdarzeń (2 642 wiersze: 329 zamówień z 2016) | 23 696 zdarzeń, 40 479 wierszy, 5 262 zakupy | 86 min | 7,8 wierszy/s, szczyty obcinane do 20/s |
| prezentacja (Black Friday) | 2017-11-20 | 2017-12-10 | 1440 | 50 | 169 734 zdarzeń (282 617 wierszy) | 29 308 zdarzeń, 50 956 wierszy, 6 798 zakupów | ≈ 25 min (limiter wydłuża 24.11) | 40/s; 24.11 bez limitu dałby 109/s |
| pełna historia | 2017-01-01 | 2018-08-31 | 1440 | 50 | 1 581 zdarzeń | 492 230 zdarzeń, 807 648 wierszy | ≈ 10 h | 22/s |

Domyślne wartości są celowo małe (wymaganie `notes.txt`): kilka wierszy na sekundę nie obciąża Kafki ani Sparka na laptopie, a 86 minut wystarcza na testy integracyjne i e2e. Po `SIM_END` symulator zapisuje fazę `done`, loguje podsumowanie i **czeka bezczynnie** (kontener zdrowy, nic nie pisze). `make reset` zaczyna od nowa; zmiana `SIM_END` na późniejszą w `.env` i restart kontenera wznawia odtwarzanie od zapisanego kursora; zmiana `SIM_START` po pierwszym starcie jest ignorowana z ostrzeżeniem (preload już się odbył z poprzednią wartością).

### Zamówienie → zdarzenia

| Krok | Czas zdarzenia | Operacje |
|---|---|---|
| zakup | `order_purchase_timestamp` | `INSERT customers` (ON CONFLICT DO NOTHING), `INSERT orders (status='created', daty przyszłe = NULL)`, `INSERT order_items`, `INSERT order_payments`; jedna transakcja |
| akceptacja | `order_approved_at` | `UPDATE orders SET order_status='approved', order_approved_at=…` |
| wysyłka | `order_delivered_carrier_date` | `UPDATE … order_status='shipped', order_delivered_carrier_date=…` |
| dostawa | `order_delivered_customer_date` | `UPDATE … order_status='delivered', order_delivered_customer_date=…` |
| recenzja | `review_creation_date` | `INSERT order_reviews` |
| status końcowy | ostatni znany krok + opóźnienie | `UPDATE order_status`, dla `canceled` dodatkowo `DELETE order_items` |

Kolumna `order_estimated_delivery_date` jest znana w chwili zakupu, więc wstawiana od razu. **Wartości kolumn są zawsze oryginalne z CSV**; normalizacja (niżej) przesuwa tylko chwilę emisji zdarzenia, dzięki czemu po pełnym przebiegu `shop.orders` jest identyczne z plikiem. Opóźnienie statusu końcowego jest deterministyczne (1 dzień + `crc32(order_id) mod 2 dni`), więc strumień zdarzeń jest identyczny przy każdym starcie i po wznowieniu. Zdarzenia są uporządkowane po `(czas, order_id, krok)`; krok: 0 zakup, 1 akceptacja, 2 wysyłka, 3 dostawa, 5 status końcowy, 10+i i-ta recenzja zamówienia (po `review_id`). Ten klucz jest też kursorem wznawiania (§5).

### Rozkład statusów końcowych i jak je odtwarzamy

| Status | Zamówień | Odtworzenie |
|---|---|---|
| delivered | 96 478 | pełny cykl; 8 zamówień nie ma daty dostawy i dostaje `UPDATE status='delivered'` jako status końcowy (bez daty) |
| shipped | 1 107 | kończy na wysyłce (brak daty dostawy) |
| canceled | 625 | kroki z dat (484 akceptacje, 75 wysyłek, 6 dostaw), potem `UPDATE status='canceled'` + **`DELETE order_items`** (461 zamówień ma pozycje) |
| unavailable | 609 | po akceptacji + opóźnienie; prawie żadne (603) nie ma pozycji |
| invoiced / processing | 314 / 301 | po akceptacji + opóźnienie; zostają w tym stanie |
| created / approved | 5 / 2 | zostają w tym stanie; daty kroków powyżej statusu końcowego (gdyby były) nie są stosowane |

Łącznie: 493 937 zdarzeń, 810 456 zmian wierszy (c=514 642, u=295 272, d=542) na całym zbiorze.

### Normalizacja czasu (anomalie w danych)

| Anomalia | Liczba | Reguła |
|---|---|---|
| wysyłka przed akceptacją | 1 359 | czas kroku = max(własny czas, czas poprzedniego kroku); kolejność kroków stała |
| dostawa przed wysyłką | 23 | jw. |
| wysyłka przed zakupem | 166 | jw. (łącznie przesuniętych jest 1 443 kroków) |
| recenzja przed zakupem | 74 na poziomie sekund (64 na poziomie dat: `review_creation_date` ma zawsze godzinę 00:00) | czas recenzji = max(recenzja, zakup) |
| recenzja przed dostawą | 8 320 na poziomie sekund, 5 126 na poziomie dat (5%) | zostawiamy, tak jest w rzeczywistości |
| brak daty kroku (np. 14 dostarczonych bez `approved_at`, 2 bez daty przewoźnika) | — | krok pomijany |
| recenzje anulowanych zamówień | 609 | normalne zdarzenie recenzji |

### Tempo i ochrona przed przeciążeniem
Ruch jest bardzo nierówny. Najwięcej zamówień było w Black Friday 2017-11-24 (1 176 zamówień, 6 565 zmian wierszy, czyli 7× średnia dzienna). Dlatego:

```
SIM_SPEED            # mnożnik czasu (1440 = 1 dzień historii na minutę)
MAX_EVENTS_PER_SEC   # twardy limit zmian wierszy/s; po przekroczeniu zegar symulacji zwalnia (backpressure), nic nie jest gubione
SIM_START / SIM_END  # okno czasowe (data = 00:00:00 / 23:59:59, albo pełny "YYYY-MM-DD HH:MM:SS")
```

Mechanizm (`simulator/clock.py`): `SimClock` liczy czas symulacji od kotwicy (`czas_sym = kotwica + (teraz − kotwica_wall) × SIM_SPEED`); przed każdym zdarzeniem symulator śpi, aż zegar dojdzie do jego czasu. `RateLimiter` wycenia zdarzenie na `liczba_wierszy / MAX_EVENTS_PER_SEC` sekund: następne zdarzenie może ruszyć dopiero, gdy poprzednie „zapłaciło” za swoje wiersze, więc w żadnej sekundzie nie startuje więcej zdarzeń niż limit (jedna transakcja jest atomowa, więc liczba wierszy może przekroczyć limit o mniej niż jedno zdarzenie). Gdy limiter musiał czekać, zegar symulacji jest **cofany do czasu zdarzenia** (nowa kotwica): w szczycie symulacja zwalnia zamiast gromadzić zaległości, a po szczycie biegnie dalej z `SIM_SPEED` od miejsca, w którym jest. Zakup z 21 pozycjami (24 wiersze) przy limicie 20/s zajmuje więc 1,2 s. Recenzje mają zawsze godzinę 00:00, więc każda północ to seria kilkudziesięciu `INSERT order_reviews` naraz, którą limiter rozkłada na kilka sekund.

| SIM_SPEED | Czas pełnego przebiegu (608 dni) | Średnio zmian/s | Black Friday bez limitu |
|---|---|---|---|
| 720 | ~20 h | ~11 | ~55/s |
| 1440 | ~10 h | ~22 | ~109/s |
| 2880 | ~5 h | ~44 | ~219/s |

## 5. Rozszerzenie: odporność

- **Ponawianie połączenia:** jeśli Postgres jeszcze nie wstał, skrypt czeka (co 2 s, log co 30 s) i próbuje ponownie; utrata połączenia w trakcie (restart Postgresa) jest łapana, połączenie odnawiane, a przebieg wznawiany z kursora w bazie. `SIGTERM` (`docker stop`) kończy pracę po bieżącej transakcji.
- **Wznawianie po restarcie:** tabela kontrolna `sim.sim_state` (jeden wiersz, `id = 1`, poza publikacją Debezium) trzyma fazę (`preload` → `replay` → `done`), okno (`sim_start`, `sim_end`, `sim_speed`), **kursor** `(sim_clock, cursor_order_id, cursor_step)` = klucz ostatniego zastosowanego zdarzenia, liczniki (`events_applied`, `orders_inserted`, `rows_inserted/updated/deleted`) i znaczniki czasu (`started_at`, `preload_finished_at`, `finished_at`, `updated_at`). `UPDATE sim.sim_state` jest **w tej samej transakcji** co zdarzenie (jedno zdarzenie = jedna transakcja w `replay`, paczka 1 000 zdarzeń = jedna transakcja w `preload`). Po restarcie symulator buduje ten sam deterministyczny strumień i zaczyna od pierwszego zdarzenia o kluczu większym niż kursor, a zegar rusza od `sim_clock`: zero duplikatów i zero strat bez polegania na `ON CONFLICT` (ten zostaje tylko na `customers`, jak w projekcie). Czas przestoju nie jest nadrabiany.
- **Healthcheck:** plik `/tmp/simulator/heartbeat` (treść = faza, mtime = ostatni „oddech”, co ≤ 1 s także podczas czekania na zegar). `healthcheck.py` zwraca 0, gdy plik ma < 30 s **i** faza to `replay` lub `done`. Kontener jest więc zdrowy dopiero po preloadzie, co pozwala `connect-init` (etap 2) zależeć od `simulator: service_healthy` i zarejestrować konektor tak, żeby snapshot objął całą historię sprzed `SIM_START` (`start_period` 120 s mieści preload pełnej historii). Kontener: `restart: unless-stopped`, limit 512 MB (zmierzone 246 MB RSS dla okna domyślnego, ok. 380 MB dla pełnej historii). Uwaga: po `docker kill` Docker **nie** restartuje kontenera z `unless-stopped` (traktuje to jak ręczne zatrzymanie); `docker compose up -d simulator` go podnosi.
- **Log postępu** co `SIM_LOG_EVERY` (10 s), na stdout:
  `[sim 2017-11-24 14:00:12] phase=replay orders=31877 rows=228410 (c=161203 u=64990 d=2217) rate=19.7/s throttled=96%`
  (`rate` = wiersze/s w ostatnim okresie; `throttled` = udział czasu, w którym limiter wstrzymywał zdarzenia). Na starcie logowany jest wykryty schemat (typy, PK, FK, odrzucone FK z liczbą brakujących wartości).

## 6. Zjawiska z prawdziwych danych przydatne w dalszych etapach

- **Przeprowadzki klientów:** 2 997 osób złożyło więcej niż jedno zamówienie, a **250 z nich zmieniło kod pocztowy** (122 zmieniło miasto). W warstwie gold Spark może zbudować historię wymiaru klienta (SCD typu 2) po `customer_unique_id` na prawdziwych danych, bez sztucznie generowanych zmian.
- **Spóźnienia:** 8,1% dostarczonych zamówień przyszło po `order_estimated_delivery_date`. Mediana czasu od zakupu do dostawy to 10,2 dnia, a od zakupu do akceptacji 0,3 h. To gotowe wskaźniki dla dashboardu.
- **Geografia:** 27 stanów, z czego SP ma 42%, RJ 13%, MG 12%. Mapa per stan nie potrzebuje tabeli geolocation.
- **Płatności:** credit_card 74%, boleto 19%, voucher 6%; raty do 24. Są też przypadki brzegowe: 3× `not_defined`, 9 płatności o wartości 0, 1 zamówienie bez płatności.
- **Pozycje zamówień:** 89% zamówień ma jedną pozycję, maksymalnie jest ich 21. 775 zamówień nie ma żadnej (unavailable i canceled).

## 7. Zdarzenia syntetyczne (opcjonalne, wyłączane zmienną środowiskową)

**Nie zaimplementowane w etapie 1** (otwarte: potrzebne do testu ewolucji schematu w etapach 2–3; ma je włączać zmienna środowiskowa, domyślnie wyłączone). Wszystko poza tymi dwoma punktami pochodzi z prawdziwych danych:
- **Ewolucja schematu:** w zadanym momencie `ALTER TABLE orders ADD COLUMN delivery_channel TEXT` i wypełnianie tej kolumny. Sprawdza przejście zmiany schematu przez AVRO, Schema Registry i Sparka (testy w [03-debezium.md](03-debezium.md) i dalszych).
- **Korekty cen:** sporadyczny `UPDATE order_items SET price=…`.

## 8. Testy

Identyfikatory testów: `tests/unit/test_simulator_*.py` (bez Dockera, pakiet importowany przez `pythonpath` w `pyproject.toml`) i `tests/integration/test_simulator.py` (działający stos; usługa `simulator` jest zdrowa dopiero po preloadzie, więc fixture `stack` gwarantuje istnienie tabel).

| Poziom | Test | Oczekiwany wynik | Identyfikator |
|---|---|---|---|
| jednostkowy | detekcja typów na próbkach | zip `01037` → `TEXT`; kolumna z pustymi → `INTEGER NULL`; `58.90` → `NUMERIC(10,2)`; BOM nie psuje nagłówka | `test_simulator_schema.py::test_zip_code_with_leading_zero_is_text`, `::test_integer_column_with_empty_values_is_nullable_integer`, `::test_amount_is_numeric_10_2`, `::test_timestamp_bigint_mixed_and_empty_columns`, `::test_bom_does_not_break_the_header`, `::test_multiline_quoted_fields_and_width_errors` |
| jednostkowy | detekcja PK | `orders` → `order_id` (nie `customer_id`); `order_reviews` → `(review_id, order_id)`; `order_items` → `(order_id, order_item_id)` | `::test_pk_orders_prefers_order_id_over_unique_customer_id`, `::test_pk_reviews_is_pair_because_review_id_is_not_unique`, `::test_pk_items_and_payments_are_pairs`, `::test_pk_without_id_column_falls_back_to_first_unique_column` |
| jednostkowy | detekcja FK | `orders.customer_id → customers` wykryte; `products → translation` odrzucone (2 kategorie bez tłumaczenia) | `::test_fk_accepts_orders_customer_id_and_rejects_products_translation` |
| jednostkowy | DDL i polityka indeksów | typy, PK, FK w `CREATE TABLE`; dokładnie 4 dodatkowe indeksy FK; rodzice przed dziećmi | `::test_ddl_has_detected_types_keys_and_fk_indexes`, `::test_creation_order_puts_parents_first` |
| jednostkowy (`slow`) | schemat na prawdziwych danych | cała tabela z §3 (PK, FK, odrzucony FK, typy pułapek) na `data/olist` | `::test_real_data_gives_the_schema_of_the_design_doc` |
| jednostkowy | zamówienie → zdarzenia | dostarczone zamówienie daje sekwencję c → u(approved) → u(shipped) → u(delivered) → c(review); przyszłe daty puste przy INSERT | `test_simulator_events.py::test_delivered_order_gives_create_update_update_update_review` |
| jednostkowy | normalizacja czasu | wysyłka przed akceptacją → przesunięta na czas akceptacji (wartość kolumny oryginalna); brak daty → krok pominięty; recenzja przed zakupem → czas zakupu | `::test_shipment_before_approval_is_moved_to_the_approval_time`, `::test_missing_step_date_skips_the_step`, `::test_review_before_purchase_is_moved_to_the_purchase_time`, `::test_review_after_purchase_keeps_its_time` |
| jednostkowy | statusy końcowe | `canceled` → UPDATE statusu + DELETE pozycji po opóźnieniu 1–3 dni; `invoiced` zostaje w tym stanie; `canceled` po dostawie i `delivered` bez daty dostawy; `shipped`/`created`/`approved` kończą na ostatnim kroku | `::test_canceled_order_updates_status_and_deletes_its_items`, `::test_invoiced_order_stays_invoiced_without_delete`, `::test_canceled_after_delivery_and_delivered_without_delivery_date`, `::test_shipped_and_created_orders_end_at_their_last_step`, `::test_final_status_delay_is_deterministic_and_bounded` |
| jednostkowy | porządek i kursor | strumień posortowany po `(czas, order_id, krok)` bez powtórzeń klucza; wznowienie zwraca dokładnie zdarzenia po kursorze; podział na preload/replay; czysty model stanu | `::test_event_stream_is_sorted_and_resumes_strictly_after_the_cursor`, `::test_split_window_preload_before_start_and_replay_inside`, `::test_summarize_is_the_state_an_uninterrupted_run_produces` |
| jednostkowy | limit tempa | przy szczycie zegar symulacji zwalnia (cofnięty do czasu zdarzenia), liczba zdarzeń/s ≤ `MAX_EVENTS_PER_SEC`, żadne zdarzenie nie ginie; zegar ×SIM_SPEED; limiter wycenia wiersze | `test_simulator_clock.py::test_backpressure_caps_the_rate_holds_the_clock_back_and_loses_nothing`, `::test_sim_clock_runs_speed_times_faster_than_wall_time`, `::test_rate_limiter_paces_row_changes`, `::test_pacer_waits_for_the_simulated_clock_when_ahead_of_schedule` |
| jednostkowy | konfiguracja | domyślne okno i tempo są małe; daty graniczne; błędne wartości odrzucone | `::test_config_defaults_are_a_short_window_at_a_low_rate`, `::test_config_parses_bounds_and_rejects_bad_values` |
| integracyjny (Postgres) | tworzenie tabel | 8 tabel z wykrytymi typami (dokładna lista kolumn), PK, FK, dokładnie 4 indeksy FK, `REPLICA IDENTITY FULL`, katalogi (32 951 / 3 095 / 71), widoki `sim.v_truth_*`, wiersz `sim.sim_state` | `test_simulator.py::test_tables_created_with_detected_types_pks_fks_and_indexes`; reguła indeksów i plan `DELETE`: `test_postgres.py::test_every_fk_column_has_a_leading_index`, `::test_delete_order_items_by_order_id_uses_index` |
| integracyjny (Postgres) | healthcheck | kontener `healthy`, `healthcheck.py` zwraca 0, plik heartbeat niesie fazę z `sim_state` | `test_simulator.py::test_healthcheck_reports_the_phase` |
| integracyjny (Postgres) | przerwanie w środku transakcji zakupu | **właściwy writer symulatora** (`Database.apply` w procesie potomnym) wykonuje `INSERT customers/orders/order_items/order_payments`, blokuje się na ostatniej instrukcji (`UPDATE sim.sim_state`, bo test trzyma blokadę wiersza) i dostaje `SIGKILL`: w bazie nie zostaje nic, kursor nietknięty; drugi wariant: błąd na ostatnim `INSERT` (duplikat PK płatności) cofa klienta, zamówienie i pozycje | `test_simulator.py::test_writer_killed_before_its_last_statement_leaves_no_half_order`, `::test_purchase_failing_on_its_last_statement_is_rolled_back_entirely` |
| integracyjny (Postgres) | odmowa startu | tabele `shop.*` istnieją, a `sim.sim_state` nie ma wiersza → `RuntimeError` z `make reset`, zanim cokolwiek zostanie utworzone (§2) | `test_simulator.py::test_refuses_to_start_when_tables_exist_without_a_state_row` |
| integracyjny (Postgres, `slow`) | `SIGTERM` | `docker compose stop` kończy proces kodem 0 po bieżącej transakcji (log „SIGTERM received”), kursor zgodny z czystym modelem; `up -d` wznawia od `sim_clock` bez duplikatów | `test_simulator.py::test_sigterm_stops_after_the_current_transaction_and_restart_resumes` |
| integracyjny (Postgres) | nieświeży heartbeat | proces zamrożony `SIGSTOP` → po 30 s `healthcheck.py` zwraca 1 i Docker notuje nieudane sprawdzenia; po `SIGCONT` znów zdrowy | `test_simulator.py::test_stale_heartbeat_fails_the_healthcheck` |
| integracyjny (Postgres, `slow`) | zmiana `SIM_START` po starcie | restart z innym `SIM_START` loguje ostrzeżenie „keeping the stored one”, `sim_state.sim_start` bez zmian, stan zgodny z czystym modelem | `test_simulator.py::test_changed_sim_start_on_restart_is_ignored_with_a_warning` |
| e2e (etap 6, ⏳) | koniec okna | po `SIM_END` faza `done`, kontener bezczynny i zdrowy; późniejszy `SIM_END` w `.env` + restart wznawia od kursora (§4) | własny, krótki stos w `tests/e2e/` (okno kilku dni), zaplanowany w [08-operations.md](08-operations.md) §10 |
| integracyjny (Postgres) | stan bazy = czysty model | w jednym snapshocie `REPEATABLE READ`: liczby wierszy, liczniki c/u/d i zamówienia per status z przychodem równe wynikowi zastosowania preloadu + zdarzeń do kursora w czystym modelu (`events.summarize`) | `test_simulator.py::test_database_state_equals_the_pure_fold_of_events_up_to_the_cursor` |
| integracyjny (Postgres, `slow`) | `kill` symulatora i restart | `docker compose kill` + `up -d`: log „resuming”, bez ponownej inicjalizacji, zegar i liczniki kontynuują od `sim_state`, brak duplikatów, liczby zgodne z przebiegiem bez przerwy (ten sam czysty model co wyżej) | `test_simulator.py::test_kill_and_restart_resumes_from_sim_state_without_duplicates` |

Testy „koniec okna” (faza `done`, późniejszy `SIM_END`) wymagają własnego stosu z oknem kilku dni, więc należą do e2e etapu 6. Test „liczby zgodne z przebiegiem bez przerwy” nie uruchamia drugiego przebiegu: strumień zdarzeń jest deterministyczny, więc stan po przebiegu bez przerwy do kursora `k` to z definicji wynik zastosowania zdarzeń `≤ k` w czystym modelu, z którym porównywana jest baza.

## 9. Rozstrzygnięcia z implementacji (etap 1)

- **Domyślne tempo i okno:** `SIM_SPEED=1440`, `MAX_EVENTS_PER_SEC=20`, `SIM_START=2017-01-05`, `SIM_END=2017-03-31` (§4); zestaw na prezentację w `.env.example`.
- **Po `SIM_END`:** faza `done`, kontener czeka bezczynnie i jest zdrowy; `make reset` zaczyna od nowa, późniejszy `SIM_END` wznawia.
- **Wznawianie:** dokładny kursor zamiast polegania na `ON CONFLICT` (§5); `docker kill` nie restartuje kontenera automatycznie; `SIGTERM` kończy po bieżącej transakcji (test w §8).
- **Atomowość zakupu** sprawdzana na właściwym writerze: test blokuje wiersz `sim.sim_state` (`SELECT … FOR UPDATE`), przez co `Database.apply` zatrzymuje się dokładnie na ostatniej instrukcji transakcji zakupu, i zabija proces `SIGKILL` (§8). Bez haków testowych w kodzie produkcyjnym.
- **Zachowania sprawdzone tylko testem integracyjnym na żywym stosie:** odmowa startu na pół-zainicjalizowanej bazie, nieświeży heartbeat → niezdrowy, zignorowana zmiana `SIM_START`. **Odłożone do e2e (etap 6):** faza `done` po `SIM_END` i wznowienie po późniejszym `SIM_END`.
- **Wartości kolumn** zawsze oryginalne; normalizacja czasu dotyczy tylko chwili emisji (§4).
- **Marker dla Debezium (etap 2):** zdrowie kontenera = preload zakończony; dodatkowo `sim.sim_state.phase` i `preload_finished_at` dla testów (rola `reader`).
- **Liczby z projektu poprawione na danych:** pełne okno to ≈ 808 tys. zmian wierszy (nie 706 tys.; w projekcie nie liczono `INSERT customers`); recenzje przed zakupem: 74 na poziomie sekund (64 na poziomie dat); recenzje przed dostawą: 8 320 na poziomie sekund (5 126 na poziomie dat); 8 dostarczonych zamówień bez daty dostawy wymaga osobnego `UPDATE`.
- **Zużycie zasobów:** start kontenera (wczytanie CSV, detekcja, budowa zdarzeń) ok. 10 s; preload 1 582 zdarzeń < 0,1 s; 246 MB RSS dla okna domyślnego; stos od zera zdrowy w 48 s.
- **Zdarzenia syntetyczne (§7):** odłożone do etapu 2–3.
