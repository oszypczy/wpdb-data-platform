# Business System: symulator sklepu internetowego

Kontener z Pythonem, który udaje działający sklep internetowy na prawdziwych danych **Olist Brazilian E-Commerce**. Pisze wyłącznie do PostgreSQL. Debezium przechwytuje te zmiany i przekazuje dalej. Wartości liczbowe w tym dokumencie zostały sprawdzone na pełnym zbiorze danych (`data/olist/`, pobranym 2026-10-18).

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
| `olist_order_reviews_dataset.csv` | 99 224 | zdarzenie po dostawie |
| `olist_geolocation_dataset.csv` | 1 000 163 | **nieużywany** (poza repozytorium); mapa w dashboardzie korzysta z GeoJSON stanów |

Wymóg „3 CSV + dynamiczne tabele” spełniają pliki katalogowe. Ten sam mechanizm tworzy jednak tabele dla wszystkich plików.

`customer_id` w Olist jest **unikalny dla każdego zamówienia** (99 441 = liczba zamówień). Prawdziwą osobę identyfikuje `customer_unique_id` (96 096 osób). Dlatego klient jest wstawiany w chwili swojego zamówienia, a nie wszyscy naraz na starcie.

## 2. Część podstawowa

1. Wczytanie plików CSV (nagłówki → kolumny).
2. Dynamiczne `CREATE TABLE` bez schematu zapisanego na sztywno w kodzie.
3. Wstawianie danych w paczkach.

## 3. Rozszerzenie: rozpoznawanie typów i kluczy

Dla każdej kolumny wybierany jest najwęższy pasujący typ: `INTEGER/BIGINT` → `NUMERIC(p,s)` → `TIMESTAMP` → `TEXT`. Puste wartości oznaczają kolumnę nullable.

Reguły potwierdzone na danych:

| Pułapka | Przykład z danych | Reguła |
|---|---|---|
| Kody pocztowe wyglądają na liczby | `customer_zip_code_prefix = "01037"`: 23 995 klientów ma zero wiodące | wartość cyfrowa z zerem wiodącym → `TEXT` |
| Puste wartości w kolumnach liczbowych | `product_photos_qty`: 610 pustych, `product_weight_g`: 2 puste | `INTEGER NULL` |
| Kwoty | `price` ma max. 2 miejsca po przecinku, max 6 735,00 | `NUMERIC(10,2)` |
| Czas bez strefy | lokalny czas Brazylii; 85 recenzji ma godzinę `01:00` z powodu zmiany czasu (DST 2017-10-15) | `TIMESTAMP` (bez tz) |
| BOM w pliku | `product_category_name_translation.csv` zaczyna się od `﻿` | odczyt jako `utf-8-sig` |
| Literówki w nagłówkach | `product_name_lenght` | nazwy kolumn przenoszone 1:1 z nagłówka |

**Klucze główne (PK):** kolumna `*_id`, unikalna i nigdy pusta. Gdy kandydatów jest kilku, wygrywa ta, której nazwa odpowiada tabeli. W `orders` unikalne są zarówno `order_id`, jak i `customer_id`. Jeśli nie ma PK jednokolumnowego, szukana jest para kolumn:

| Tabela | PK wykryty na danych |
|---|---|
| customers / sellers / products / orders | `customer_id` / `seller_id` / `product_id` / `order_id` |
| order_items | `(order_id, order_item_id)` |
| order_payments | `(order_id, payment_sequential)` |
| order_reviews | `(review_id, order_id)`: **`review_id` nie jest unikalny** (814 duplikatów) |

**Klucze obce (FK):** pasująca nazwa kolumny **oraz** sprawdzenie, że wszystkie wartości występują w tabeli nadrzędnej. Na danych w 100% spełnione są:
`orders.customer_id → customers`, `order_items.order_id → orders`, `order_items.product_id → products`, `order_items.seller_id → sellers`, `order_payments.order_id → orders`, `order_reviews.order_id → orders`.

Samo dopasowanie nazw nie wystarcza, co widać w dwóch przypadkach:
- **`products.product_category_name → translation`:** 2 kategorie bez tłumaczenia (`pc_gamer`, `portateis_cozinha_e_preparadores_de_alimentos`), więc FK nie jest zakładany.
- **`*_zip_code_prefix → geolocation`:** zip nie jest unikalny w geolocation, a 157 kodów klientów w niej nie występuje.

## 4. Rozszerzenie: odtwarzanie historii sklepu

### Okno czasowe
Zakres danych to 2016-09-04 → 2018-10-17, ale krawędzie są bezużyteczne:
- **2016:** 329 zamówień, brak listopada.
- **2018-09/10:** 20 zamówień, z czego 19 anulowanych.

Domyślnie **`SIM_START=2017-01-01`, `SIM_END=2018-08-31`**, co daje 99 092 zamówienia przez 608 dni i około **706 tys. zmian wierszy** w bazie.

Wszystko sprzed `SIM_START` jest ładowane hurtem przed startem konektora. Debezium widzi to jako **snapshot (`op=r`)**, a dalej jako strumień `c/u/d`. Ten sam silnik wykonuje „przewinięcie” historii i odtwarzanie na żywo.

### Zamówienie → zdarzenia

| Krok | Czas | Operacje |
|---|---|---|
| zakup | `order_purchase_timestamp` | `INSERT customers` (ON CONFLICT DO NOTHING), `INSERT orders (status='created', daty przyszłe = NULL)`, `INSERT order_items`, `INSERT order_payments` |
| akceptacja | `order_approved_at` | `UPDATE orders SET status='approved', order_approved_at=…` |
| wysyłka | `order_delivered_carrier_date` | `UPDATE … status='shipped', order_delivered_carrier_date=…` |
| dostawa | `order_delivered_customer_date` | `UPDATE … status='delivered', order_delivered_customer_date=…` |
| recenzja | `review_creation_date` | `INSERT order_reviews` |
| status końcowy | patrz niżej | `UPDATE` / `DELETE` |

Kolumna `order_estimated_delivery_date` jest znana w chwili zakupu, więc wstawiana od razu.

### Rozkład statusów końcowych i jak je odtwarzamy

| Status | Zamówień | Odtworzenie |
|---|---|---|
| delivered | 96 478 | pełny cykl |
| shipped | 1 107 | kończy na wysyłce (brak daty dostawy) |
| canceled | 625 | `UPDATE status='canceled'` po ostatnim znanym kroku + opóźnienie, potem **`DELETE order_items`** (461 zamówień ma pozycje) |
| unavailable | 609 | po akceptacji + opóźnienie; prawie żadne (603) nie ma pozycji |
| invoiced / processing | 314 / 301 | po akceptacji + opóźnienie; zostają w tym stanie |
| created / approved | 5 / 2 | zostają w tym stanie |

### Normalizacja czasu (anomalie w danych)

| Anomalia | Liczba | Reguła |
|---|---|---|
| wysyłka przed akceptacją | 1 359 | czas kroku = max(własny czas, czas poprzedniego kroku); kolejność kroków stała |
| dostawa przed wysyłką | 23 | jw. |
| wysyłka przed zakupem | 166 | jw. |
| recenzja przed zakupem | 64 | czas recenzji = max(recenzja, zakup) |
| recenzja przed dostawą | 5 126 (5%) | zostawiamy, tak jest w rzeczywistości |
| brak daty kroku (np. 14 dostarczonych bez `approved_at`) | — | krok pomijany |
| recenzje anulowanych zamówień | 609 | normalne zdarzenie recenzji |

### Tempo i ochrona przed przeciążeniem
Ruch jest bardzo nierówny. Najwięcej zamówień było w Black Friday 2017-11-24 (1 176 zamówień, czyli 7× średnia dzienna). Dlatego:

```
SIM_SPEED            # mnożnik czasu (1440 = 1 dzień historii na minutę)
MAX_EVENTS_PER_SEC   # twardy limit; po przekroczeniu zegar symulacji zwalnia (backpressure), nic nie jest gubione
SIM_START / SIM_END  # okno czasowe
```

| SIM_SPEED | Czas pełnego przebiegu | Średnio zmian/s | W szczycie (Black Friday) |
|---|---|---|---|
| 720 | ~20 h | ~10 | ~70 |
| 1440 | ~10 h | ~19 | ~140 |
| 2880 | ~5 h | ~39 | ~280 |

Wartości domyślne ustalimy w trakcie implementacji. Pełny przebieg trwa godziny, więc na prezentację ustawiamy krótsze okno lub wyższy `SIM_SPEED` z limitem `MAX_EVENTS_PER_SEC`.

## 5. Rozszerzenie: odporność

- **Ponawianie połączenia:** jeśli Postgres jeszcze nie wstał, skrypt czeka i próbuje ponownie.
- **Wznawianie po restarcie:** pozycja zegara symulacji jest zapisywana w tabeli kontrolnej `sim_state`, wyłączonej z publikacji Debezium. Zapis odbywa się w tej samej transakcji co zdarzenia. Po restarcie symulator kontynuuje, a `ON CONFLICT` chroni przed duplikatami.
- **Log postępu**, np. co 10 s:
  `[sim 2017-11-24 14:00] orders=31877 rows=228410 (c=161203 u=64990 d=2217) rate=98.7/s throttled`

## 6. Zjawiska z prawdziwych danych przydatne w dalszych etapach

- **Przeprowadzki klientów:** 2 997 osób złożyło więcej niż jedno zamówienie, a **250 z nich zmieniło kod pocztowy** (122 zmieniło miasto). W warstwie gold Spark może zbudować historię wymiaru klienta (SCD typu 2) po `customer_unique_id` na prawdziwych danych, bez sztucznie generowanych zmian.
- **Spóźnienia:** 8,1% dostarczonych zamówień przyszło po `order_estimated_delivery_date`. Mediana czasu od zakupu do dostawy to 10,2 dnia, a od zakupu do akceptacji 0,3 h. To gotowe wskaźniki dla dashboardu.
- **Geografia:** 27 stanów, z czego SP ma 42%, RJ 13%, MG 12%. Mapa per stan nie potrzebuje tabeli geolocation.
- **Płatności:** credit_card 74%, boleto 19%, voucher 6%; raty do 24. Są też przypadki brzegowe: 3× `not_defined`, 9 płatności o wartości 0, 1 zamówienie bez płatności.
- **Pozycje zamówień:** 89% zamówień ma jedną pozycję, maksymalnie jest ich 21. 775 zamówień nie ma żadnej (unavailable i canceled).

## 7. Zdarzenia syntetyczne (opcjonalne, wyłączane zmienną środowiskową)

Wszystko poza tymi dwoma punktami pochodzi z prawdziwych danych:
- **Ewolucja schematu:** w zadanym momencie `ALTER TABLE orders ADD COLUMN delivery_channel TEXT` i wypełnianie tej kolumny. Sprawdza przejście zmiany schematu przez AVRO, Schema Registry i Sparka (testy w [03-debezium.md](03-debezium.md) i dalszych).
- **Korekty cen:** sporadyczny `UPDATE order_items SET price=…`.

## 8. Testy

| Poziom | Test | Oczekiwany wynik |
|---|---|---|
| jednostkowy | detekcja typów na próbkach | zip `01037` → `TEXT`; kolumna z pustymi → `INTEGER NULL`; `58.90` → `NUMERIC(10,2)`; BOM nie psuje nagłówka |
| jednostkowy | detekcja PK | `orders` → `order_id` (nie `customer_id`); `order_reviews` → `(review_id, order_id)`; `order_items` → `(order_id, order_item_id)` |
| jednostkowy | detekcja FK | `orders.customer_id → customers` wykryte; `products → translation` odrzucone (2 kategorie bez tłumaczenia) |
| jednostkowy | zamówienie → zdarzenia | dostarczone zamówienie daje sekwencję c → u(approved) → u(shipped) → u(delivered) → c(review); przyszłe daty puste przy INSERT |
| jednostkowy | normalizacja czasu | wysyłka przed akceptacją → przesunięta na czas akceptacji; brak daty → krok pominięty; recenzja przed zakupem → czas zakupu |
| jednostkowy | statusy końcowe | `canceled` → UPDATE statusu + DELETE pozycji; `invoiced` zostaje w tym stanie |
| jednostkowy | limit tempa | przy szczycie zegar symulacji zwalnia, liczba zdarzeń/s ≤ `MAX_EVENTS_PER_SEC`, żadne zdarzenie nie ginie |
| integracyjny (Postgres) | tworzenie tabel | tabele z wykrytymi typami, PK, FK i indeksami |
| integracyjny (Postgres) | przerwanie w środku transakcji zakupu | w bazie brak połowicznego zamówienia |
| integracyjny (Postgres) | `kill` symulatora i restart | zegar kontynuuje od `sim_state`, brak duplikatów, liczby zgodne z przebiegiem bez przerwy |
