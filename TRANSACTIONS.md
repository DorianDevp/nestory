# Plan: transakcje optymistyczne (OCC) z Engine

Założenia projektowe dla warstwy transakcyjnej nestory. Status każdej części:
**✅ zrobione**, **zdecydowane**, **otwarte**.

## Model współbieżności

**B / optymistyczny (OCC).** Żadnych locków w czasie myślenia klienta.
Walidacja dopiero na commicie, po `version`. **Pierwszy commit wygrywa**,
przegrany dostaje `ErrConflict`. Wersjonowanie i lock są **per-`Resource`**
(odpowiednik per-row locka), nie globalny lock na store. Inwariant
`pointer stability` trzymamy — zapis idzie przez `*Resource.item`, element
nigdy się nie rusza.

## Warstwy

### `Resource[T]` (chunkstore.go, istnieje)

```go
type Resource[T any] struct {
    item    *T
    version int
    mu      sync.RWMutex
}
```

Jednostka. `version` to wersja *teraz*; `mu` pracuje tylko w sekcji krytycznej
commitu.

### `Engine` (nowy, generyczny, ponad wszystkimi DB)

- `txs map[txId][]entry` — **płaska lista entry per transakcja**.
- `bind map[any]*entry` — odwrotny indeks `snapshot *T (zboksowany) → entry`,
  daje wiązanie `Get → Update` w **O(1)** bez krążącego tokena.
- Nigdy nie nazywa konkretnego `T`; do otypowanej logiki sięga przez
  `baseRegistry` za type-erased interfejsem DB.

### `entry` (nowy, goła dana, type-erased)

```go
type entry struct {
    typ  string // do sortu (typ,id) + lookupu w baseRegistry
    id   int    // do odnalezienia żywego Resource
    ver  int    // wersja WTEDY (z Get); "teraz" leży w Resource
    work any    // *T, głęboka kopia
}
```

Bez `*Resource[T]`, bez generyka. Żywy `Resource` i otypowany `apply`
rozwiązuje się na commicie przez `baseRegistry[typ]`.

### `DB[T]` (istnieje → cienki front + dom metadanych)

- **Kanoniczny per typ** — idempotentny `Open` przez `baseRegistry`. **✅ zrobione.**
- Pole `tx Tx[T]` **wylatuje** — przy kanonicznym DB to współdzielony
  singleton-bomba; stan tx żyje w Engine, per operacja.
- Trzyma **metadane** liczone przy `Open`.
- Implementuje type-erased interfejs, który Engine woła: lock / sprawdź wersję /
  otypowany apply dla *własnego* typu po `id`.
- API: `Get(id)`, `Update(*T)`, `UpdateWithin(id, fn)`.

### Metadane (nowe, na DB, liczone przy `Open` ze schematu)

- Pola skalarne — do płytkiego diffa.
- Pola-relacje + typ docelowy + flagi `cascade:persist,delete,...` — sterują
  głęboką kopią przy `Get` i wykryciem zmiany relacji.
- Plan inwalidacji: pole → które indeksy ruszyć (zrównoleglalne).
- Głębokość.

## Przepływy

### `Get(id)`

1. Engine zakłada/dokleja kontrakt pod `txId`.
2. DB **głęboko kopiuje poddrzewo osiągalne przez kaskadę** (User + jego relacje
   `cascade`) → kopie robocze.
3. Dla każdego skopiowanego zasobu: `append` do listy
   `entry{typ, id, ver=Resource.version, work=kopia}` + wpis `bind[kopia]=entry`.
4. Zwraca korzeniowy `*T` (snapshot). **Zero locków.**

### `Update(*T)` / `UpdateWithin(id, fn)`

- `UpdateWithin`: robi `Get` wewnętrznie, woła `fn(kopia)`, potem ta sama
  ścieżka commitu. Jednostrzał, okno ~0.
- `Update(ptr)`: klient zmutował snapshot; `bind[ptr] → entry → txId`.

**Commit (Engine):**

1. Weź `txs[txId]`, **posortuj po `(typ,id)`**.
2. **Lock** każdego `Resource` w tej kolejności (deadlock-free).
3. **Walidacja**: `Resource.version == entry.ver`? niezgodność → patrz „Konflikt".
4. **Apply** (otypowany, przez DB): diff `live` vs `entry.work` (płytki,
   sterowany metadanymi); dla zmienionych — zapis przez `item`, `version++`,
   inwalidacja indeksów. Zmienione relacje to już osobne entry (kaskada zrobiła
   je przy `Get`).
5. Unlock wszystkich, **eksmituj tx**.

### Konflikt — odpowiedź (a)

- Niezgodność wersji → **nie eksmituj**. Odśwież `*entry.work` świeżym `live`,
  podbij `entry.ver`, zwróć `ErrConflict`. Klient trzyma **ten sam wskaźnik** z
  aktualną zawartością, sam decyduje co powtórzyć, woła `Update` ponownie.
- Maszyna stanów `bind`: commit OK → eksmisja; konflikt → refresh-in-place;
  jawny abort → eksmisja.

## Zasady, które to spinają

- Locki tylko na commicie, w globalnym porządku `(typ,id)` → brak deadlocka
  (działa też cross-typ, np. `{User,Account}` z przeciwnych korzeni).
- Diff = **precyzja** (co zapisać/bumpnąć/zinwalidować); poprawność daje
  **wersja**. Diff jest płytki per-zasób, kaskada to **szerokość listy**, nie
  głębokość rekursji — zrównoleglalne.
- Głęboka kopia = izolacja. **Warunek krytyczny:** kopia nie może zawierać
  żywych wskaźników osiągalnych przez relacje, bo wtedy
  `work.Account.balance = 50` to przypadkowy `Unsafe`.
- Żywe wskaźniki **tylko** przez osobne drzwi `Unsafe`
  (`chunkStore → *Resource → zasób`).
- ABA: licznik `version` (nie porównanie wartości) łapie podróż A→B→A, bo idzie
  +2. To powód, dla którego konflikt wykrywamy po wersji, nie po wartości.

## Otwarte (do rozstrzygnięcia)

1. **Wydawanie i grupowanie `txId`.** Jeden `Get` (z kaskadą) = jedna tx —
   jasne. Ale „dwa niezależne `Get` w jednej transakcji" bez jawnego `Begin` —
   jak je skleić pod jednym `txId`? Realna luka modelu begin-less.
2. **`Get` tylko do odczytu** — czy zakłada kontrakt? Jeśli tak, kto go sprząta,
   gdy nigdy nie przyjdzie `Update` (timeout? jawny `Close`?).
3. **Type-erased interfejs DB**, który woła Engine — dokładny zestaw metod
   (`lock(id)`, `versionOf(id)`, `apply(id, work)`).
4. **Mechanika inwalidacji indeksów** + jej wielowątkowość.
5. **`Unsafe`** — pełna semantyka.

## Następny ruch

Metadane na DB (żeby `apply` miał z czego diffować) → szkielet `Engine` +
`entry` + `commit`. `Open` (już idempotentny) podpina DB do Engine.
