# System kaskadowej selekcji zdjęć — specyfikacja techniczna

**Status:** propozycja do implementacji  
**Projekt:** `filecluster`  
**Język implementacji:** Python 3.12  
**Główny interfejs:** CLI  

## 1. Cel dokumentu

Dokument definiuje system, który klasyfikuje materiały z katalogu `inbox`
według ich przydatności dla osobistej biblioteki zdjęć. Ma stanowić kompletne
wejście dla programisty lub agenta kodującego.

System ma odróżniać fotografie warte zachowania, na przykład portrety, zdjęcia
rodzinne, krajobrazy i zdjęcia z podróży, od materiałów użytkowych, takich jak:

- screenshoty,
- paragony i faktury,
- dokumenty,
- etykiety,
- strony książek,
- zdjęcia produktów,
- przypadkowe lub bardzo słabe technicznie ujęcia.

Nie należy implementować selekcji jako pojedynczej klasyfikacji binarnej.
Rozpoznanie rodzaju materiału, jakość techniczna, estetyka oraz osobiste
preferencje są osobnymi sygnałami. Wynik końcowy powstaje w kaskadzie, a drogie
modele analizują wyłącznie przypadki nierozstrzygnięte przez tańsze etapy.

## 2. Decyzje projektowe

Poniższe decyzje są wiążące dla pierwszej implementacji:

1. Funkcja powstaje jako osobne polecenie `filecluster curate`. Nie zmienia
   domyślnego działania istniejącego `filecluster run`.
2. Domyślny tryb jest lokalny, deterministyczny i nie wysyła zdjęć do usług
   zewnętrznych.
3. Polecenie jest domyślnie dry-run. Zapis wymaga `--execute`.
4. System nigdy nie usuwa plików. W trybie zapisu kopiuje lub przenosi je do
   katalogów `keep`, `review` i `reject`.
5. Nazwa `reject` oznacza „nie importuj automatycznie”, a nie „bezpowrotnie
   usuń”.
6. Wszystkie wyniki per plik są dostępne w raporcie i cache. Terminal pokazuje
   wyłącznie zagregowane, ograniczone objętościowo informacje.
7. Awaria modelu lub dekodera nie może skierować zdjęcia do `reject`.
   Nierozpoznany materiał trafia do `review`.
8. Wideo pozostaje poza pierwszym zakresem klasyfikacji i domyślnie trafia do
   `review` z przyczyną `unsupported_media_type`.
9. Pierwszy użyteczny wariant ma działać bez modelu VLM. VLM jest późniejszym,
   opcjonalnym etapem dla przypadków niepewnych.

## 3. Zakres

### 3.1. Zakres pierwszej wersji

- odczyt metadanych oraz cech pliku,
- rozpoznawanie oczywistych screenshotów za pomocą reguł,
- obliczanie lekkich cech obrazu,
- opcjonalny OCR i pomiar udziału tekstu,
- klasyfikacja semantyczna encoderem vision-language,
- osobna ocena jakości technicznej,
- wynik `keep`, `review` albo `reject`,
- wyjaśnienie wyniku przez listę kodów przyczyn,
- cache wyników oparty na zawartości pliku i wersji konfiguracji,
- raport CSV oraz podsumowanie JSON,
- plan operacji oddzielony od wykonania,
- bezpieczne kopiowanie lub przenoszenie z obsługą kolizji nazw.

### 3.2. Późniejszy zakres

- uczenie klasyfikatora osobistych preferencji na decyzjach użytkownika,
- ocena estetyczna NIMA, MUSIQ, TOPIQ albo równoważnym modelem,
- opcjonalna eskalacja do lokalnego lub zdalnego VLM,
- obsługa klatek reprezentatywnych z wideo,
- rozpoznawanie członków rodziny,
- interaktywny interfejs do zatwierdzania katalogu `review`.

### 3.3. Poza zakresem

- automatyczne kasowanie plików,
- trenowanie dużego modelu od zera,
- synchronizacja chmurowa,
- publiczne tagowanie osób,
- zastąpienie istniejącego grupowania wydarzeń według czasu,
- gwarantowanie, że algorytm rozpozna emocjonalną wartość zdjęcia.

## 4. Terminologia i model wyniku

### 4.1. Decyzje końcowe

```python
class CurationDecision(StrEnum):
    KEEP = "keep"
    REVIEW = "review"
    REJECT = "reject"
```

- `keep`: materiał może zostać przekazany do istniejącego procesu klastrowania.
- `review`: system nie ma wystarczającej pewności lub wykrył konflikt sygnałów.
- `reject`: materiał z wysokim prawdopodobieństwem jest użytkowy albo nie spełnia
  minimalnych kryteriów.

### 4.2. Klasy semantyczne

Klasy muszą być stabilnymi identyfikatorami zapisanymi po angielsku:

```text
personal_people
family_home
portrait
landscape
city_travel
event
pet
artistic_photo
screenshot
document
receipt_invoice
book_page
label_packaging
product_reference
whiteboard_notes
low_information
other
```

Model może zwrócić więcej niż jedną klasę. Przykładowo zdjęcie osoby trzymającej
paragon może mieć jednocześnie wyniki `personal_people=0.71` oraz
`receipt_invoice=0.64`. Taki konflikt powinien prowadzić do `review`, nie do
automatycznego odrzucenia.

### 4.3. Wymiary oceny

Każdy plik otrzymuje co najmniej:

- `utility_probability`: prawdopodobieństwo materiału użytkowego,
- `personal_probability`: prawdopodobieństwo osobistej fotografii,
- `technical_quality`: jakość techniczna w zakresie `[0, 1]`,
- `aesthetic_score`: opcjonalna ocena estetyczna w zakresie `[0, 1]`,
- `preference_score`: opcjonalny wynik modelu osobistych preferencji,
- `confidence`: pewność decyzji końcowej,
- `reasons`: uporządkowana lista kodów przyczyn,
- `stage_trace`: wyniki poszczególnych etapów.

Brakujący sygnał jest zapisywany jako `None`, a nie jako zero.

## 5. Architektura

### 5.1. Przepływ

```text
Discover files
      |
      v
Fingerprint + cache lookup
      |
      +------ cache hit ------> cached result
      |
      v
Stage 1: metadata rules
      |
      +------ high confidence -> decision
      |
      v
Stage 2: lightweight image features + optional OCR
      |
      +------ high confidence -> decision
      |
      v
Stage 3: semantic image encoder
      |
      +------ high confidence -> decision
      |
      v
Stage 4: quality/aesthetic/preference scoring
      |
      +------ confident fusion -> decision
      |
      v
Stage 5: optional VLM escalation
      |
      v
KEEP / REVIEW / REJECT
      |
      v
Operation plan -> confirmation -> execute
```

Etap może zakończyć analizę tylko wtedy, gdy spełnia jawnie skonfigurowany próg
pewności. Nie należy ukrywać logiki zakończenia w implementacji konkretnego
modelu.

### 5.2. Podział odpowiedzialności

Proponowane nowe moduły:

```text
src/filecluster/curation/
  __init__.py
  types.py                 # enumy, dataclasses, protokoły
  configuration.py         # CurationSettings i walidacja
  pipeline.py              # orkiestracja kaskady
  rules.py                 # reguły metadanych
  image_features.py        # lekkie cechy obrazu
  scoring.py               # fuzja sygnałów i progi
  catalog.py               # osobny cache SQLite
  operations.py            # plan kopiowania/przenoszenia
  reporting.py             # wiersze raportu, bez renderowania terminala
  providers/
    base.py                # interfejsy providerów
    semantic.py            # SigLIP 2 lub wymienny encoder
    ocr.py                 # wymienny provider OCR
    quality.py             # opcjonalny model jakości
    preference.py          # późniejszy klasyfikator użytkownika
    vlm.py                 # późniejsza eskalacja
```

Pliki istniejące, które wymagają zmiany:

| Plik | Zmiana |
|---|---|
| `src/filecluster/cli.py` | Dodać polecenie `curate` i mapowanie wyjątków na kody wyjścia |
| `src/filecluster/ui.py` | Dodać ograniczone podsumowanie, progress i potwierdzenie planu |
| `src/filecluster/configuration.py` | Tylko wspólne ustawienia ścieżek, jeśli będą potrzebne |
| `pyproject.toml` | Dodać zależności opcjonalne dla semantyki i OCR |
| `README.md` | Dodać krótki opis użycia po ukończeniu funkcji |
| `CHANGELOG.md` | Uzupełnić sekcję `Unreleased` |

Nie należy dodawać logiki klasyfikacji do `file_cluster.py`,
`image_grouper.py` ani `ui.py`.

### 5.3. Kontrakty etapów

```python
@dataclass(frozen=True)
class MediaItem:
    path: Path
    relative_path: str
    size: int
    mtime: float
    sha256: str
    media_type: str
    extension: str


@dataclass(frozen=True)
class StageResult:
    stage: str
    scores: Mapping[str, float]
    labels: tuple[str, ...] = ()
    reasons: tuple[str, ...] = ()
    terminal_decision: CurationDecision | None = None
    confidence: float | None = None
    model_id: str | None = None
    duration_ms: float = 0.0


@dataclass(frozen=True)
class CurationResult:
    item: MediaItem
    decision: CurationDecision
    confidence: float
    scores: Mapping[str, float | None]
    labels: tuple[str, ...]
    reasons: tuple[str, ...]
    stage_trace: tuple[StageResult, ...]
    pipeline_version: str
```

Etapy powinny implementować protokół:

```python
class CurationStage(Protocol):
    name: str

    def analyze(
        self,
        item: MediaItem,
        context: CurationContext,
    ) -> StageResult: ...
```

`pipeline.py` odpowiada za kolejność, obsługę błędów, cache i wcześniejsze
zakończenie. Provider nie może przenosić plików ani renderować terminala.

## 6. Szczegóły kaskady

### 6.1. Etap 0: odkrywanie, fingerprint i cache

1. Odkryć obsługiwane pliki w katalogu wejściowym.
2. Sortować po względnej ścieżce, aby zapewnić deterministyczny limit.
3. Zebrać `size`, `mtime`, rozszerzenie i typ materiału.
4. Dla cache wykonać najpierw szybkie sprawdzenie `(relative_path, size, mtime)`.
5. Gdy wpis nie istnieje lub jest nieaktualny, obliczyć SHA-256 zawartości.
6. Trafienie cache wymaga zgodności:
   - SHA-256,
   - `pipeline_version`,
   - fingerprintu konfiguracji,
   - identyfikatorów i wersji aktywnych modeli.

Nie należy używać samej nazwy pliku ani `mtime` jako trwałej tożsamości.
Istniejący `utlis.hash_file()` domyślnie używa SHA-1, dlatego nowy cache powinien
mieć własną, strumieniową funkcję SHA-256 albo jawnie wywołać helper z SHA-256.

### 6.2. Etap 1: metadane i reguły

Reguły mają generować sygnały, nie tylko wynik binarny:

- rozszerzenie PNG,
- brak fotograficznego EXIF,
- obecność EXIF `Software`,
- szerokość, wysokość i proporcje,
- zgodność rozdzielczości z profilem znanego ekranu,
- bardzo długi lub panoramiczny obraz,
- nazwa sugerująca screenshot,
- format obrazu zwykle generowany przez aplikację,
- orientacja i liczba kanałów.

Przykładowe kody przyczyn:

```text
metadata.screenshot_filename
metadata.screen_resolution_match
metadata.png_without_camera_exif
metadata.camera_exif_present
metadata.unsupported_media_type
```

Reguły nie mogą odrzucić pliku wyłącznie dlatego, że jest PNG lub nie ma EXIF.
Automatyczny `reject` na tym etapie wymaga zgodności co najmniej dwóch silnych
sygnałów albo jednego sygnału jednoznacznego pochodzącego z metadanych systemu.

Lista rozdzielczości ekranów nie może być zakodowana na stałe w logice. Powinna
pochodzić z wersjonowanego pliku danych i tolerować obrót obrazu.

### 6.3. Etap 2: lekkie cechy obrazu i OCR

Obraz należy otwierać przez Pillow, stosować orientację EXIF i tworzyć jedną
pomniejszoną reprezentację roboczą. Maksymalny bok domyślnie wynosi 1024 px.
Pełny obraz nie powinien pozostawać w pamięci po zakończeniu analizy pliku.

Minimalny zestaw cech:

| Cecha | Sugerowana metoda |
|---|---|
| Ostrość | wariancja Laplasjanu na luminancji |
| Jasność | średnia i percentyle luminancji |
| Prześwietlenie | udział pikseli powyżej progu |
| Niedoświetlenie | udział pikseli poniżej progu |
| Kontrast | odchylenie i rozstęp percentylowy luminancji |
| Entropia | entropia histogramu |
| Kolorowość | metryka Haslera–Süsstrunka albo prostszy odpowiednik |
| Krawędzie | gęstość krawędzi |
| Jednolite tło | udział największych skupień jasności/koloru |
| Proporcje | szerokość do wysokości |

OCR powinien zwracać:

- liczbę bloków tekstu,
- średnią pewność,
- liczbę znaków,
- udział powierzchni zajętej przez pola tekstowe,
- liczbę linii,
- opcjonalnie same napisy.

Treść OCR jest potencjalnie wrażliwa. Domyślnie cache i raport zapisują wyłącznie
agregaty, nigdy pełny tekst. Provider OCR powinien działać lokalnie. Zalecany
jest lekki backend ONNX, na przykład RapidOCR/PaddleOCR w wariancie mobilnym,
ale interfejs nie może zależeć od jednego dostawcy.

Silny sygnał dokumentu to kombinacja wysokiej gęstości tekstu, jasnego tła,
prostokątnej geometrii i małej różnorodności kolorów. Wysoka gęstość tekstu
samodzielnie nie wystarcza do odrzucenia zdjęcia miasta lub szyldu.

### 6.4. Etap 3: klasyfikacja semantyczna

Preferowanym encoderem startowym jest wielojęzyczny wariant SigLIP 2 dostępny
w rozmiarze praktycznym dla komputera użytkownika. Provider musi umożliwiać
zamianę modelu bez zmiany pipeline.

Implementacja:

1. Wczytać model leniwie dopiero wtedy, gdy co najmniej jeden plik dotrze do
   etapu semantycznego.
2. Grupować obrazy w małe batche.
3. Wybrać urządzenie w kolejności: jawne ustawienie, MPS/CUDA, CPU.
4. Wyłączyć obliczanie gradientów.
5. Znormalizować embeddingi.
6. Obliczyć podobieństwo cosinusowe do embeddingów promptów.
7. Cache'ować embeddingi promptów dla modelu i wersji zestawu promptów.
8. Zapisywać wynik klasyfikacji, a sam embedding obrazu tylko wtedy, gdy
   włączono uczenie preferencji.

Każda klasa powinna mieć kilka promptów, aby ograniczyć zależność od
pojedynczego sformułowania. Prompt bank jest wersjonowany i przechowywany jako
plik YAML lub JSON. Przykład:

```yaml
screenshot:
  positive:
    - "a screenshot from a mobile phone application"
    - "a captured phone screen with user interface elements"
  negative:
    - "a photograph taken with a camera"
receipt_invoice:
  positive:
    - "a photo of a receipt, bill, or invoice"
    - "a photographed paper receipt with prices and totals"
personal_people:
  positive:
    - "a personal family photograph with people"
    - "a candid photograph of friends or relatives"
```

Wynik klasy powinien być agregatem promptów, na przykład średnią najlepszych
dwóch podobieństw. Surowe podobieństwo cosinusowe nie jest skalibrowanym
prawdopodobieństwem. Przed nazwaniem go prawdopodobieństwem należy wykonać
kalibrację na zestawie walidacyjnym, np. temperature scaling albo regresję
logistyczną.

### 6.5. Etap 4: jakość, estetyka i preferencje

Pierwsza wersja wykorzystuje wyłącznie wynik jakości technicznej z etapu 2.
Niska jakość techniczna nie może automatycznie odrzucać materiału zawierającego
ludzi, zwierzęta lub silny sygnał osobistej fotografii.

Późniejszy model estetyczny ma implementować ten sam kontrakt providera.
Możliwe modele to NIMA, MUSIQ, TOPIQ lub predyktor oparty na embeddingach.

Model preferencji użytkownika powinien być małym modelem nad zamrożonymi
embeddingami:

- regresja logistyczna jako wariant podstawowy,
- liniowy SVM jako alternatywa,
- MLP dopiero po zebraniu odpowiednio dużego zbioru.

Minimalny zbiór do uruchomienia trenowania to 100 ręcznie zatwierdzonych
przykładów każdej z decyzji `keep` i `reject`. Podział train/validation musi być
wykonany grupami czasowymi lub wydarzeniami, a nie losowo per zdjęcie, aby
podobne ujęcia z jednej serii nie trafiły do obu części.

### 6.6. Etap 5: opcjonalny VLM

VLM jest wyłączony domyślnie. Może otrzymać tylko pliki:

- pozostające w `review`,
- z konfliktem sygnałów,
- z pewnością w konfigurowalnym paśmie niepewności.

Provider powinien obsługiwać lokalny model oraz opcjonalnego dostawcę zdalnego,
ale zdalne przetwarzanie wymaga jawnego `--allow-remote-vlm`. Interfejs ma
oczekiwać ustrukturyzowanego JSON:

```json
{
  "decision": "keep",
  "confidence": 0.82,
  "labels": ["personal_people", "event"],
  "reasons": ["people are the primary subject", "camera photograph"]
}
```

Odpowiedź musi zostać zwalidowana. Niepoprawna odpowiedź, timeout albo błąd
sieci kończy się `review`. Tekst modelu nie jest wykonywany ani używany jako
ścieżka pliku.

## 7. Fuzja wyników i progi

### 7.1. Reguła bazowa

Końcowy wynik warto liczyć jawnie:

```text
keep_score =
    w_personal   * personal_probability
  + w_quality    * technical_quality
  + w_aesthetic  * aesthetic_score
  + w_preference * preference_score
  - w_utility    * utility_probability
```

Składniki `None` należy pominąć, a aktywne wagi znormalizować. Początkowe wagi
to konfiguracja startowa do kalibracji, nie uniwersalna prawda:

```yaml
weights:
  personal: 0.50
  technical_quality: 0.15
  aesthetic: 0.10
  preference: 0.25
  utility_penalty: 0.65
thresholds:
  keep: 0.70
  reject: 0.30
  minimum_confidence: 0.75
  conflict_margin: 0.12
```

Przed dostępnością estetyki lub modelu preferencji ich wagi są pomijane.

### 7.2. Reguły bezpieczeństwa

Niezależnie od wyniku liniowego:

- silny sygnał `personal_people`, `family_home`, `pet` albo `event` blokuje
  automatyczny `reject`, chyba że jednoznacznie wykryto screenshot;
- konflikt silnego sygnału osobistego i użytkowego daje `review`;
- błąd dowolnego wymaganego etapu daje `review`;
- wynik w paśmie pomiędzy progami daje `review`;
- jakość techniczna nie może być jedyną przyczyną `reject` w pierwszej wersji;
- `reject` wymaga co najmniej jednego kodu przyczyny z kategorii semantycznej
  albo jednoznacznej reguły screenshota.

### 7.3. Pewność

Pewność decyzji nie jest maksymalnym podobieństwem modelu. Powinna uwzględniać:

- odległość wyniku od najbliższego progu,
- margines między dwiema najlepszymi klasami,
- zgodność niezależnych etapów,
- kalibrację modelu,
- obecność błędów lub brakujących sygnałów.

Do czasu przygotowania kalibracji wszystkie graniczne wyniki powinny trafiać
do `review`.

## 8. Konfiguracja

Ustawienia kaskady powinny być osobną klasą Pydantic:

```python
class CurationSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="FILECLUSTER_CURATION_",
        env_file=".env",
        extra="ignore",
    )

    cache_path: Path | None = None
    config_path: Path | None = None
    max_image_side: int = 1024
    batch_size: int = 8
    device: Literal["auto", "cpu", "mps", "cuda"] = "auto"
    enable_ocr: bool = True
    enable_semantic: bool = True
    enable_quality: bool = True
    enable_preference: bool = False
    enable_vlm: bool = False
    allow_remote_vlm: bool = False
    keep_threshold: float = 0.70
    reject_threshold: float = 0.30
    minimum_confidence: float = 0.75
```

Wymagania walidacji:

- wszystkie wyniki i progi należą do `[0, 1]`,
- `reject_threshold < keep_threshold`,
- batch ma wartość co najmniej 1,
- zdalny VLM nie może być aktywny bez `allow_remote_vlm`,
- plik konfiguracji i prompt bank są hashowane do fingerprintu konfiguracji.

Domyślny cache:

```text
<inbox>/.filecluster-curation.db
```

Katalog cache musi być ignorowany podczas odkrywania materiałów.

## 9. CLI

### 9.1. Podstawowe użycie

```bash
# Analiza bez zmian na dysku
filecluster curate -i inbox -o curated

# Analiza i kopiowanie do keep/review/reject
filecluster curate -i inbox -o curated --execute --copy

# Analiza i przenoszenie
filecluster curate -i inbox -o curated --execute --move

# Pełny raport bez renderowania per plik w terminalu
filecluster curate -i inbox -o curated --report curation.csv

# Wynik zagregowany dla automatyzacji
filecluster curate -i inbox -o curated --json
```

### 9.2. Opcje

| Opcja | Znaczenie |
|---|---|
| `-i, --inbox-dir PATH` | wymagany katalog wejściowy |
| `-o, --output-dir PATH` | wymagany katalog docelowy |
| `--execute` | wykonuje przygotowany plan |
| `--copy` | kopiuje pliki, zalecany wariant początkowy |
| `--move` | przenosi pliki |
| `--report PATH` | pełny raport CSV |
| `--json` | zagregowany wynik JSON na stdout |
| `--config PATH` | konfiguracja kaskady |
| `--cache PATH` | jawna ścieżka cache |
| `--limit N` | deterministycznie analizuje pierwsze N plików |
| `--device VALUE` | `auto`, `cpu`, `mps`, `cuda` |
| `--without-ocr` | wyłącza OCR |
| `--without-semantic` | wyłącza model semantyczny |
| `--enable-vlm` | włącza skonfigurowany VLM |
| `--allow-remote-vlm` | jawnie pozwala przesyłać obraz |
| `--force-recompute` | ignoruje cache wyników |
| `-Y, --yes` | pomija potwierdzenie przed zapisem |
| `-v/-vv`, `-q` | zgodne z obecnym CLI |

`--copy` i `--move` są wzajemnie wykluczające. W trybie `--execute` brak obu
oznacza `--copy`, aby pierwsze uruchomienie było bezpieczniejsze.

### 9.3. Kody wyjścia

- `0`: analiza lub wykonanie zakończone poprawnie,
- `1`: błąd środowiska/modelu uniemożliwił ukończenie całego przebiegu,
- `2`: niepoprawne argumenty lub konfiguracja,
- `130`: przerwanie przez użytkownika.

Pojedynczy uszkodzony plik nie przerywa całego przebiegu. Otrzymuje `review`
oraz `processing.decode_error`.

## 10. Plan operacji na plikach

Klasyfikacja ma pozostać czysta względem systemu plików. Po zakończeniu powstaje
plan:

```python
@dataclass(frozen=True)
class CurationFileOp:
    src: Path
    dst: Path
    decision: CurationDecision
    mode: Literal["copy", "move", "skip"]
```

Ścieżki docelowe:

```text
<output>/keep/<original-relative-path>
<output>/review/<original-relative-path>
<output>/reject/<original-relative-path>
```

Należy wykorzystać albo uogólnić `DestinationAllocator` z
`file_operations.py`. Żaden plik nie może zostać nadpisany. Dry-run i realne
wykonanie muszą wyliczać identyczne nazwy docelowe.

Przed pierwszą operacją zapisu wyświetlane jest jedno potwierdzenie całego
planu. Przerwanie podczas wykonywania zatrzymuje kolejne operacje, ale nie
cofa bezpiecznie ukończonych kopii/przeniesień. Raport powinien rozróżniać
operacje `planned`, `completed` i `failed`.

## 11. Cache i trwałość

### 11.1. Osobny katalog SQLite

Nie należy rozszerzać od razu `LibraryCatalog`, ponieważ opisuje bibliotekę
docelową i klastry. `CurationCatalog` jest osobnym komponentem z własną wersją
schematu.

Minimalny schemat:

```sql
CREATE TABLE schema_version (
    version INTEGER PRIMARY KEY
);

CREATE TABLE files (
    sha256 TEXT PRIMARY KEY,
    last_path TEXT NOT NULL,
    size INTEGER NOT NULL,
    mtime REAL NOT NULL,
    media_type TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE analyses (
    sha256 TEXT NOT NULL,
    pipeline_version TEXT NOT NULL,
    config_fingerprint TEXT NOT NULL,
    model_fingerprint TEXT NOT NULL,
    decision TEXT NOT NULL,
    confidence REAL NOT NULL,
    scores_json TEXT NOT NULL,
    labels_json TEXT NOT NULL,
    reasons_json TEXT NOT NULL,
    stage_trace_json TEXT NOT NULL,
    analyzed_at TEXT NOT NULL,
    PRIMARY KEY (
        sha256,
        pipeline_version,
        config_fingerprint,
        model_fingerprint
    )
);

CREATE TABLE feedback (
    sha256 TEXT PRIMARY KEY,
    user_decision TEXT NOT NULL,
    source TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
```

SQL używa parametrów, nie interpolacji tekstowej. Połączenie powinno używać
WAL i `synchronous=NORMAL`, zgodnie z istniejącym `LibraryCatalog`.

### 11.2. Wersjonowanie modeli

`model_fingerprint` zawiera dla każdego providera:

- identyfikator modelu,
- revision/commit,
- checksum lokalnych wag, jeśli jest dostępny,
- wersję preprocessora,
- wersję prompt banku.

Nie należy pobierać automatycznie dowolnej najnowszej rewizji modelu. Model ma
być przypięty do konkretnej wersji. Brak lokalnych wag może uruchomić
kontrolowane pobranie tylko wtedy, gdy konfiguracja na to pozwala.

## 12. Raportowanie i UI

### 12.1. Terminal

Terminal pokazuje:

- liczbę odkrytych plików,
- trafienia cache,
- postęp poszczególnych etapów,
- liczbę `keep`, `review`, `reject`,
- najczęstsze przyczyny, maksymalnie 10,
- szacowany i rzeczywisty czas,
- podsumowanie planu operacji.

Nie wolno wypisywać jednego wiersza na plik. Należy użyć istniejących
`Reporter`, `ProgressSink` i wzorców z `ui.py`.

### 12.2. CSV

Raport per plik zawiera:

```text
source_path
destination_path
sha256
decision
confidence
top_label
labels
personal_probability
utility_probability
technical_quality
aesthetic_score
preference_score
reasons
completed_stage
cache_hit
operation_status
duration_ms
pipeline_version
model_fingerprint
```

Listy zapisuje się jako stabilny JSON w polu CSV. Raport nie zawiera tekstu OCR,
embeddingów ani pełnej odpowiedzi VLM.

### 12.3. JSON

`--json` zapisuje na stdout tylko ograniczone podsumowanie:

```json
{
  "files": 1000,
  "cache_hits": 720,
  "decisions": {
    "keep": 610,
    "review": 140,
    "reject": 250
  },
  "errors": 3,
  "elapsed_seconds": 84.2,
  "executed": false
}
```

Logi pozostają na stderr.

## 13. Zależności i dystrybucja

Podstawowa instalacja `filecluster` nie powinna bezwarunkowo pobierać frameworka
ML i wag modeli. Zalecane dodatki:

```toml
[project.optional-dependencies]
curation = [
  # wybrany runtime/model semanticzny,
  # lekki backend OCR,
  # zależność do dekodowania HEIC, jeśli potrzebna
]
curation-vlm = [
  # zależności wymagane tylko przez lokalny VLM
]
```

Agent implementujący powinien przed wyborem bibliotek sprawdzić:

- obsługę Python 3.12,
- CPU oraz Apple Silicon/MPS,
- rozmiar instalacji i wag,
- licencję kodu i modelu,
- możliwość przypięcia rewizji,
- pracę offline po pobraniu modelu.

Jeżeli ciężkie zależności nie są zainstalowane, CLI ma podać krótką instrukcję
instalacji dodatku, zamiast kończyć się nieczytelnym `ImportError`.

## 14. Wydajność

Projekt może przetwarzać około 50 tysięcy plików. Wymagania:

- jeden przebieg odczytu pliku na potrzeby fingerprintu, jeżeli to możliwe,
- brak trzymania wielu pełnych obrazów w pamięci,
- batchowanie wyłącznie etapu modelowego,
- ograniczona kolejka producer/consumer,
- domyślnie nie więcej niż 2 GB dodatkowego RAM na CPU,
- cache zapewniający, że niezmienione pliki nie są ponownie inferowane,
- pomiar czasu każdego etapu,
- możliwość wyłączenia OCR, semantyki i VLM.

Budżety do pomiaru, a nie gwarantowane wartości:

| Tryb | Docelowy charakter działania |
|---|---|
| cache hit | tysiące plików/s |
| metadane/reguły | setki–tysiące plików/s |
| lekkie cechy | dziesiątki–setki obrazów/s |
| OCR | zależne od backendu, zwykle kilka–kilkadziesiąt obrazów/s |
| encoder | batchowany; mierzyć osobno CPU, MPS i CUDA |
| VLM | sekundy na obraz; tylko mały odsetek wejścia |

Test benchmarkowy powinien raportować przepustowość i szczytową pamięć dla
próbki co najmniej 1000 obrazów o różnych rozdzielczościach.

## 15. Prywatność i bezpieczeństwo

- Domyślnie wszystkie obliczenia są lokalne.
- Pełny tekst OCR nie jest utrwalany.
- Embeddingi nie trafiają do CSV ani logów.
- Ścieżki mogą zawierać dane osobowe, dlatego zwykłe logi powinny preferować
  liczniki; przykłady ścieżek są dozwolone dopiero przy `-vv` i muszą być
  ograniczone liczbowo.
- Zdalny VLM wymaga jawnej zgody flagą.
- Tokeny usług zewnętrznych pochodzą ze zmiennych środowiskowych lub
  bezpiecznego magazynu, nigdy z pliku konfiguracji zapisywanego do repozytorium.
- Odpowiedzi modeli i tekst OCR są danymi niezaufanymi.
- Ścieżki docelowe są tworzone wyłącznie na podstawie kontrolowanego enumu
  decyzji i bezpiecznej względnej ścieżki. Należy odrzucić `..`, ścieżki
  absolutne i dowiązania wychodzące poza inbox.
- Pobierane wagi muszą pochodzić ze skonfigurowanego źródła i przypiętej rewizji.

## 16. Obsługa błędów i przypadki brzegowe

| Przypadek | Oczekiwane zachowanie |
|---|---|
| Uszkodzony obraz | `review`, kod `processing.decode_error` |
| Nieobsługiwany HEIC/RAW | `review`, instrukcja instalacji dekodera |
| Wideo | `review`, `metadata.unsupported_media_type` |
| Brak EXIF | kontynuować analizę pikseli |
| Ogromny/decompression-bomb image | bezpiecznie odmówić dekodowania, `review` |
| Obraz obrócony przez EXIF | zastosować transpozycję przed cechami i modelem |
| Przezroczysty PNG | skomponować na neutralnym tle przed inferencją |
| CMYK lub grayscale | jawna konwersja do RGB |
| Ten sam plik pod inną nazwą | wykorzystać wynik po SHA-256 |
| Zmieniony plik pod tą samą nazwą | unieważnić wpis przez size/mtime i hash |
| Niedostępny model | `review` lub kontrolowany błąd całego etapu według konfiguracji |
| Sprzeczne modele | `review` |
| Kolizja nazwy docelowej | bezpieczny suffix, nigdy nadpisanie |
| Przerwanie | zakończyć dalsze operacje i zwrócić 130 |
| Pusty inbox | sukces z zerowym podsumowaniem |
| Plik znika podczas analizy | diagnostyka i pominięcie operacji |

## 17. Plan testów

### 17.1. Testy jednostkowe

Nowe pliki:

```text
tests/curation/test_types.py
tests/curation/test_configuration.py
tests/curation/test_rules.py
tests/curation/test_image_features.py
tests/curation/test_scoring.py
tests/curation/test_pipeline.py
tests/curation/test_catalog.py
tests/curation/test_operations.py
tests/curation/test_reporting.py
tests/curation/test_providers.py
```

Testy muszą obejmować:

- walidację progów i wzajemnie wykluczających ustawień,
- deterministyczne reguły metadanych,
- cechy obrazu dla syntetycznych obrazów,
- brak automatycznego odrzucenia tylko za brak EXIF,
- konflikty sygnałów prowadzące do `review`,
- wszystkie reguły bezpieczeństwa z sekcji 7.2,
- cache hit i invalidację każdego fingerprintu,
- migrację schematu SQLite,
- błędy dekodowania i providerów,
- serializację CSV/JSON,
- kolizje nazw bez nadpisania,
- identyczność planu dry-run i wykonania,
- brak wywołania drogiego etapu po terminalnej decyzji wcześniejszego etapu,
- leniwe ładowanie modeli,
- brak tekstu OCR i embeddingów w raporcie.

Providery modelowe są mockowane. Zwykły zestaw testów nie pobiera wag i nie
potrzebuje internetu ani GPU.

### 17.2. Testy integracyjne CLI

- `curate --help`,
- pusty inbox,
- dry-run nie modyfikuje plików,
- `--execute --copy`,
- `--execute --move`,
- potwierdzenie i anulowanie,
- `--yes`,
- `--limit` wybiera deterministyczny zestaw,
- `--json` nie zawiera innego tekstu na stdout,
- raport ma oczekiwane kolumny,
- brak opcjonalnej zależności daje czytelny komunikat,
- ponowny przebieg wykorzystuje cache.

### 17.3. Zbiór ewaluacyjny

Należy utworzyć prywatny, niecommitowany zbiór manifestów:

```text
evaluation/
  manifest.csv
  images/   # wpisane do .gitignore
```

Manifest:

```text
relative_path,gold_decision,gold_labels,event_group,notes
```

Metryki:

- precision/recall/F1 dla każdej klasy,
- macierz pomyłek,
- precision dla `reject`,
- odsetek `review`,
- false reject rate dla osobistych zdjęć,
- Expected Calibration Error albo Brier score,
- przepustowość i cache hit rate.

Najważniejszą metryką jest **false reject rate** dla zdjęć osobistych.
Optymalizacja powinna preferować większy katalog `review` zamiast błędnego
odrzucenia ważnych zdjęć.

## 18. Kryteria akceptacji pierwszej wersji

Pierwsza wersja jest kompletna, gdy:

1. `filecluster curate` działa jako niezależne polecenie.
2. Domyślne uruchomienie nie modyfikuje plików.
3. Klasyfikacja zwraca `keep`, `review` lub `reject` wraz z pewnością i kodami
   przyczyn.
4. Działa etap reguł, lekkich cech oraz wymienny provider semantyczny.
5. OCR można włączyć i wyłączyć bez zmiany pipeline.
6. System nie odrzuca automatycznie zdjęcia tylko z powodu braku EXIF albo
   niskiej ostrości.
7. Awaria per plik prowadzi do `review` i nie przerywa przebiegu.
8. Cache unika ponownej inferencji dla niezmienionych plików i jest poprawnie
   unieważniany po zmianie konfiguracji lub modelu.
9. Dry-run i wykonanie tworzą identyczny plan nazw docelowych.
10. Żaden istniejący plik docelowy nie jest nadpisywany.
11. Terminal nie generuje wyjścia per plik.
12. CSV zawiera pełne wyniki per plik, ale nie zawiera OCR text ani embeddingów.
13. `main()` istniejącego klastra pozostaje cichy przy wywołaniu bibliotecznym.
14. Istniejące testy nadal przechodzą.
15. Przechodzą `make test`, `make check` i `make type` z uwzględnieniem
    istniejącego statusu type checkera w projekcie.

## 19. Kolejność implementacji

### Faza 1: bezpieczny szkielet bez ML

1. Typy, konfiguracja i kontrakty providerów.
2. `CurationCatalog` wraz z migracjami i fingerprintami.
3. Reguły metadanych i lekkie cechy obrazu.
4. Bazowa fuzja wyników.
5. Plan operacji, dry-run, CSV i JSON.
6. Polecenie CLI oraz UI.
7. Testy wszystkich powyższych elementów.

Rezultat: działający lokalny klasyfikator heurystyczny, który większość
niejednoznacznych przypadków kieruje do `review`.

### Faza 2: semantyka

1. Wybrać i przypiąć konkretny wariant SigLIP 2.
2. Dodać opcjonalny pakiet zależności.
3. Zaimplementować preprocessing, batching i wybór urządzenia.
4. Dodać wersjonowany prompt bank.
5. Przygotować kalibrację na prywatnym zbiorze ewaluacyjnym.
6. Zmierzyć jakość i wydajność.

### Faza 3: OCR

1. Wybrać lekki backend działający na Pythonie 3.12 i Apple Silicon.
2. Zaimplementować provider zwracający wyłącznie agregaty.
3. Skalibrować wykrywanie dokumentów, paragonów i stron książek.

OCR może zostać wykonany przed fazą 2, jeżeli priorytetem są dokumenty.

### Faza 4: osobiste preferencje

1. Zapisywać jawne korekty użytkownika w tabeli `feedback`.
2. Cache'ować embeddingi w osobnej tabeli lub pliku macierzowym.
3. Dodać trening regresji logistycznej.
4. Wersjonować model i mierzyć wynik na podziale eventowym.

### Faza 5: estetyka i VLM

1. Dodać provider jakości estetycznej.
2. Dodać eskalację tylko dla niepewnego pasma.
3. Wymagać jawnej zgody dla dostawcy zdalnego.
4. Porównać przyrost jakości z kosztem i czasem.

## 20. Weryfikacja i komendy

Po każdej fazie agent powinien uruchomić najwęższe testy, a przed zakończeniem:

```bash
uv sync --group dev
uv run pytest tests/curation
uv run pytest tests/test_cli.py tests/test_file_operations.py
make check
make type
make test
```

Testy integracyjne korzystające z prawdziwych wag modelu powinny mieć osobny
marker, na przykład `model`, i nie mogą być częścią szybkiego testu
jednostkowego:

```bash
uv run pytest -m model
```

Agent ma raportować dokładnie, które komendy uruchomił i które zostały
pominięte.

## 21. Ryzyka i sposoby ograniczenia

| Ryzyko | Skutek | Ograniczenie |
|---|---|---|
| Subiektywność pojęcia „warte zachowania” | wiele błędów granicznych | `review` oraz późniejszy model preferencji |
| Błędne odrzucenie ważnego zdjęcia | duża szkoda użytkownika | brak kasowania, wysoki próg `reject`, reguły ochronne |
| Zmiana jakości po aktualizacji modelu | niedeterministyczne wyniki | przypięte rewizje i fingerprint modelu |
| Ciężkie zależności | trudna instalacja | extras i wymienne providery |
| Wolny przebieg dla 50k plików | zła użyteczność | cache, batching i wcześniejsze zakończenie |
| Prywatne dane w OCR/VLM | wyciek informacji | lokalne działanie, brak tekstu w cache, opt-in dla sieci |
| Prompt bias | niestabilne klasy | wiele promptów i kalibracja |
| Przeuczenie na podobnych zdjęciach | zawyżone metryki | podział train/test według wydarzeń |
| Brak dekodera HEIC/RAW | duży katalog `review` | opcjonalny decoder i czytelna diagnostyka |

## 22. Instrukcja dla agenta implementującego

Przed kodowaniem:

1. Przeczytaj `AGENTS.md`, ten dokument oraz `docs/retro-spec.md`.
2. Sprawdź aktualny stan repozytorium i nie naruszaj zmian użytkownika.
3. Zweryfikuj aktualne API wybranego modelu i licencje zależności.
4. Rozpisz implementację na fazy. Nie próbuj dostarczyć VLM przed działającym
   szkieletem, cache i raportowaniem.

Podczas implementacji:

- zachowaj obecny podział logiki, orkiestracji i UI,
- użyj dependency injection dla providerów,
- nie pobieraj modeli w czasie importu modułu,
- nie dodawaj wyjścia per plik do terminala,
- nie zmieniaj zachowania `filecluster run`,
- nie wykonuj operacji na plikach przed zbudowaniem i zatwierdzeniem całego
  planu,
- dodawaj testy równolegle z kolejnymi etapami.

Jeżeli wybór biblioteki ML, wariantu modelu lub licencji okaże się niejednoznaczny,
agent powinien zatrzymać się po fazie 1 i przedstawić zmierzone opcje zamiast
samodzielnie dodawać ciężką zależność produkcyjną.

## 23. Źródła techniczne

- SigLIP 2: <https://arxiv.org/abs/2502.14786>
- Florence-2: <https://www.microsoft.com/en-us/research/publication/florence-2-advancing-a-unified-representation-for-a-variety-of-vision-tasks/>
- MUSIQ: <https://research.google/blog/musiq-assessing-image-aesthetic-and-technical-quality-with-multi-scale-transformers/>
- LAION Aesthetic Predictor: <https://github.com/LAION-AI/aesthetic-predictor>
- PaddleOCR: <https://github.com/PaddlePaddle/PaddleOCR>

Źródła wskazują klasy rozwiązań, ale nie zastępują testu licencji, zgodności
wersji i benchmarku na docelowym sprzęcie przed dodaniem zależności.
