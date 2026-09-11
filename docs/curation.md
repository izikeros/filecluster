# Curation subpackage (`filecluster.curation`)

Sorts inbox media into `keep`, `review` and `reject` **before** event clustering,
so utility material (screenshots, receipts, documents, product shots, whiteboard
photos) never enters a personal photo library.

The design it implements is `docs/cascaded-photo-curation-system.md`. This
document describes what exists in the code today, how to run and configure it,
where the extension points are, and what is still missing.

Status: **Phase 1 complete.** The cheap deterministic stages are fully
implemented and tested. The model-backed stages exist as contracts plus one real
provider each for semantics and OCR; aesthetics and VLM escalation are explicit
seams that refuse to run rather than guess.

---

## Contents

- [Quick start](#quick-start)
- [The decision model](#the-decision-model)
- [The cascade](#the-cascade)
- [Signals and reason codes](#signals-and-reason-codes)
- [Configuration](#configuration)
- [The verdict cache](#the-verdict-cache)
- [Reports and automation](#reports-and-automation)
- [Library use](#library-use)
- [Extension points](#extension-points)
- [Module map](#module-map)
- [Testing](#testing)
- [Integrating with the main CLI](#integrating-with-the-main-cli)
- [Current limitations](#current-limitations)
- [Prerequisites for semantic embeddings and VLM](#prerequisites-for-semantic-embeddings-and-vlm)
- [Pending work and next steps](#pending-work-and-next-steps)
- [What is needed to unlock the full potential](#what-is-needed-to-unlock-the-full-potential)

---

## Quick start

The subpackage ships its own entry point while it matures:

```bash
# Dry run: analyse the inbox, print the plan, write nothing.
python -m filecluster.curation -i ~/inbox -o ~/curated

# Apply it, moving files instead of copying them.
python -m filecluster.curation -i ~/inbox -o ~/curated --execute --move

# Per-file CSV plus a machine-readable summary.
python -m filecluster.curation -i ~/inbox -o ~/curated \
    --report curation.csv --json
```

Output layout, created only with `--execute`:

```
~/curated/
├── keep/     ← photographs worth importing
├── review/   ← everything uncertain; look here first
└── reject/   ← utility material; still on disk, never deleted
```

Each file keeps its path relative to the inbox inside its decision folder, and
name collisions get a ` (1)` suffix through the same `DestinationAllocator` the
clustering plan uses. The names shown in a dry run are the names a real run
writes.

To see the model-backed stages, install the extra and enable them:

```bash
pip install "filecluster[curation]"      # torch, transformers, rapidocr
python -m filecluster.curation -i ~/inbox -o ~/curated \
    --with-semantic --with-ocr --device auto
```

Without the extra the run still works: it degrades to fewer signals, prints a
one-line install hint, and routes anything it cannot settle to `review`.

---

## The decision model

Three decisions, and the asymmetry between them is the whole point:

| Decision | Meaning | Cost of being wrong |
| --- | --- | --- |
| `keep` | Import it into the library. | A utility file to delete later: one click. |
| `review` | A human has to look. | A folder to skim. |
| `reject` | Do not import automatically. | A lost family photo. Not recoverable by clicking. |

Because the third column is not symmetric, the pipeline holds these invariants:

1. **Nothing is ever deleted.** `reject` moves or copies a file into
   `reject/`, and that is all it means.
2. **Unknown means review.** A missing file, an undecodable image, an oversized
   image, a video, a broken provider or an exception anywhere in the cascade all
   produce `review` — not `reject`, and not `keep` either, whatever the partial
   evidence that did arrive happened to suggest.
3. **Reject needs to know *what* the file is.** Technical quality and metadata
   alone can never discard a file: the verdict requires a semantic reason
   (`semantic.utility`, `semantic.screenshot`, `vlm.decision`) or a decisive
   operating-system screenshot marker.
4. **Protected subjects block reject.** If a person, pet, home or event is a
   likely subject, a low score becomes `review`.
5. **Conflicting strong signals mean review.** Personal and utility evidence
   both above `0.55` sends the file to a human whichever way the arithmetic
   landed.
6. **Low confidence blocks a reject.** Confidence is the distance from the
   nearest threshold, not a model's self-reported certainty, so an automatic
   `reject` — whether handed down early by a stage or produced by fusion — has
   to clear `thresholds.minimum_confidence` as well. The floor is deliberately
   one-sided: applying it to `keep` too would merely re-impose a stricter keep
   threshold, invisibly, since the configured number would no longer be the one
   in force.

All six rules live in `scoring._apply_safety_rules`, and each one appends its own
reason code so a verdict can always be explained.

### How the score is built

```
keep_score = Σ(wᵢ · signalᵢ) / Σ(wᵢ)  −  utility_penalty · utility_probability
```

Only signals that were actually measured take part, and the remaining weights are
renormalised. Adding an aesthetic model later therefore shifts the mix rather
than the overall scale. Defaults:

| Weight | Default | Signal |
| --- | --- | --- |
| `personal` | 0.50 | `personal_probability` |
| `technical_quality` | 0.15 | `technical_quality` |
| `aesthetic` | 0.10 | `aesthetic_score` (unmeasured in Phase 1) |
| `preference` | 0.25 | `preference_score` (unmeasured in Phase 1) |
| `utility_penalty` | 0.65 | subtracted, using `utility_probability` |

| Threshold | Default | Meaning |
| --- | --- | --- |
| `keep` | 0.70 | at or above → `keep` |
| `reject` | 0.30 | at or below → `reject` candidate, subject to the safety rules |
| `minimum_confidence` | 0.75 | an automatic `reject` below this becomes `review` |
| `conflict_margin` | 0.12 | semantic top-two gap below this adds `semantic.low_margin` |
| `vlm_band` | `(0.35, 0.75)` | confidence band that qualifies for VLM escalation |

Confidence is computed in band units: `0.5 + distance_from_threshold / (keep −
reject)`, and it is multiplied by `0.95` while the personal signal is heuristic
rather than semantic, so uncalibrated evidence cannot produce certainty.

Because that value is a monotone function of the margin, requiring it of a
fused verdict is equivalent to moving the threshold: at the defaults, a `reject`
must actually reach `0.20` rather than `0.30`. That extra caution is charged to
the destructive direction only. Until calibration lands (see *Pending work*),
read `minimum_confidence` as "how much further past the reject threshold a file
has to be", not as a probability.

---

## The cascade

Stages run cheapest first and each one is a pure measurement. The pipeline owns
ordering, early exit, caching, error containment and the counters; a stage never
writes files, never renders, and never decides on its own except where noted.

| # | Stage | Cost | Needs | May end the cascade? |
| --- | --- | --- | --- | --- |
| 1 | `MetadataRuleStage` | header read only | — | yes: `reject` for an unambiguous screenshot, `review` for a video |
| 2 | `ImageFeatureStage` | one decode per file | Pillow, numpy | `review` on decode failure |
| 3 | `OcrStage` | tens of ms | `curation` extra | no |
| 4 | `SemanticStage` | model inference | `curation` extra | no |
| 5 | `QualityStage` | model inference | not implemented | no |
| 6 | `VlmStage` | seconds | not implemented | yes, on its own judgement |

Stage 6 is not part of the ordered cascade: it runs after fusion, only for files
whose confidence falls inside `vlm_band`.

### Stage 1 — metadata and filename rules

Reads size and EXIF headers through Pillow without decoding pixels, so it stays
cheap on a 100 MP file. Signals: screenshot filename patterns (English, Polish,
German, Spanish, French, Czech), screenshot software in the EXIF `Software` tag,
an image size matching a known screen resolution (orientation-insensitive, ±4 px,
from the versioned `data/screen_resolutions.json`), an app-only container format,
the same format without camera EXIF, and an extreme aspect ratio.

The stage may terminate with `reject` only on a decisive operating-system marker
(confidence 0.95) or two independent strong signals agreeing (0.85). Everything
else becomes a prior handed to the later stages, because "no EXIF" and "is a PNG"
describe plenty of photographs worth keeping.

### Stage 2 — pixel features

Decodes each file exactly once into a working image: EXIF orientation applied,
transparency flattened onto neutral grey, CMYK and grayscale converted, long side
reduced to `max_image_side` (default 1024) using `draft()` where the JPEG decoder
allows it. Files declaring more than `max_pixels` (default 80 MP) are refused, so
a decompression bomb cannot exhaust memory mid-batch. The working image lives on
the `CurationContext` and is released after each file, so later stages reuse one
decode.

Measurements: log-scaled Laplacian sharpness, brightness with 5th/95th
percentiles, clipping fractions, contrast, entropy, Hasler-Süsstrunk
colourfulness, Sobel edge density, uniform-background share. Three derived
signals follow:

- `technical_quality` — sharpness, exposure and contrast fused.
- `features.document_evidence` — bright background, flat background and texty
  edge density, **gated by colourlessness**. The gate is what stops a busy,
  colourful street photograph from reading as a page of text.
- `features.photographic_evidence` — colour, texture and background variety.

### Stage 3 — OCR density

`RapidOcrProvider` returns aggregates only: block, line and character counts,
mean confidence and text area fraction. **Recognised strings are never returned
or stored**, because OCR output of a personal library is sensitive and nothing
downstream needs the words. The aggregates are combined into
`ocr.text_density_evidence`, and `0.55` or more on that raises
`ocr.high_text_density`.

### Stage 4 — semantic classification

`SigLipSemanticProvider` runs a pinned `google/siglip2-base-patch16-224` against
the 17-label prompt bank in `data/prompt_bank.json`, averaging the best prompts
per label. `torch` and `transformers` are imported lazily inside the provider, so
a plain `filecluster run` never loads an ML runtime.

| Group | Labels |
| --- | --- |
| Personal | `personal_people`, `family_home`, `portrait`, `landscape`, `city_travel`, `event`, `pet`, `artistic_photo` |
| Utility | `screenshot`, `document`, `receipt_invoice`, `book_page`, `label_packaging`, `product_reference`, `whiteboard_notes`, `low_information` |
| Protected (blocks reject) | `personal_people`, `family_home`, `pet`, `event` |
| Neutral | `other` |

When this stage runs, its `personal_probability` and `utility_probability` win
outright over the heuristic blend. When it does not, `resolve_signals` assembles
them from the metadata prior (weight 0.7) and pixel evidence (0.3), and records
the source so confidence can be discounted.

### Stage 5 — aesthetics

Not implemented. `AestheticProvider` raises `MissingDependencyError` on
construction rather than scoring everything the same, because a constant signal
shifts every verdict by the same amount and looks like a working model. The
fusion step handles the gap by dropping the weight, not by substituting zero.

### Stage 6 — VLM escalation

Not implemented. `LocalVlmProvider` is a seam; `parse_vlm_response` is finished
and treats the model answer as untrusted input: JSON validated, decision checked
against the enum, confidence clamped to `[0, 1]`, labels filtered through the
known-label whitelist, reasons capped at 4 entries of 200 characters. A remote
endpoint additionally requires `allow_remote_vlm`, so pixels never leave the
machine by accident.

---

## Signals and reason codes

Two namespaces travel with every result. **Signals** are the named numbers the
fusion step consumes plus the raw measurements behind them:

| Key | Range | Source |
| --- | --- | --- |
| `personal_probability` | 0-1 | semantic, else heuristic blend |
| `utility_probability` | 0-1 | semantic, else strongest of metadata / pixels / OCR |
| `technical_quality` | 0-1 | pixel features |
| `aesthetic_score` | 0-1 | quality provider (absent in Phase 1) |
| `preference_score` | 0-1 | preference provider (absent in Phase 1) |
| `rules.*` | mixed | `personal_prior`, `utility_evidence`, `strong_signals` |
| `features.*` | mixed | every raw measurement plus the two evidence scores |
| `ocr.*` | mixed | aggregates plus `text_density_evidence` |
| `semantic.<label>` | 0-1 | per-label similarity |
| `vlm.confidence` | 0-1 | escalation answer |

Only the five named signals appear in report columns. The rest stay in
`CurationResult.scores` for debugging.

**Reason codes** are namespaced by the stage that emits them and are part of the
report format, so they are treated as an API: renaming one requires bumping
`PIPELINE_VERSION`. The families are `metadata.*`, `features.*`, `ocr.*`,
`semantic.*`, `vlm.*`, `fusion.*` and `processing.*`; `reasons.py` is the full
list with a comment on each.

---

## Configuration

Three sources, later ones winning: a config file, environment variables with the
`FILECLUSTER_CURATION_` prefix, then CLI flags.

```jsonc
// curation.json
{
  "enable_semantic": true,
  "enable_ocr": true,
  "max_image_side": 1024,
  "device": "mps",
  "weights":    { "personal": 0.55, "utility_penalty": 0.70 },
  "thresholds": { "keep": 0.72, "reject": 0.28, "minimum_confidence": 0.80 }
}
```

```bash
python -m filecluster.curation -i ~/inbox -o ~/out --config curation.json
FILECLUSTER_CURATION_DEVICE=cpu python -m filecluster.curation -i ~/inbox -o ~/out
```

YAML is accepted when PyYAML happens to be installed; JSON always works, so the
base install stays dependency-free.

### CLI options

| Option | Effect |
| --- | --- |
| `-i, --inbox-dir` | Directory to curate (must exist). |
| `-o, --output-dir` | Where `keep/`, `review/` and `reject/` are created. |
| `--execute` | Apply the plan. Without it nothing is written. |
| `--copy` / `--move` | Copy (default with `--execute`) or move. Mutually exclusive. |
| `--report FILE` | Write the 19-column per-file CSV. |
| `--json` | Print the aggregate JSON summary instead of the tables. |
| `--config FILE` | Load settings from JSON (or YAML). |
| `--cache PATH` | Cache database location. |
| `--limit N` | Analyse the first N files in path order. Deterministic. |
| `--device` | `auto`, `cpu`, `mps` or `cuda`. |
| `--with-semantic` / `--without-semantic` | Turn the semantic stage on or off. |
| `--with-ocr` / `--without-ocr` | Turn the OCR stage on or off. |
| `--enable-vlm` | Escalate uncertain files to a VLM. |
| `--allow-remote-vlm` | Permit sending images to a remote VLM service. |
| `--force-recompute` | Ignore cached verdicts, still write new ones. |
| `--no-cache` | Neither read nor write the cache. |
| `-Y, --yes` | Skip the confirmation before writing. |
| `--color / --no-color`, `-v`, `-q` | Rendering and verbosity. |

Exit codes match the rest of the tool: `0` success (a declined confirmation
counts as success), `1` at least one file operation failed, `2` bad usage,
`130` interrupted.

---

## The verdict cache

A SQLite database (`.filecluster-curation.db` inside the inbox by default, WAL
mode) with four tables: `files`, `analyses`, `feedback`, `schema_version`.

A cached verdict is reused only when **all four** of these match:

- the file's content SHA-256,
- `PIPELINE_VERSION` (bumped whenever a stage changes how it measures),
- the configuration fingerprint (every weight, threshold and toggle, plus a
  digest of the config file and prompt bank; paths are excluded on purpose),
- the model fingerprint (name, model id, revision, preprocessor version, prompt
  bank version and weights checksum of every active provider).

So a threshold tweak invalidates exactly as much as swapping a checkpoint does,
and a heuristic-only run has its own stable cache namespace. Digests are reused
across runs when a file's path, size and mtime are unchanged, which avoids
re-reading every byte. A foreign schema version drops the analyses rather than
migrating them field by field, since they are always recomputable.

The `feedback` table records explicit user corrections for later preference
training. Nothing writes to it yet (see [Pending work](#pending-work-and-next-steps)).

---

## Reports and automation

`--report` writes one row per file with these columns:

```
source_path, destination_path, sha256, decision, confidence, top_label, labels,
personal_probability, utility_probability, technical_quality, aesthetic_score,
preference_score, reasons, completed_stage, cache_hit, operation_status,
duration_ms, pipeline_version, model_fingerprint
```

`--json` prints aggregates only, so the payload is the same size for 50 files and
50,000: counts per decision, cache hits, errors, elapsed time, the three
fingerprints, the ten most common reasons and the operation summary.

Neither output ever contains OCR text, embeddings or raw model answers: a report
of a personal photo library is easy to share by accident.

---

## Library use

```python
from filecluster.curation import (
    CurationDecision, CurationSettings, OperationMode,
    build_operation_plan, curate, execute_plan,
)

run = curate("/photos/inbox", CurationSettings(enable_semantic=False))
print(run.decision_counts())          # {'keep': 12, 'review': 30, 'reject': 8}

for result in run.results_for(CurationDecision.REVIEW):
    print(result.item.relative_path, result.confidence, result.reasons)

plan = build_operation_plan(run.results, "/photos/curated", OperationMode.COPY)
execute_plan(plan)                     # planning and execution stay separate
```

`curate()` renders nothing and writes no media files, so a caller gets verdicts
and decides what to do with them. Injecting providers is explicit:

```python
from filecluster.curation import Providers
from filecluster.curation.providers.semantic import SigLipSemanticProvider

providers = Providers(semantic=SigLipSemanticProvider(device="mps"))
run = curate("/photos/inbox", CurationSettings(enable_semantic=True), providers)
```

---

## Extension points

**A new provider** implements one protocol from `providers/base.py` (`info()`
plus one method) and is passed in through `Providers`. Nothing else changes: the
pipeline never imports a model directly.

```python
class MyAesthetic:
    def info(self) -> ProviderInfo:
        return ProviderInfo(name="quality", model_id="nima-v1", revision="abc123")

    def score(self, image: object) -> float:
        return 0.0  # [0, 1]
```

Return a `ProviderInfo` that pins everything that can change an answer; it feeds
the cache key, so a silent checkpoint swap would otherwise serve stale verdicts.

**A new stage** implements the `CurationStage` protocol (`name` plus
`analyze(item, context) -> StageResult`) and is inserted in `build_stages()` in
cost order. Read the working image from `context.image` instead of decoding
again. Return signals and reason codes; set `terminal_decision` only if the stage
can genuinely settle the file, and remember that the pipeline honours an early
`reject` only above `minimum_confidence`.

**New reason codes** go in `reasons.py`. Adding one is safe; renaming or removing
one changes the report contract and needs a `PIPELINE_VERSION` bump.

---

## Module map

| Module | Responsibility |
| --- | --- |
| `types.py` | Decisions, labels, `MediaItem`, `StageResult`, `CurationResult`, `CurationContext`, stage protocol |
| `configuration.py` | `CurationSettings`, `Weights`, `Thresholds`, `fingerprint()`, `load_settings()` |
| `reasons.py` | Reason-code vocabulary and the strong/decisive/semantic sets |
| `exceptions.py` | `CurationError` and friends, including the install-hint error |
| `rules.py` | Stage 1: screen-resolution table, EXIF facts, `MetadataRuleStage` |
| `image_features.py` | Stage 2: `load_working_image`, feature maths, `ImageFeatureStage` |
| `provider_stages.py` | Adapters turning providers into stages, with failure containment |
| `providers/` | Contracts (`base.py`) plus `semantic`, `ocr`, `quality`, `preference`, `vlm` |
| `scoring.py` | Signal resolution, keep score, confidence, safety rules, escalation test |
| `pipeline.py` | Discovery, fingerprinting, cascade, early exit, cache, `curate()` |
| `catalog.py` | SQLite verdict cache and its schema lifecycle |
| `operations.py` | Destination planning and execution, kept apart so dry runs are truthful |
| `reporting.py` | CSV rows, JSON summary, top reasons |
| `ui.py` | Bounded Rich rendering: banner, results grid, top reasons, confirmation |
| `cli.py` | Typer command, provider construction, exit codes |
| `hashing.py` | Streaming SHA-256 |
| `data/` | `screen_resolutions.json`, `prompt_bank.json`, both versioned |

Discovery skips dot-files, its own databases and `SKIP_DIR_NAMES`, refuses
symlinks that resolve outside the inbox, and sorts before applying `--limit` so a
truncated run is reproducible.

---

## Testing

```bash
uv run pytest tests/curation          # 221 tests
uv run pytest tests                   # full suite, 755 tests
```

The suite downloads no models and needs no GPU: providers are faked through
`tests/curation/conftest.py`, which also builds synthetic photos, documents and
screenshots with numpy and Pillow. It covers the safety rules one by one, cache
round trips and invalidation, dry-run/real-run destination equality, and the
failure paths (corrupt file, missing file, decompression bomb, broken provider).

---

## Integrating with the main CLI

Deliberately not wired up yet. When it should become a first-class command, two
lines in `src/filecluster/cli.py`:

```python
from filecluster.curation.cli import curate_cmd

app.command("curate")(curate_cmd)
```

The command already follows the conventions of the other commands: dry run by
default, `--execute` to apply, `--report`/`--json` for automation, results on
stdout and logs on stderr, output bounded regardless of file count.

---

## Current limitations

Read these before trusting a verdict.

1. **Scores are not calibrated probabilities.** They are comparable within one
   run and useful for ranking; `personal_probability = 0.8` does not mean 80 %.
2. **Without the semantic model, `reject` only fires on unambiguous
   screenshots.** Documents, receipts and product shots land in `review` by
   design, because rule 3 of the decision model requires semantic evidence.
3. **Aesthetics and VLM escalation do not exist yet**, so 10 % + 25 % of the
   weight is simply unused (renormalised away) in Phase 1.
4. **Weights and thresholds are starting points**, chosen for plausibility and
   never fitted to labelled data.
5. **Videos always go to `review`.** Only the extension is inspected; no frame is
   decoded.
6. **The preference model has no data path.** It trains and scores correctly, but
   nothing populates the `feedback` table, so it can never reach its 100
   examples per class.
7. **Per-file processing.** The semantic provider accepts a batch, the pipeline
   passes one image at a time.
8. **The prompt bank is untested against a real corpus.** The 17 labels and their
   prompts are a reasonable first guess, no more.

---

## Prerequisites for semantic embeddings and VLM

The cascade already supports semantic classification, but it does not yet expose
reusable image embeddings or provide an operational VLM backend. Implement these
capabilities in the following order rather than treating the VLM as a replacement
for calibrated semantic evidence.

### 1. Define the purpose of the embeddings

Decide which downstream features consume the embedding before changing its
lifecycle:

- Zero-shot semantic labels already work and do not require persisted embeddings.
- Personal preference learning needs the embedding associated with each explicit
  user correction.
- Similarity search or duplicate detection would require a separate index,
  retrieval policy and retention policy; it should not be added implicitly as
  part of preference learning.

Keep embeddings in memory unless a concrete feature requires persistence. Never
include them in CSV or JSON reports.

### 2. Build a representative labelled evaluation set

Collect roughly 1,000-2,000 files from the target library and label each one
`keep`, `review` or `reject`. Include difficult boundary cases: screenshots of
photos, photographed documents, receipts containing people, scanned prints,
whiteboards, product photos with sentimental value and utility material that
contains a protected subject.

Split evaluation data by event, not randomly, so near-duplicates from one burst
cannot appear in both training and validation. Define the acceptable
false-reject rate for protected subjects before selecting thresholds; reducing
the review pile is secondary to that constraint.

### 3. Complete the embedding data path

The contracts already anticipate this path:

- `SemanticPrediction.embedding` exists in `providers/base.py`.
- `SigLipSemanticProvider` computes normalised image features but currently
  discards them after calculating prompt similarities.
- `SemanticStage` consumes label scores but does not pass the embedding to a
  preference provider.
- `LogisticPreferenceProvider` can train and score embeddings once they reach it.

Return each normalised image feature from `SigLipSemanticProvider`, carry it in a
private in-memory field on `CurationContext`, and let a preference stage consume
it after semantic classification. Do not put it in `StageResult.scores`, because
that mapping is for scalar signals and participates in reporting and caching.

If training requires persistence, add a dedicated cache table keyed by file
SHA-256 and the complete semantic model fingerprint. Store the embedding
dimension and serialization format with the record, and invalidate the record
whenever the model or preprocessing identity changes.

### 4. Pin the embedding model and preprocessing

The embedding space is a versioned data format. `ProviderInfo` must identify:

- model ID and immutable revision,
- preprocessing version,
- embedding dimension and normalisation method,
- prompt-bank version where classification results are involved,
- weights checksum when an immutable upstream revision is unavailable.

Never train preference weights using one embedding fingerprint and score with
another. Loading a preference model against an incompatible embedding
fingerprint must fail explicitly rather than silently returning misleading
scores.

### 5. Provide a feedback and training workflow

Add a workflow that records corrections made while reviewing results. Each
training example needs the file content hash, corrected decision, semantic model
fingerprint, embedding and an event or group identifier. The existing
`feedback` table can hold the correction metadata; embedding storage should be
separate so verdict rows and reports remain small.

`LogisticPreferenceProvider` requires at least 100 examples per class. Train
with event-grouped validation, report class counts and validation quality, and
activate the `preference` weight only when a compatible trained model is
available. Missing or stale preference weights remain an absent signal, never a
zero.

### 6. Batch semantic inference

The semantic provider accepts a sequence, but the pipeline currently passes one
image at a time. Before scaling to large libraries, collect bounded batches and
perform one encoder forward pass per batch. Preserve the existing guarantees:

- cache hits do not enter a batch,
- one corrupt image cannot fail other files,
- decoded working images remain memory-bounded,
- output order stays deterministic,
- per-file duration, reasons and failure handling remain available.

Benchmark CPU, Apple Silicon MPS and CUDA before choosing defaults. Treat batch
size as part of runtime tuning, not model identity, unless it measurably changes
answers.

### 7. Choose and pin a VLM backend

Evaluate candidate runtimes and checkpoints against licence and redistribution
terms, checkpoint size, RAM/VRAM requirements, CPU/MPS/CUDA support,
quantisation support, latency and reliability of structured JSON output.

Implement `VlmProvider.info()` and `judge(image)`. Keep `VlmStage`,
`VlmJudgement`, `PROMPT` and `parse_vlm_response()` as the trust boundary.
`judge()` should return only the validated decision, confidence, known labels
and short reasons; raw model text must not enter reports or path handling.

### 8. Keep VLM escalation narrow and conservative

Run the VLM only after ordinary fusion and initially only for results that would
otherwise be `review` and whose confidence lies inside `thresholds.vlm_band`.
Do not send files already settled by decisive metadata or a confidently fused
verdict.

The VLM prompt may include bounded upstream context such as top semantic labels,
OCR density and conflicting signal names, but not recognised OCR text. A
timeout, malformed response, unavailable model, unknown decision or internal
error must produce `review`.

Initially allow only `review -> keep` and `review -> reject` transitions. A VLM
`reject` must still satisfy the global minimum-confidence rule, protected-subject
guard and strong-signal conflict rule. The VLM must not bypass
`scoring._apply_safety_rules` merely because `VlmStage` returns a terminal
decision.

### 9. Make remote inference explicitly opt-in

Local processing remains the default. A hosted endpoint requires both
`vlm_endpoint` and `allow_remote_vlm`; document which pixels and metadata leave
the machine. Use explicit connection and inference timeouts, and never log image
data, embeddings, OCR text, full prompts or raw VLM responses.

Include endpoint model identity, revision and preprocessing in the provider
fingerprint. A generic endpoint URL is not enough to guarantee cache validity.

### 10. Calibrate before enabling either capability by default

Measure the semantic and VLM stages independently:

| Capability | Required measurements |
| --- | --- |
| Semantic classification | Per-label precision and recall, protected-subject false rejects, throughput and memory |
| Preference model | Class balance, event-grouped validation quality and calibration |
| VLM escalation | Escalation rate, accuracy of changed verdicts, false rejects, review-volume reduction and latency |

Enable preference scoring only after its validation target is met. Enable VLM
escalation by default only if it reduces manual review without violating the
protected-subject false-reject target. Recommended implementation order:

```text
labelled evaluation set
  -> return embeddings from SigLIP
  -> feedback and embedding storage
  -> preference training and calibration
  -> semantic batching
  -> local VLM provider
  -> VLM escalation evaluation
```

---

## Pending work and next steps

Ordered by what unblocks the most.

### 1. Build a labelled evaluation set (blocks everything below)

Nothing else can be judged without it. Target roughly 1,000-2,000 files from the
user's own library, hand-labelled `keep` / `review` / `reject`, stratified across
the awkward cases: photos of documents, screenshots of photos, whiteboards,
scanned prints, receipts on a wooden table, product shots that are actually
memories. Split by event, never at random, or near-duplicates from one burst will
leak between halves and inflate every number.

Then measure what actually matters: **the false-reject rate on protected
subjects, at whatever threshold gives a tolerable review volume.** A single
rejected family photo outweighs a lot of tidiness.

### 2. Calibrate weights and thresholds

With the set in place, fit the five weights and three thresholds instead of
guessing them, and fit a calibration map (Platt scaling or isotonic regression on
the semantic margins) so `personal_probability` becomes a probability. That
single change makes `confidence_for` meaningful and lets the confidence floor be
set from a target error rate rather than by feel.

### 3. Validate the semantic stage end to end

`SigLipSemanticProvider` is written but has never run against real files in this
repository's tests. Needed: a manual run over a few hundred images, per-label
precision and recall, and prompt-bank iteration (`data/prompt_bank.json` is
versioned and feeds the cache fingerprint, so revisions invalidate cleanly). Also
worth measuring: throughput on Apple Silicon MPS versus CPU, to size the batch.

### 4. Close the feedback loop

The `feedback` table and `LogisticPreferenceProvider` exist; the wiring does not.
Needed: a way for the user to correct a verdict (the obvious one is a
`filecluster curate --learn` pass that reads what actually ended up where after a
review), `put_feedback` calls on those corrections, embedding storage for the
corrected files, and a training command that reports validation accuracy. Note
`MIN_EXAMPLES_PER_CLASS = 100` per class before training will run at all.

### 5. Implement the aesthetic provider

Pick a predictor (NIMA, MUSIQ or TOPIQ), pin the checkpoint, implement
`QualityProvider`, and decide whether the extra install size belongs in the
`curation` extra or its own. The seam is ready; the decision is about licence,
size and throughput.

### 6. Implement the VLM provider

`parse_vlm_response` and the prompt are done. What remains is choosing a runtime
and a local checkpoint, implementing `judge()`, and confirming the escalation
band earns its cost: measure how many `review` files it converts and how many of
those conversions are correct before enabling it by default.

### 7. Wire it into the main CLI and the clustering flow

Two lines add the command. The more interesting integration is a `filecluster
run --curate` that curates first and then clusters only the `keep` set, sharing
one pass over the inbox. That needs a decision about where `review` sits in that
flow: a folder the user drains by hand, or a queue the next run picks up.

### 8. Housekeeping

- A `curate cache` subcommand for `stats`, `prune` and `vacuum`; the catalog
  methods exist and are tested but nothing exposes them.
- Batch the semantic stage (collect N working images, one forward pass).
- Video handling beyond the extension check: one decoded keyframe would let the
  existing pixel and semantic stages work on video too.
- Document the packaged JSON data files in the user guide once curation becomes a
  supported command rather than a subpackage.

---

## What is needed to unlock the full potential

The implementation is deliberately conservative: it will not reject what it
cannot name. So the ceiling of what it can do today is set by evidence, not by
code. Four things raise it, in order of leverage.

**1. Semantic evidence, installed and validated.** This is the single largest
unlock and needs no new code. `pip install "filecluster[curation]"` plus
`--with-semantic` moves documents, receipts, book pages, product shots and
whiteboards from `review` into `reject`, because it satisfies the "reject needs
to know what the file is" rule. Expect the review pile to shrink by most of its
volume. Cost: roughly 2 GB of dependencies and a model download.

**2. A labelled set from the user's own library.** Curation is a matter of taste,
and taste is not transferable. Without local labels the thresholds are a guess
and the confidence numbers are ordinal at best. With a few thousand labelled
files, the same code produces calibrated scores, thresholds fitted to a chosen
error rate, and a defensible statement like "at this setting, 2 % of files need
review and no protected subject was rejected in the evaluation".

**3. The feedback loop closed.** The preference model is what turns a generic
classifier into *this* user's classifier, and it carries 25 % of the weight while
contributing nothing. It only needs plumbing: capture corrections, store the
embeddings the semantic stage already computes, train when the class balance
allows. This is the difference between "not a screenshot" and "the kind of photo
you actually keep".

**4. The remaining two providers.** Aesthetics (10 %) sharpens the ranking inside
the keep set, which matters for choosing among near-duplicates from one burst.
VLM escalation earns its keep only on the residual band after the first three
items are done, and should be measured before it is trusted.

Two things do **not** need to change to reach that ceiling: the stage contracts
and the cache design. Every unlock above is a provider, a data file or a
configuration value, and all of them already participate in the fingerprint, so
verdicts invalidate correctly as the system learns.
