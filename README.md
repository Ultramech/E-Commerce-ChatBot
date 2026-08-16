# 🛒 Saarthi — Flipkart Electronics Chatbot

> *Saarthi* (सारथी) means "guide / charioteer" — your guide to Flipkart electronics.
> A grounded RAG + Text-to-SQL assistant with an Airflow-orchestrated ETL pipeline.

[![CI](https://github.com/Ultramech/E-Commerce-ChatBot/actions/workflows/ci.yml/badge.svg)](https://github.com/Ultramech/E-Commerce-ChatBot/actions/workflows/ci.yml)
[![Daily data refresh](https://github.com/Ultramech/E-Commerce-ChatBot/actions/workflows/refresh-data.yml/badge.svg)](https://github.com/Ultramech/E-Commerce-ChatBot/actions/workflows/refresh-data.yml)

A grounded shopping assistant for an Indian electronics catalog (mobiles,
laptops, headphones, smartwatches, televisions, tablets, earbuds). Product data
is scraped from Flipkart by an **Airflow-orchestrated ETL pipeline**, and the
chatbot answers every question from a **real data source** — never from the
model's imagination:

- **FAQ** → retrieval-augmented answers over a store-policy knowledge base
- **SQL** → live text-to-SQL queries against the product catalog
- **Small-talk** → short, on-brand chit-chat

The two themes of this project are **measurably reducing LLM hallucination** and
**a reproducible data pipeline**:
cross-encoder reranking + corrective-RAG gating, self-correcting SQL, a router
confidence gate that asks clarifying questions, an **evaluation harness** with an
LLM-as-judge faithfulness metric, and an **ETL pipeline** (Airflow) that keeps
the catalog fresh.

---

## Data pipeline (ETL, orchestrated by Airflow)

```mermaid
flowchart LR
    F[(Flipkart<br/>7 categories)] -->|scrape| E[extract<br/>etl/scrape.py]
    E --> RAW[(CSV snapshot)]
    RAW --> T[transform<br/>brand, types]
    T --> V[validate<br/>data quality]
    V --> L[load]
    L --> DB[(SQLite catalog<br/>~14.6k products)]

    subgraph Airflow DAG
      direction LR
      S[scrape_flipkart] --> B[build_catalog]
    end
```

The real work lives in the framework-agnostic `etl/` package, so the pipeline is
unit-testable and runs **with or without Airflow**. Airflow just schedules it and
gives a visual task graph. The scrape is snapshotted to a CSV so CI and the app
never depend on a live scrape.

### Scrape coverage

Page depth is a CLI argument, so the same scraper serves both refresh tiers. Every
run issues **7 category searches × `--pages`**, plus **26 brand-targeted searches
× 3 pages = 78 pages** (`BRAND_QUERIES` in `etl/scrape.py`) — the generic category
search is dominated by cheap listings, so flagship brands would otherwise never
appear in the catalog.

| Run | `--pages` | Pages requested | Returned data | Source |
| --- | --- | --- | --- | --- |
| Daily shallow | 5 | **113** | **59** (54 × HTTP 403) | measured, CI run 2026-08-15 |
| Weekly deep | 60 | ≤ 498 | — | not yet executed |
| Manual / local | 20 | ≤ 218 | 0 — every request 403s | measured from a residential IP |

**Flipkart blocks roughly half the requests.** In the measured daily run all 33
searches were attempted and all 113 pages were requested, but only 59 returned
HTML — the other 54 came back `403 Forbidden`. That is not a failure mode, it is
the normal operating condition, and the pipeline is built around it: a failed
request is logged and skipped (`_collect` continues rather than aborting), the
`--min-rows` guard discards a run that came back too thin, and the upsert means a
page missed today is simply picked up tomorrow. That run still refreshed the
snapshot to **14,616 products**.

Note that page depth, not exhaustion, is the binding constraint at the daily
setting: `_collect` breaks early only when a page yields no *new* products, and at
5 pages deep no search ever gets that far. Early exit only starts to matter at the
weekly depth, which has not run yet — so 498 remains a bound, not a measurement.
Scraping from a residential IP is blocked outright, so use the committed snapshot
for local work.

### Catalog schema — one table, seven views

The loader writes a **single `product` table** (24 columns, `product_link` as
primary key) and generates **seven read-only views**, one per category, each a
filtered column-subset of that table (`etl/load.py`):

```sql
CREATE VIEW laptops AS
  SELECT product_link, title, brand, rank, price, discount, avg_rating,
         total_ratings, processor, ram_gb, storage_gb, storage_type,
         screen_inch, os
  FROM product WHERE category = 'laptops';
```

Views split the *interface* without splitting the *storage*:

- **Narrower text-to-SQL prompts.** A laptop question shows the model
  `table: laptops` with 14 relevant columns — never `anc` or `panel_type`, which
  are NULL for every laptop. Fewer irrelevant columns means fewer invented
  predicates and fewer self-correction retries.
- **One upsert identity.** The daily refresh relies on
  `ON CONFLICT(product_link) DO UPDATE`. Across seven physical tables, a product
  Flipkart recategorizes would exist as two live rows with no constraint to catch it.
- **Cross-category browsing stays trivial.** "Just browsing" targets `product`
  directly instead of a seven-way `UNION ALL`.
- **No duplication, no drift.** A view is a stored query, so there is nothing to
  sync; `_create_views` regenerates all seven from one column spec on every load.

The tradeoff is a NULL-sparse table (24 columns is the union of all seven spec
sets) and no per-category constraints — both negligible at this scale, with
validation handled in `etl/validate.py` instead.

## Chatbot architecture

```mermaid
flowchart TD
    U([User message]) --> R{Semantic router<br/>cosine similarity}
    R -->|confidence &lt; 0.35| C[Ask a clarifying question]
    R -->|faq| FAQ[FAQ RAG pipeline]
    R -->|sql| SQL[Text-to-SQL pipeline]
    R -->|small-talk| ST[Small-talk]

    M[(Window + running summary memory)] -.context.-> FAQ
    M -.context.-> SQL
    M -.context.-> ST

    FAQ --> A([Grounded answer])
    SQL --> A
    ST --> A
    C --> A
```

### FAQ pipeline — retrieve → rerank → corrective-RAG gate

```mermaid
flowchart LR
    Q[Query] --> E[Embed + ChromaDB<br/>top-5 candidates]
    E --> RR[Cross-encoder rerank<br/>ms-marco MiniLM]
    RR --> G{Top score<br/>&ge; threshold?}
    G -->|no| IDK["Refuse: 'I'm not sure'<br/>(no hallucination)"]
    G -->|yes| L[LLM answers from<br/>top-2 contexts only]
    L --> O[Answer]
```

### SQL pipeline — generate → validate → self-correct

```mermaid
flowchart TD
    Q[Question] --> GEN[LLM generates SQL]
    GEN --> V{SELECT-only<br/>& executes?}
    V -->|error| FIX[Feed error back<br/>retry once]
    FIX --> GEN
    V -->|ok| RES{Rows returned?}
    RES -->|none| NF["'No products found'<br/>(no invented items)"]
    RES -->|yes| NL["Render rows deterministically<br/>(prices straight from DB,<br/>no LLM in the number path)"]
    NL --> O[Answer]
```

> **Prices are guaranteed correct.** The result list is formatted in Python
> directly from the query rows, so every price/discount/rating shown is exactly
> what's in the database — the LLM never transcribes a number. This also drops an
> LLM call, so SQL answers are faster and cheaper.

---

## Why this is more than a basic chatbot

| Problem in the original POC | Fix in this version |
| --- | --- |
| One product category, notebook-loaded | **7-category ETL pipeline** scraping ~14.6k live Flipkart products |
| No orchestration | **Airflow DAG** (scrape → build) with the logic in a testable `etl/` package |
| Always answered FAQs even when retrieval was irrelevant | Cross-encoder rerank + **corrective-RAG gate** (refuse below threshold) |
| SQL chain invented products on empty results | Explicit "no products found" + **SELECT-only** validation |
| One bad query = dead end | **Self-correcting** retry that feeds the SQL error back to the model |
| Router guessed on ambiguous input | **Confidence gate** → asks a clarifying question |
| No conversation context | **Memory layer** (window + running summary) for follow-ups |
| No way to know if it works | **Eval harness**: routing accuracy, LLM-as-judge faithfulness, SQL success |
| Hardcoded API key in source | Env-based config + `.env.example`, key never committed |
| No tests / CI / deploy | **pytest + ruff + GitHub Actions**, deployed on **Streamlit Cloud** with a **daily auto-refresh** |

---

## Metrics

Run the offline evaluation against the labeled golden set:

```bash
python eval/evaluate.py     # writes eval/reports/report.json and metrics.png
```

- **Routing accuracy** — predicted intent vs. labeled intent
- **Faithfulness** — LLM-as-judge: is the answer supported by the retrieved context?
- **Grounding rate** — keyword check that expected facts appear
- **SQL success rate** — product queries that execute and return usable rows
- **Avg latency** — per-query wall-clock time

---

## Tech stack

`Streamlit` · `Groq (gpt-oss-120b)` · `ChromaDB` · `sentence-transformers`
(bi-encoder retrieval + cross-encoder rerank) · `SQLite` · `Flipkart scraping
(requests + BeautifulSoup)` · `Apache Airflow` · `pytest` · `ruff`
· `GitHub Actions (CI + scheduled refresh)` · `Streamlit Community Cloud`

## Project structure

```
app/        chatbot: router, faq, sql, smalltalk, memory, llm, config
etl/        scrape.py + extract/transform/validate/load + pipeline.py
airflow/    dags/ + README (run with `airflow standalone`, no Docker)
eval/       golden_dataset.json + evaluate.py (metrics + charts)
tests/      unit + smoke tests (no API key needed)
.github/    CI workflow + daily data-refresh workflow
```

---

## Setup & run

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Add credentials — copy the template and fill in your Groq key:
   ```bash
   cp .env.example app/.env   # then edit app/.env
   ```
   Get a free key at <https://console.groq.com/keys>.
3. Run the app (it builds the catalog DB from the committed snapshot on first run):
   ```bash
   streamlit run app/main.py
   ```
   To refresh the snapshot from Flipkart first:
   `pip install -r requirements-dev.txt && python -m etl.scrape --pages 20`

### Run the tests

```bash
pip install -r requirements-dev.txt
pytest -q
```

### Run the ETL in Airflow (local, no Docker)

See [`airflow/README.md`](airflow/README.md) — `pip install apache-airflow` then
`airflow standalone`.

---

## Deployment & daily refresh

**Deploy (Streamlit Community Cloud):** point it at this repo with
`app/main.py` as the entry file, and add `GROQ_API_KEY` (and optionally
`GROQ_MODEL`) under the app's **Secrets**. The app builds its own database on
startup, so there is no separate build step. Every push to `main` auto-redeploys.

**Daily refresh (tiered + idempotent):** a **daily shallow** scrape at 00:00 IST
([`refresh-data.yml`](.github/workflows/refresh-data.yml), `--pages 5` — the top
pages where prices and ratings actually move) and a **weekly deep** scrape
([`refresh-data-weekly.yml`](.github/workflows/refresh-data-weekly.yml),
`--pages 60` — to pick up genuinely new products); see
[Scrape coverage](#scrape-coverage) for what those page counts expand to. Both
**upsert by `product_link`** — re-scraped items update in place,
new items are added, and identical data produces a byte-identical CSV, so a
commit (and redeploy) only happens when something actually changed. If a scrape
is throttled, the last good snapshot is kept.

```mermaid
flowchart LR
    CRON[GitHub Actions<br/>daily cron] -->|scrape| SNAP[(updated snapshot)]
    SNAP -->|commit + push| GH[(GitHub main)]
    GH -->|auto-redeploy| APP[Streamlit Cloud app]
```

The Airflow DAG schedules the same pipeline and is the orchestration showcase;
the cron is what actually runs daily without an always-on server.
