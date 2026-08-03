# Ubiquitous Language

## Data Collection Workflow

| Term | Definition | Aliases to avoid |
| ---- | ---------- | ----------------- |
| **Collector** | An action that fetches raw financial data from an external API or data source and writes it directly to the database | Fetcher, gatherer, crawler, API client |
| **Raw Data** | Unprocessed data written directly to the database by a **Collector** in its original form from the source | Source data, external data, ingested data |
| **Stager** | An action that transforms, validates, and processes **Raw Data** and writes it to staging tables for consumption | Transformer, processor, ETL stage |
| **Staging Table** | A database table containing processed **Raw Data** prepared by a **Stager** for downstream analysis or use | Intermediate table, transformed data |
| **Action** | A runnable unit of work that executes a **Collector** or **Stager** operation | Job, worker, operator, task unit |
| **Task** | A scheduled execution of an **Action** with a name, dependencies, and execution state | Work item, scheduled action |
| **OHLCV** | The standard set of daily market price fields: Open, High, Low, Close, and Volume | Price fields, market data fields, tick data |
| **is_staged** | A boolean flag on **Raw Data** tables that indicates whether a **Stager** has already processed the row | Processed flag, staged flag |

## Data Persistence & Conflict Handling

| Term | Definition | Aliases to avoid |
| ---- | ---------- | ----------------- |
| **Conflict Key** | A set of column(s) that uniquely identify a row and are used to detect duplicate inserts (e.g., ticker + ex_dividend_date for dividends) | Primary key, unique constraint, composite key |
| **Conflict Resolution** | The database behavior when an insert encounters a row with a matching **Conflict Key**: either skip the insert (DO NOTHING) or update the existing row (DO UPDATE) | Upsert behavior, duplicate handling |
| **DO NOTHING** | A conflict resolution strategy where duplicate inserts are silently ignored and the existing row remains unchanged | Ignore conflicts, no-op |
| **DO UPDATE** | A conflict resolution strategy where duplicate inserts overwrite the existing row's columns with incoming values | Upsert, replace, update on conflict |
| **Update Semantics** | The set of rules governing which columns are updated and how (e.g., full replace, selective fields, NULL-safe merge) | Update rules, merge behavior |

## Task Scheduling & Execution

| Term | Definition | Aliases to avoid |
| ---- | ---------- | ----------------- |
| **Task Dependency** | A declared requirement that a **Task** must complete successfully before another **Task** can begin execution | Prerequisite, parent task, upstream task |
| **DAG** (Directed Acyclic Graph) | The scheduling model that represents **Tasks** as nodes and **Task Dependencies** as edges, ensuring no circular dependencies exist | Dependency graph, execution graph |
| **Schedule** | The system that manages the DAG, validates dependencies, and coordinates task execution | Orchestrator, scheduler engine |
| **Execution State** | The current lifecycle state of a **Task**: Pending, Running, Finished, Failed, or Cancelled | Status, phase, state |
| **Execution Mode** | How a **Task** runs: Once, Continuously (with kill signal), RepeatLimited (fixed count), or RepeatForDuration (time-bounded) | Run type, repetition strategy |
| **Runnable** | The trait that **Actions** implement to define how a unit of work executes asynchronously | Executable, operation |

## Data Sources

| Term | Definition | Aliases to avoid |
| ---- | ---------- | ----------------- |
| **NYSE Events** | Raw tick/event data collected from the New York Stock Exchange | Stock exchange data, market events |
| **NYSE Instruments** | Reference data about listed securities and instruments on the NYSE, staged from raw data | Instrument master, security reference |
| **SEC Companies** | Corporate filing and registration data from the Securities and Exchange Commission, collected and staged | Company filings, regulatory data |
| **Polygon** | External data source providing stock market, dividend, and daily OHLCV data via API | Market data provider |
| **Financial Modeling Prep** | External data source providing company profile, market capitalization, and financial metrics via API | FinMod, financial data provider |
| **Xfinlink** | External data source providing daily **OHLCV**, market cap, and related price fields via API with hourly rate limiting | Financial data provider |
| **Dividend** | A distribution of cash or stock to shareholders, collected from Polygon with ticker, ex-dividend date, and payment details | Dividend payment, corporate distribution |

## Symbol Management

| Term | Definition | Aliases to avoid |
| ---- | ---------- | ----------------- |
| **Symbol** | The canonical identifier for a financial instrument (e.g. `AAPL`) as used in the database and within the system | Ticker (in DB context), issue symbol, stock symbol |
| **Source Symbol Warden** | The `source_symbol_warden` database table that tracks per-source availability of each **Symbol**, preventing redundant requests | Symbol exclusion list, skip list |
| **Unavailable Symbol** | A **Symbol** that a data source has confirmed does not exist or cannot be resolved, recorded in the **Source Symbol Warden** | Missing symbol, unresolvable ticker |
| **Warden Cutoff** | The duration after which an **Unavailable Symbol** is eligible for re-checking (currently 30 days for Polygon Dividends and Xfinlink) | Re-check period, exclusion duration |
| **Symbol Selection Priority** | The 3-tier ordered strategy for selecting which **Symbol** to collect next: (1) symbols with clean IPO date and no data, (2) symbols without clean IPO date and no data, (3) symbols with stale data | Collection order, symbol queue |
| **Start Date** | The computed earliest date for which to request data for a given **Symbol**: the maximum of (last collected date + 1 day) and (today − API history limit) | Fetch start, collection start, resume date |
| **Cursor Pagination** | An API pagination mechanism where the server returns a `next_cursor` token and a `has_more` flag; the client passes the token back to retrieve the next page | Page-based pagination, offset pagination |

## Configuration & Management

| Term | Definition | Aliases to avoid |
| ---- | ---------- | ----------------- |
| **TaskSetting** | Configuration entry defining a **Task** name, **Action** type, and optional filters (fields, sources) from configuration YAML | Task config, task definition |
| **TaskDependency** | Configuration entry specifying a **Task** name and its required prerequisite **Task** names from configuration YAML | Dependency declaration, task prerequisite |
| **Collector Source** | A filter attribute on a **Task** to include or exclude data from specific market sources (e.g., NYSE, Polygon, SEC) | Source filter, data provider filter |
| **Secret Keys** | API credentials and authentication tokens loaded from configuration for external data sources | API keys, authentication, credentials |

## API Key Management & Rate Limiting

| Term | Definition | Aliases to avoid |
| ---- | ---------- | ----------------- |
| **API Key** | A credential that authorizes requests to an external data source API; managed by the **KeyManager** for rate-limiting | API credential, authentication token, secret |
| **KeyManager** | The system component that manages a pool of **API Keys**, tracks usage counters, and enforces rate limits | Key pool, credential manager |
| **ApiKeyPlatform** | An enumeration of data source types (e.g., Polygon, FinancialModelingPrep, Xfinlink) used to organize **API Keys** by provider | Provider type, key type |
| **Rate Limit** | A constraint on API request frequency (e.g., 40 requests per hour for **Xfinlink**) enforced by **KeyManager** to prevent quota exhaustion | Request quota, throttle, frequency limit |
| **Exhaustion** | The state of an **API Key** after reaching its **Rate Limit**, triggering a transition to Status::Exhausted | Quota reached, rate limit hit, throttled |
| **Refresh** | The process of resetting an **API Key**'s usage counter when its rate-limit window expires (e.g., at calendar hour boundary for **Xfinlink**) | Reset, recover, unthrottle |
| **Calendar Hour** | The rate-limit window for **Xfinlink** keys: a one-hour period aligned to clock hours (00:00–00:59, 01:00–01:59, etc.) | Hourly window, rate-limit period |
| **Usage Counter** | A numeric field on an **API Key** tracking the number of requests made in the current **Rate Limit** window | Request count, quota usage |

## Relationships

- A **Task** has exactly one **Action** that it executes
- A **Task** may have zero or more **Task Dependencies** that must complete before it runs
- A **Collector** writes **Raw Data** to the database
- A **Stager** reads **Raw Data** and writes **Staging Tables**
- The **Schedule** enforces all **Task Dependencies** by building and validating the **DAG** before execution
- A **TaskSetting** defines what **Collector** or **Stager** a **Task** runs
- A **TaskDependency** defines when a **Task** is eligible to run
- A **Dividend** record is identified by its **Conflict Key** (ticker, ex_dividend_date) to detect duplicates on insert
- When a **Dividend** insert conflicts with an existing row, **DO UPDATE** overwrites the row per **Update Semantics**
- The **KeyManager** manages **API Keys** organized by **ApiKeyPlatform** (Polygon, FinancialModelingPrep, Xfinlink)
- Each **API Key** has a **Usage Counter** and **Rate Limit** enforced by the **KeyManager**
- An **API Key** transitions to **Exhaustion** when its **Usage Counter** reaches the **Rate Limit**
- An **API Key** transitions back to Ready when its rate-limit window **Refresh**es (e.g., at the next **Calendar Hour** boundary for **Xfinlink**)
- A **Collector** requests an **API Key** from the **KeyManager** by **ApiKeyPlatform** to make API requests to external data sources
- A **Collector** consults the **Source Symbol Warden** before processing a **Symbol** to skip **Unavailable Symbols** within the **Warden Cutoff** window
- When a **Symbol** is confirmed absent from a data source, the **Collector** records today's date in the **Source Symbol Warden**; after the **Warden Cutoff** the **Symbol** becomes eligible for re-checking
- A **Collector** uses **Symbol Selection Priority** to select the next **Symbol**, and computes a **Start Date** to avoid re-fetching already-collected data
- **Cursor Pagination** is used when a data source returns `has_more: true`; the **Collector** follows each `next_cursor` until `has_more: false`
- **Raw Data** tables carry an **is_staged** flag set to `false` on insert; a **Stager** sets it to `true` after processing

## Flagged Ambiguities

- **"Task" vs "Action"**: Historically these terms may be conflated, but they are distinct:
  - An **Action** is the reusable definition of work (e.g., NyseEventCollector, NyseInstrumentStager)
  - A **Task** is a scheduled execution of an **Action** with name, state, and dependencies
  - Recommendation: Always say "**Task** runs an **Action**" to clarify the boundary

- **Raw Data vs Staging Tables**: Both are database artifacts but serve different purposes:
  - **Raw Data** is written by a **Collector** and is source-system-native format
  - **Staging Tables** are written by a **Stager**, transformed, and ready for consumption
  - Recommendation: Reference the source (**Collector** vs **Stager**) when disambiguating

- **Conflict Resolution Strategies**: DO NOTHING vs DO UPDATE represent fundamentally different data governance policies:
  - **DO NOTHING** treats incoming data as potentially stale and preserves the current DB state
  - **DO UPDATE** treats incoming data as fresh and overwrites the current state
  - Recommendation: Clearly document which strategy applies to each data source (**Dividend** uses DO UPDATE)

- **Rate Limit Window Boundaries**: Different **APIs** use different rate-limit windows:
  - **Polygon** uses a 1-minute rolling window: **Exhaustion** lasts 1 minute, then **Refresh**
  - **FinancialModelingPrep** uses a calendar day with reset at 19:00 UTC: **Exhaustion** lasts until the next calendar day at 19:00
  - **Xfinlink** uses a **Calendar Hour** boundary: **Exhaustion** lasts until the next hour boundary (e.g., 14:00–14:59 window, reset at 15:00)
  - Recommendation: Document the **Rate Limit** window and **Refresh** behavior explicitly when adding a new **API Key** type or **ApiKeyPlatform**

- **"symbol" vs "ticker"**: The API response field from Xfinlink is named `ticker`; the database column is named `symbol`:
  - **Symbol** is the canonical in-system term (used in DB columns, internal code, and config)
  - **ticker** is the external API field name and should only appear in deserialization structs that map from the API response
  - Recommendation: Always use **Symbol** in domain discussions, DB schemas, and internal code; reserve "ticker" strictly for API response struct field names

## Example dialogue

> **Dev:** "We need a new **Collector** for Xfinlink market cap data. Where do I start?"

> **Domain Expert:** "First check the **Source Symbol Warden** — it tells you which **Symbols** are **Unavailable** so you skip them. Then use **Symbol Selection Priority** to pick the next **Symbol**: start with symbols that have a clean IPO date and no data yet, then move to the low-priority ones, then check for stale data."

> **Dev:** "How far back do I request data for each **Symbol**?"

> **Domain Expert:** "Compute the **Start Date**: take the most recent **business_date** in the database for that **Symbol**, add one day, then compare it to today minus one year. Use whichever is later. Xfinlink only keeps one year of history, so there's no point requesting further back."

> **Dev:** "What if the API returns `has_more: true`?"

> **Domain Expert:** "That's **Cursor Pagination**. Keep requesting with the `next_cursor` token until `has_more` is false. In practice it won't trigger for Xfinlink because a year of daily data is under 365 rows — well within one page — but the loop must be there for correctness."

> **Dev:** "What if the **Symbol** doesn't exist in Xfinlink at all?"

> **Domain Expert:** "Check `meta.tickers_unresolved` in the response. If the **Symbol** appears there, record today's date in the **Source Symbol Warden**. The **Warden Cutoff** is 30 days — after that, the **Symbol** is eligible for re-checking. Use **DO NOTHING** on inserts so re-runs don't overwrite existing **Raw Data**."

> **Dev:** "One last thing — when I store the data, should the column be called `ticker` to match the API?"

> **Domain Expert:** "No. Inside the system, always use **Symbol**. The word 'ticker' only belongs in the deserialization struct that maps the raw API JSON. Once data enters the database or any internal logic, it's a **Symbol**."

