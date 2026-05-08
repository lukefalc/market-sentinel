# market-sentinel Runbook

This runbook is the simple day-to-day guide for running `market-sentinel`.

## What The Project Currently Does

`market-sentinel` is a local Python project that helps analyse FTSE 350 and
S&P 500 stocks.

It currently can:

- Load stock universe CSV files.
- Update the FTSE 350 and S&P 500 universe CSVs from Wikipedia.
- Store data in a local DuckDB database.
- Download daily price data with `yfinance`.
- Calculate simple moving averages.
- Detect moving average crossover signals.
- Analyse dividends.
- Add simple dividend risk flags.
- Generate Excel and PDF reports.
- Optionally send a plain-text daily email alert summary.

It does not currently send iPhone notifications.

## Open Terminal And Go To The Project Folder

Open Terminal on your Mac:

1. Press `Command + Space`.
2. Type `Terminal`.
3. Press `Return`.

Then go to the project folder:

```bash
cd ~/Documents/Codex/2026-05-03/create-a-beginner-friendly-python-project
cd market-sentinel
```

If Terminal says the folder does not exist, open Finder and check where the
project folder is saved.

## Activate The Virtual Environment

From inside the project folder, run:

```bash
source .venv/bin/activate
```

When it works, your Terminal prompt usually starts with `(.venv)`.

If `.venv` does not exist yet, create it and install the project:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Run Tests

Make sure the virtual environment is active, then run:

```bash
PYTHONPATH=src python3 -m pytest
```

If you see `No module named pytest`, the virtual environment is probably not
active or the development dependencies have not been installed. Run:

```bash
source .venv/bin/activate
pip install -e ".[dev]"
```

Then try the test command again.

## Daily Button

The usual daily command is:

```bash
PYTHONPATH=src python3 scripts/run_daily_process.py
```

This runs the lightweight daily steps in order:

1. Load universe CSV files.
2. Update market data incrementally.
3. Check data health.
4. Calculate moving averages incrementally.
5. Detect crossovers.
6. Calculate dividend risk flags.
7. Generate charts.
8. Generate the PDF report.
9. Generate the Excel report.

The daily process skips dividend history downloads by default. This keeps
the normal daily run focused on recent prices, moving averages, crossovers, and
reports. Risk flags still use the latest dividend metrics already stored in the
database.

The older fast daily command is still available if needed:

```bash
PYTHONPATH=src python3 scripts/run_daily_fast.py
```

This normal daily process also skips dividend downloads by default. To force a
one-off dividend refresh during the daily process, run:

```bash
PYTHONPATH=src python3 scripts/run_daily_process.py --include-dividends
```

To check report readiness without running the full daily process, run:

```bash
PYTHONPATH=src python3 scripts/check_data_health.py
```

The health check shows total securities by market, missing price histories,
stale prices, short price histories, and missing moving-average data. It prints
`Data health: OK`, `Data health: Warning`, or `Data health: Action needed`.
Warnings do not stop report generation; they make data gaps visible in the
console, PDF first page, and Excel `Summary` sheet.

## Weekly Button

Run weekly maintenance when you want the fuller weekly refresh:

```bash
PYTHONPATH=src python3 scripts/run_weekly_maintenance.py
```

This is the preferred weekly command. It is a wrapper around the existing
working weekly full process, so it runs the same workflow as:

PYTHONPATH=src python3 scripts/run_weekly_full.py
```

Use weekly maintenance once a week when you want to refresh the slower weekly
steps, including dividend metrics, reports, and the optional email alert. The
script prints timing logs around each step and a final weekly summary.

If data health shows `Warning`, read the counts in the console or Excel
`Summary` sheet. Stale prices may clear on the next market-data update. Short
price histories usually mean a ticker needs more backfill time or has limited
available history. If data health shows `Action needed`, look first for tickers
with no price history or failed update rows.

The main scripts print a start time, finish time, and elapsed seconds. Use these
timing logs to see where runtime is being spent before changing any settings or
optimising further.

The PDF report is the main daily report to read. It is chart-led: each selected
stock gets one landscape page with a trend chart and a short explanation of why
that stock was selected. The first page is a compact index of the stocks
included in the chart report. The index, chart page title, and candidate card
show each stock's market marker, such as `S&P 500` or `FTSE 350`.

The PDF selects stocks with moving-average crossovers from the past 7 days,
up to the configured chart limit. The Excel workbook keeps the fuller crossover
history and detailed data tables for deeper checking.

Charts can also be generated separately when you want the supporting picture
files:

```bash
PYTHONPATH=src python3 scripts/generate_charts.py
```

If you have changed chart settings or want to ignore existing cached PNG files,
force regeneration:

```bash
PYTHONPATH=src python3 scripts/generate_charts.py --force
```

By default, charts are kept simple: they show close price as a thin black dotted
line plus the 7-day, 30-day, and 50-day moving averages over the latest 180
stored price rows.
The project limits chart generation to 50 tickers, prioritised by the most
recent crossover date. Chart image titles include the same market marker.
Charts load the latest configured number of stored price rows by security ID,
so FTSE 350 tickers ending in `.L` use their own database rows rather than any
ticker text inference. If a ticker has fewer rows than `chart_lookback_days`,
chart generation prints a warning and shows all available rows.
The chart SMA overlays are calculated directly from the loaded chart prices so
the 7-day, 30-day, and 50-day lines span the visible chart window. The stored
`moving_average_signals` table is still used for crossovers, reports, and
candidate grades.

Market data has three modes.

Incremental mode is for normal daily runs. For each ticker, it checks the latest
stored price date first. Tickers that already look current are skipped, stale
tickers are downloaded from the latest stored date minus a small overlap buffer,
and tickers with no price history use the normal full-history download:

```bash
PYTHONPATH=src python3 scripts/update_market_data.py
```

Daily lookback mode remains available in the lower-level price loader, but the
daily process and `scripts/update_market_data.py` use incremental mode.

Backfill mode is for first-time setup or occasional historical refreshes. It
downloads a larger historical period and can take much longer:

```bash
PYTHONPATH=src python3 scripts/backfill_market_data.py
```

To backfill FTSE 350 only, use the dedicated market filter:

```bash
PYTHONPATH=src python3 scripts/backfill_market_data.py --market "FTSE 350"
```

The fast daily process uses incremental mode only. It does not run the slower
backfill and does not update dividend history unless explicitly enabled.

Both modes process tickers in yfinance batches. This is normal and helps the
project handle large universes like the S&P 500 without looking stuck.

By default, each batch has 20 tickers, incremental mode overlaps the latest
stored price date by 5 days, tickers are considered current when their latest
stored price date is today, the latest expected market date, or within a small
3-day stale threshold, historical backfill mode downloads at least 2 years, and
the project pauses 3 seconds between batches.

Failed price updates are saved here:

```text
outputs/failed_price_updates.csv
```

To change this, edit `config/settings.yaml`:

```yaml
price_download_batch_size: 20
price_backfill_period: 5y
historical_backfill_years: 2
price_daily_lookback_days: 10
price_update_overlap_days: 5
skip_price_update_if_latest_date_is_today: true
price_update_stale_after_days: 3
price_download_pause_seconds: 3
run_dividends_in_daily_fast: false
run_dividends_in_daily_process: false
```

During the update you should see:

- Total tickers checked.
- Tickers skipped as current.
- Tickers updated incrementally.
- Tickers needing full download.
- Rows written for each download batch.
- Failed tickers.
- A final summary.

Dividend downloads are also processed in batches. By default, each dividend
batch has 20 tickers, failed tickers are retried once in groups of 5, and the
project pauses 3 seconds between batches.

Failed dividend updates are saved here:

```text
outputs/failed_dividend_updates.csv
```

Dividend batch settings live in `config/settings.yaml`:

```yaml
dividend_download_batch_size: 20
dividend_download_pause_seconds: 3
dividend_retry_batch_size: 5
```

Moving average calculation also shows ticker-by-ticker progress. The normal
daily script now runs in incremental mode, so it only recalculates recent or
missing SMA rows instead of rebuilding the full moving-average history each day.
It still loads enough recent price history to calculate longer averages such as
the 200-day SMA.

The setting lives in `config/settings.yaml`:

```yaml
moving_average_history_days: 260
moving_average_incremental_recent_days: 10
moving_average_price_history_buffer_days: 230
```

Use the daily incremental calculation during normal runs:

```bash
PYTHONPATH=src python3 scripts/calculate_moving_averages.py
```

Use the full backfill script only when you deliberately want to rebuild stored
moving-average history:

```bash
PYTHONPATH=src python3 scripts/backfill_moving_averages.py
```

## Update The FTSE 350 And S&P 500 Universes

The FTSE 350 and S&P 500 universe CSVs can be refreshed from Wikipedia. The
FTSE 350 updater builds the UK universe from FTSE 100 plus FTSE 250
constituent tables, then deduplicates tickers. FTSE 350 is the main UK
universe. If `ftse_100.csv` still exists, it is superseded by `ftse_350.csv`
and is not loaded by the normal universe loader.

First update the FTSE 350 and S&P 500 CSVs:

```bash
PYTHONPATH=src python3 scripts/update_ftse350_universe.py
PYTHONPATH=src python3 scripts/update_sp500_universe.py
```

Then load the universe into the local database:

```bash
PYTHONPATH=src python3 scripts/load_universe.py
```

Then run the daily process:

```bash
PYTHONPATH=src python3 scripts/run_daily_process.py
```

This only updates the universe CSV files. It does not download prices by itself.

To prepare FTSE 350 after loading the universe, run this one-off historical
sequence:

```bash
PYTHONPATH=src python3 scripts/update_ftse350_universe.py
PYTHONPATH=src python3 scripts/load_universe.py
PYTHONPATH=src python3 scripts/backfill_market_data.py --market "FTSE 350"
PYTHONPATH=src python3 scripts/backfill_moving_averages.py
PYTHONPATH=src python3 scripts/detect_crossovers.py
PYTHONPATH=src python3 scripts/generate_charts.py --force
PYTHONPATH=src python3 scripts/generate_pdf_report.py
PYTHONPATH=src python3 scripts/generate_excel_report.py
```

## Where The Database Is Saved

The DuckDB database location is controlled by `database_path` in
`config/settings.yaml`.

The recommended shared OneDrive location is:

```yaml
database_path: ~/Library/CloudStorage/OneDrive-Personal/Finance/MarketSentinel/Data/market_sentinel.duckdb
```

The `~` is expanded automatically to your Mac home folder. When the project
opens the database, it creates the parent `Data` folder if it does not already
exist.

To use the same Market Sentinel database across Macs:

1. Install and sign in to OneDrive on each Mac.
2. Make sure this folder is available in OneDrive:

```text
~/Library/CloudStorage/OneDrive-Personal/Finance/MarketSentinel/Data
```

3. Clone the code from GitHub on each Mac.
4. Keep `config/settings.yaml` pointing at the OneDrive `database_path`.
5. Run the database initialisation or daily process from only one Mac at a time.

DuckDB is a single-file database, so avoid running Market Sentinel against the
same OneDrive database concurrently on multiple devices. Let OneDrive finish
syncing before switching devices.

If `database_path` is missing from `config/settings.yaml`, the project falls
back to the local development database:

```text
data/market_sentinel.duckdb
```

## Where Reports Are Saved

Excel reports are saved here:

```text
~/Library/CloudStorage/OneDrive-Personal/Finance/MarketSentinel/Excel
```

The Excel workbook is the detailed decision workbook. It keeps the existing
data tabs and also includes:

- `Summary`: a Daily Action Summary and Data Health section with candidate
  counts by setup grade,
  portfolio status, market, dividend risk flags, highest score, position-size
  review checks, review-priority counts, missing/stale data counts, and report
  readiness status.
- `Trade Candidates`: all recent crossover candidates, including every action
  grade, sorted from strong buy setups through strong sell setups. Use Trade
  Candidates for daily review. Use Trade Journal for permanent decisions and
  outcomes.
- `Position Sizing`: a simple editable planning calculator with example values
  for trading capital, risk per trade, entry price, and stop price. It uses
  basic formulas for maximum risk, risk per unit/share/point, and suggested
  position size.
- `Trade Journal`: a blank table for recording review decisions, planned entry
  and stop levels, whether a trade was taken, exit details, result, and notes.

The position sizing sheet is a planning calculator, not financial advice. The
example values are placeholders to edit before using the workbook for review.
Trade Candidates also includes automated position sizing columns. The planning
entry defaults to latest close, the planning stop defaults to the 20-day
reference level with a 50-day SMA fallback, and suggested size uses the
configured example capital and risk percentage. Position sizing is a planning
calculation only. It does not account for fees, slippage, taxes, liquidity, or
personal circumstances.
The generated workbook can be refreshed each run, so keep lasting review notes,
decisions, and outcomes in `Trade Journal`.
Ticker-level workbook sheets include a `Market` column so S&P 500 and FTSE 350
stocks can be filtered and reviewed separately.
Trade candidate rows also include a `Portfolio Status` marker so you can see
whether a ticker is already held, on the watchlist, both, or a new candidate.
Reports use that marker to make review order practical: held names are shown
first, then watchlist names, then new candidates. The Excel `Summary` sheet also
shows counts for held, watchlist, new, S&P 500, and FTSE 350 candidates.

Optional local portfolio files live here:

```text
config/portfolio/holdings.csv
config/portfolio/watchlist.csv
```

The holdings file uses:

```text
ticker,name,market,quantity,average_cost,notes
```

The watchlist file uses:

```text
ticker,name,market,reason,notes
```

If either file is missing, Market Sentinel treats it as empty and continues.
PDF and Excel reports show only compact portfolio markers such as `Held`,
`Watchlist`, `Held + Watchlist`, or `New`; PDF cards include quantity when it is
present.

PDF reports are saved here:

```text
~/Library/CloudStorage/OneDrive-Personal/Finance/MarketSentinel/PDF
```

Each PDF starts with an index page, then keeps one chart per landscape page.
Under each chart is a compact trade candidate review card. The card explains
why the stock was selected, the crossover signal and date, latest close price,
suggested review levels, market/index, and basic risk notes.
Stock pages use a compact one-page layout: a short header, a large landscape
chart, and a wrapped setup card underneath. The card keeps setup grade, score,
market marker, signal details, why/caution text, planning reference levels, and
one key risk note on the same page.

These card levels are planning references, not trading instructions. They are
controlled in `config/settings.yaml`:

```yaml
candidate_stop_short_window_days: 20
candidate_trailing_stop_percent: 20
candidate_include_50_sma_stop: true
candidate_include_20_day_extreme_stop: true
candidate_include_trailing_reference: true
candidate_grade_stop_distance_warning_percent: 12
candidate_recent_strong_days: 2
pdf_max_charts_total: 50
pdf_max_charts_per_market: 25
chart_show_crossover_marker: false
chart_show_20_day_reference: false
chart_show_possible_flag_pattern: true
chart_include_sma_periods:
  - 7
  - 30
  - 50
chart_show_200_day_sma: false
flag_prior_trend_days: 45
flag_consolidation_days: 20
flag_min_prior_move_percent: 8
flag_max_consolidation_range_percent: 12
pdf_include_setup_grades:
  - Strong Buy Setup
  - Strong Sell Setup
```

For bullish signals, the card shows the latest 50-day SMA when available, the
lowest close from the latest 20 trading rows, and a 20% trailing reference below
the latest close. For bearish signals, it shows the latest 50-day SMA when
available, the highest close from the latest 20 trading rows, and a 20%
trailing reference above the latest close. Missing values are shown as `Not
available`.

Each candidate also receives an `Action grade`:

- `Strong Buy Setup`
- `Buy Setup`
- `Track Only`
- `Sell Setup`
- `Strong Sell Setup`

This is a rule-based setup grade, not personalised financial advice. It is
based only on technical conditions already stored by the project, including
crossover recency, latest close versus the 50-day SMA, 7/30/50-day SMA
alignment, dividend risk flags for bullish setups, and whether the stop/review
distance is wider than the configured warning percentage.

PDF chart pages include only the grades listed in `pdf_include_setup_grades`.
By default, the PDF shows only `Strong Buy Setup` and `Strong Sell Setup`
candidates. `Buy Setup`, `Sell Setup`, and `Track Only` candidates are not shown
in the PDF, although the underlying candidate grading can still calculate them.

PDF chart selection is market balanced. By default, the report includes up to
25 `S&P 500` candidates and up to 25 `FTSE 350` candidates, with unused slots
available to the other market up to the 50-chart PDF maximum. Within each
market, selected charts are sorted by higher score, strong buy before strong
sell when scores tie, most recent crossover, and ticker. The first-page index
includes only the stocks that have chart pages in the PDF and shows a compact
included count by market.

The chart itself stays deliberately simple: close price plus the 7-day, 30-day,
and 50-day SMAs. Crossover markers and 20-day high/low reference lines are off
by default because the planning levels already appear in the candidate card.
When `chart_show_possible_flag_pattern` is enabled, the chart may draw two
subtle guide lines labelled `Possible flag pattern` if a conservative recent
consolidation channel is detected after a strong prior move. This is only a
visual guide, not a confirmed pattern. The 200-day SMA is off by default to keep
the PDF chart clean.

Supporting chart images are saved here:

```text
~/Library/CloudStorage/OneDrive-Personal/Finance/MarketSentinel/Charts
```

The `~` means your home folder. On a Mac, that is usually:

```text
/Users/your-name
```

If OneDrive is not available, the project falls back to local output folders:

```text
outputs/excel
outputs/pdf
outputs/charts
```

## If GitHub Desktop Shows Changes

GitHub Desktop shows changes whenever files have been edited, created, or
generated.

Before committing:

- Read the changed file list.
- Commit source code, tests, config examples, and documentation changes.
- Do not commit private settings, generated databases, or generated reports.
- If you are unsure about a file, leave it uncommitted and ask for help.

It is normal for GitHub Desktop to show changes after running the project,
because reports and database files may be generated.

## Files That Should Not Be Committed

Do not commit:

- `.env`
- `.venv/`
- `data/`
- `outputs/`
- Generated `.xlsx` reports
- Generated `.pdf` reports
- `__pycache__/`
- `.pytest_cache/`
- `.DS_Store`

These are local files. They are either private, machine-specific, or generated
again when the project runs.

It is safe to commit `.env.example` because it contains example values only.

## Common Harmless Warnings

### LibreSSL Warning

You may see a warning mentioning `LibreSSL`, `urllib3`, or OpenSSL.

This is common on some Mac Python installations. It is usually harmless for this
project if the command finishes successfully.

If the daily process completes and reports are created, you can ignore it.

### yfinance Messages

`yfinance` may print warnings when a ticker has no data, is unavailable, or is
temporarily blocked by Yahoo Finance.

The project is designed to continue with the remaining tickers where possible.

### Email Disabled Message

You may see:

```text
Email alerts are disabled. No email was sent.
```

That is normal. It means email is switched off in your environment settings.

## Common Mistakes

### Accidentally Pasting Output Into Terminal

Sometimes Terminal output gets copied and pasted back into Terminal by mistake.

If that happens:

1. Press `Control + C`.
2. Wait for a fresh prompt.
3. Paste only the command you want to run.

Commands usually start with something like `python3`, `PYTHONPATH=src`, `source`,
`pip`, or `cd`.

Do not paste report text, error explanations, or long output blocks as commands.

### Running From The Wrong Folder

If Terminal says it cannot find `scripts/run_daily_process.py`, you are probably
not in the project folder.

Run:

```bash
pwd
```

Then go back to the project folder:

```bash
cd ~/Documents/Codex/2026-05-03/create-a-beginner-friendly-python-project
cd market-sentinel
```

### Forgetting The Virtual Environment

If Python cannot find packages like `duckdb`, `pytest`, `yaml`, `openpyxl`, or
`yfinance`, activate the virtual environment:

```bash
source .venv/bin/activate
```

### Committing Secrets

Never commit real passwords, API keys, or email app passwords.

Put private values in `.env`. Keep `.env.example` as a safe template with fake
example values.

## Quick Reference

Go to the project:

```bash
cd ~/Documents/Codex/2026-05-03/create-a-beginner-friendly-python-project
cd market-sentinel
```

Activate the virtual environment:

```bash
source .venv/bin/activate
```

Run tests:

```bash
PYTHONPATH=src python3 -m pytest
```

Run the fast daily process:

```bash
PYTHONPATH=src python3 scripts/run_daily_fast.py
```

Generate chart images:

```bash
PYTHONPATH=src python3 scripts/generate_charts.py
```

To bypass cached chart images:

```bash
PYTHONPATH=src python3 scripts/generate_charts.py --force
```

Update FTSE 350 and S&P 500, load the universe, then run the daily process:

```bash
PYTHONPATH=src python3 scripts/update_ftse350_universe.py
PYTHONPATH=src python3 scripts/update_sp500_universe.py
PYTHONPATH=src python3 scripts/load_universe.py
PYTHONPATH=src python3 scripts/run_daily_fast.py
```
