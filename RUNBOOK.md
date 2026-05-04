# market-sentinel Runbook

This runbook is the simple day-to-day guide for running `market-sentinel`.

## What The Project Currently Does

`market-sentinel` is a local Python project that helps analyse FTSE 350 and
S&P 500 stocks.

It currently can:

- Load stock universe CSV files.
- Update the S&P 500 universe CSV from Wikipedia.
- Store data in a local DuckDB database.
- Download daily price data with `yfinance`.
- Calculate simple moving averages.
- Detect moving average crossover signals.
- Analyse dividends.
- Add simple dividend risk flags.
- Generate Excel and PDF reports.
- Optionally send a plain-text daily email alert summary.

It does not currently send iPhone notifications.

It does not currently update the FTSE 350 universe from the internet.

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

## Run The Full Daily Process

The current safe daily command is:

```bash
PYTHONPATH=src python3 scripts/run_daily_process.py
```

This runs the daily steps in order:

1. Load universe CSV files.
2. Update market data.
3. Calculate moving averages.
4. Detect crossovers.
5. Calculate dividends.
6. Calculate dividend risk flags.
7. Generate the Excel report.
8. Generate the PDF report.
9. Send the optional daily alert email.

Email is safe by default. If email alerts are disabled, the project prints a
friendly message and does not send anything.

Market data has two modes.

Daily update mode is for normal daily runs. It downloads only recent prices:

```bash
PYTHONPATH=src python3 scripts/update_market_data.py
```

Backfill mode is for first-time setup or occasional historical refreshes. It
downloads a larger historical period and can take much longer:

```bash
PYTHONPATH=src python3 scripts/backfill_market_data.py
```

The full daily process uses daily update mode only. It does not run the slower
backfill.

Both modes process tickers in yfinance batches. This is normal and helps the
project handle large universes like the S&P 500 without looking stuck.

By default, each batch has 20 tickers, daily mode downloads the last 10 days,
backfill mode downloads 5 years, and the project pauses 3 seconds between
batches.

Failed price updates are saved here:

```text
outputs/failed_price_updates.csv
```

To change this, edit `config/settings.yaml`:

```yaml
price_download_batch_size: 20
price_backfill_period: 5y
price_daily_lookback_days: 10
price_download_pause_seconds: 3
```

During the update you should see:

- Total tickers.
- Current batch number.
- Tickers in the current batch.
- Rows written for the batch.
- Failed tickers for the batch.
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

## Update The S&P 500 Universe

The S&P 500 universe can be refreshed from Wikipedia.

First update the S&P 500 CSV:

```bash
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

This only updates the S&P 500 CSV. It does not update FTSE 350, and it does not
download prices by itself.

## Where Reports Are Saved

Excel reports are saved here:

```text
~/Library/CloudStorage/OneDrive-Personal/Finance/MarketSentinel/Excel
```

PDF reports are saved here:

```text
~/Library/CloudStorage/OneDrive-Personal/Finance/MarketSentinel/PDF
```

The `~` means your home folder. On a Mac, that is usually:

```text
/Users/your-name
```

If OneDrive is not available, the project falls back to local output folders:

```text
outputs/excel
outputs/pdf
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

Run the full daily process:

```bash
PYTHONPATH=src python3 scripts/run_daily_process.py
```

Update S&P 500, load the universe, then run the daily process:

```bash
PYTHONPATH=src python3 scripts/update_sp500_universe.py
PYTHONPATH=src python3 scripts/load_universe.py
PYTHONPATH=src python3 scripts/run_daily_process.py
```
