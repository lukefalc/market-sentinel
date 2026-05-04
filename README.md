# market-sentinel

`market-sentinel` is a beginner-friendly Python project for analysing FTSE 350
and S&P 500 stocks.

The project is designed to grow gradually, with local CSV universe files,
DuckDB storage, analytics, and Excel/PDF reports.

## Planned Features

- Analyse FTSE 350 and S&P 500 shares.
- Calculate simple moving averages.
- Detect moving average crossover signals.
- Analyse dividend yield.
- Estimate total return using price movement plus dividends.
- Store cleaned market data and analysis outputs.
- Generate Excel reports.
- Generate PDF reports.
- Send alerts when important signals are detected.

## Project Structure

```text
market-sentinel/
  src/market_sentinel/
    data/          Data loading, validation, and future download code
    analytics/     Moving averages, crossover detection, dividends, returns
    database/      Database connection and storage helpers
    reports/       Excel and PDF report generation
    alerts/        Alert rules and notification helpers
    config/        Application settings and configuration loading
  scripts/         Small command-line scripts for running project tasks
  config/          User-editable configuration files
  tests/           Automated tests
```

## Getting Started

Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
```

Install the project in editable mode:

```bash
pip install -e ".[dev]"
```

Run tests:

```bash
pytest
```

## Environment Variables

Copy `.env.example` to `.env` when you are ready to configure local settings:

```bash
cp .env.example .env
```

Do not commit `.env` because it may contain private credentials later.

### Email Alerts

Email alerts are optional and disabled by default. Settings are read from
environment variables, usually through your local `.env` file.

Required variables when email is enabled:

```text
MARKET_SENTINEL_EMAIL_ENABLED=true
MARKET_SENTINEL_SMTP_HOST=smtp.example.com
MARKET_SENTINEL_SMTP_PORT=587
MARKET_SENTINEL_SMTP_USERNAME=your_email@example.com
MARKET_SENTINEL_SMTP_PASSWORD=your_app_password_here
MARKET_SENTINEL_EMAIL_FROM=your_email@example.com
MARKET_SENTINEL_EMAIL_TO=your_email@example.com
```

Use an app password from your email provider where possible. Do not commit real
passwords or secrets.

Send the alert summary manually with:

```bash
PYTHONPATH=src python3 scripts/send_daily_alert_email.py
```

The daily process runner includes email as the final optional step. If email is
disabled, it prints a friendly message and does not send anything.

## Updating Stock Universes

Stock universe files live in:

- `config/universes/ftse_350.csv`
- `config/universes/sp_500.csv`

The S&P 500 CSV can be updated from Wikipedia. FTSE 350 is still manual for now.

To refresh the S&P 500 universe:

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

Market data has two modes:

- Daily update mode is for normal daily runs. It downloads only recent data.
- Backfill mode is for first-time setup or occasional historical refreshes.

The normal daily command is:

```bash
PYTHONPATH=src python3 scripts/update_market_data.py
```

The historical backfill command is:

```bash
PYTHONPATH=src python3 scripts/backfill_market_data.py
```

Both modes use yfinance batches so large universes, such as the S&P 500, show
regular progress. The default batch size is 50 tickers, the daily lookback is
10 days, the backfill period is 5 years, and the pause between batches is
1 second.

You can adjust this in `config/settings.yaml`:

```yaml
price_download_batch_size: 50
price_backfill_period: 5y
price_daily_lookback_days: 10
price_download_pause_seconds: 1
```

For FTSE 350, update the CSV file manually when you want to add, remove, or
correct stocks.

Keep this exact header row:

```csv
ticker,name,market,region,currency,sector
```

Each row must include values for:

- `ticker`
- `name`
- `market`
- `region`
- `currency`

The `sector` field should be filled in when you know it, but it can be left
blank temporarily.

Example rows:

```csv
AAPL,Apple,S&P 500,United States,USD,Technology
HSBA.L,HSBC Holdings,FTSE 350,United Kingdom,GBP,Financial Services
```

Tips for manual updates:

- Use Yahoo Finance-style tickers where possible, such as `AAPL` or `HSBA.L`.
- S&P 500 rows can be refreshed with `scripts/update_sp500_universe.py`.
- Keep FTSE 350 rows in `ftse_350.csv`.
- If a company name contains a comma, wrap the name in quotes.
- Save the file as plain CSV, not an Excel workbook.
- After editing, run `python3 scripts/load_universe.py` from the project folder.

## Current Status

This repository now includes local universe loading, an S&P 500 universe
updater, market data updates, analytics, risk flags, Excel/PDF reports, and
optional email summaries. FTSE 350 constituent downloading is not implemented
yet.
