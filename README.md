# market-sentinel

`market-sentinel` is a beginner-friendly Python project for analysing FTSE 350 and S&P 500 stocks.

The project is designed to grow gradually. The current version is a modular skeleton only: it does not download market data yet.

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

## Current Status

This repository contains the project skeleton only. Data downloading, calculations, storage, reports, and alerts are intentionally left as future implementation work.
