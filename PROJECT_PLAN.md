# Project Plan

This plan breaks `market-sentinel` into beginner-friendly phases. Each phase should be useful on its own and small enough to test.

## Phase 1: Project Skeleton

- Create the folder structure.
- Add documentation.
- Add placeholder modules with clear responsibilities.
- Add basic project configuration.

## Phase 2: Data Models and Sample Data

- Define simple stock price and dividend data formats.
- Add small sample CSV files for local testing.
- Write validation helpers for required columns and date formats.

## Phase 3: Analytics

- Implement simple moving average calculations.
- Detect moving average crossovers.
- Calculate dividend yield.
- Calculate total return.
- Add tests using small in-memory datasets.

## Phase 4: Storage

- Choose a simple database option, such as SQLite.
- Store stock metadata, prices, dividends, and analysis results.
- Add migration or setup scripts.

## Phase 5: Reports

- Generate Excel reports with summary tabs and stock-level details.
- Generate PDF reports with concise charts and tables.
- Keep report templates simple and reusable.

## Phase 6: Alerts

- Define alert rules for crossover events and dividend changes.
- Add email or messaging integrations later.
- Keep alerts separate from analytics so they can be tested independently.

## Phase 7: Data Downloading

- Add providers for FTSE 350 and S&P 500 data.
- Cache downloaded data.
- Handle provider errors and missing values gracefully.

## Guiding Principles

- Prefer clear code over clever code.
- Add tests when adding behaviour.
- Keep modules small and focused.
- Make each feature work with sample data before connecting live data.
