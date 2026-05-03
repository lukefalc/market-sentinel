# Architecture

`market-sentinel` uses a modular architecture so each part of the project has a clear job.

## High-Level Flow

```text
Configuration
    |
    v
Data loading and validation
    |
    v
Analytics
    |
    v
Database storage
    |
    +--> Reports
    |
    +--> Alerts
```

## Modules

### `market_sentinel.config`

Loads settings such as file paths, database location, ticker lists, and report options.

### `market_sentinel.data`

Will eventually download and load market data. For early development, this package should work with sample files before live data providers are added.

### `market_sentinel.analytics`

Contains calculation logic. This package should not know where data came from or how reports are generated.

Planned responsibilities:

- Simple moving averages.
- Moving average crossovers.
- Dividend yield.
- Total return.

### `market_sentinel.database`

Handles database setup, connections, inserts, and queries. Other modules should not contain raw database details.

### `market_sentinel.reports`

Turns analysis results into human-readable Excel and PDF reports.

### `market_sentinel.alerts`

Checks analysis results against alert rules and prepares notifications.

## Design Notes

- Keep downloading separate from analytics.
- Keep analytics separate from reporting.
- Keep configuration in one place.
- Use simple Python data structures first, then introduce richer models only when needed.
- Prefer functions with clear inputs and outputs.
