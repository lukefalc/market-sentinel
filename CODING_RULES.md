# Coding Rules

These rules are intended to keep the project easy to understand for beginners.

## General Style

- Write readable code before clever code.
- Use descriptive names for modules, functions, and variables.
- Keep functions small and focused on one task.
- Add docstrings to public modules and functions.
- Prefer explicit code over hidden magic.

## Project Boundaries

- Data modules load, clean, and validate data.
- Analytics modules calculate results.
- Database modules store and retrieve data.
- Report modules format output.
- Alert modules decide when something should be flagged.
- Configuration modules load settings.

## Error Handling

- Raise clear exceptions when input data is missing or invalid.
- Include helpful messages that explain what went wrong.
- Avoid silently ignoring bad data.

## Testing

- Add tests for every calculation.
- Use small datasets in tests.
- Test edge cases such as missing values, empty inputs, and short price histories.

## Dependencies

- Add dependencies only when they solve a real problem.
- Prefer well-known libraries with good documentation.
- Keep optional reporting dependencies separate when possible.

## Documentation

- Update `README.md` when setup steps change.
- Update `PROJECT_PLAN.md` when phases are completed or changed.
- Update `ARCHITECTURE.md` when module responsibilities change.
