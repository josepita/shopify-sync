# Repository Guidelines

## Project Structure & Module Organization
- Source in `src/` grouped by domain: `csv_processor/`, `database/`, `shopify/`, `sync/`, `utils/`.
- Tests in `tests/` mirroring `src/` (e.g., `tests/test_csv_processor.py`).
- Configuration in `config/` and environment via `.env` (see `.env.example`).
- Runtime data in `data/` (with `data/csv_archive/`) and logs in `logs/`.
- Utility scripts in `tools/` (e.g., `tools/detect_missing_references.py`).

## Build, Test, and Development Commands
- Create env and install deps:
  - `python3 -m venv .venv && source .venv/bin/activate`
  - `pip install -r requirements.txt`
- Run sync locally:
  - Normal: `python src/sync/catalog.py`
  - Forced: `python src/sync/catalog.py --force`
- Helpful tools:
  - Missing refs: `python tools/detect_missing_references.py --csv data/current.csv`
  - Update categories/mappings: `python tools/update_categories.py`, `python tools/update_variant_mappings.py`
- Tests:
  - If using pytest: `pytest -q`
  - Or run scripts directly: `python tests/test_csv_processor.py`

## Coding Style & Naming Conventions
- Python 3, PEP 8, 4-space indentation.
- Names: modules/functions `snake_case`, classes `CamelCase`, constants `UPPER_SNAKE_CASE`.
- Type hints where practical; prefer small, focused modules under the correct package.
- Logging with `logging` (no prints in library code). No secrets in logs.

## Testing Guidelines
- Place tests in `tests/` as `test_*.py`, mirroring `src/` structure.
- Prefer deterministic unit tests; mock external I/O (DB, HTTP, email). 
- Minimum: cover critical paths in `csv_processor`, `sync`, and `database`.
- Run with `.env` configured; avoid relying on production credentials.

## Commit & Pull Request Guidelines
- Commits: concise, imperative mood; scope optional (e.g., `sync: handle forced mode`). Spanish is acceptable (project history uses it).
- PRs: clear description, what/why, steps to reproduce and verify, linked issue, and relevant logs/screenshots.
- Include notes on config or migrations when applicable; keep PRs focused and small.

## Security & Configuration Tips
- Copy `.env.example` to `.env` and fill values; never commit `.env`.
- Protect Shopify tokens and DB credentials. Do not store CSVs with PII.
- Ensure `data/` and `logs/` exist and are writable in your environment.
