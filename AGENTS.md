# Repository Guidelines

## Project Structure & Module Organization
-  initializes the local DuckDB catalog and should stay focused on lightweight bootstrap steps; place heavier transforms inside new modules under .
- SQL models live in  (e.g. ); number files incrementally to preserve run order when orchestrating.
- Materialized data artefacts exist in  (such as ); treat this directory as disposable and avoid committing large exports.
- Keep reference documentation or design notes in ; generate notebooks or diagrams there instead of the project root.
- Use the checked-in  stub for local isolation, but do not commit additional site-packages files—each contributor should rebuild their environment locally.

## Build, Test, and Development Commands
-  followed by  (or  on Windows) creates and activates the workspace environment.
-  refreshes Python dependencies; run it whenever requirements change or after pulling .
-  provisions the  schema inside  and serves as a quick health check that DuckDB bindings are functional.
-  opens an interactive session against the local warehouse for ad-hoc validation queries.

## Coding Style & Naming Conventions
- Follow PEP 8 with 4-space indentation, snake_case modules, and UpperCamelCase classes; prefer explicit imports over wildcards.
- Type hint public functions and dataclasses to document schemas moving through the pipeline.
- Keep SQL keywords uppercase, identifiers snake_case, and terminate statements with semicolons to ease diff review.
- When adding configuration, reference keys via  and document expected variables in .

## Testing Guidelines
- Add unit or integration coverage with ; place tests under  mirroring the source tree and name files .
- Use DuckDB temporary schemas for data tests so they remain idempotent; clean up any tables at teardown.
- Validate seed SQL by asserting row counts or hash totals against fixtures before promoting changes to shared environments.

## Commit & Pull Request Guidelines
- Write commit subjects in the imperative mood ("Add DuckDB seed check") and include context-rich bodies when touching data models.
- Squash noisy install commits before opening a PR; keep history focused on logical changes.
- Reference linked issues in the PR description, call out schema or data contract modifications, and attach screenshots for user-facing artifacts.
- Ensure CI or local  runs are green before requesting review; note any intentionally skipped checks in the PR summary.

## Data & Security Notes
- Never commit generated  files or raw PHI/PII outputs; use  rules for transient exports.
- Store secrets in a local  file and load them with ; rotate credentials immediately if they leak.
- When sharing samples, redact payer or patient identifiers and limit extracts to the minimum columns needed for debugging.
