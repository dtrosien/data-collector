# Agents

**Before working on this project, read [UBIQUITOUS_LANGUAGE.md](UBIQUITOUS_LANGUAGE.md) at the start of every session to maintain consistent domain terminology.**

**Lessons learned**: Communication patterns, best practices, and infrastructure decisions are documented in [AI/lessons-learned/](AI/lessons-learned/). Review these before starting refactoring, testing, or infrastructure work.

This document describes the runtime "agents" (actions) available in this repository. The project executes tasks as ActionType instances (see src/actions/action.rs). Tasks are scheduled and executed by Application using the DAG scheduler (src/dag_schedule) and configured in configuration/base.yaml + environment-specific YAML.

## Agent types

- Collectors (fetch data and write raw/staging data):
  - NyseEventsCollect
  - NyseInstrumentsCollect
  - SecCompaniesCollect
  - PolygonGroupedDaily
  - PolygonOpenClose
  - MassiveDividends
  - FinancialmodelingprepMarketCapCollect
  - FinancialmodelingprepCompanyProfileCollet
  - Dummy (test stub)

- Stagers (process or move collected data to staging tables):
  - NyseInstrumentsStage
  - SecCompaniesStage
  - PolygonGroupedDailyStager
  - FinmodCompanyProfileStage
  - FinmodMarketCapStager

## Where code lives
- Action types and factory: src/actions/action.rs
- Collectors: src/actions/collect/*
- Stagers: src/actions/stage/*
- Task scheduling and execution: src/dag_schedule/*
- Application bootstrap: src/startup.rs and src/main.rs
- Configuration schema: src/configuration.rs

## Configuration
- Tasks and task dependencies are configured via configuration/base.yaml and environment overlays (configuration/local.yaml, configuration/production.yaml).
- Each TaskSetting has a name and an ActionType. Only tasks referenced by task_dependencies are scheduled.
- Secrets (API keys) are provided in configuration.application.secrets and loaded into the KeyManager at runtime.

## Running
- Locally: set up DATABASE_URL (see .env), then `cargo run --bin data_collector` or use the scripts/docker instructions in README.md.
- Docker: follow Dockerfile and README build steps.

## Adding a new agent
1. Implement a Collector or Stager under src/actions/collect or src/actions/stage and implement the Runnable trait.
2. Add a matching ActionType variant in src/actions/action.rs and wire it in create_action().
3. Add a TaskSetting and TaskDependency entry in configuration/base.yaml (or environment file).
4. Add migrations if new DB tables are required (migrations/).

## Refactoring best practices
- **Before refactoring**: Always run `cargo test` to establish baseline. All tests must pass before making changes.
- **After refactoring**: Run `cargo test` again to verify all tests still pass with identical results. Do not commit refactored code until green tests confirm no behavior was altered.

## Notes
- API keys: see src/api_keys and SecretKeys in src/configuration.rs.
- Task behavior and retries are implemented in src/dag_schedule/task.rs.

(Generated summary file)
