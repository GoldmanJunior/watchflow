# Contributing to watchflow

Thank you for your interest in contributing!

## Setup

```bash
git clone https://github.com/ton-username/watchflow
cd watchflow
pip install -e ".[dev]"
```

## Running tests

```bash
pytest tests/ -v
```

## Project structure

```
watchflow/
├── watchflow/
│   ├── __init__.py          # Public API
│   └── core/
│       ├── graph.py         # DAG and TaskNode
│       ├── decorators.py    # @task decorator
│       ├── runner.py        # Sequential runner
│       ├── parallel.py      # Parallel runner
│       ├── utils.py         # Retry and timeout
│       └── visualizer.py    # ASCII and Mermaid output
├── tests/
│   ├── test_graph.py
│   └── test_runner.py
├── pyproject.toml
└── README.md
```

## Guidelines

- Every new feature must come with tests
- Keep zero external dependencies in the core library
- Follow the existing code style — one responsibility per module
- Add docstrings to public functions

## Good first issues

If you are new to the project, start with one of these:

- `dry_run` mode — print execution order without running tasks
- `JSONBackend` — persist results to a JSON file
- Async support — run tasks with `asyncio`
- Tag filtering — `run(tags=["etl"])` to run a subset of tasks
