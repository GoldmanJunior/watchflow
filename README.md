# watchflow

> A lightweight Python task scheduler with dependency resolution.

[![CI](https://github.com/GoldmanJunior/watchflow/actions/workflows/ci.yml/badge.svg)](https://github.com/GoldmanJunior/watchflow/actions)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://pypi.org/project/watchflow/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

**watchflow** fills the gap between a simple cron job and a full-blown orchestrator like Airflow.
Define tasks with a decorator, declare dependencies, and let watchflow figure out the execution order.

```python
from watchflow import task, run

@task
def fetch_data():
    return load_csv("data.csv")

@task(depends_on=["fetch_data"])
def clean_data(fetch_data):
    return [row for row in fetch_data if row is not None]

@task(depends_on=["fetch_data"])
def validate_data(fetch_data):
    return len(fetch_data) > 0

@task(depends_on=["clean_data", "validate_data"])
def train_model(clean_data, validate_data):
    if not validate_data:
        raise ValueError("Invalid data")
    return model.fit(clean_data)

run()
```

## Why watchflow?

| | cron + bash | watchflow | Airflow / Prefect |
|---|---|---|---|
| Dependency resolution | No | Yes | Yes |
| Pure Python API | No | Yes | Yes |
| Zero config | Yes | Yes | No |
| Parallel execution | No | Yes | Yes |
| Setup time | 1 min | 1 min | 30+ min |

## Installation

```bash
pip install watchflow
```

## Features

- `@task` decorator with `depends_on` for dependency declaration
- Automatic topological sort — no need to think about execution order
- Parallel execution of independent tasks with `run(parallel=True)`
- Retry with exponential backoff — `@task(retries=3, retry_delay=1.0)`
- Timeout per task — `@task(timeout=30.0)`
- DAG visualization in ASCII and Mermaid format
- Zero external dependencies — pure Python stdlib

## Usage

### Basic pipeline

```python
from watchflow import task, run

@task
def step_one():
    return 42

@task(depends_on=["step_one"])
def step_two(step_one):
    return step_one * 2

run()
```

### Parallel execution

```python
run(parallel=True)
```

Independent tasks run concurrently using `ThreadPoolExecutor`.

### Retry and timeout

```python
@task(retries=3, retry_delay=1.0, retry_backoff=2.0, timeout=30.0)
def fetch_from_api():
    return requests.get("https://api.example.com/data").json()
```

### Visualize the DAG

```python
from watchflow import get_dag
from watchflow.core.visualizer import print_graph

print_graph(get_dag())
# DAG watchflow (4 tasks)
# └── [ ] fetch_data
#     ├── [ ] clean_data
#     │   └── [ ] train_model
#     └── [ ] validate_data
#         └── [ ] train_model

print_graph(get_dag(), fmt="mermaid")
```

## Contributing

Contributions are welcome! Here are some good first issues to start with:

- Add a `dry_run` mode that prints the execution plan without running tasks
- Add a `JSONBackend` to persist execution results
- Add support for async tasks with `asyncio`
- Add a `@task(tags=["etl"])` feature to run subsets of the pipeline
- Improve the ASCII visualization for diamond-shaped DAGs

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT
