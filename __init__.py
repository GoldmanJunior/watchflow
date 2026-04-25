"""
watchflow — Un scheduler Python léger avec résolution de dépendances.

Usage typique :

    from watchflow import task, run

    @task
    def fetch_data():
        return [1, 2, 3]

    @task(depends_on=["fetch_data"])
    def clean_data(fetch_data):
        return [x for x in fetch_data if x > 1]

    @task(depends_on=["clean_data"])
    def train_model(clean_data):
        print(f"Entraînement sur {len(clean_data)} samples")

    run()
"""

from .core.decorators import task, get_dag, reset_dag
from .core.runner import run, Runner
from .core.graph import DAG, TaskNode

__version__ = "0.1.0"
__all__ = ["task", "run", "Runner", "DAG", "TaskNode", "get_dag", "reset_dag"]
