"""
test_runner.py — Tests pour le runner séquentiel et le parallèle.
"""

import time
import pytest
from watchflow.core.graph import DAG, TaskNode
from watchflow.core.runner import Runner
from watchflow.core.parallel import ParallelRunner, _build_levels


# ─────────────────────────────────────────────
# Tests — Runner séquentiel
# ─────────────────────────────────────────────

def test_runner_basic_execution():
    """Un pipeline simple s'exécute et retourne success=True."""
    dag = DAG()
    dag.add_task(TaskNode(lambda: 42, name_override="compute"))

    result = Runner(dag, verbose=False).run()

    assert result.success is True
    assert result.tasks["compute"]["status"] == "done"
    assert result.tasks["compute"]["result"] == 42

def test_runner_passes_results_between_tasks():
    """Le résultat d'une tâche est passé à ses dépendants."""
    dag = DAG()
    dag.add_task(TaskNode(lambda: [1, 2, 3], name_override="fetch"))
    dag.add_task(TaskNode(
        lambda fetch: [x * 2 for x in fetch],
        depends_on=["fetch"],
        name_override="double"
    ))

    result = Runner(dag, verbose=False).run()

    assert result.tasks["double"]["result"] == [2, 4, 6]


def test_runner_stops_on_error():
    """stop_on_error=True arrête le pipeline dès le premier échec."""
    def failing(): raise ValueError("boom")
    def should_not_run(): return "never"

    dag = DAG()
    dag.add_task(TaskNode(failing))
    dag.add_task(TaskNode(should_not_run, depends_on=["failing"]))

    result = Runner(dag, stop_on_error=True, verbose=False).run()

    assert result.success is False
    assert result.tasks["failing"]["status"] == "failed"
    assert "should_not_run" not in result.tasks


def test_runner_continues_on_error():
    """stop_on_error=False continue malgré un échec."""
    call_log = []

    def failing(): raise ValueError("boom")
    def independent():
        call_log.append("ran")
        return "ok"

    dag = DAG()
    dag.add_task(TaskNode(failing))
    dag.add_task(TaskNode(independent))

    result = Runner(dag, stop_on_error=False, verbose=False).run()

    assert "ran" in call_log
    assert result.tasks["independent"]["status"] == "done"


# ─────────────────────────────────────────────
# Tests — build_levels
# ─────────────────────────────────────────────

def test_build_levels_simple():
    """fetch est niveau 0, clean est niveau 1."""
    def fetch(): pass
    def clean(): pass

    dag = DAG()
    dag.add_task(TaskNode(fetch))
    dag.add_task(TaskNode(clean, depends_on=["fetch"]))

    order = dag.topological_sort()
    levels = _build_levels(order)

    assert levels[0][0].name == "fetch"
    assert levels[1][0].name == "clean"


def test_build_levels_parallel_tasks():
    """clean et validate doivent être dans le même niveau."""
    def fetch(): pass
    def clean(): pass
    def validate(): pass
    def train(): pass

    dag = DAG()
    dag.add_task(TaskNode(fetch))
    dag.add_task(TaskNode(clean,    depends_on=["fetch"]))
    dag.add_task(TaskNode(validate, depends_on=["fetch"]))
    dag.add_task(TaskNode(train,    depends_on=["clean", "validate"]))

    order = dag.topological_sort()
    levels = _build_levels(order)

    level_1_names = {t.name for t in levels[1]}
    assert level_1_names == {"clean", "validate"}
    assert levels[2][0].name == "train"


# ─────────────────────────────────────────────
# Tests — Retry et Timeout
# ─────────────────────────────────────────────

def test_retry_succeeds_after_failures():
    """Une tâche instable réussit après plusieurs tentatives."""
    attempts = {"count": 0}

    def unstable():
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise ConnectionError("not yet")
        return "ok"

    dag = DAG()
    dag.add_task(TaskNode(unstable, retries=3, retry_delay=0.01))

    result = Runner(dag, verbose=False).run()

    assert result.success is True
    assert attempts["count"] == 3


def test_timeout_fails_slow_task():
    """Une tâche trop lente est arrêtée par le timeout."""
    def slow():
        time.sleep(5)

    dag = DAG()
    dag.add_task(TaskNode(slow, timeout=0.05))

    result = Runner(dag, verbose=False).run()

    assert result.success is False
    assert "timeout" in result.tasks["slow"]["error"].lower() or "dépassé" in result.tasks["slow"]["error"].lower()
