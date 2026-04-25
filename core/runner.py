"""
runner.py — Exécute les tâches séquentiellement avec retry et timeout.
"""

from __future__ import annotations
import time
import functools
import traceback
from typing import Any

from .graph import DAG, TaskNode
from .utils import run_with_retry, run_with_timeout


class ExecutionResult:
    """Résumé de l'exécution complète du pipeline."""

    def __init__(self):
        self.tasks: dict[str, dict] = {}
        self.success: bool = True
        self.total_duration: float = 0.0

    def add(self, name: str, status: str, result: Any, duration: float, error: Exception | None = None):
        self.tasks[name] = {
            "status": status,
            "result": result,
            "duration": round(duration, 4),
            "error": str(error) if error else None,
        }
        if status == "failed":
            self.success = False

    def summary(self) -> str:
        lines = [
            "",
            "=" * 52,
            "  watchflow — Résumé d'exécution",
            "=" * 52,
        ]
        for name, info in self.tasks.items():
            icon = "OK  " if info["status"] == "done" else "FAIL"
            lines.append(f"  [{icon}]  {name:<28} {info['duration']:.3f}s")
            if info["error"]:
                lines.append(f"           Erreur : {info['error']}")
        lines.append("-" * 52)
        status_str = "SUCCESS" if self.success else "FAILED"
        lines.append(f"  Statut : {status_str} | Durée totale : {self.total_duration:.3f}s")
        lines.append("=" * 52)
        return "\n".join(lines)


def _call_task(task: TaskNode, dep_results: dict[str, Any]) -> Any:
    """
    Appelle la fonction d'une tâche en enveloppant avec retry et timeout.

    L'ordre d'enveloppement est important :
    - On applique d'abord le timeout (limite globale de la tentative)
    - Puis le retry relance si la tentative échoue dans le timeout
    """
    def execute():
        return task.func(**dep_results) if dep_results else task.func()

    if task.timeout is not None:
        def execute_with_timeout():
            return run_with_timeout(execute, task.timeout)
        call = execute_with_timeout
    else:
        call = execute

    if task.retries > 0:
        return run_with_retry(
            call,
            retries=task.retries,
            delay=task.retry_delay,
            backoff=task.retry_backoff,
        )
    else:
        return call()


class Runner:
    """Exécute séquentiellement les tâches d'un DAG."""

    def __init__(self, dag: DAG, stop_on_error: bool = True, verbose: bool = True):
        self.dag = dag
        self.stop_on_error = stop_on_error
        self.verbose = verbose

    def run(self) -> ExecutionResult:
        result = ExecutionResult()
        pipeline_start = time.perf_counter()

        try:
            order = self.dag.topological_sort()
        except ValueError as e:
            print(f"[watchflow] Erreur DAG : {e}")
            result.success = False
            return result

        if self.verbose:
            names = [t.name for t in order]
            print(f"\n[watchflow] Pipeline de {len(order)} tâche(s) : {' → '.join(names)}\n")

        results_registry: dict[str, Any] = {}

        for task in order:
            retry_info = f" (retry x{task.retries})" if task.retries > 0 else ""
            timeout_info = f" (timeout {task.timeout}s)" if task.timeout else ""

            if self.verbose:
                print(f"[watchflow] {task.name}{retry_info}{timeout_info} ...", end=" ", flush=True)

            task.status = "running"
            start = time.perf_counter()

            try:
                dep_results = {
                    dep: results_registry[dep]
                    for dep in task.depends_on
                    if results_registry.get(dep) is not None
                }

                task_result = _call_task(task, dep_results)

                duration = time.perf_counter() - start
                task.status = "done"
                task.result = task_result
                results_registry[task.name] = task_result
                result.add(task.name, "done", task_result, duration)

                if self.verbose:
                    print(f"OK ({duration:.3f}s)")

            except Exception as e:
                duration = time.perf_counter() - start
                task.status = "failed"
                task.error = e
                result.add(task.name, "failed", None, duration, error=e)

                if self.verbose:
                    print(f"FAIL ({duration:.3f}s)")
                    print(f"         {type(e).__name__}: {e}")

                if self.stop_on_error:
                    if self.verbose:
                        print(f"\n[watchflow] Arrêt suite à l'échec de '{task.name}'.")
                    break

        result.total_duration = time.perf_counter() - pipeline_start

        if self.verbose:
            print(result.summary())

        return result


def run(dag: DAG | None = None, parallel: bool = False, **kwargs) -> ExecutionResult:
    """
    Lance le pipeline.

    Paramètres :
        dag      : DAG à exécuter (None = DAG global)
        parallel : utiliser le runner parallèle (True) ou séquentiel (False)
        **kwargs : options passées au runner (stop_on_error, verbose, max_workers...)
    """
    from .decorators import get_dag
    target_dag = dag or get_dag()

    if parallel:
        from .parallel import ParallelRunner
        return ParallelRunner(target_dag, **kwargs).run()

    return Runner(target_dag, **kwargs).run()
