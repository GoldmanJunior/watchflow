"""
parallel.py — Exécution parallèle des tâches indépendantes.

Stratégie : on regroupe les tâches par "niveau d'exécution".
Un niveau contient toutes les tâches dont les dépendances sont
entièrement satisfaites par les niveaux précédents.

Exemple :
    fetch_data          -> niveau 0
    clean_data          -> niveau 1  (dépend de fetch_data)
    validate_data       -> niveau 1  (dépend de fetch_data)
    train_model         -> niveau 2  (dépend de clean_data ET validate_data)

Les tâches d'un même niveau s'exécutent en parallèle via ThreadPoolExecutor.
On attend que tout le niveau soit terminé avant de passer au suivant.
"""

from __future__ import annotations
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from .graph import DAG, TaskNode
from .runner import ExecutionResult


def _build_levels(order: list[TaskNode]) -> list[list[TaskNode]]:
    """
    Regroupe les tâches par niveau d'exécution parallèle.

    Algorithme :
    - Une tâche est au niveau N si toutes ses dépendances sont
      dans des niveaux < N.
    - On parcourt les tâches dans l'ordre topologique (déjà résolu),
      donc les dépendances sont toujours traitées avant leurs enfants.

    Retourne une liste de listes : [[niveau0], [niveau1], [niveau2], ...]
    """
    task_level: dict[str, int] = {}

    for task in order:
        if not task.depends_on:
            task_level[task.name] = 0
        else:
            # max + 1 : niveau minimum pour que toutes les dépendances soient finies
            max_dep_level = max(task_level[dep] for dep in task.depends_on)
            task_level[task.name] = max_dep_level + 1

    if not task_level:
        return []

    max_level = max(task_level.values())
    levels: list[list[TaskNode]] = [[] for _ in range(max_level + 1)]

    # ordre topologique préservé pour garder un ordre stable au sein d'un niveau
    for task in order:
        levels[task_level[task.name]].append(task)

    return levels


class ParallelRunner:
    """
    Exécute les tâches indépendantes en parallèle.

    Les tâches d'un même niveau s'exécutent dans un ThreadPoolExecutor.
    On attend que chaque niveau soit entièrement terminé avant de passer
    au niveau suivant — c'est la garantie que les dépendances sont toujours
    satisfaites.

    Paramètres :
        max_workers : nombre max de threads simultanés (None = auto, basé sur les CPUs)
        stop_on_error : arrêter le pipeline si une tâche échoue
        verbose : afficher le suivi en temps réel
    """

    def __init__(
        self,
        dag: DAG,
        max_workers: int | None = None,
        stop_on_error: bool = True,
        verbose: bool = True,
    ):
        self.dag = dag
        self.max_workers = max_workers
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

        levels = _build_levels(order)

        if self.verbose:
            total = len(order)
            print(f"\n[watchflow] Pipeline parallèle — {total} tâche(s) en {len(levels)} niveau(x)\n")
            for i, level in enumerate(levels):
                names = ", ".join(t.name for t in level)
                parallel_note = " (parallèle)" if len(level) > 1 else ""
                print(f"  Niveau {i}{parallel_note} : {names}")
            print()

        # Pas de Lock nécessaire : les niveaux s'exécutent séquentiellement,
        # donc jamais deux threads n'écrivent la même clé simultanément.
        results_registry: dict[str, Any] = {}

        for level_idx, level_tasks in enumerate(levels):
            if self.verbose:
                names = [t.name for t in level_tasks]
                print(f"[watchflow] Niveau {level_idx} : lancement de {names}")

            level_failed = False

            if len(level_tasks) == 1:
                # Optimisation : évite le coût de création d'un thread pour rien
                task = level_tasks[0]
                self._execute_task(task, results_registry, result)
                if task.status == "failed":
                    level_failed = True
            else:
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    future_to_task = {
                        executor.submit(
                            self._execute_task, task, results_registry, result
                        ): task
                        for task in level_tasks
                    }

                    for future in as_completed(future_to_task):
                        task = future_to_task[future]
                        try:
                            future.result()  # propage les exceptions si il y en a
                        except Exception:
                            level_failed = True

            if level_failed and self.stop_on_error:
                if self.verbose:
                    print(f"\n[watchflow] Arrêt du pipeline (niveau {level_idx} en échec).")
                break

        result.total_duration = time.perf_counter() - pipeline_start

        if self.verbose:
            print(result.summary())

        return result

    def _execute_task(
        self,
        task: TaskNode,
        results_registry: dict[str, Any],
        result: ExecutionResult,
    ) -> None:
        """
        Exécute une tâche individuelle et met à jour les registres.

        Cette méthode est appelée dans un thread par le ThreadPoolExecutor.
        Elle est thread-safe car :
        - Chaque tâche écrit dans results_registry[task.name] (clé unique)
        - On ne lit que les dépendances du niveau précédent (déjà finies)
        """
        if self.verbose:
            print(f"  -> {task.name} ...", flush=True)

        task.status = "running"
        start = time.perf_counter()

        try:
            dep_results = {
                dep: results_registry[dep]
                for dep in task.depends_on
                if results_registry.get(dep) is not None
            }

            task_result = task.func(**dep_results) if dep_results else task.func()

            duration = time.perf_counter() - start
            task.status = "done"
            task.result = task_result
            results_registry[task.name] = task_result

            result.add(task.name, "done", task_result, duration)

            if self.verbose:
                print(f"  OK {task.name} ({duration:.3f}s)", flush=True)

        except Exception as e:
            duration = time.perf_counter() - start
            task.status = "failed"
            task.error = e
            result.add(task.name, "failed", None, duration, error=e)

            if self.verbose:
                print(f"  FAIL {task.name} : {type(e).__name__}: {e}", flush=True)
                traceback.print_exc()
