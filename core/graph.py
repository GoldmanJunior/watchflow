"""
graph.py — Le coeur de watchflow : représentation et résolution du DAG.

Un DAG (Directed Acyclic Graph) est un graphe où :
- les arêtes ont une direction (A -> B signifie "A doit s'exécuter avant B")
- il n'existe aucun cycle (on ne peut pas revenir à un noeud déjà visité)

Ce module contient deux choses :
  1. TaskNode  : représente une tâche et ses dépendances
  2. DAG       : contient toutes les tâches et sait les ordonner
"""

from __future__ import annotations
from typing import Callable, Any


class TaskNode:
    """
    Représente une tâche dans le graphe.

    Une tâche c'est simplement :
    - une fonction Python à exécuter
    - une liste de noms de tâches dont elle dépend
    - un état (pending, running, done, failed)
    """

    def __init__(
        self,
        func: Callable,
        depends_on: list[str] | None = None,
        name_override: str | None = None,
        retries: int = 0,
        retry_delay: float = 1.0,
        retry_backoff: float = 1.0,
        timeout: float | None = None,
    ):
        self.func = func
        self.name = name_override or func.__name__
        self.depends_on: list[str] = depends_on or []
        self.status: str = "pending"
        self.result: Any = None
        self.error: Exception | None = None
        self.retries: int = retries
        self.retry_delay: float = retry_delay
        self.retry_backoff: float = retry_backoff
        self.timeout: float | None = timeout

    def __repr__(self) -> str:
        return f"TaskNode(name={self.name!r}, depends_on={self.depends_on!r}, status={self.status!r})"


class DAG:
    """
    Directed Acyclic Graph — le registre de toutes les tâches.

    Responsabilités :
    - stocker les TaskNode
    - vérifier qu'il n'y a pas de cycles
    - retourner un ordre d'exécution valide via le tri topologique
    """

    def __init__(self):
        # Un dict garantit l'unicité des noms et un accès en O(1)
        self._tasks: dict[str, TaskNode] = {}

    def add_task(self, task: TaskNode) -> None:
        """Ajoute une tâche au graphe."""
        if task.name in self._tasks:
            raise ValueError(
                f"Une tâche nommée '{task.name}' existe déjà. "
                f"Chaque fonction doit avoir un nom unique."
            )
        self._tasks[task.name] = task

    def _validate_dependencies(self) -> None:
        """
        Vérifie que toutes les dépendances référencées existent.

        Si une tâche déclare depends_on=["fetch_data"] mais que
        fetch_data n'est pas enregistrée, on lève une erreur claire.
        """
        for task in self._tasks.values():
            for dep_name in task.depends_on:
                if dep_name not in self._tasks:
                    raise ValueError(
                        f"La tâche '{task.name}' dépend de '{dep_name}' "
                        f"qui n'est pas enregistrée dans le DAG."
                    )

    def topological_sort(self) -> list[TaskNode]:
        """
        Retourne les tâches dans un ordre d'exécution valide.

        Algorithme utilisé : DFS (Depth-First Search) avec détection de cycles.

        Principe :
        - On visite chaque tâche en profondeur (on descend dans ses dépendances)
        - Si on retombe sur une tâche déjà en cours de visite -> cycle détecté
        - Une tâche est ajoutée à l'ordre APRÈS que toutes ses dépendances
          ont été ajoutées (c'est ce qui garantit l'ordre correct)

        Complexité : O(V + E) où V = nombre de tâches, E = nombre de dépendances
        """
        self._validate_dependencies()

        visited: set[str] = set()
        visiting: set[str] = set()     # ensemble séparé pour détecter les cycles
        order: list[TaskNode] = []

        def visit(name: str) -> None:
            """
            Visite récursive d'une tâche et de ses dépendances.

            La récursion ici c'est la clé : avant d'ajouter une tâche
            à l'ordre, on visite d'abord toutes ses dépendances.
            Elles seront donc toujours placées avant elle.
            """
            if name in visited:
                return

            if name in visiting:
                raise ValueError(
                    f"Cycle détecté impliquant la tâche '{name}'. "
                    f"Les dépendances circulaires sont interdites dans un DAG."
                )

            visiting.add(name)

            task = self._tasks[name]
            for dep_name in task.depends_on:
                visit(dep_name)

            visiting.remove(name)
            visited.add(name)
            order.append(task)

        for name in self._tasks:
            visit(name)

        return order

    def get_task(self, name: str) -> TaskNode:
        """Récupère une tâche par son nom."""
        if name not in self._tasks:
            raise KeyError(f"Tâche '{name}' introuvable dans le DAG.")
        return self._tasks[name]

    def reset(self) -> None:
        """Remet toutes les tâches à l'état 'pending' (pour ré-exécuter le pipeline)."""
        for task in self._tasks.values():
            task.status = "pending"
            task.result = None
            task.error = None

    def __len__(self) -> int:
        return len(self._tasks)

    def __repr__(self) -> str:
        return f"DAG(tasks={list(self._tasks.keys())})"
