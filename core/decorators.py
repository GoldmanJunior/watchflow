"""
decorators.py — Le décorateur @task qui est l'API publique de watchflow.

Un décorateur c'est une fonction qui wrape une autre fonction.
Quand tu écris :

    @task(depends_on=["fetch_data"])
    def clean_data():
        ...

Python exécute en coulisses :
    clean_data = task(depends_on=["fetch_data"])(clean_data)

Donc task(depends_on=...) retourne un décorateur,
et ce décorateur reçoit la fonction clean_data.

C'est ce qu'on appelle un "décorateur avec arguments" ou "décorateur factory".
"""

from __future__ import annotations
import functools
from typing import Callable

from .graph import DAG, TaskNode


# Singleton : une seule instance partagée par défaut.
_default_dag = DAG()


def get_dag() -> DAG:
    """Retourne le DAG global. Utilisé par le runner."""
    return _default_dag


def reset_dag() -> None:
    """Réinitialise le DAG global (utile pour les tests)."""
    global _default_dag
    _default_dag = DAG()


def task(
    func: Callable | None = None,
    *,
    depends_on: list[str] | None = None,
    name: str | None = None,
) -> Callable:
    """
    Décorateur qui enregistre une fonction comme tâche dans le DAG.

    Peut s'utiliser de deux façons :

    1. Sans arguments (décorateur direct) :
        @task
        def fetch_data():
            ...

    2. Avec arguments :
        @task(depends_on=["fetch_data"])
        def clean_data():
            ...

    Pourquoi deux façons ? Parce que dans le cas 1, Python passe
    directement la fonction à @task. Dans le cas 2, @task(depends_on=...)
    doit d'abord retourner un décorateur, qui lui recevra la fonction.

    On gère les deux cas avec le paramètre `func` :
    - Si func est fourni  -> utilisation directe (@task sans parenthèses)
    - Si func est None    -> utilisation avec arguments (@task(...))
    """

    def decorator(f: Callable) -> Callable:
        task_name = name or f.__name__
        node = TaskNode(f, depends_on=depends_on, name_override=task_name)
        _default_dag.add_task(node)

        # Sans wraps, f.__name__ retournerait 'wrapper' à la place du nom réel.
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            # Permet d'appeler la fonction directement hors runner (tests unitaires).
            return f(*args, **kwargs)

        wrapper._task_node = node
        return wrapper

    if func is not None:
        return decorator(func)
    else:
        return decorator
