"""
utils.py — Utilitaires réutilisables : retry et timeout.

Ces deux mécanismes sont indépendants du reste de watchflow.
On les implémente comme des fonctions pures pour qu'ils soient
testables et réutilisables dans les deux runners (séquentiel et parallèle).
"""

from __future__ import annotations
import time
import threading
from typing import Callable, Any


def run_with_retry(
    func: Callable,
    retries: int = 0,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,),
) -> Any:
    """
    Exécute une fonction en réessayant en cas d'échec.

    Paramètres :
        func       : la fonction à exécuter (sans arguments, utiliser functools.partial si besoin)
        retries    : nombre de tentatives supplémentaires (0 = pas de retry)
        delay      : délai initial entre les tentatives (secondes)
        backoff    : multiplicateur du délai à chaque tentative (2.0 = exponentiel)
        exceptions : tuple des exceptions à attraper (les autres remontent immédiatement)

    Exemple de comportement avec retries=3, delay=1.0, backoff=2.0 :
        Tentative 1 -> échec -> attente 1.0s
        Tentative 2 -> échec -> attente 2.0s
        Tentative 3 -> échec -> attente 4.0s
        Tentative 4 -> succès ou exception finale

    Pourquoi le backoff exponentiel ?
    Pour ne pas surcharger un service déjà en difficulté.
    C'est la stratégie utilisée par AWS, GCP, et la plupart des APIs.
    """
    attempt = 0
    current_delay = delay
    last_exception: Exception | None = None

    while attempt <= retries:
        try:
            return func()
        except exceptions as e:
            last_exception = e
            if attempt < retries:
                print(
                    f"    [retry] Tentative {attempt + 1}/{retries + 1} échouée "
                    f"({type(e).__name__}: {e}). "
                    f"Nouvelle tentative dans {current_delay:.1f}s..."
                )
                time.sleep(current_delay)
                current_delay *= backoff
            attempt += 1

    raise last_exception


def run_with_timeout(func: Callable, timeout: float) -> Any:
    """
    Exécute une fonction avec une limite de temps.

    Si la fonction dépasse `timeout` secondes, on lève une TimeoutError.

    Implémentation : on lance la fonction dans un thread séparé.
    Le thread principal attend `timeout` secondes. Si le thread n'est pas
    terminé, on lève l'exception.

    Limitation connue : en Python, on ne peut pas tuer un thread de force.
    Le thread continue de tourner en arrière-plan mais son résultat est ignoré.
    C'est une limitation du GIL — pour un vrai kill, il faudrait multiprocessing.
    Pour des tâches I/O courantes, ce comportement est acceptable.

    Paramètres :
        func    : la fonction à exécuter
        timeout : durée maximale en secondes
    """
    result_container: list[Any] = [None]
    exception_container: list[Exception | None] = [None]

    def target():
        try:
            result_container[0] = func()
        except Exception as e:
            exception_container[0] = e

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout=timeout)

    if thread.is_alive():
        raise TimeoutError(
            f"La tâche a dépassé la limite de {timeout}s."
        )

    if exception_container[0] is not None:
        raise exception_container[0]

    return result_container[0]
