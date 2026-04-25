"""
test_graph.py — Tests unitaires pour le DAG et le tri topologique.

Convention pytest :
- fichier commence par test_
- fonctions commencent par test_
- on utilise assert pour vérifier

Chaque test vérifie UNE seule chose.
C'est le principe du test unitaire : isoler chaque comportement.
"""

import pytest
from watchflow.core.graph import DAG, TaskNode
from watchflow.core.visualizer import ascii_graph

# ─────────────────────────────────────────────
# Helpers — fonctions vides pour les tests
# On ne met pas de logique dedans, ce sont juste
# des fonctions nommées pour remplir le DAG.
# ─────────────────────────────────────────────

def fetch(): pass
def clean(): pass
def validate(): pass
def train(): pass
def save(): pass


# ─────────────────────────────────────────────
# Tests — TaskNode
# ─────────────────────────────────────────────

def test_tasknode_default_name():
    """Le nom d'un TaskNode est le nom de la fonction par défaut."""
    node = TaskNode(fetch)
    assert node.name == "fetch"

def test_visualizer_contains_task_names():
    def fetch(): pass
    def clean(): pass
    def validate(): pass
    def train(): pass
    
    dag = DAG()
    dag.add_task(TaskNode(fetch))
    dag.add_task(TaskNode(clean,    depends_on=["fetch"]))
    dag.add_task(TaskNode(validate, depends_on=["fetch"]))
    dag.add_task(TaskNode(train,    depends_on=["clean", "validate"]))
    
    output= ascii_graph(dag)
    
    assert "fetch" in output
    assert "clean" in output
    assert "validate" in output
    assert "train" in output

def test_tasknode_name_override():
    """On peut surcharger le nom avec name_override."""
    node = TaskNode(fetch, name_override="my_fetch")
    assert node.name == "my_fetch"


def test_tasknode_default_status():
    """Un nouveau TaskNode est en statut 'pending'."""
    node = TaskNode(fetch)
    assert node.status == "pending"


def test_tasknode_no_dependencies():
    """Sans depends_on, la liste de dépendances est vide."""
    node = TaskNode(fetch)
    assert node.depends_on == []


# ─────────────────────────────────────────────
# Tests — DAG : ajout de tâches
# ─────────────────────────────────────────────

def test_dag_add_task():
    """On peut ajouter une tâche et la retrouver dans le DAG."""
    dag = DAG()
    dag.add_task(TaskNode(fetch))
    assert len(dag) == 1


def test_dag_duplicate_name_raises():
    """Ajouter deux tâches avec le même nom lève une ValueError."""
    dag = DAG()
    dag.add_task(TaskNode(fetch))
    with pytest.raises(ValueError, match="existe déjà"):
        dag.add_task(TaskNode(fetch))


def test_dag_get_task():
    """get_task retourne la bonne tâche par son nom."""
    dag = DAG()
    node = TaskNode(fetch)
    dag.add_task(node)
    assert dag.get_task("fetch") is node


def test_dag_get_unknown_task_raises():
    """get_task lève KeyError si la tâche n'existe pas."""
    dag = DAG()
    with pytest.raises(KeyError):
        dag.get_task("inexistant")


# ─────────────────────────────────────────────
# Tests — Tri topologique
# ─────────────────────────────────────────────

def test_topological_sort_linear():
    """
    Pipeline linéaire : fetch -> clean -> train
    L'ordre doit respecter les dépendances.
    """
    dag = DAG()
    dag.add_task(TaskNode(fetch))
    dag.add_task(TaskNode(clean, depends_on=["fetch"]))
    dag.add_task(TaskNode(train, depends_on=["clean"]))

    order = dag.topological_sort()
    names = [t.name for t in order]

    assert names.index("fetch") < names.index("clean")
    assert names.index("clean") < names.index("train")


def test_topological_sort_convergence():
    """
    Deux branches convergentes : fetch -> clean -> train
                                        validate -> train
    train doit toujours être en dernier.
    """
    dag = DAG()
    dag.add_task(TaskNode(fetch))
    dag.add_task(TaskNode(clean,    depends_on=["fetch"]))
    dag.add_task(TaskNode(validate, depends_on=["fetch"]))
    dag.add_task(TaskNode(train,    depends_on=["clean", "validate"]))

    order = dag.topological_sort()
    names = [t.name for t in order]

    assert names[-1] == "train"
    assert names.index("fetch") < names.index("clean")
    assert names.index("fetch") < names.index("validate")


def test_topological_sort_cycle_raises():
    """Un cycle entre deux tâches doit lever une ValueError."""
    def a(): pass
    def b(): pass

    dag = DAG()
    dag.add_task(TaskNode(a, depends_on=["b"]))
    dag.add_task(TaskNode(b, depends_on=["a"]))

    with pytest.raises(ValueError, match="Cycle"):
        dag.topological_sort()


def test_topological_sort_unknown_dependency_raises():
    """Dépendre d'une tâche non enregistrée lève une ValueError."""
    dag = DAG()
    dag.add_task(TaskNode(clean, depends_on=["fetch"]))  # fetch n'est pas dans le DAG

    with pytest.raises(ValueError, match="fetch"):
        dag.topological_sort()


def test_dag_reset():
    """reset() remet toutes les tâches à 'pending'."""
    dag = DAG()
    node = TaskNode(fetch)
    node.status = "done"
    node.result = 42
    dag.add_task(node)

    dag.reset()

    assert node.status == "pending"
    assert node.result is None
