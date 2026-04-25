"""
visualizer.py — Visualisation ASCII et Mermaid du DAG.

Deux formats de sortie :
- ASCII  : affichage direct dans le terminal, zéro dépendance
- Mermaid: format texte que GitHub rend automatiquement en diagramme

Pourquoi séparer la visualisation du DAG ?
Même raison que le Runner : responsabilité unique.
Le DAG sait ce qu'il contient, le Visualizer sait comment l'afficher.
"""

from __future__ import annotations
from .graph import DAG


def _build_children(dag: DAG) -> dict[str, list[str]]:
    """
    Construit un dict enfants à partir des dépendances.

    Le DAG stocke les dépendances (parents) de chaque tâche.
    Pour afficher un arbre, on a besoin de l'inverse : les enfants.

    parents  : clean.depends_on = ["fetch"]   -> fetch est parent de clean
    enfants  : fetch -> [clean, validate]      -> fetch a deux enfants

    C'est une inversion de la structure — un pattern courant en théorie des graphes.
    """
    children: dict[str, list[str]] = {name: [] for name in dag._tasks}
    for task in dag._tasks.values():
        for dep in task.depends_on:
            children[dep].append(task.name)
    return children


def _find_roots(dag: DAG) -> list[str]:
    """Retourne les tâches sans dépendances (racines du graphe)."""
    return [
        name for name, task in dag._tasks.items()
        if not task.depends_on
    ]


def ascii_graph(dag: DAG) -> str:
    """
    Génère une représentation ASCII du DAG.

    Exemple de sortie :
        fetch_data
        ├── clean_data
        │   └── train_model
        └── validate_data
            └── train_model

    Algorithme : DFS récursif depuis chaque racine.
    On passe un préfixe qui s'adapte à la profondeur et à la position
    (dernier enfant ou non) pour dessiner les branches correctement.
    """
    children = _build_children(dag)
    lines = []

    def draw(name: str, prefix: str, is_last: bool) -> None:
        connector = "└── " if is_last else "├── "
        task = dag._tasks[name]

        status_icons = {
            "pending": " ",
            "running": "~",
            "done":    "✓",
            "failed":  "✗",
        }
        icon = status_icons.get(task.status, " ")

        lines.append(f"{prefix}{connector}[{icon}] {name}")

        child_prefix = prefix + ("    " if is_last else "│   ")
        kids = children[name]
        for i, child in enumerate(kids):
            draw(child, child_prefix, is_last=(i == len(kids) - 1))

    roots = _find_roots(dag)
    lines.append(f"\n  DAG watchflow ({len(dag)} tâche(s))\n")

    for i, root in enumerate(roots):
        is_last_root = (i == len(roots) - 1)
        connector = "└── " if is_last_root else "├── "
        task = dag._tasks[root]
        status_icons = {"pending": " ", "running": "~", "done": "✓", "failed": "✗"}
        icon = status_icons.get(task.status, " ")
        lines.append(f"  {connector}[{icon}] {root}")

        child_prefix = "      " if is_last_root else "  │   "
        kids = children[root]
        for j, child in enumerate(kids):
            draw(child, child_prefix, is_last=(j == len(kids) - 1))

    return "\n".join(lines)


def mermaid_graph(dag: DAG) -> str:
    """
    Génère un diagramme au format Mermaid.

    Mermaid est un langage de diagrammes que GitHub rend
    automatiquement dans les README et issues. Exemple :

        ```mermaid
        graph TD
            fetch_data --> clean_data
            fetch_data --> validate_data
            clean_data --> train_model
        ```

    Ce format est parfait pour la documentation du projet.
    """
    lines = ["graph TD"]
    for task in dag._tasks.values():
        if not task.depends_on:
            lines.append(f"    {task.name}")
        for dep in task.depends_on:
            lines.append(f"    {dep} --> {task.name}")
    return "\n".join(lines)



def print_graph(dag: DAG, fmt: str = "ascii") -> None:
    """
    Affiche le graphe dans le terminal.

    Paramètres :
        fmt : "ascii" (défaut) ou "mermaid"
    """
    if fmt == "mermaid":
        print(mermaid_graph(dag))
    else:
        print(ascii_graph(dag))
