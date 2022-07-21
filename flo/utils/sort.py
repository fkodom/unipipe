from __future__ import annotations

from typing import List, Sequence

import networkx as nx

from flo.dsl import Component


def topological_sort(components: Sequence[Component]) -> List[Component]:
    digraph = nx.DiGraph(
        [
            (_input, component.name)
            for component in components
            for _input in component.inputs.values()
        ]
    )
    ordered = nx.topological_sort(digraph)
    components_by_name = {component.name: component for component in components}
    result = [components_by_name.get(name, None) for name in ordered]
    return [x for x in result if x is not None]
