from networkx import DiGraph, draw
import matplotlib.pyplot as plt
from tqdm import tqdm

def create_graph(graph_raw: dict) -> DiGraph:
    g = DiGraph()
    for k in tqdm(graph_raw.keys()):
        g.add_node(k)
        for c in graph_raw[k]:
            g.add_edge(k, c[0],amount=c[1])
    return g

def draw_graph(graph: DiGraph) -> None:
    draw(graph, with_labels=True, font_weight='bold')
