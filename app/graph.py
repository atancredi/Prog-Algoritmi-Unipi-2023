import networkx as nx
from json import loads
import matplotlib.pyplot as plt

# buona implementazione di come creare un grafo con la libreria e plottarlo
# TODO cambiare la mia implementazione della creazione del grafo per renderla più vicina a questa
# TODO eventualmente cambiare linguaggio di programmazione per la creazione del grafo, l'analisi conviene farla in python caricando il json
# TODO estrarre le componenti connesse piu grandi e plottare quelle piuttosto che roba a cazzo

if __name__ == "__main__":
    with open("graph.json","r") as f:
        cnt = f.read()
        if cnt[-1] != "}":
            cnt += "}"
        print("loading")
        _cnt = loads(cnt)
        graph = nx.Graph()
        stop = 1000
        for node_id in _cnt.keys():
            # cosi sto tenendo solo le componenti connesse (tolgo i nodi isolati)
            if _cnt[node_id] != 1:
                graph.add_node(node_id)
                for edge in _cnt[node_id]:
                    graph.add_edge(node_id,edge[0],amount=edge[1])
                stop -= 1
            if stop == 0:
                break
                
        print("loaded")
        nx.draw(graph, with_labels=True, font_weight='bold')
        plt.savefig("graph.png")