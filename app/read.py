from json import load
from tqdm import tqdm

def read_json_graph():
    graph = {}
    print("Loading graph from file")
    graph = load(open("graph.json", "r"))
    print(graph[list(graph.keys())[0]])
    return graph

def find_connected_components(id:int, graph:dict,l):
    for conn in graph[id]:
        find_connected_components(conn[0], graph,l)
        l += 1
    return l

def execute(graph):
    l = 0
    print(find_connected_components(list(graph.keys())[0],graph,l))

if __name__ == "__main__":
    read_json_graph()