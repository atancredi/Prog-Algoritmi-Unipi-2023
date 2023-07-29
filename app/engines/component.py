import json
from tqdm import tqdm

def find_connections(ins, out_map:dict):
    _connections = []
    try:
        for _in in ins:
            # cerco l'amount dell'arco entrante negli outputs sfruttando prevTxId e prevTxPos
            if f"{_in[0]},{_in[1]}" in out_map.keys():
                _connections.append((_in[0],out_map[f"{_in[0]},{_in[1]}"]))
    except Exception:
        print("pd2")

    return _connections

def load() -> dict:
    # load the biggest connected component
    # returns the adjacency list of the biggest connected component
    folder="raw_data"
    # create a backup, load data, put to json with index and txId and dump to file
    path=folder+"/transactions.csv"
    # copyfile(abspath(path), path.split(".")[0]+"_bak.csv")
    print("Loading transactions")
    transactions = []
    with open(path, "r") as f:
        f.readline()
        for l in f.readlines():
            d = l.split(",")
            # txId, isCoinbase, fee
            transactions.append((int(d[2]),int(d[3]),int(d[4])))
    print(f"Loaded {len(transactions)} transactions")

    # create a backup, load data, put to list with index and txId
    path=folder+"/inputs.csv"
    print("Loading inputs")
    in_map = {}
    with open(path, "r") as f:
        f.readline()
        for l in f.readlines():
            d = l.split(",")
            if int(d[0]) in in_map.keys():
                in_map[int(d[0])].append((int(d[1]),int(d[2])))
            else: in_map[int(d[0])] = [(int(d[1]),int(d[2]))]
            # txId, prevTxId, prevTxPos
    print(f"Loaded {len(in_map.keys())} inputs")

    # create a backup, load data, put to list with index and txId
    out_map = {}
    path=folder+"/outputs.csv"
    print("Loading outputs")
    with open(path, "r") as f:
        f.readline()
        _ = 0
        for l in f.readlines():
            d = l.split(",")
            # txId, txPos, amount
            out_map[f"{d[0]},{d[1]}"] = int(d[3])
    print(f"Loaded {len(out_map.keys())} outputs")

    # read comp id
    comp = json.load(open("results/max_component_.json","r"))

    # read edges list
    # create adjacency list of biggest connected component
    graph = {}
    for c in tqdm(comp):
        if c in in_map.keys():
            _connections = find_connections(in_map[c],out_map)
        graph[c] = _connections
    
    return graph
    
def analyze(graph):
    def is_cyclic(graph):
        visited = set()
        rec_stack = set()

        for node in graph:
            if node not in visited and dfs(node, visited, rec_stack, graph):
                print("cycle detected for node")
                return True

        return False

    def dfs(node, visited, rec_stack, graph):
        visited.add(node)
        rec_stack.add(node)

        for neighbor, weight in tqdm(graph[node]):
            if neighbor not in visited:
                if dfs(neighbor, visited, rec_stack, graph):
                    print(rec_stack)
                    return True
            elif neighbor in rec_stack:
                print(rec_stack)
                return True

        rec_stack.remove(node)
        return False

    print("analyzing graph")

    print("is cyclic?")
    print(is_cyclic(graph))

    # devo renderla non direzionata
    undirected_graph = {}

    print("making graph undirected with provided condiction")
    for node, neighbors in tqdm(graph.items()):
        if node not in undirected_graph:
            undirected_graph[node] = set()

        for neighbor in neighbors:

            # amt dell'arco node, neighbor
            # amt dell'arco neightbor, node
            undirected_graph[node].add(neighbor)

            if neighbor not in undirected_graph:
                undirected_graph[neighbor] = set()

            undirected_graph[neighbor].add(node)
    
    json.dump(undirected_graph, open("results/max_component_undirected.json", "w"))

    print("is cyclic now?")
    print(is_cyclic(undirected_graph))