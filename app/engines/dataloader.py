from json import dump
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

def load(folder="raw_data"):

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

    return (transactions, in_map, out_map)

def analyze(data):

    transactions, in_map, out_map = data[0],data[1],data[2]

    # Creation of adjacency list and all isolated components
    graph_isolated_nodes = {}
    
    tq = tqdm(transactions)
    with open("results/edgeslist.txt","a") as f:
        for tr in tq:
            tq.set_description(f"node {tr[0]}")

            # cerco gli archi entranti nel nodo con txId = tr
            _connections = []

            if tr[0] in in_map.keys():
                _connections = find_connections(in_map[tr[0]],out_map)
                for conn in _connections:
                    f.write(f"{tr[0]} {conn[0]} {conn[1]}\n")

            graph_isolated_nodes[tr[0]] = {tr[0]:_connections}

    dump(graph_isolated_nodes, open("results/graph_isolated_nodes.json", "w"))
    