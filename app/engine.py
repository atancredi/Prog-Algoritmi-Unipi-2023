from shutil import copyfile
from os.path import abspath
from json import dump

from sorting import bubble_sort
from tqdm import tqdm

from networkx import Graph

def read(file, start=0):
    for l in enumerate(file, start=start):
        yield l

def load_data():

    # create a backup, load data, put to json with index and txId and dump to file
    path="raw_data/transactions.csv"
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
    path="raw_data/inputs.csv"
    print("Loading inputs")
    inputs = []
    with open(path, "r") as f:
        f.readline()
        for l in f.readlines():
            d = l.split(",")
            # txId, prevTxId, prevTxPos
            inputs.append((int(d[0]),int(d[1]),int(d[2])))
    print(f"Loaded {len(inputs)} inputs")

    # create a backup, load data, put to list with index and txId
    path="raw_data/outputs.csv"
    print("Loading outputs")
    outputs = []
    with open(path, "r") as f:
        f.readline()
        for l in f.readlines():
            d = l.split(",")
            # txId, txPos, amount
            outputs.append((int(d[0]),int(d[1]),int(d[3])))
    print(f"Loaded {len(outputs)} outputs")

    return (transactions, inputs, outputs)

def load_graph(data) -> Graph:
    transactions, inputs, outputs = data[0],data[1],data[2]

    graph = Graph()
    tq = tqdm(transactions)
    
    for tr in tq:
        tq.set_description(f"node {tr[0]}")
        graph.add_node(tr[0])

        # cerco gli archi entranti nel nodo con txId = tr
        __ = 0
        for _in in inputs:

            if _in[0] == tr[0]:
                # cerco l'amount dell'arco entrante negli outputs sfruttando prevTxId e prevTxPos
                _  = 0
                for _out in outputs:
                    _cicli_out += 1
                    if _out[0] == _in[1] and _out[1] == _in[2]:
                        graph.add_edge(_tr[0],_in[1],amount=_out[2])
                        break # solo una ce ne sta
                
                    _ += 1
                if _in[0] != inputs[__+1][0]:
                    break
            elif _in[0] > tr[0]:
                break
            __ += 1
                
    return graph
    
def dump_to_json(data):
    transactions, inputs, outputs = data[0],data[1],data[2]

    graph = {}
    tq = tqdm(transactions)
    _cicli_out = 0
    
    try:
        for tr in tq:
            tq.set_description(f"node {tr[0]}")
            _connections = []

            # cerco gli archi entranti nel nodo con txId = tr
            __ = 0
            for _in in inputs:

                if _in[0] == tr[0]:
                    # cerco l'amount dell'arco entrante negli outputs sfruttando prevTxId e prevTxPos
                    _  = 0
                    for _out in outputs:
                        _cicli_out += 1
                        if _out[0] == _in[1] and _out[1] == _in[2]:
                            _connections.append((_in[1],_out[2]))
                            break # solo una ce ne sta
                    
                        _ += 1
                    if _in[0] != inputs[__+1][0]:
                        break
                elif _in[0] > tr[0]:
                    break
                __ += 1
                    
            tq.set_postfix({"cicli_out":_cicli_out})
            if len(_connections) > 0:
                graph[tr[0]] = _connections
                        
    except KeyboardInterrupt:
        pass
    
    dump(graph, open("graph.json", "w"))


def execute(data):
    dump_to_json(data)