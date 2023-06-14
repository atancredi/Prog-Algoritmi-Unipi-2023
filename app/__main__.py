import polars as pd
from typing import Optional
from json import dumps

from logging import Logger
from graph import Graph, Node, Arch, Orientation
from utils import logger
from timed import timed

REDUCED_LINE_LIMIT = 1000
DATA_FOLDER = "raw_data"

def get_query(filename:str, lenght: int = REDUCED_LINE_LIMIT,_reduced: bool = False):
    f = pd.scan_csv(filename,ignore_errors=True,separator=",")
    query = f.slice(0,lenght if _reduced else None)

    return query

@timed
def get_all_for_transaction(query: pd.LazyFrame, tx_id : int, _logger: Optional[Logger] = None):
    tr = query.filter(pd.col("txId") == tx_id).collect()
    tot = []

    for t in tr.iter_rows(named=True):
        tot.append({
            "tx": t,
            "inputs": [t for t in get_query(DATA_FOLDER+"/inputs.csv").filter(pd.col("txId") == t["txId"]).collect().iter_rows(named=True)],
            "outputs": [t for t in get_query(DATA_FOLDER+"/outputs.csv").filter(pd.col("txId") == t["txId"]).collect().iter_rows(named=True)]
        })
    
    return tot

@timed
def get_node_from_transaction(transaction_query: pd.LazyFrame, id: int, l: Logger) -> Node:
    node_info = get_all_for_transaction(transaction_query, id,l)[0]

    n = Node()
    n.id = node_info["tx"]["txId"]

    # TODO: sono giuste le orientazioni?

    for _ in node_info["inputs"]:
        d = [i for i in o.filter(pd.col("txId") == _["prevTxId"]).filter(pd.col("txPos") == _["prevTxPos"]).collect().iter_rows(named=True)][0]
        a = Arch(_["prevTxId"],d["addressId"],d["amount"],Orientation.FROM)
        if a.id != n.id or True: # NOSONAR
            n.inputs.append(a)
    
    for _ in node_info["outputs"]:
        a = Arch(_["txId"],_["addressId"],_["amount"], Orientation.TO)
        if a.id != n.id or True: # NOSONAR
            n.outputs.append(a)
    return n

@timed
def get_nodes(tr,l):
    nodes = []
    r = 0
    for t in tr.collect().iter_rows(named=True):
        nn = get_node_from_transaction(tr,t["txId"],l)
        nodes.append(nn)
        l.info("node: "+str(nn.id))
        r += 1
    return nodes,r

if __name__ == '__main__':
    l = logger.new_logger()
    tr = get_query(DATA_FOLDER+"/transactions.csv", _reduced=True, lenght=1)
    o = get_query(DATA_FOLDER+"/outputs.csv")
    
    # this contains all informations about a transaction (node)
    l.info("Start getting nodes")
    nodes, r = get_nodes(tr,l)
    
    print("Nodes:")
    print(len(nodes), r)

    for n in nodes:
        print(n.__dict__)
