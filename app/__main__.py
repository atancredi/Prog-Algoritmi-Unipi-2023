from logging import Logger
import polars as pd
from typing import Optional
import logger
from os.path import exists, abspath, basename
from os import remove
from shutil import copyfile

from tqdm import tqdm
from json_stream import streamable_dict
from json import dump, loads
from timed import timed

DATA_FOLDER = "raw_data"

# Funzione che filla il contenuto di un nodo
def get_connections(tx_id : int, __in: pd.LazyFrame, __out: pd.LazyFrame, l: Logger):
    connections = []
    for t in __in.filter(pd.col("txId") == tx_id).collect().iter_rows(named=True):
        weight = __out.filter(pd.col("txId") == t["prevTxId"]).filter(pd.col("txPos") == t["prevTxPos"]).select(pd.col("amount")).collect().get_column("amount").to_list()
        if len(weight) == 1:
            connections.append((t["prevTxId"],weight[0]))
        elif len(weight) > 1:
            l.warning(f"more than one amount for txId: {tx_id}",extra={"weight":weight})
        else:
            l.warning(f"no amount for txId: {tx_id}")
    return connections


@timed
def collect_transactions(graph_file="graph.json", _logger: Optional[Logger] = None):
    
    l.info("Loading transactions.csv")
    __tr = pd.scan_csv(DATA_FOLDER+"/transactions.csv")
    __tr.drop("blockId")
    __tr.drop("")

    existing_keys = []
     # try to load an existing graph
    if exists(graph_file):
        bak = basename(graph_file).split(".")[0]+"_bak.json"
        if exists(bak):
            remove(bak)
        
        copyfile(abspath(graph_file), bak)
        with open(graph_file, "r") as f:
            r = f.read()
        if r[-1] != "}":
            r += "}"
        graph = loads(r)
        existing_keys = [int(i) for i in list(graph.keys())]
        _logger.info(f"loaded {len(existing_keys)} existing keys, with last id being {existing_keys[-1]}")

    return __tr.filter(~pd.col("txId").is_in(existing_keys)).collect().iter_rows(named=True)

@streamable_dict
def duce(transactions,__in: pd.LazyFrame,__out: pd.LazyFrame, l: Logger):
    try:
        progress = tqdm(transactions)
        for transaction in progress:
            progress.set_description(f"processing {transaction['txId']}",True)
            n = get_connections(transaction["txId"], __in, __out, l)
            yield transaction["txId"], (n if len(n) > 0 else transaction["isCoinbase"])
    except Exception as e:
        print(e)
        l.error(f"Error on transaction with ID {transaction['txId']}")

if __name__ == '__main__':
    l = logger.new_logger()

    GRAPH_FILE = "graph.json"

    # pre load all data! (dropping columns)
    transactions = collect_transactions(_logger=l)
    l.info("Loading inputs.csv")
    __in = pd.scan_csv(DATA_FOLDER+"/inputs.csv",has_header=True)
    l.info("Loading outputs.csv")
    __out = pd.scan_csv(DATA_FOLDER+"/outputs.csv",has_header=True)
    __out.drop("addressId")
    __out.drop("scriptType")
    
    # nodo difettoso
    # print(get_connections(1077,__in,__out))

    try:
        graph = duce(transactions,__in,__out,l)
        dump(graph, open(GRAPH_FILE, "a"))
    except KeyboardInterrupt:
        l.warning("keyboard interruption")
    except Exception as ex:
        print(ex)
        l.error("exception")
    
    with open(GRAPH_FILE, "r") as f:
        rr = f.read()
    rr = rr.replace("{", ",")
    rr = "{" + rr[1:]
    with open(GRAPH_FILE, "w") as f:
        f.write(rr)
    
    l.info("end of program")