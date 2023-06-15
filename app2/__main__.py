from logging import Logger
import polars as pd
from typing import Optional
import logger

from tqdm import tqdm
from json_stream import streamable_dict
from json import dump
from timed import timed

DATA_FOLDER = "raw_data"

# Funzione che filla il contenuto di un nodo
def get_connections(tx_id : int, __in: pd.LazyFrame, __out: pd.LazyFrame):
    connections = []
    for t in __in.filter(pd.col("txId") == tx_id).collect().iter_rows(named=True):
        weight = __out.filter(pd.col("txId") == tx_id).filter(pd.col("txPos") == t["prevTxPos"]).select(pd.col("amount")).collect().get_column("amount").to_list()[0]
        connections.append((t["prevTxId"],weight))
    return connections

@streamable_dict
def duce(__tr: pd.LazyFrame,__in: pd.LazyFrame,__out: pd.LazyFrame, l: Logger):
    try:
        for transaction in tqdm([i for i in __tr.collect().iter_rows(named=True)]):
            n = get_connections(transaction["txId"], __in, __out)
            yield transaction["txId"], (n if len(n) > 0 else transaction["isCoinbase"])
    except Exception as e:
        print(e)
        l.error(f"Error on transaction with ID {transaction['txId']}")

if __name__ == '__main__':
    l = logger.new_logger()

    # pre load all data into ram! (dropping columns)
    l.info("Loading transactions.csv")
    __tr = pd.scan_csv(DATA_FOLDER+"/transactions.csv")
    __tr.drop("blockId")
    __tr.drop("")
    l.info("Loading inputs.csv")
    __in = pd.scan_csv(DATA_FOLDER+"/inputs.csv")
    l.info("Loading outputs.csv")
    __out = pd.scan_csv(DATA_FOLDER+"/outputs.csv")
    __out.drop("addressId")
    __out.drop("scriptType")
    
    graph = duce(__tr,__in,__out,l)
    dump(graph, open("graph.json", "w"))
    