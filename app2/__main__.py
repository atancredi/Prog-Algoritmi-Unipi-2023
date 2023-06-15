from logging import Logger
import polars as pd
from typing import Optional
import logger

from json import dumps
from timed import timed

REDUCED_LINE_LIMIT = 1000
DATA_FOLDER = "raw_data"


def iterate(iterable):
    return [x for x in iterable]

# Funzione che filla il contenuto di un nodo
def get_nodes(tx_id : int, __in: pd.LazyFrame, __out: pd.LazyFrame):
    connections = []
    for t in __in.filter(pd.col("txId") == transaction["txId"]).collect().iter_rows(named=True):
        connections.append((t["prevTxId"],
                            [tt for tt in __out.filter(pd.col("txId") == tx_id).filter(pd.col("txPos") == t["prevTxPos"]).collect().iter_rows(named=True)][0]["amount"]))
    return connections

from tqdm import tqdm
if __name__ == '__main__':
    l = logger.new_logger()

    # pre load all data into ram!
    __tr = pd.scan_csv(DATA_FOLDER+"/transactions.csv")
    __in = pd.scan_csv(DATA_FOLDER+"/inputs.csv")
    __out = pd.scan_csv(DATA_FOLDER+"/outputs.csv")
    
    graph = {}

    transactions = [i for i in tr.collect().iter_rows(named=True)]
    for transaction in tqdm(transactions):
        # l.info(f"Reading transaction {transaction['txId']}")
        if transaction["txId"] not in graph:
            # l.debug(f"Adding transaction {transaction['txId']} to graph")
            n = get_nodes(transaction["txId"])
            graph[transaction["txId"]] = n
            print(n)
