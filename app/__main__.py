import polars as pd
from typing import Optional

from logging import Logger
from utils import logger

REDUCED_LINE_LIMIT = 1000
DATA_FOLDER = "raw_data"

def get_query(filename:str, lenght: int = REDUCED_LINE_LIMIT,_reduced: bool = False):
    f = pd.scan_csv(filename,ignore_errors=True,separator=",")
    query = f.slice(0,lenght if _reduced else None)

    return query

def timed(f):
    def wrapper(*args, **kwargs):
        t =logger.Timelog()
        _logger = kwargs.get("_logger")
        if _logger:
            _logger.debug("start")
        with t:
            ret = f(*args, **kwargs)
        if _logger:
            _logger.info("stop: {_time:.3f}".format(_time=t.time))
        return ret
    return wrapper

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

if __name__ == '__main__':
    l = logger.new_logger()
    tr = get_query(DATA_FOLDER+"/transactions.csv")