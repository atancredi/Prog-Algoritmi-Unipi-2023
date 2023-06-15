import logger

def timed(f):
    def wrapper(a,b,_logger):
        t =logger.Timelog()
        if _logger:
            _logger.debug(f"start getting node {b}")
        with t:
            ret = f(a,b)
        if _logger:
            _logger.info("stop: {_time:.3f}".format(_time=t.time),extra=ret)
        return ret
    return wrapper