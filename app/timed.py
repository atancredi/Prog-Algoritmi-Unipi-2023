from utils import logger

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