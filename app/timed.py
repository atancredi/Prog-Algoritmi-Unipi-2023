import logger

def timed(f, _no_start=False):
    def wrapper(*args,**kwargs):
        t =logger.Timelog()
        _logger = kwargs["_logger"]
        if _logger and not _no_start:
            _logger.debug(f"start {f.__name__}")
        with t:
            ret = f(*args,**kwargs)
        if _logger:
            _logger.info("stop: {_time:.3f}".format(_time=t.time))
        return ret
    return wrapper