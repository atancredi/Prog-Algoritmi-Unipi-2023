import sys
from engines import component as d
from importlib import reload
from traceback import print_exc

data = None
if __name__ == "__main__":
    while True:
        if not data:
            data = d.load()
        try:
           d.analyze(data)
        except KeyboardInterrupt:
            print("Interrupted")
        except Exception as ex:
            print(ex)
            print_exc()
            print("aaex")
            
        print("Press enter to re-run the script, CTRL-C to exit")
        sys.stdin.readline()
        reload(d)
        # reload(d)