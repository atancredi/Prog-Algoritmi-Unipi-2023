import sys
import engine as d
import read as rr
from importlib import reload

data = None
if __name__ == "__main__":
    while True:
        if not data:
            data = rr.read_json_graph()
        try:
            rr.execute(data)
        except Exception as ex:
            print(ex)
            
        print("Press enter to re-run the script, CTRL-C to exit")
        sys.stdin.readline()
        reload(rr)
        # reload(d)