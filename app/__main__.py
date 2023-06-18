import sys
import engine as d
from importlib import reload

data = None
if __name__ == "__main__":
    while True:
        if not data:
            data = d.load_data()
        try:
            d.execute(data)
        except Exception as ex:
            print(ex)
            
        print("Press enter to re-run the script, CTRL-C to exit")
        sys.stdin.readline()
        reload(d)