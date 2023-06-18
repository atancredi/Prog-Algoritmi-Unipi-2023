from engine import load_data, dump_to_json

def load():
    return load_data()

def execute(data):
    dump_to_json(data)