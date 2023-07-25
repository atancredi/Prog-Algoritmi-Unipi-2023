from time import time
from tqdm import tqdm
from typing import List, Any, Dict, Tuple
from json import dump

def _read_edges(filename):
    start = time()
    _ = 0
    with open(filename, "r") as f:
        while True:
            line = f.readline()
            if not line:
                break
            yield tuple([int(x) for x in line.replace("\n","").split(" ")])
            _ += 1
    print(f"Loaded edges in {time()-start} seconds")

def load(filename = "results/edgeslist.txt"):
    return list(_read_edges(filename))
    
def analyze(data):

    class DSUResult:
        isolated_nodes: int
        n_connected_components: int
        max_component_lenght : int
        max_component: Any

        def __init__(self) -> None:
            self.isolated_nodes = 0
            self.n_connected_components = 0
            self.max_component_id = 0
            self.max_component_lenght = 0

        def compute(self, components):
            for c in components:
                if len(components[c]) == 0:
                    self.isolated_nodes += 1 
                if len(components[c]) > self.max_component_lenght:
                    self.max_component_lenght = len(components[c])
                    self.max_component = components[c]
                self.n_connected_components += 1

    class DisjointSet:

        # Disjoint Set data structure with union by rank and find with path compression
        # Time complexity: circa O(n) for reasonable n (n < 10^600)

        edges: List[Any]
        parents: Dict[Any,Any]
        ranks: Dict[Any,Any]
        graph: Dict[Any,List[Tuple[int,int]]]

        def __init__(self, data):
            self.parents = {}
            self.ranks = {}
            self.cycles = []
            self.graph = {}
            self.edges = data
        
        # find the root of a component
        def find(self, node):
            # new node
            if node not in self.parents:
                self.parents[node] = node
                self.ranks[node] = 0
            
            # node has parent but it is not the root
            # find root of the node
            if self.parents[node] != node:
                self.parents[node] = self.find(self.parents[node])
            
            return self.parents[node]

        # merge two components
        def union(self,node1,node2):
            root1 = self.find(node1)
            root2 = self.find(node2)

            # if different root merge them
            if root1 != root2:
                # check if merging creates a cycle
                if self.ranks[root1] < self.ranks[root2]:
                    self.parents[root1] = root2
                elif self.ranks[root1] > self.ranks[root2]:
                    self.parents[root2] = root1
                else:
                    self.parents[root2] = root1
                    self.ranks[root1] += 1

                    # Cycle detected - in which component?
                    self.cycles.append((root1,root2))
        
        def initialize(self):
            for d in tqdm(self.edges):
                # create adj list
                if int(d[0]) not in self.graph.keys():
                    self.graph[int(d[0])] = []
                if (int(d[1]),int(d[2])) not in self.graph[d[0]]:
                    self.graph[int(d[0])].append((int(d[1]),int(d[2])))

                # init DSU structure
                if self.union(int(d[0]), int(d[1])):
                    # Cycle detected
                    return None

        def find_components(self):
            components = {}
            for node in self.parents.keys():
                root = self.find(node)
                if root not in components:
                    components[root] = dict()

                if node in self.graph.keys(): components[root][node] = self.graph[node]
                else: components[root][node] = []

            return components

    dsu = DisjointSet(data)
    dsu.initialize()

    components = dsu.find_components()
    if components is None:
        print("cycle")
    else:
        res = DSUResult()
        res.compute(components)
        print(f"isolated nodes: {res.isolated_nodes}")
        print(f"n connected components: {res.n_connected_components}")
        print(f"max component lenght: {res.max_component_lenght}")
        
        dump(res.max_component, open("results/max_component.json", "w"))
