from enum import Enum
from typing import List

class Graph:
    pass

class Orientation(Enum):
    TO: int = 0
    FROM: int = 1

class Arch:
    id: int
    amount: int
    addressId: int #NOSONAR
    orientation: Orientation

    def __init__(self, id, addressId, amount, orientation: Orientation) -> None: # NOSONAR
        self.id = id
        self.addressId = addressId
        self.amount = amount
        self.orientation = orientation
            

    @property
    def __dict__(self) -> dict:
        return {"id": self.id,
             "amount": self.amount,
             "addressId": self.addressId,
             "orientation": self.orientation.name}


class Node:
    id: int
    inputs: List[Arch] = []
    outputs: List[Arch] = []

    @property
    def __dict__(self) -> dict:
        return {"id": self.id,
             "inputs": [i.__dict__ for i in self.inputs],
             "outputs":  [i.__dict__ for i in self.outputs]}