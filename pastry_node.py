from typing import List, Optional, Dict
from dht_hash import DHTHasher

class PastryNode:
    def __init__(self, ip: str, port: int, m_bits: int = 160, b: int = 4, l: int = 16, m: int = 32):

        self.ip = ip
        self.port = port
        self.address = f"{ip}:{port}"
        self.hasher = DHTHasher(m_bits)
        self.m_bits = m_bits
        self.id = self.hasher.hash_node_id(self.address)
        self.hex_id = self.hasher.get_hex_id(self.id, digits=m_bits//4)
        
        self.b = b
        self.base = 2 ** b
        self.leaf_set_size = l
        self.neighborhood_size = m
        

        self.leaf_smaller: List['PastryNode'] = []

        self.leaf_larger: List['PastryNode'] = []
        

        self.neighborhood_set: List['PastryNode'] = []
        

        self.num_rows = self.m_bits // self.b
        self.routing_table: Dict[int, Dict[int, 'PastryNode']] = {}
        
        self.data = {} 

    def __repr__(self):
        return f"<PastryNode {self.address} ID:{self.hex_id[:8]}...>"

    def _shared_prefix_length(self, other_id_hex: str) -> int:

        length = 0
        min_len = min(len(self.hex_id), len(other_id_hex))
        while length < min_len and self.hex_id[length] == other_id_hex[length]:
            length += 1
        return length

    def add_node(self, node: 'PastryNode'):

        if node.id == self.id:
            return

        self._update_leaf_set(node)
        self._update_routing_table(node)
        self._update_neighborhood_set(node)

    def _update_leaf_set(self, node: 'PastryNode'):

        if node.id < self.id:

            if node not in self.leaf_smaller:
                self.leaf_smaller.append(node)

                self.leaf_smaller.sort(key=lambda x: x.id, reverse=True)

                if len(self.leaf_smaller) > self.leaf_set_size // 2:
                    self.leaf_smaller = self.leaf_smaller[:self.leaf_set_size // 2]
        else:

            if node not in self.leaf_larger:
                self.leaf_larger.append(node)

                self.leaf_larger.sort(key=lambda x: x.id)
 
                if len(self.leaf_larger) > self.leaf_set_size // 2:
                    self.leaf_larger = self.leaf_larger[:self.leaf_set_size // 2]

    def _update_routing_table(self, node: 'PastryNode'):
        """Updates the routing table with a new node."""
        sh_len = self._shared_prefix_length(node.hex_id)
        

        if sh_len >= self.num_rows:
            return

        digit = int(node.hex_id[sh_len], 16) 
        
        if sh_len not in self.routing_table:
            self.routing_table[sh_len] = {}
            
        if digit not in self.routing_table[sh_len]:
            self.routing_table[sh_len][digit] = node
        else:
            pass

    def _update_neighborhood_set(self, node: 'PastryNode'):

        if node not in self.neighborhood_set:
            self.neighborhood_set.append(node)

            if len(self.neighborhood_set) > self.neighborhood_size:
                self.neighborhood_set.pop(0) 
    def get_leaf_set(self) -> List['PastryNode']:

        return self.leaf_smaller + self.leaf_larger

    def is_in_leaf_set_range(self, key_id: int) -> bool:

        if not self.leaf_smaller and not self.leaf_larger:
            return True
        
        min_id = self.leaf_smaller[0].id if self.leaf_smaller else self.id
        max_id = self.leaf_larger[-1].id if self.leaf_larger else self.id
        
        # Handle wrap-around
        if min_id <= max_id:
            return min_id <= key_id <= max_id
        else:
            return key_id >= min_id or key_id <= max_id

    def find_closest_in_leaf_set(self, key_id: int) -> 'PastryNode':

        candidates = self.get_leaf_set() + [self]
        
        def distance(node_id: int) -> int:

            if node_id >= key_id:
                return node_id - key_id
            else:
                return (2 ** self.m_bits) - key_id + node_id
        
        return min(candidates, key=lambda n: distance(n.id))

    def route(self, key_id: int, hops: int = 0, visited: Optional[set] = None) -> tuple['PastryNode', int]:
        if visited is None:
            visited = set()
        
        if self.id in visited or hops > 100:
            return (self, hops)
        
        visited.add(self.id)
        
        key_hex = self.hasher.get_hex_id(key_id, digits=self.m_bits//4)
        shared_prefix = self._shared_prefix_length(key_hex)
        
        if self.is_in_leaf_set_range(key_id):
            closest = self.find_closest_in_leaf_set(key_id)
            if closest is self:
                return (self, hops)
            if closest.id not in visited:
                return closest.route(key_id, hops + 1, visited)
            return (self, hops)
        
        if shared_prefix < self.num_rows:
            next_digit = int(key_hex[shared_prefix], 16)
            
            if shared_prefix in self.routing_table and next_digit in self.routing_table[shared_prefix]:
                next_node = self.routing_table[shared_prefix][next_digit]
                if next_node.id not in visited:
                    return next_node.route(key_id, hops + 1, visited)
            
            for digit in range(self.base):
                if shared_prefix in self.routing_table and digit in self.routing_table[shared_prefix]:
                    candidate = self.routing_table[shared_prefix][digit]
                    if candidate.id not in visited:
                        candidate_key_prefix = self._shared_prefix_length_between(candidate.hex_id, key_hex)
                        if candidate_key_prefix > shared_prefix:
                            return candidate.route(key_id, hops + 1, visited)
        
        for row_idx in range(shared_prefix + 1, self.num_rows):
            if row_idx in self.routing_table:
                for digit, node in self.routing_table[row_idx].items():
                    if node.id not in visited:
                        node_key_prefix = self._shared_prefix_length_between(node.hex_id, key_hex)
                        if node_key_prefix > shared_prefix:
                            return node.route(key_id, hops + 1, visited)
        
        closest_leaf = self.find_closest_in_leaf_set(key_id)
        if closest_leaf is not self and closest_leaf.id not in visited:
            return closest_leaf.route(key_id, hops + 1, visited)
        
        return (self, hops)

    def _shared_prefix_length_between(self, hex_id1: str, hex_id2: str) -> int:

        length = 0
        min_len = min(len(hex_id1), len(hex_id2))
        while length < min_len and hex_id1[length] == hex_id2[length]:
            length += 1
        return length

    def insert(self, key: str, value) -> tuple[bool, int]:

        key_id = self.hasher.hash_key(key)
        responsible_node, hops = self.route(key_id)
        responsible_node.data[key] = value
        return (True, hops)

    def lookup(self, key: str) -> tuple[Optional[any], int]:
 
        key_id = self.hasher.hash_key(key)
        responsible_node, hops = self.route(key_id)
        value = responsible_node.data.get(key)
        return (value, hops)

    def delete(self, key: str) -> tuple[bool, int]:

        key_id = self.hasher.hash_key(key)
        responsible_node, hops = self.route(key_id)
        if key in responsible_node.data:
            del responsible_node.data[key]
            return (True, hops)
        return (False, hops)

    def update(self, key: str, value) -> tuple[bool, int]:

        return self.insert(key, value)

    def join(self, introducer: Optional['PastryNode'] = None):

        if introducer is None:

            return
        

        all_nodes = set()
        to_visit = [introducer]
        visited = set()
        
        while to_visit:
            current = to_visit.pop(0)
            if current.id in visited:
                continue
            visited.add(current.id)
            all_nodes.add(current)
            

            for node in current.get_leaf_set():
                if node.id not in visited:
                    to_visit.append(node)
            for node in current.neighborhood_set:
                if node.id not in visited:
                    to_visit.append(node)
            for row in current.routing_table.values():
                for node in row.values():
                    if node.id not in visited:
                        to_visit.append(node)
        

        for node in all_nodes:
            self.add_node(node)
            node.add_node(self)
        
        # Transfer data: request keys we're now responsible for from other nodes
        for node in all_nodes:
            keys_to_transfer = []
            for key, value in list(node.data.items()):
                key_id = self.hasher.hash_key(key)
                # Check if we are now the closest to this key
                responsible, _ = self.route(key_id)
                if responsible is self:
                    keys_to_transfer.append((key, value))
            
            for key, value in keys_to_transfer:
                self.data[key] = value
                del node.data[key]

    def leave(self, transfer_data: bool = True):
        """
        Leave the Pastry network gracefully.
        """
        if transfer_data:

            all_nodes = self.get_leaf_set()
            if all_nodes:

                closest = min(all_nodes, key=lambda n: abs(n.id - self.id))
                for key, value in self.data.items():
                    closest.data[key] = value
        

        all_known_nodes = set(self.get_leaf_set() + self.neighborhood_set)
        for row in self.routing_table.values():
            all_known_nodes.update(row.values())
        
        for node in all_known_nodes:
            node._remove_node(self)
        

        self.data.clear()
        self.leaf_smaller.clear()
        self.leaf_larger.clear()
        self.neighborhood_set.clear()
        self.routing_table.clear()

    def _remove_node(self, leaving_node: 'PastryNode'):

        if leaving_node in self.leaf_smaller:
            self.leaf_smaller.remove(leaving_node)
        if leaving_node in self.leaf_larger:
            self.leaf_larger.remove(leaving_node)
        
        if leaving_node in self.neighborhood_set:
            self.neighborhood_set.remove(leaving_node)
        
        for row_idx in list(self.routing_table.keys()):
            for digit in list(self.routing_table[row_idx].keys()):
                if self.routing_table[row_idx][digit] == leaving_node:
                    del self.routing_table[row_idx][digit]
            if not self.routing_table[row_idx]:
                del self.routing_table[row_idx]
