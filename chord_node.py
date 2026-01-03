from typing import List, Optional
from dht_hash import DHTHasher

class ChordNode:
    def __init__(self, ip: str, port: int, m_bits: int = 160, successor_list_size: int = 3):
        self.ip = ip
        self.port = port
        self.address = f"{ip}:{port}"
        self.hasher = DHTHasher(m_bits)
        self.m_bits = m_bits
        self.id = self.hasher.hash_node_id(self.address)
        self.successor: 'ChordNode' = self
        self.predecessor: Optional['ChordNode'] = None
        self.finger_table: List['ChordNode'] = [self] * self.m_bits
        self.next_finger = 0
        self.data = {}
        self.successor_list_size = successor_list_size
        self.successor_list: List['ChordNode'] = []
        self.replicas: Dict[str, any] = {}

    def __repr__(self):
        return f"<ChordNode {self.address} ID:{self.hasher.get_hex_id(self.id)[:8]}...>"

    def update_finger_table(self, index: int, node: 'ChordNode'):
        if 0 <= index < self.m_bits:
            self.finger_table[index] = node
            if index == 0:
                self.successor = node
        else:
            raise IndexError(f"Finger table index {index} out of range (0-{self.m_bits-1})")

    def find_successor(self, id: int) -> 'ChordNode':
        if self.hasher.in_range(id, self.id, self.successor.id, inclusive_start=False, inclusive_end=True):
            return self.successor
        else:
            n0 = self.closest_preceding_node(id)
            if n0 is self:
                return self.successor
            
            return n0.find_successor(id)

    def closest_preceding_node(self, id: int) -> 'ChordNode':
        for i in range(self.m_bits - 1, -1, -1):
            finger_node = self.finger_table[i]
            if self.hasher.in_range(finger_node.id, self.id, id, inclusive_start=False, inclusive_end=False):
                return finger_node
        return self

    def init_finger_table(self, verbose: bool = False):
        for i in range(self.m_bits):
            start = (self.id + (2 ** i)) % (2 ** self.m_bits)
            self.finger_table[i] = self.find_successor(start)
            if i == 0:
                self.successor = self.finger_table[0]
    
    def create_ring(self):
        self.predecessor = None
        self.successor = self
        self.finger_table = [self] * self.m_bits

    def join(self, introducer: Optional['ChordNode'] = None, init_fingers: bool = True, transfer_data: bool = True):
        if introducer is None:
            self.create_ring()
            return
        
        self.predecessor = None
        
        try:
            self.successor = introducer.find_successor(self.id)
            self.finger_table[0] = self.successor
            
            if self.successor is not self:
                self.successor.notify(self)
                if transfer_data:
                    self._transfer_data_from_successor()
            
            if init_fingers:
                self.init_finger_table()
        except Exception:
            self.create_ring()
    
    def _transfer_data_from_successor(self):
        if self.successor is None or self.successor is self:
            return
        
        if self.predecessor is None:
            keys_to_transfer = dict(self.successor.data)
        else:
            keys_to_transfer = self.successor._get_keys_for_range(self.predecessor.id, self.id)
        
        for key, value in keys_to_transfer.items():
            self.data[key] = value
            if key in self.successor.data:
                del self.successor.data[key]

    def stabilize(self):
        if self.successor is None or self.successor is self:
            return
        
        try:
            x = self.successor.predecessor
            if x and x is not self and \
               self.hasher.in_range(x.id, self.id, self.successor.id, 
                                   inclusive_start=False, inclusive_end=False):
                self.update_successor(x)
            self.successor.notify(self)
            self._update_successor_list()
        except AttributeError:
            self.successor.notify(self)
            self._update_successor_list()
        except Exception:
            self._handle_successor_failure()

    def notify(self, node: 'ChordNode'):
        if node is None or node is self:
            return
        
        if (self.predecessor is None) or \
           (self.predecessor is not node and 
            self.hasher.in_range(node.id, self.predecessor.id, self.id, 
                                inclusive_start=False, inclusive_end=False)):
            old_predecessor = self.predecessor
            self.predecessor = node
            if old_predecessor is not None:
                self._transfer_data_to_predecessor(node, old_predecessor)
            self._redistribute_keys()

    def fix_fingers(self, verbose: bool = False):
        start = (self.id + (2 ** self.next_finger)) % (2 ** self.m_bits)
        self.finger_table[self.next_finger] = self.find_successor(start)
        if self.next_finger == 0:
            self.successor = self.finger_table[0]
        self.next_finger = (self.next_finger + 1) % self.m_bits
    
    def leave(self, transfer_data: bool = True):
        if self.successor is self:
            return
        
        if transfer_data and self.successor:
            self._transfer_data_to_successor()
        
        if self.predecessor and self.predecessor is not self:
            self.predecessor.successor = self.successor
            self.predecessor.finger_table[0] = self.successor
        
        if self.successor and self.successor is not self:
            if self.successor.predecessor is self:
                self.successor.predecessor = self.predecessor if self.predecessor is not self else None
        
        self.predecessor = None
        self.successor = self
        self.finger_table = [self] * self.m_bits
        self.data.clear()
    
    def _transfer_data_to_successor(self):
        if self.successor is None or self.successor is self:
            return
        
        for key, value in self.data.items():
            self.successor.data[key] = value
    
    def _transfer_data_to_predecessor(self, new_predecessor: 'ChordNode', old_predecessor: Optional['ChordNode'] = None):
        if new_predecessor is None or new_predecessor is self:
            return
        
        if old_predecessor is None:
            old_predecessor_id = None
        else:
            old_predecessor_id = old_predecessor.id
        
        if old_predecessor_id is None:
            keys_to_transfer = self._get_keys_for_range(new_predecessor.id, self.id)
        else:
            keys_to_transfer = self._get_keys_for_range(new_predecessor.id, old_predecessor_id)
        
        for key, value in keys_to_transfer.items():
            new_predecessor.data[key] = value
            if key in self.data:
                del self.data[key]
    
    def update_successor(self, new_successor: 'ChordNode'):
        if new_successor is None:
            return
        self.successor = new_successor
        self.finger_table[0] = new_successor
    
    def update_predecessor(self, new_predecessor: Optional['ChordNode']):
        self.predecessor = new_predecessor
    
    def insert(self, key: str, value) -> bool:
        key_id = self.hasher.hash_key(key)
        responsible_node = self.find_successor(key_id)
        responsible_node.data[key] = value
        return True
    
    def lookup(self, key: str):
        key_id = self.hasher.hash_key(key)
        responsible_node = self.find_successor(key_id)
        return responsible_node.data.get(key)
    
    def delete(self, key: str) -> bool:
        key_id = self.hasher.hash_key(key)
        responsible_node = self.find_successor(key_id)
        if key in responsible_node.data:
            del responsible_node.data[key]
            return True
        return False
    
    def _redistribute_keys(self):
        if self.predecessor is None:
            return
        
        keys_to_redistribute = []
        
        for key, value in list(self.data.items()):
            key_id = self.hasher.hash_key(key)
            if not self.hasher.in_range(key_id, self.predecessor.id, self.id, 
                                       inclusive_start=False, inclusive_end=True):
                keys_to_redistribute.append((key, value))
        
        for key, value in keys_to_redistribute:
            if key in self.data:
                del self.data[key]
            key_id = self.hasher.hash_key(key)
            new_responsible = self.find_successor(key_id)
            if key not in new_responsible.data:
                new_responsible.data[key] = value
    
    def _get_keys_for_range(self, start_id: int, end_id: int) -> dict:
        keys_in_range = {}
        for key, value in self.data.items():
            key_id = self.hasher.hash_key(key)
            if self.hasher.in_range(key_id, start_id, end_id, 
                                   inclusive_start=False, inclusive_end=True):
                keys_in_range[key] = value
        return keys_in_range
    
    def _update_successor_list(self):
        self.successor_list = []
        current = self.successor
        
        for _ in range(self.successor_list_size):
            if current is None or current is self:
                break
            self.successor_list.append(current)
            if hasattr(current, 'successor'):
                current = current.successor
            else:
                break
    
    def _handle_successor_failure(self):
        if not self.successor_list:
            self.successor = self
            self.finger_table[0] = self
            return
        
        for candidate in self.successor_list:
            try:
                if hasattr(candidate, 'id'):
                    self.successor = candidate
                    self.finger_table[0] = candidate
                    self._update_successor_list()
                    return
            except Exception:
                continue
        
        self.successor = self
        self.finger_table[0] = self
    
    def check_predecessor(self):
        if self.predecessor is not None:
            try:
                if not hasattr(self.predecessor, 'id'):
                    self.predecessor = None
            except Exception:
                self.predecessor = None
    
    def replicate_data(self):
        if not self.successor_list:
            return
        
        for successor in self.successor_list:
            try:
                if hasattr(successor, 'replicas'):
                    for key, value in self.data.items():
                        successor.replicas[key] = value
            except Exception:
                continue
    
    def recover_data_from_replicas(self):
        if self.predecessor is None:
            return
        
        try:
            if hasattr(self.predecessor, 'data'):
                for key, value in self.predecessor.data.items():
                    key_id = self.hasher.hash_key(key)
                    if self.hasher.in_range(key_id, self.predecessor.id, self.id,
                                          inclusive_start=False, inclusive_end=True):
                        if key not in self.data:
                            self.data[key] = value
        except Exception:
            pass
        
        for key, value in list(self.replicas.items()):
            if key not in self.data:
                key_id = self.hasher.hash_key(key)
                responsible = self.find_successor(key_id)
                if responsible is self:
                    self.data[key] = value
                    del self.replicas[key]
