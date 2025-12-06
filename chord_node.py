from typing import List, Optional, Union
from dht_hash import DHTHasher

class ChordNode:
    def __init__(self, ip: str, port: int, m_bits: int = 160):

        self.ip = ip
        self.port = port
        self.address = f"{ip}:{port}"
        self.hasher = DHTHasher(m_bits)
        self.m_bits = m_bits
        self.id = self.hasher.hash_node_id(self.address)
        self.successor: 'ChordNode' = self
        self.predecessor: Optional['ChordNode'] = None
        self.finger_table: List['ChordNode'] = [self] * self.m_bits
        self.next_finger = 0  # Track next finger to fix (round-robin)
        self.data = {}
        
        print(f"[Node Init] Created Node at {self.address} with ID {self.hasher.get_hex_id(self.id)}")

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

        if verbose:
            print(f"\n[Finger Init] Initializing finger table for {self.address}...")
        
        for i in range(self.m_bits):

            start = (self.id + (2 ** i)) % (2 ** self.m_bits)
            

            self.finger_table[i] = self.find_successor(start)
            

            if i == 0:
                self.successor = self.finger_table[0]
            
            if verbose:
                print(f"  Finger[{i}] start={self.hasher.get_hex_id(start)[:8]}... -> {self.finger_table[i].address}")
        
        if verbose:
            print(f"[Finger Init] Finger table initialization complete.\n")
    
    def create_ring(self):
        self.predecessor = None
        self.successor = self
        self.finger_table = [self] * self.m_bits
        print(f"[Ring Ops] {self.address} created a new ring.")

    def join(self, introducer: 'ChordNode', init_fingers: bool = True):

        if introducer:
            self.predecessor = None
            try:
                self.successor = introducer.find_successor(self.id)
                self.finger_table[0] = self.successor

                print(f"[Ring Ops] {self.address} joining ring via {introducer.address}. Successor set to {self.successor.address}.")
                

                if init_fingers:
                    self.init_finger_table()
            except Exception as e:
                print(f"[Error] Could not join ring via {introducer.address}: {e}")
                self.create_ring()
        else:
            self.create_ring()

    def stabilize(self):
        x = self.successor.predecessor
        
        if x and self.hasher.in_range(x.id, self.id, self.successor.id, inclusive_start=False, inclusive_end=False):
            self.successor = x
            self.finger_table[0] = x
        
        self.successor.notify(self)

    def notify(self, node: 'ChordNode'):
        if (self.predecessor is None) or \
           self.hasher.in_range(node.id, self.predecessor.id, self.id, inclusive_start=False, inclusive_end=False):

            self.predecessor = node

    def fix_fingers(self, verbose: bool = False):

        start = (self.id + (2 ** self.next_finger)) % (2 ** self.m_bits)
        

        old_finger = self.finger_table[self.next_finger]
        self.finger_table[self.next_finger] = self.find_successor(start)
        

        if self.next_finger == 0:
            self.successor = self.finger_table[0]
        
        if verbose:
            print(f"[Fix Finger] {self.address} fixed finger[{self.next_finger}]: {old_finger.address} -> {self.finger_table[self.next_finger].address}")
        

        self.next_finger = (self.next_finger + 1) % self.m_bits
