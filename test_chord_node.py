import unittest
from chord_node import ChordNode
from dht_hash import DHTHasher

class TestChordNode(unittest.TestCase):
    def setUp(self):
        self.m_bits = 5
        
        self.n1 = ChordNode("1.1.1.1", 8000, self.m_bits)
        self.n2 = ChordNode("2.2.2.2", 8000, self.m_bits)
        self.n3 = ChordNode("3.3.3.3", 8000, self.m_bits)
        
        self.n1.id = 0
        self.n2.id = 10
        self.n3.id = 20
        
        self.n1.successor = self.n2
        self.n2.successor = self.n3
        self.n3.successor = self.n1
        
        self.n1.successor = self.n2
        self.n2.successor = self.n3
        self.n3.successor = self.n1
        
        self.n1.predecessor = self.n3
        self.n2.predecessor = self.n1
        self.n3.predecessor = self.n2
        

        self.n1.finger_table = [self.n2, self.n2, self.n2, self.n2, self.n3]
        
        self.n2.finger_table = [self.n3, self.n3, self.n3, self.n3, self.n1]
        

        self.n3.finger_table = [self.n1, self.n1, self.n1, self.n1, self.n2]

    def test_closest_preceding_node(self):

        self.assertEqual(self.n1.closest_preceding_node(5), self.n1)
        

        self.assertEqual(self.n1.closest_preceding_node(15), self.n2)
        

        self.assertEqual(self.n1.closest_preceding_node(25), self.n3)

    def test_find_successor_local(self):

        self.assertEqual(self.n1.find_successor(5), self.n2)
        
        self.assertEqual(self.n1.find_successor(10), self.n2)

    def test_find_successor_remote(self):
        self.assertEqual(self.n1.find_successor(15), self.n3)
        
        self.assertEqual(self.n1.find_successor(25), self.n1)

    def test_consistency(self):
        key_id = 15 
        self.assertEqual(self.n1.find_successor(key_id), self.n3)
        self.assertEqual(self.n2.find_successor(key_id), self.n3)
        self.assertEqual(self.n3.find_successor(key_id), self.n3)
        
        key_id = 25 
        self.assertEqual(self.n1.find_successor(key_id), self.n1)
        self.assertEqual(self.n2.find_successor(key_id), self.n1)
        self.assertEqual(self.n3.find_successor(key_id), self.n1)
