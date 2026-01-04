import unittest
from bplus_tree import BPlusTree

class TestBPlusTree(unittest.TestCase):
    def setUp(self):
        self.tree = BPlusTree(order=5)

    def test_insert_and_search(self):
        self.tree.insert("key1", "value1")
        self.tree.insert("key2", "value2")
        self.tree.insert("key3", "value3")
        
        self.assertEqual(self.tree.search("key1"), "value1")
        self.assertEqual(self.tree.search("key2"), "value2")
        self.assertEqual(self.tree.search("key3"), "value3")
        self.assertIsNone(self.tree.search("key4"))

    def test_delete(self):
        self.tree.insert("key1", "value1")
        self.tree.insert("key2", "value2")
        
        self.assertTrue(self.tree.delete("key1"))
        self.assertIsNone(self.tree.search("key1"))
        self.assertEqual(self.tree.search("key2"), "value2")
        self.assertFalse(self.tree.delete("key3"))

    def test_range_query(self):
        for i in range(10):
            self.tree.insert(i, f"value{i}")
        
        results = self.tree.range_query(3, 7)
        self.assertEqual(len(results), 5)

    def test_dict_interface(self):
        self.tree["key1"] = "value1"
        self.tree["key2"] = "value2"
        
        self.assertEqual(self.tree["key1"], "value1")
        self.assertIn("key1", self.tree)
        self.assertNotIn("key3", self.tree)
        
        del self.tree["key1"]
        self.assertNotIn("key1", self.tree)

    def test_get_method(self):
        self.tree.insert("key1", "value1")
        
        self.assertEqual(self.tree.get("key1"), "value1")
        self.assertIsNone(self.tree.get("key2"))
        self.assertEqual(self.tree.get("key2", "default"), "default")

    def test_keys_values_items(self):
        self.tree.insert("a", 1)
        self.tree.insert("b", 2)
        self.tree.insert("c", 3)
        
        keys = self.tree.keys()
        self.assertEqual(sorted(keys), ["a", "b", "c"])
        
        values = self.tree.values()
        self.assertEqual(sorted(values), [1, 2, 3])
        
        items = self.tree.items()
        self.assertEqual(sorted(items), [("a", 1), ("b", 2), ("c", 3)])

    def test_large_dataset(self):
        for i in range(1000):
            self.tree.insert(i, f"value{i}")
        
        self.assertEqual(len(self.tree), 1000)
        self.assertEqual(self.tree.search(500), "value500")
        
        results = self.tree.range_query(100, 200)
        self.assertEqual(len(results), 101)

    def test_clear(self):
        self.tree.insert("key1", "value1")
        self.tree.insert("key2", "value2")
        
        self.tree.clear()
        self.assertEqual(len(self.tree), 0)
        self.assertIsNone(self.tree.search("key1"))

if __name__ == '__main__':
    unittest.main()
