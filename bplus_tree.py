from typing import Any, Optional, List, Tuple

class BPlusNode:
    def __init__(self, order: int, is_leaf: bool = False):
        self.order = order
        self.is_leaf = is_leaf
        self.keys = []
        self.values = []
        self.children = []
        self.next_leaf = None

    def is_full(self) -> bool:
        return len(self.keys) >= self.order - 1

    def split(self) -> Tuple['BPlusNode', Any]:
        mid = len(self.keys) // 2
        
        if self.is_leaf:
            new_node = BPlusNode(self.order, is_leaf=True)
            new_node.keys = self.keys[mid:]
            new_node.values = self.values[mid:]
            new_node.next_leaf = self.next_leaf
            self.next_leaf = new_node
            
            self.keys = self.keys[:mid]
            self.values = self.values[:mid]
            
            return new_node, new_node.keys[0]
        else:
            new_node = BPlusNode(self.order, is_leaf=False)
            mid_key = self.keys[mid]
            
            new_node.keys = self.keys[mid + 1:]
            new_node.children = self.children[mid + 1:]
            
            self.keys = self.keys[:mid]
            self.children = self.children[:mid + 1]
            
            return new_node, mid_key


class BPlusTree:
    def __init__(self, order: int = 10):
        self.order = order
        self.root = BPlusNode(order, is_leaf=True)
        self._size = 0

    def search(self, key: Any) -> Optional[Any]:
        node = self._find_leaf(key)
        
        for i, k in enumerate(node.keys):
            if k == key:
                return node.values[i]
        return None

    def _find_leaf(self, key: Any) -> BPlusNode:
        node = self.root
        
        while not node.is_leaf:
            i = 0
            while i < len(node.keys) and key >= node.keys[i]:
                i += 1
            node = node.children[i]
        
        return node

    def insert(self, key: Any, value: Any):
        node = self._find_leaf(key)
        
        self._insert_into_leaf(node, key, value)
        self._size += 1
        
        if node.is_full():
            self._split_and_propagate(node)

    def _insert_into_leaf(self, node: BPlusNode, key: Any, value: Any):
        i = 0
        while i < len(node.keys) and key > node.keys[i]:
            i += 1
        
        node.keys.insert(i, key)
        node.values.insert(i, value)

    def _split_and_propagate(self, node: BPlusNode):
        path = self._find_path(node)
        
        while node.is_full():
            new_node, mid_key = node.split()
            
            if node == self.root:
                new_root = BPlusNode(self.order, is_leaf=False)
                new_root.keys = [mid_key]
                new_root.children = [node, new_node]
                self.root = new_root
                return
            
            parent = path.pop()
            self._insert_into_parent(parent, mid_key, new_node)
            node = parent

    def _find_path(self, target: BPlusNode) -> List[BPlusNode]:
        path = []
        node = self.root
        
        while node != target:
            path.append(node)
            if node.is_leaf:
                break
            
            key = target.keys[0] if target.keys else 0
            i = 0
            while i < len(node.keys) and key >= node.keys[i]:
                i += 1
            node = node.children[i]
        
        return path

    def _insert_into_parent(self, parent: BPlusNode, key: Any, child: BPlusNode):
        i = 0
        while i < len(parent.keys) and key > parent.keys[i]:
            i += 1
        
        parent.keys.insert(i, key)
        parent.children.insert(i + 1, child)

    def delete(self, key: Any) -> bool:
        node = self._find_leaf(key)
        
        for i, k in enumerate(node.keys):
            if k == key:
                node.keys.pop(i)
                node.values.pop(i)
                self._size -= 1
                return True
        return False

    def range_query(self, start_key: Any, end_key: Any) -> List[Any]:
        results = []
        node = self._find_leaf(start_key)
        
        while node:
            for i, key in enumerate(node.keys):
                if start_key <= key <= end_key:
                    results.append(node.values[i])
                elif key > end_key:
                    return results
            node = node.next_leaf
        
        return results

    def get(self, key: Any, default=None) -> Any:
        result = self.search(key)
        return result if result is not None else default

    def keys(self) -> List[Any]:
        keys = []
        node = self._get_leftmost_leaf()
        
        while node:
            keys.extend(node.keys)
            node = node.next_leaf
        
        return keys

    def values(self) -> List[Any]:
        vals = []
        node = self._get_leftmost_leaf()
        
        while node:
            vals.extend(node.values)
            node = node.next_leaf
        
        return vals

    def items(self) -> List[Tuple[Any, Any]]:
        items = []
        node = self._get_leftmost_leaf()
        
        while node:
            for i in range(len(node.keys)):
                items.append((node.keys[i], node.values[i]))
            node = node.next_leaf
        
        return items

    def _get_leftmost_leaf(self) -> BPlusNode:
        node = self.root
        while not node.is_leaf:
            node = node.children[0]
        return node

    def clear(self):
        self.root = BPlusNode(self.order, is_leaf=True)
        self._size = 0

    def __len__(self) -> int:
        return self._size

    def __getitem__(self, key: Any) -> Any:
        result = self.search(key)
        if result is None:
            raise KeyError(key)
        return result

    def __setitem__(self, key: Any, value: Any):
        existing = self.search(key)
        if existing is not None:
            self.delete(key)
        self.insert(key, value)

    def __delitem__(self, key: Any):
        if not self.delete(key):
            raise KeyError(key)

    def __contains__(self, key: Any) -> bool:
        return self.search(key) is not None

    def __iter__(self):
        return iter(self.keys())
