import hashlib
from typing import Union


class DHTHasher:

    
    def __init__(self, m_bits: int = 160):
        self.m_bits = m_bits
        self.ring_size = 2 ** m_bits
        
    def hash_key(self, key: str) -> int:
        normalized_key = key.strip().lower()
        key_bytes = normalized_key.encode('utf-8')
        hash_obj = hashlib.sha1(key_bytes) 
        hex_digest = hash_obj.hexdigest()
        hash_int = int(hex_digest, 16)
        identifier = hash_int % self.ring_size
        return identifier
    
    def hash_node_id(self, node_address: str) -> int:

        return self.hash_key(node_address)
    
    def distance(self, id1: int, id2: int, clockwise: bool = True) -> int:
        if clockwise:
            if id2 >= id1:
                return id2 - id1
            else:
                return self.ring_size - id1 + id2
        else:
            if id1 >= id2:
                return id1 - id2
            else:
                return self.ring_size - id2 + id1
    
    def in_range(self, identifier: int, start: int, end: int, 
                 inclusive_start: bool = False, inclusive_end: bool = True) -> bool:
        if start == end:
            if not inclusive_start and not inclusive_end:
                return identifier != start
            return True
        
        if start < end:
            if inclusive_start and inclusive_end:
                return start <= identifier <= end
            elif inclusive_start:
                return start <= identifier < end
            elif inclusive_end:
                return start < identifier <= end
            else:
                return start < identifier < end
        else:
            if inclusive_start and inclusive_end:
                return identifier >= start or identifier <= end
            elif inclusive_start:
                return identifier >= start or identifier < end
            elif inclusive_end:
                return identifier > start or identifier <= end
            else:
                return identifier > start or identifier < end
    
    def get_hex_id(self, identifier: int, digits: int = 40) -> str:
        return format(identifier, f'0{digits}x')
    
    def get_pastry_prefix(self, identifier: int, prefix_length: int = 4, 
                          digit_base: int = 16) -> str:

        hex_id = self.get_hex_id(identifier)
        
        if digit_base == 16:
            return hex_id[:prefix_length]
        elif digit_base == 4:
            base4_digits = []
            for hex_char in hex_id:
                val = int(hex_char, 16)

                base4_digits.append(str((val >> 2) & 0x3))
                base4_digits.append(str(val & 0x3))
            return ''.join(base4_digits[:prefix_length])
        else:
            raise ValueError(f"Unsupported digit_base: {digit_base}")


def hash_title(title: str, m_bits: int = 160) -> int:
    hasher = DHTHasher(m_bits)
    return hasher.hash_key(title)


if __name__ == "__main__":

    print("=" * 80)
    print("DHT Hash Function Demo")
    print("=" * 80)
    

    hasher = DHTHasher(m_bits=160)
    
    print(f"\nIdentifier space: {hasher.m_bits} bits")
    print(f"Ring size: 2^{hasher.m_bits} = {hasher.ring_size}")
    

    movie_titles = [
        "The Shawshank Redemption",
        "The Godfather",
        "The Dark Knight",
        "Pulp Fiction",
        "Forrest Gump",
        "Inception",
        "The Matrix",
        "Goodfellas",
        "The Silence of the Lambs",
        "Interstellar"
    ]
    
    print("\n" + "-" * 80)
    print("Hashing Movie Titles:")
    print("-" * 80)
    
    hashed_titles = []
    for title in movie_titles:
        identifier = hasher.hash_key(title)
        hex_id = hasher.get_hex_id(identifier)
        pastry_prefix = hasher.get_pastry_prefix(identifier, prefix_length=8)
        
        hashed_titles.append((title, identifier))
        
        print(f"\nTitle: {title}")
        print(f"  Decimal ID: {identifier}")
        print(f"  Hex ID:     {hex_id}")
        print(f"  Pastry Prefix (8 digits): {pastry_prefix}")
    

    print("\n" + "-" * 80)
    print("Hashing Node Addresses:")
    print("-" * 80)
    
    node_addresses = [
        "192.168.1.1:8000",
        "192.168.1.2:8000",
        "10.0.0.5:9000",
        "node-1.dht.local:7000",
        "node-2.dht.local:7000"
    ]
    
    for address in node_addresses:
        node_id = hasher.hash_node_id(address)
        hex_id = hasher.get_hex_id(node_id)
        print(f"\nNode: {address}")
        print(f"  ID: {hex_id[:16]}...")
    

    print("\n" + "-" * 80)
    print("Distance Calculation (Chord Ring):")
    print("-" * 80)
    
    id1 = hashed_titles[0][1]
    id2 = hashed_titles[1][1]
    
    dist_clockwise = hasher.distance(id1, id2, clockwise=True)
    dist_counter = hasher.distance(id1, id2, clockwise=False)
    
    print(f"\nID1 ({hashed_titles[0][0]}): {id1}")
    print(f"ID2 ({hashed_titles[1][0]}): {id2}")
    print(f"Clockwise distance (ID1 → ID2): {dist_clockwise}")
    print(f"Counter-clockwise distance (ID1 → ID2): {dist_counter}")
    

    print("\n" + "-" * 80)
    print("Range Checking:")
    print("-" * 80)
    
    start = hashed_titles[0][1]
    end = hashed_titles[2][1]
    test_id = hashed_titles[1][1]
    
    in_range = hasher.in_range(test_id, start, end, inclusive_start=False, inclusive_end=True)
    
    print(f"\nRange: ({start}, {end}]")
    print(f"Test ID: {test_id} ({hashed_titles[1][0]})")
    print(f"In range: {in_range}")
    
    print("\n" + "=" * 80)
    print("Demo Complete!")
    print("=" * 80)
