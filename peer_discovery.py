import threading
import time
import queue
from typing import List, Optional, Dict, Tuple, Set
from chord_node import ChordNode
import random


class PeerRegistry:
    def __init__(self):
        self.peers: Dict[str, Tuple[str, int, float, int]] = {}
        self.lock = threading.Lock()
        self.heartbeat_timeout = 30.0
        
    def register_peer(self, address: str, ip: str, port: int, node_id: int) -> bool:
        with self.lock:
            self.peers[address] = (ip, port, time.time(), node_id)
            return True
    
    def unregister_peer(self, address: str) -> bool:
        with self.lock:
            if address in self.peers:
                del self.peers[address]
                return True
            return False
    
    def get_peers(self) -> List[Tuple[str, int, int]]:
        with self.lock:
            current_time = time.time()
            active_peers = []
            expired = []
            
            for address, (ip, port, timestamp, node_id) in self.peers.items():
                if current_time - timestamp < self.heartbeat_timeout:
                    active_peers.append((ip, port, node_id))
                else:
                    expired.append(address)
            
            for addr in expired:
                del self.peers[addr]
            
            return active_peers
    
    def get_peer_by_address(self, address: str) -> Optional[Tuple[str, int, int]]:
        with self.lock:
            if address in self.peers:
                ip, port, _, node_id = self.peers[address]
                return (ip, port, node_id)
            return None
    
    def get_random_peer(self) -> Optional[Tuple[str, int, int]]:
        peers = self.get_peers()
        if peers:
            return random.choice(peers)
        return None
    
    def get_closest_peer(self, target_id: int) -> Optional[Tuple[str, int, int]]:
        with self.lock:
            if not self.peers:
                return None
            
            min_distance = float('inf')
            closest = None
            
            for address, (ip, port, timestamp, node_id) in self.peers.items():
                if time.time() - timestamp < self.heartbeat_timeout:
                    distance = abs(target_id - node_id)
                    if distance < min_distance:
                        min_distance = distance
                        closest = (ip, port, node_id)
            
            return closest
    
    def heartbeat(self, address: str) -> bool:
        with self.lock:
            if address in self.peers:
                ip, port, _, node_id = self.peers[address]
                self.peers[address] = (ip, port, time.time(), node_id)
                return True
            return False
    
    def get_peer_count(self) -> int:
        with self.lock:
            current_time = time.time()
            active_count = sum(1 for _, (_, _, timestamp, _) in self.peers.items() 
                             if current_time - timestamp < self.heartbeat_timeout)
            return active_count
    
    def get_all_peer_addresses(self) -> List[str]:
        with self.lock:
            current_time = time.time()
            return [addr for addr, (_, _, timestamp, _) in self.peers.items()
                   if current_time - timestamp < self.heartbeat_timeout]


class BootstrapServer:
    def __init__(self, registry: PeerRegistry):
        self.registry = registry
        self.bootstrap_nodes: List[Tuple[str, int]] = []
        self.lock = threading.Lock()
        
    def add_bootstrap_node(self, ip: str, port: int):
        with self.lock:
            self.bootstrap_nodes.append((ip, port))
            address = f"{ip}:{port}"
            
    def get_bootstrap_nodes(self) -> List[Tuple[str, int]]:
        with self.lock:
            return self.bootstrap_nodes.copy()
    
    def announce_peer(self, ip: str, port: int, node_id: int) -> List[Tuple[str, int, int]]:
        address = f"{ip}:{port}"
        self.registry.register_peer(address, ip, port, node_id)
        return self.registry.get_peers()
    
    def get_introducer_for_node(self, node_id: int) -> Optional[Tuple[str, int, int]]:
        closest = self.registry.get_closest_peer(node_id)
        if closest:
            return closest
        return self.registry.get_random_peer()


class PeerDiscovery:
    def __init__(self, bootstrap_server: BootstrapServer):
        self.bootstrap_server = bootstrap_server
        self.registry = bootstrap_server.registry
        
    def discover_peers(self, announcing_node: ChordNode) -> List[Tuple[str, int, int]]:
        return self.bootstrap_server.announce_peer(
            announcing_node.ip, 
            announcing_node.port, 
            announcing_node.id
        )
    
    def find_introducer(self, node: ChordNode) -> Optional[ChordNode]:
        introducer_info = self.bootstrap_server.get_introducer_for_node(node.id)
        
        if introducer_info is None:
            bootstrap_nodes = self.bootstrap_server.get_bootstrap_nodes()
            if bootstrap_nodes:
                ip, port = random.choice(bootstrap_nodes)
                return ChordNode(ip, port, node.m_bits)
            return None
        
        ip, port, node_id = introducer_info
        introducer = ChordNode(ip, port, node.m_bits)
        introducer.id = node_id
        return introducer
    
    def join_network(self, node: ChordNode) -> bool:
        self.discover_peers(node)
        
        introducer = self.find_introducer(node)
        
        if introducer is None:
            node.create_ring()
            return True
        
        node.join(introducer, init_fingers=True, transfer_data=True)
        return True
    
    def leave_network(self, node: ChordNode) -> bool:
        node.leave(transfer_data=True)
        self.registry.unregister_peer(node.address)
        return True
    
    def get_network_size(self) -> int:
        return self.registry.get_peer_count()


class HeartbeatService:
    def __init__(self, registry: PeerRegistry, node: ChordNode, interval: float = 5.0):
        self.registry = registry
        self.node = node
        self.interval = interval
        self.running = False
        self.thread = None
        
    def start(self):
        if not self.running:
            self.running = True
            self.thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
            self.thread.start()
    
    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=2.0)
    
    def _heartbeat_loop(self):
        while self.running:
            self.registry.heartbeat(self.node.address)
            time.sleep(self.interval)


class PeerMonitor:
    def __init__(self, registry: PeerRegistry, check_interval: float = 10.0):
        self.registry = registry
        self.check_interval = check_interval
        self.running = False
        self.thread = None
        self.failure_callbacks = []
        
    def register_failure_callback(self, callback):
        self.failure_callbacks.append(callback)
    
    def start(self):
        if not self.running:
            self.running = True
            self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
            self.thread.start()
    
    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=2.0)
    
    def _monitor_loop(self):
        previous_peers = set()
        
        while self.running:
            current_peers = set(self.registry.get_all_peer_addresses())
            
            failed_peers = previous_peers - current_peers
            for peer_addr in failed_peers:
                for callback in self.failure_callbacks:
                    callback(peer_addr)
            
            previous_peers = current_peers
            time.sleep(self.check_interval)


class PeerAnnouncer:
    def __init__(self, bootstrap_server: BootstrapServer):
        self.bootstrap_server = bootstrap_server
        self.announcement_queue = queue.Queue()
        self.running = False
        self.thread = None
        
    def announce(self, node: ChordNode):
        self.announcement_queue.put(node)
    
    def start(self):
        if not self.running:
            self.running = True
            self.thread = threading.Thread(target=self._announcement_loop, daemon=True)
            self.thread.start()
    
    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=2.0)
    
    def _announcement_loop(self):
        while self.running:
            try:
                node = self.announcement_queue.get(timeout=1.0)
                self.bootstrap_server.announce_peer(node.ip, node.port, node.id)
                self.announcement_queue.task_done()
            except queue.Empty:
                continue

