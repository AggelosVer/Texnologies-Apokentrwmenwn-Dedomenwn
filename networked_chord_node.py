from chord_node import ChordNode
from network_layer import NetworkSimulator, NodeMailbox, MessageType, RemoteNodeProxy
from typing import Optional


class NetworkedChordNode(ChordNode):
    def __init__(self, ip: str, port: int, m_bits: int = 160, network: NetworkSimulator = None):
        super().__init__(ip, port, m_bits)
        
        if network is None:
            network = NetworkSimulator()
        
        self.network = network
        self.mailbox = NodeMailbox(self.address, network)
        self.mailbox.register_handler(self._handle_message)
        self.network.register_node(self.address, self.mailbox)
        self.mailbox.start()
    
    def _handle_message(self, message):
        msg_type = message.msg_type
        payload = message.payload
        
        if msg_type == MessageType.PING:
            self.mailbox.send_response(message, {'id': self.id, 'address': self.address})
        
        elif msg_type == MessageType.FIND_SUCCESSOR:
            key_id = payload['key_id']
            successor = super().find_successor(key_id)
            self.mailbox.send_response(message, {
                'successor_address': successor.address,
                'successor_id': successor.id
            })
        
        elif msg_type == MessageType.GET_PREDECESSOR:
            pred = self.predecessor
            if pred:
                self.mailbox.send_response(message, {
                    'predecessor_address': pred.address,
                    'predecessor_id': pred.id
                })
            else:
                self.mailbox.send_response(message, {
                    'predecessor_address': None,
                    'predecessor_id': None
                })
        
        elif msg_type == MessageType.GET_SUCCESSOR:
            succ = self.successor
            self.mailbox.send_response(message, {
                'successor_address': succ.address,
                'successor_id': succ.id
            })
        
        elif msg_type == MessageType.NOTIFY:
            node_addr = payload['node_address']
            node_id = payload.get('node_id')
            
            remote_node = RemoteNodeProxy(node_addr, self.mailbox, self.m_bits)
            if node_id:
                remote_node._id = node_id
            
            super().notify(remote_node)
        
        elif msg_type == MessageType.INSERT:
            key = payload['key']
            value = payload['value']
            super().insert(key, value)
            self.mailbox.send_response(message, {'success': True})
        
        elif msg_type == MessageType.LOOKUP:
            key = payload['key']
            result = super().lookup(key)
            self.mailbox.send_response(message, {'result': result})
        
        elif msg_type == MessageType.DELETE:
            key = payload['key']
            success = super().delete(key)
            self.mailbox.send_response(message, {'success': success})
        
        elif msg_type == MessageType.STABILIZE:
            super().stabilize()
            self.mailbox.send_response(message, {'success': True})
        
        elif msg_type == MessageType.FIX_FINGERS:
            super().fix_fingers()
            self.mailbox.send_response(message, {'success': True})
    
    def find_successor(self, key_id: int):
        if self.hasher.in_range(key_id, self.id, self.successor.id, 
                                inclusive_start=False, inclusive_end=True):
            if isinstance(self.successor, NetworkedChordNode):
                return self.successor
            else:
                return RemoteNodeProxy(self.successor.address, self.mailbox, self.m_bits)
        else:
            n0 = self.closest_preceding_node(key_id)
            if n0 is self:
                return self.successor
            
            if isinstance(n0, RemoteNodeProxy):
                return n0.find_successor(key_id)
            elif isinstance(n0, NetworkedChordNode):
                remote_n0 = RemoteNodeProxy(n0.address, self.mailbox, self.m_bits)
                remote_n0._id = n0.id
                return remote_n0.find_successor(key_id)
            else:
                return n0.find_successor(key_id)
    
    def insert(self, key: str, value) -> bool:
        key_id = self.hasher.hash_key(key)
        responsible_node = self.find_successor(key_id)
        
        if isinstance(responsible_node, RemoteNodeProxy):
            result = self.mailbox.send(
                responsible_node.address,
                MessageType.INSERT,
                {'key': key, 'value': value},
                wait_response=True,
                timeout=5.0
            )
            return result.get('success', False) if result else False
        else:
            responsible_node.data[key] = value
            return True
    
    def lookup(self, key: str):
        key_id = self.hasher.hash_key(key)
        responsible_node = self.find_successor(key_id)
        
        if isinstance(responsible_node, RemoteNodeProxy):
            result = self.mailbox.send(
                responsible_node.address,
                MessageType.LOOKUP,
                {'key': key},
                wait_response=True,
                timeout=5.0
            )
            return result.get('result') if result else None
        else:
            return responsible_node.data.get(key)
    
    def delete(self, key: str) -> bool:
        key_id = self.hasher.hash_key(key)
        responsible_node = self.find_successor(key_id)
        
        if isinstance(responsible_node, RemoteNodeProxy):
            result = self.mailbox.send(
                responsible_node.address,
                MessageType.DELETE,
                {'key': key},
                wait_response=True,
                timeout=5.0
            )
            return result.get('success', False) if result else False
        else:
            if key in responsible_node.data:
                del responsible_node.data[key]
                return True
            return False
    
    def join(self, introducer: 'ChordNode', init_fingers: bool = True, transfer_data: bool = True):
        if introducer is None:
            self.create_ring()
            return
        
        self.predecessor = None
        
        try:
            if isinstance(introducer, NetworkedChordNode):
                remote_introducer = RemoteNodeProxy(introducer.address, self.mailbox, self.m_bits)
                remote_introducer._id = introducer.id
            else:
                remote_introducer = introducer
            
            self.successor = remote_introducer.find_successor(self.id)
            
            if self.successor:
                self.finger_table[0] = self.successor
                
                if self.successor is not self:
                    if isinstance(self.successor, RemoteNodeProxy):
                        self.successor.notify(self)
                    else:
                        self.successor.notify(self)
            
            if init_fingers:
                self.init_finger_table()
        except Exception:
            self.create_ring()
    
    def stabilize(self):
        if self.successor is None or self.successor is self:
            return
        
        try:
            if isinstance(self.successor, RemoteNodeProxy):
                x = self.successor.predecessor
            else:
                x = self.successor.predecessor
            
            if x and x is not self:
                if isinstance(x, RemoteNodeProxy):
                    x_id = x.id
                else:
                    x_id = x.id
                
                if self.hasher.in_range(x_id, self.id, self.successor.id,
                                       inclusive_start=False, inclusive_end=False):
                    self.update_successor(x)
            
            if isinstance(self.successor, RemoteNodeProxy):
                self.successor.notify(self)
            else:
                self.successor.notify(self)
        except Exception:
            pass
    
    def shutdown(self):
        self.mailbox.stop()
        self.network.unregister_node(self.address)
