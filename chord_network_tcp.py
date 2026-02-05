from typing import Optional, Any, Dict

from chord_node import ChordNode
from network_node_tcp import NetworkNodeTCP
from message_protocol import (
    Message, RequestMessage, ResponseMessage, MessageType,
    create_response
)


class ChordNetworkNode(NetworkNodeTCP):
    def __init__(
        self,
        ip: str = "127.0.0.1",
        port: int = 5000,
        m_bits: int = 160,
        timeout: float = 5.0,
        enable_metrics: bool = True
    ):
        chord_node = ChordNode(ip=ip, port=port, m_bits=m_bits)
        
        super().__init__(
            dht_node=chord_node,
            listen_ip=ip,
            listen_port=port,
            timeout=timeout,
            enable_metrics=enable_metrics
        )
        
        self.chord_node: ChordNode = chord_node
        self.remote_nodes: Dict[str, 'RemoteChordNode'] = {}
    
    def get_remote_node(self, address: str) -> 'RemoteChordNode':
        if address not in self.remote_nodes:
            self.remote_nodes[address] = RemoteChordNode(address, self)
        return self.remote_nodes[address]
    
    def _handle_request(self, request: Message) -> ResponseMessage:
        try:
            operation = request.msg_type
            payload = request.payload
            
            args = payload.get('args', [])
            kwargs = payload.get('kwargs', {})
            
            if operation == MessageType.FIND_SUCCESSOR:
                node_id = args[0] if args else kwargs.get('id')
                result_node = self.chord_node.find_successor(node_id)
                result = self._serialize_node(result_node)
                return create_response(request, result=result, success=True)
            
            elif operation == MessageType.CLOSEST_PRECEDING_NODE:
                node_id = args[0] if args else kwargs.get('id')
                result_node = self.chord_node.closest_preceding_node(node_id)
                result = self._serialize_node(result_node)
                return create_response(request, result=result, success=True)
            
            elif operation == MessageType.GET_PREDECESSOR:
                result = self._serialize_node(self.chord_node.predecessor) if self.chord_node.predecessor else None
                return create_response(request, result=result, success=True)
            
            elif operation == MessageType.GET_SUCCESSOR:
                result = self._serialize_node(self.chord_node.successor)
                return create_response(request, result=result, success=True)
            
            elif operation == MessageType.NOTIFY:
                return create_response(request, result=True, success=True)
            
            elif operation == MessageType.INSERT:
                key = args[0] if args else kwargs.get('key')
                value = args[1] if len(args) > 1 else kwargs.get('value')
                result = self.chord_node.insert(key, value)
                return create_response(request, result=result, success=True)
            
            elif operation == MessageType.LOOKUP:
                key = args[0] if args else kwargs.get('key')
                result = self.chord_node.lookup(key)
                return create_response(request, result=result, success=True)
            
            elif operation == MessageType.DELETE:
                key = args[0] if args else kwargs.get('key')
                result = self.chord_node.delete(key)
                return create_response(request, result=result, success=True)
            
            elif operation == MessageType.GET_NODE_INFO:
                result = {
                    'address': self.address,
                    'id': self.chord_node.id,
                    'hex_id': self.chord_node.hasher.get_hex_id(self.chord_node.id)[:16]
                }
                return create_response(request, result=result, success=True)
            
            elif operation == MessageType.GET_DATA:
                return create_response(request, result=dict(self.chord_node.data), success=True)
            
            elif operation == MessageType.TRANSFER_KEYS:
                keys_data = args[0] if args else kwargs.get('keys_data')
                for key, value in keys_data.items():
                    self.chord_node.data[key] = value
                return create_response(request, result=True, success=True)
            
            elif operation == MessageType.GET_KEYS_FOR_RANGE:
                start_id = args[0] if args else kwargs.get('start_id')
                end_id = args[1] if len(args) > 1 else kwargs.get('end_id')
                result = self.chord_node._get_keys_for_range(start_id, end_id)
                return create_response(request, result=result, success=True)
            
            elif operation == MessageType.PING:
                return create_response(request, result=True, success=True)
            
            elif operation == MessageType.GET_SUCCESSOR_LIST:
                successor_list = [self._serialize_node(node) for node in self.chord_node.successor_list]
                return create_response(request, result=successor_list, success=True)
            
            elif operation == MessageType.CHECK_PREDECESSOR:
                self.chord_node.check_predecessor()
                return create_response(request, result=True, success=True)
            
            elif operation == MessageType.UPDATE:
                key = args[0] if args else kwargs.get('key')
                value = args[1] if len(args) > 1 else kwargs.get('value')
                result = self.chord_node.update(key, value)
                return create_response(request, result=result, success=True)
            
            else:
                return create_response(
                    request,
                    success=False,
                    error=f"Unknown operation: {operation}"
                )
        
        except Exception as e:
            return create_response(request, success=False, error=str(e))
    
    def _serialize_node(self, node: Optional[ChordNode]) -> Optional[Dict]:
        if node is None:
            return None
        
        return {
            'address': node.address,
            'id': node.id,
            'hex_id': node.hasher.get_hex_id(node.id)[:16],
            'is_self': (node is self.chord_node)
        }
    
    def insert(self, key: str, value: Any) -> bool:
        return self.chord_node.insert(key, value)
    
    def lookup(self, key: str) -> Any:
        return self.chord_node.lookup(key)
    
    def delete(self, key: str) -> bool:
        return self.chord_node.delete(key)
    
    def update(self, key: str, value: Any) -> bool:
        return self.chord_node.update(key, value)
    
    def __repr__(self):
        return f"<ChordNetworkNode {self.address} ID:{self.chord_node.hasher.get_hex_id(self.chord_node.id)[:8]}...>"


class RemoteChordNode:
    def __init__(self, address: str, local_node: ChordNetworkNode):
        self.address = address
        self.local_node = local_node
        
        self._id: Optional[int] = None
        self._hex_id: Optional[str] = None
        
        self._fetch_info()
    
    def _fetch_info(self):
        try:
            info = self.local_node.send_request(
                self.address,
                MessageType.GET_NODE_INFO
            )
            self._id = info['id']
            self._hex_id = info['hex_id']
        except Exception as e:
            pass
    
    @property
    def id(self) -> int:
        if self._id is None:
            self._fetch_info()
        return self._id
    
    def find_successor(self, node_id: int) -> 'RemoteChordNode':
        result = self.local_node.send_request(
            self.address,
            MessageType.FIND_SUCCESSOR,
            node_id
        )
        
        if result is None:
            return self
        
        if result.get('is_self'):
            return self
        
        return self.local_node.get_remote_node(result['address'])
    
    def closest_preceding_node(self, node_id: int) -> 'RemoteChordNode':
        result = self.local_node.send_request(
            self.address,
            MessageType.CLOSEST_PRECEDING_NODE,
            node_id
        )
        
        if result is None or result.get('is_self'):
            return self
        
        return self.local_node.get_remote_node(result['address'])
    
    @property
    def predecessor(self) -> Optional['RemoteChordNode']:
        result = self.local_node.send_request(
            self.address,
            MessageType.GET_PREDECESSOR
        )
        
        if result is None:
            return None
        
        return self.local_node.get_remote_node(result['address'])
    
    @property
    def successor(self) -> 'RemoteChordNode':
        result = self.local_node.send_request(
            self.address,
            MessageType.GET_SUCCESSOR
        )
        
        if result is None or result.get('is_self'):
            return self
        
        return self.local_node.get_remote_node(result['address'])
    
    def notify(self, node: ChordNode):
        self.local_node.send_request(
            self.address,
            MessageType.NOTIFY,
            {'address': node.address, 'id': node.id}
        )
    
    def __repr__(self):
        return f"<RemoteChordNode {self.address} ID:{self._hex_id if self._hex_id else '?'}>"
