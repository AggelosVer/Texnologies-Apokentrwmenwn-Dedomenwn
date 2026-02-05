from typing import Optional, Any, Dict, List, Tuple

from pastry_node import PastryNode
from network_node_tcp import NetworkNodeTCP
from message_protocol import (
    Message, RequestMessage, ResponseMessage, MessageType,
    create_response
)


class PastryNetworkNode(NetworkNodeTCP):
    def __init__(
        self,
        ip: str = "127.0.0.1",
        port: int = 5000,
        m_bits: int = 160,
        b: int = 4,
        l: int = 16,
        m: int = 32,
        timeout: float = 5.0,
        enable_metrics: bool = True
    ):
        pastry_node = PastryNode(ip=ip, port=port, m_bits=m_bits, b=b, l=l, m=m)
        
        super().__init__(
            dht_node=pastry_node,
            listen_ip=ip,
            listen_port=port,
            timeout=timeout,
            enable_metrics=enable_metrics
        )
        
        self.pastry_node: PastryNode = pastry_node
        self.remote_nodes: Dict[str, 'RemotePastryNode'] = {}
    
    def get_remote_node(self, address: str) -> 'RemotePastryNode':
        if address not in self.remote_nodes:
            self.remote_nodes[address] = RemotePastryNode(address, self)
        return self.remote_nodes[address]
    
    def _handle_request(self, request: Message) -> ResponseMessage:
        try:
            operation = request.msg_type
            payload = request.payload
            
            args = payload.get('args', [])
            kwargs = payload.get('kwargs', {})
            
            if operation == MessageType.ROUTE:
                key_id = args[0] if args else kwargs.get('key_id')
                hops = args[1] if len(args) > 1 else kwargs.get('hops', 0)
                result_node, hop_count = self.pastry_node.route(key_id, hops)
                result = {
                    'node': self._serialize_node(result_node),
                    'hops': hop_count
                }
                return create_response(request, result=result, success=True)
            
            elif operation == MessageType.INSERT:
                key = args[0] if args else kwargs.get('key')
                value = args[1] if len(args) > 1 else kwargs.get('value')
                success, hops = self.pastry_node.insert(key, value)
                result = {'success': success, 'hops': hops}
                return create_response(request, result=result, success=True)
            
            elif operation == MessageType.LOOKUP:
                key = args[0] if args else kwargs.get('key')
                value, hops = self.pastry_node.lookup(key)
                result = {'value': value, 'hops': hops}
                return create_response(request, result=result, success=True)
            
            elif operation == MessageType.DELETE:
                key = args[0] if args else kwargs.get('key')
                success, hops = self.pastry_node.delete(key)
                result = {'success': success, 'hops': hops}
                return create_response(request, result=result, success=True)
            
            elif operation == MessageType.UPDATE:
                key = args[0] if args else kwargs.get('key')
                value = args[1] if len(args) > 1 else kwargs.get('value')
                success, hops = self.pastry_node.update(key, value)
                result = {'success': success, 'hops': hops}
                return create_response(request, result=result, success=True)
            
            elif operation == MessageType.GET_LEAF_SET:
                leaf_set = self.pastry_node.get_leaf_set()
                result = [self._serialize_node(node) for node in leaf_set]
                return create_response(request, result=result, success=True)
            
            elif operation == MessageType.GET_NODE_INFO:
                result = {
                    'address': self.address,
                    'id': self.pastry_node.id,
                    'hex_id': self.pastry_node.hex_id
                }
                return create_response(request, result=result, success=True)
            
            elif operation == MessageType.GET_DATA:
                return create_response(request, result=dict(self.pastry_node.data), success=True)
            
            elif operation == MessageType.ADD_NODE:
                node_data = args[0] if args else kwargs.get('node')
                if isinstance(node_data, dict):
                    remote_node = self.get_remote_node(node_data['address'])
                    self.pastry_node.add_node(remote_node)
                return create_response(request, result=True, success=True)
            
            elif operation == MessageType.PING:
                return create_response(request, result=True, success=True)
            
            elif operation == MessageType.GET_REPLICAS:
                return create_response(request, result=dict(self.pastry_node.replicas), success=True)
            
            else:
                return create_response(
                    request,
                    success=False,
                    error=f"Unknown operation: {operation}"
                )
        
        except Exception as e:
            return create_response(request, success=False, error=str(e))
    
    def _serialize_node(self, node: Optional[PastryNode]) -> Optional[Dict]:
        if node is None:
            return None
        
        return {
            'address': node.address,
            'id': node.id,
            'hex_id': node.hex_id,
            'is_self': (node is self.pastry_node)
        }
    
    def insert(self, key: str, value: Any) -> Tuple[bool, int]:
        return self.pastry_node.insert(key, value)
    
    def lookup(self, key: str) -> Tuple[Any, int]:
        return self.pastry_node.lookup(key)
    
    def delete(self, key: str) -> Tuple[bool, int]:
        return self.pastry_node.delete(key)
    
    def update(self, key: str, value: Any) -> Tuple[bool, int]:
        return self.pastry_node.update(key, value)
    
    def __repr__(self):
        return f"<PastryNetworkNode {self.address} ID:{self.pastry_node.hex_id[:8]}...>"


class RemotePastryNode:
    def __init__(self, address: str, local_node: PastryNetworkNode):
        self.address = address
        self.local_node = local_node
        
        self._id: Optional[int] = None
        self._hex_id: Optional[str] = None
        
        self._fetch_info()
    
    def _fetch_info(self):
        if self._id is not None:
            return
        try:
            info = self.local_node.send_request(
                self.address,
                MessageType.GET_NODE_INFO,
                timeout=2.0,
                retries=1
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
    
    @property
    def hex_id(self) -> str:
        if self._hex_id is None:
            self._fetch_info()
        return self._hex_id or "unknown"

    def add_node(self, node: Any):
        
        node_info = {
            'address': node.address,
            'id': node.id,
            'hex_id': node.hex_id
        }
        self.local_node.send_request(
            self.address,
            MessageType.ADD_NODE,
            node=node_info
        )

    @property
    def neighborhood_set(self) -> List['RemotePastryNode']:
        
        return []

    @property
    def routing_table(self) -> Dict[int, Dict[int, 'RemotePastryNode']]:
        
        return {}

    @property
    def data(self) -> Dict[str, Any]:
        
        try:
            return self.local_node.send_request(
                self.address,
                MessageType.GET_DATA
            )
        except:
            return {}
    
    def route(self, key_id: int, hops: int = 0) -> Tuple['RemotePastryNode', int]:
        result = self.local_node.send_request(
            self.address,
            MessageType.ROUTE,
            key_id,
            hops
        )
        
        node_data = result['node']
        hop_count = result['hops']
        
        if node_data is None or node_data.get('is_self'):
            return self, hop_count
        
        return self.local_node.get_remote_node(node_data['address']), hop_count
    
    def get_leaf_set(self) -> List['RemotePastryNode']:
        result = self.local_node.send_request(
            self.address,
            MessageType.GET_LEAF_SET
        )
        
        return [
            self.local_node.get_remote_node(node_data['address'])
            for node_data in result
        ]
    
    def __repr__(self):
        return f"<RemotePastryNode {self.address} ID:{self._hex_id[:8] if self._hex_id else '?'}...>"
