from typing import Any, Dict, Optional, List
from enum import Enum
import json
import time
import uuid


class MessageType(Enum):
    FIND_SUCCESSOR = "find_successor"
    CLOSEST_PRECEDING_NODE = "closest_preceding_node"
    GET_PREDECESSOR = "get_predecessor"
    GET_SUCCESSOR = "get_successor"
    NOTIFY = "notify"
    STABILIZE = "stabilize"
    FIX_FINGERS = "fix_fingers"
    UPDATE_FINGER_TABLE = "update_finger_table"
    ROUTE = "route"
    ADD_NODE = "add_node"
    GET_LEAF_SET = "get_leaf_set"
    UPDATE_ROUTING_TABLE = "update_routing_table"
    INSERT = "insert"
    LOOKUP = "lookup"
    DELETE = "delete"
    UPDATE = "update"
    JOIN = "join"
    LEAVE = "leave"
    TRANSFER_KEYS = "transfer_keys"
    GET_KEYS_FOR_RANGE = "get_keys_for_range"
    PING = "ping"
    GET_NODE_INFO = "get_node_info"
    GET_DATA = "get_data"
    GET_SUCCESSOR_LIST = "get_successor_list"
    GET_REPLICAS = "get_replicas"
    CHECK_PREDECESSOR = "check_predecessor"
    RESPONSE = "response"
    ERROR = "error"


class Message:
    def __init__(
        self,
        msg_type: MessageType,
        sender_address: str,
        receiver_address: str,
        request_id: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
        timestamp: Optional[float] = None
    ):
        self.msg_type = msg_type
        self.sender_address = sender_address
        self.receiver_address = receiver_address
        self.request_id = request_id or str(uuid.uuid4())
        self.payload = payload or {}
        self.timestamp = timestamp or time.time()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'msg_type': self.msg_type.value if isinstance(self.msg_type, MessageType) else self.msg_type,
            'sender_address': self.sender_address,
            'receiver_address': self.receiver_address,
            'request_id': self.request_id,
            'payload': self.payload,
            'timestamp': self.timestamp
        }
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())
    
    def to_bytes(self) -> bytes:
        json_str = self.to_json()
        msg_bytes = json_str.encode('utf-8')
        length = len(msg_bytes)
        return length.to_bytes(4, byteorder='big') + msg_bytes
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Message':
        msg_type_str = data.get('msg_type')
        try:
            msg_type = MessageType(msg_type_str)
        except ValueError:
            msg_type = msg_type_str
        
        instance = cls(
            msg_type=msg_type,
            sender_address=data['sender_address'],
            receiver_address=data['receiver_address'],
            request_id=data.get('request_id'),
            payload=data.get('payload', {}),
            timestamp=data.get('timestamp')
        )
        
        if msg_type == MessageType.RESPONSE:
            instance.__class__ = ResponseMessage
        else:
            instance.__class__ = RequestMessage
            
        return instance
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Message':
        data = json.loads(json_str)
        return cls.from_dict(data)
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'Message':
        if len(data) < 4:
            raise ValueError("Invalid message: too short")
        
        length = int.from_bytes(data[:4], byteorder='big')
        json_bytes = data[4:4+length]
        json_str = json_bytes.decode('utf-8')
        return cls.from_json(json_str)
    
    def __repr__(self):
        return f"<Message {self.msg_type} from {self.sender_address} to {self.receiver_address}>"


class RequestMessage(Message):
    def __init__(
        self,
        operation: MessageType,
        sender_address: str,
        receiver_address: str,
        args: Optional[List[Any]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        request_id: Optional[str] = None
    ):
        payload = {
            'operation': operation.value if isinstance(operation, MessageType) else operation,
            'args': args or [],
            'kwargs': kwargs or {}
        }
        super().__init__(
            msg_type=operation,
            sender_address=sender_address,
            receiver_address=receiver_address,
            request_id=request_id,
            payload=payload
        )
    
    @property
    def operation(self) -> str:
        return self.payload['operation']
    
    @property
    def args(self) -> List[Any]:
        return self.payload.get('args', [])
    
    @property
    def kwargs(self) -> Dict[str, Any]:
        return self.payload.get('kwargs', {})


class ResponseMessage(Message):
    def __init__(
        self,
        sender_address: str,
        receiver_address: str,
        request_id: str,
        result: Any = None,
        success: bool = True,
        error: Optional[str] = None
    ):
        payload = {
            'result': result,
            'success': success,
            'error': error
        }
        super().__init__(
            msg_type=MessageType.RESPONSE,
            sender_address=sender_address,
            receiver_address=receiver_address,
            request_id=request_id,
            payload=payload
        )
    
    @property
    def result(self) -> Any:
        return self.payload.get('result')
    
    @property
    def success(self) -> bool:
        return self.payload.get('success', True)
    
    @property
    def error(self) -> Optional[str]:
        return self.payload.get('error')


class NodeInfo:
    def __init__(self, address: str, node_id: int, node_type: str = "chord"):
        self.address = address
        self.node_id = node_id
        self.node_type = node_type
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'address': self.address,
            'node_id': self.node_id,
            'node_type': self.node_type
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'NodeInfo':
        return cls(
            address=data['address'],
            node_id=data['node_id'],
            node_type=data.get('node_type', 'chord')
        )
    
    def __repr__(self):
        return f"<NodeInfo {self.address} ID:{self.node_id}>"


def create_request(
    operation: MessageType,
    sender_address: str,
    receiver_address: str,
    *args,
    **kwargs
) -> RequestMessage:
    return RequestMessage(
        operation=operation,
        sender_address=sender_address,
        receiver_address=receiver_address,
        args=list(args),
        kwargs=kwargs
    )


def create_response(
    request: RequestMessage,
    result: Any = None,
    success: bool = True,
    error: Optional[str] = None
) -> ResponseMessage:
    return ResponseMessage(
        sender_address=request.receiver_address,
        receiver_address=request.sender_address,
        request_id=request.request_id,
        result=result,
        success=success,
        error=error
    )
