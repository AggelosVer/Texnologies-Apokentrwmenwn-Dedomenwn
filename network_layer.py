import threading
import queue
import time
import json
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum


class MessageType(Enum):
    FIND_SUCCESSOR = "find_successor"
    FIND_PREDECESSOR = "find_predecessor"
    GET_PREDECESSOR = "get_predecessor"
    GET_SUCCESSOR = "get_successor"
    NOTIFY = "notify"
    INSERT = "insert"
    LOOKUP = "lookup"
    DELETE = "delete"
    STABILIZE = "stabilize"
    FIX_FINGERS = "fix_fingers"
    PING = "ping"
    RESPONSE = "response"
    ERROR = "error"


@dataclass
class Message:
    msg_id: str
    msg_type: MessageType
    sender: str
    receiver: str
    payload: Dict[str, Any]
    timestamp: float = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()
    
    def to_dict(self):
        d = asdict(self)
        d['msg_type'] = self.msg_type.value
        return d
    
    @classmethod
    def from_dict(cls, d):
        d['msg_type'] = MessageType(d['msg_type'])
        return cls(**d)


class NetworkSimulator:
    def __init__(self, latency_ms: float = 5.0, packet_loss: float = 0.0):
        self.nodes: Dict[str, 'NodeMailbox'] = {}
        self.latency_ms = latency_ms
        self.packet_loss = packet_loss
        self.lock = threading.Lock()
        self.running = True
        self.message_count = 0
        
    def register_node(self, address: str, mailbox: 'NodeMailbox'):
        with self.lock:
            self.nodes[address] = mailbox
            
    def unregister_node(self, address: str):
        with self.lock:
            if address in self.nodes:
                del self.nodes[address]
    
    def send_message(self, message: Message) -> bool:
        import random
        
        if random.random() < self.packet_loss:
            return False
        
        with self.lock:
            if message.receiver not in self.nodes:
                return False
            
            target_mailbox = self.nodes[message.receiver]
        
        def delayed_delivery():
            time.sleep(self.latency_ms / 1000.0)
            target_mailbox.receive(message)
        
        threading.Thread(target=delayed_delivery, daemon=True).start()
        
        with self.lock:
            self.message_count += 1
        
        return True
    
    def get_message_count(self) -> int:
        with self.lock:
            return self.message_count
    
    def reset_message_count(self):
        with self.lock:
            self.message_count = 0


class NodeMailbox:
    def __init__(self, address: str, network: NetworkSimulator):
        self.address = address
        self.network = network
        self.inbox = queue.Queue()
        self.pending_responses: Dict[str, queue.Queue] = {}
        self.lock = threading.Lock()
        self.handler = None
        self.running = False
        self.worker_thread = None
        
    def register_handler(self, handler):
        self.handler = handler
    
    def start(self):
        if not self.running:
            self.running = True
            self.worker_thread = threading.Thread(target=self._process_messages, daemon=True)
            self.worker_thread.start()
    
    def stop(self):
        self.running = False
        if self.worker_thread:
            self.worker_thread.join(timeout=2.0)
    
    def receive(self, message: Message):
        self.inbox.put(message)
    
    def send(self, receiver: str, msg_type: MessageType, payload: Dict[str, Any], 
             wait_response: bool = False, timeout: float = 5.0) -> Optional[Any]:
        import uuid
        
        msg_id = str(uuid.uuid4())
        message = Message(
            msg_id=msg_id,
            msg_type=msg_type,
            sender=self.address,
            receiver=receiver,
            payload=payload
        )
        
        if wait_response:
            response_queue = queue.Queue()
            with self.lock:
                self.pending_responses[msg_id] = response_queue
        
        success = self.network.send_message(message)
        
        if not success:
            if wait_response:
                with self.lock:
                    del self.pending_responses[msg_id]
            return None
        
        if wait_response:
            try:
                response = response_queue.get(timeout=timeout)
                with self.lock:
                    del self.pending_responses[msg_id]
                return response
            except queue.Empty:
                with self.lock:
                    if msg_id in self.pending_responses:
                        del self.pending_responses[msg_id]
                return None
        
        return True
    
    def send_response(self, original_msg: Message, payload: Dict[str, Any]):
        response = Message(
            msg_id=original_msg.msg_id,
            msg_type=MessageType.RESPONSE,
            sender=self.address,
            receiver=original_msg.sender,
            payload=payload
        )
        self.network.send_message(response)
    
    def _process_messages(self):
        while self.running:
            try:
                message = self.inbox.get(timeout=0.1)
                
                if message.msg_type == MessageType.RESPONSE:
                    with self.lock:
                        if message.msg_id in self.pending_responses:
                            self.pending_responses[message.msg_id].put(message.payload)
                else:
                    if self.handler:
                        self.handler(message)
                
                self.inbox.task_done()
                
            except queue.Empty:
                continue


class RemoteNodeProxy:
    def __init__(self, address: str, mailbox: NodeMailbox, m_bits: int = 160):
        self.address = address
        self.mailbox = mailbox
        self.m_bits = m_bits
        self._id = None
    
    @property
    def id(self) -> int:
        if self._id is None:
            result = self.mailbox.send(
                self.address,
                MessageType.PING,
                {},
                wait_response=True,
                timeout=2.0
            )
            if result:
                self._id = result.get('id')
        return self._id
    
    @id.setter
    def id(self, value: int):
        self._id = value
    
    @property
    def ip(self) -> str:
        return self.address.split(':')[0]
    
    @property
    def port(self) -> int:
        return int(self.address.split(':')[1])
    
    def find_successor(self, key_id: int):
        result = self.mailbox.send(
            self.address,
            MessageType.FIND_SUCCESSOR,
            {'key_id': key_id},
            wait_response=True,
            timeout=5.0
        )
        
        if result and 'successor_address' in result:
            successor_addr = result['successor_address']
            if successor_addr == self.mailbox.address:
                return None
            proxy = RemoteNodeProxy(successor_addr, self.mailbox, self.m_bits)
            proxy._id = result.get('successor_id')
            return proxy
        return None
    
    @property
    def predecessor(self):
        result = self.mailbox.send(
            self.address,
            MessageType.GET_PREDECESSOR,
            {},
            wait_response=True,
            timeout=2.0
        )
        
        if result and result.get('predecessor_address'):
            pred_addr = result['predecessor_address']
            if pred_addr == self.mailbox.address:
                return None
            proxy = RemoteNodeProxy(pred_addr, self.mailbox, self.m_bits)
            proxy._id = result.get('predecessor_id')
            return proxy
        return None
    
    def notify(self, node):
        node_addr = node.address if hasattr(node, 'address') else str(node)
        node_id = node.id if hasattr(node, 'id') else None
        
        self.mailbox.send(
            self.address,
            MessageType.NOTIFY,
            {'node_address': node_addr, 'node_id': node_id},
            wait_response=False
        )
