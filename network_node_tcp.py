import socket
import threading
import time
from typing import Dict, Optional, Any, Callable
from queue import Queue, Empty

from message_protocol import (
    Message, RequestMessage, ResponseMessage, MessageType,
    create_request, create_response
)
from network_metrics import NetworkMetrics


class NetworkNodeTCP:
    def __init__(
        self,
        dht_node: Any,
        listen_ip: str = "127.0.0.1",
        listen_port: int = None,
        timeout: float = 5.0,
        enable_metrics: bool = True
    ):
        self.dht_node = dht_node
        self.listen_ip = listen_ip
        self.listen_port = listen_port or dht_node.port
        self.address = f"{listen_ip}:{self.listen_port}"
        self.timeout = timeout
        
        self.dht_node.ip = listen_ip
        self.dht_node.port = self.listen_port
        self.dht_node.address = self.address
        
        self.server_socket: Optional[socket.socket] = None
        self.running = False
        self.server_thread: Optional[threading.Thread] = None
        
        self.pending_responses: Dict[str, Queue] = {}
        self.response_lock = threading.Lock()
        
        self.metrics = NetworkMetrics(self.address) if enable_metrics else None
        
        self.connection_cache: Dict[str, socket.socket] = {}
        self.connection_lock = threading.Lock()
    
    def start(self):
        if self.running:
            return
        
        self.running = True
        
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.listen_ip, self.listen_port))
        self.server_socket.listen(10)
        self.server_socket.settimeout(1.0)
        
        self.server_thread = threading.Thread(target=self._server_loop, daemon=True)
        self.server_thread.start()
    
    def stop(self):
        if not self.running:
            return
        
        self.running = False
        
        with self.connection_lock:
            for conn in self.connection_cache.values():
                try:
                    conn.close()
                except:
                    pass
            self.connection_cache.clear()
        
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
        
        if self.server_thread and self.server_thread.is_alive():
            self.server_thread.join(timeout=2.0)
    
    def _server_loop(self):
        while self.running:
            try:
                client_socket, client_address = self.server_socket.accept()
                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, client_address),
                    daemon=True
                )
                client_thread.start()
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    pass
                break
    
    def _handle_client(self, client_socket: socket.socket, client_address):
        try:
            message = self._receive_message(client_socket)
            if message is None:
                return
            
            if self.metrics:
                self.metrics.record_message_received(
                    operation=message.msg_type.value if isinstance(message.msg_type, MessageType) else str(message.msg_type)
                )
            
            if message.msg_type == MessageType.RESPONSE:
                self._handle_response(message)
            else:
                response = self._handle_request(message)
                self._send_message(client_socket, response)
        
        except Exception as e:
            pass
        
        finally:
            try:
                client_socket.close()
            except:
                pass
    
    def _receive_message(self, sock: socket.socket) -> Optional[Message]:
        try:
            length_data = self._recv_exactly(sock, 4)
            if not length_data:
                return None
            
            message_length = int.from_bytes(length_data, byteorder='big')
            
            message_data = self._recv_exactly(sock, message_length)
            if not message_data:
                return None
            
            json_str = message_data.decode('utf-8')
            message = Message.from_json(json_str)
            
            return message
        
        except Exception as e:
            return None
    
    def _recv_exactly(self, sock: socket.socket, num_bytes: int) -> Optional[bytes]:
        data = b''
        while len(data) < num_bytes:
            chunk = sock.recv(num_bytes - len(data))
            if not chunk:
                return None
            data += chunk
        return data
    
    def _send_message(self, sock: socket.socket, message: Message):
        try:
            message_bytes = message.to_bytes()
            sock.sendall(message_bytes)
            
            if self.metrics:
                self.metrics.record_message_sent(
                    operation=message.msg_type.value if isinstance(message.msg_type, MessageType) else str(message.msg_type),
                    bytes_sent=len(message_bytes)
                )
        
        except Exception as e:
            raise
    
    def _handle_request(self, request: Message) -> ResponseMessage:
        try:
            return create_response(
                request,
                result=None,
                success=False,
                error="Method not implemented in base class"
            )
        
        except Exception as e:
            return create_response(
                request,
                result=None,
                success=False,
                error=str(e)
            )
    
    def _handle_response(self, response: ResponseMessage):
        request_id = response.request_id
        
        with self.response_lock:
            if request_id in self.pending_responses:
                self.pending_responses[request_id].put(response)
    
    def send_request(
        self,
        target_address: str,
        operation: MessageType,
        *args,
        **kwargs
    ) -> Any:
        request = create_request(
            operation=operation,
            sender_address=self.address,
            receiver_address=target_address,
            *args,
            **kwargs
        )
        
        response_queue = Queue()
        with self.response_lock:
            self.pending_responses[request.request_id] = response_queue
        
        if self.metrics:
            self.metrics.start_request(request.request_id, operation.value)
        
        try:
            self._send_request_to_node(target_address, request)
            
            try:
                response = response_queue.get(timeout=self.timeout)
            except Empty:
                raise TimeoutError(f"Request {request.request_id} to {target_address} timed out")
            
            if self.metrics:
                self.metrics.complete_request(
                    request.request_id,
                    operation.value,
                    success=response.success
                )
            
            if not response.success:
                raise Exception(f"Remote error: {response.error}")
            
            return response.result
        
        finally:
            with self.response_lock:
                self.pending_responses.pop(request.request_id, None)
    
    def _send_request_to_node(self, target_address: str, request: RequestMessage):
        parts = target_address.split(':')
        target_ip = parts[0]
        target_port = int(parts[1])
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        
        try:
            sock.connect((target_ip, target_port))
            self._send_message(sock, request)
            
            response = self._receive_message(sock)
            if response:
                self._handle_response(response)
        
        finally:
            sock.close()
    
    def __enter__(self):
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
    
    def __repr__(self):
        return f"<NetworkNodeTCP {self.address} running={self.running}>"
