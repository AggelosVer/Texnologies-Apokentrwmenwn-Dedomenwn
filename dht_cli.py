#!/usr/bin/env python3

import cmd
import sys
import threading
import time
from typing import Dict, List, Optional, Any
from chord_network_tcp import ChordNetworkNode
from pastry_network_tcp import PastryNetworkNode

class DHTCLI(cmd.Cmd):
    intro = """
╔══════════════════════════════════════════════════════════════╗
║          DHT Interactive CLI Tool                            ║
║          Chord & Pastry Network Management                   ║
╔══════════════════════════════════════════════════════════════╗

Type 'help' or '?' to list available commands.
Type 'help <command>' for detailed information about a command.
"""
    prompt = 'DHT> '
    
    def __init__(self):
        super().__init__()
        self.network_type: Optional[str] = None
        self.nodes: Dict[str, Any] = {}
        self.base_port = 5000
        self.next_port = self.base_port
        
    def do_init(self, arg):
        network_type = arg.strip().lower()
        
        if network_type not in ['chord', 'pastry']:
            print("Error: Network type must be 'chord' or 'pastry'")
            return
        
        if self.network_type is not None:
            print(f"Warning: Network already initialized as {self.network_type}")
            response = input("Do you want to reinitialize? (yes/no): ").strip().lower()
            if response != 'yes':
                return
            self.do_shutdown('')
        
        self.network_type = network_type
        print(f"Initialized {network_type.upper()} network")
    
    def do_add_node(self, arg):
        if self.network_type is None:
            print("Error: Network not initialized. Use 'init <chord|pastry>' first")
            return
        
        args = arg.split()
        node_name = None
        port = None
        introducer_name = None
        
        i = 0
        while i < len(args):
            if args[i] == '--port' and i + 1 < len(args):
                port = int(args[i + 1])
                i += 2
            elif args[i] == '--introducer' and i + 1 < len(args):
                introducer_name = args[i + 1]
                i += 2
            elif not args[i].startswith('--'):
                node_name = args[i]
                i += 1
            else:
                i += 1
        
        if node_name is None:
            node_name = f"node{len(self.nodes) + 1}"
        
        if node_name in self.nodes:
            print(f"Error: Node '{node_name}' already exists")
            return
        
        if port is None:
            port = self.next_port
            self.next_port += 1
        
        introducer = None
        if introducer_name:
            if introducer_name not in self.nodes:
                print(f"Error: Introducer node '{introducer_name}' not found")
                return
            introducer = self.nodes[introducer_name]
        
        try:
            if self.network_type == 'chord':
                node = ChordNetworkNode(ip="127.0.0.1", port=port)
                node.start()
                time.sleep(0.5)
                
                if introducer:
                    node.chord_node.join(introducer.chord_node)
                else:
                    node.chord_node.create_ring()
                
            else:
                node = PastryNetworkNode(ip="127.0.0.1", port=port)
                node.start()
                time.sleep(0.5)
                
                if introducer:
                    node.pastry_node.join(introducer.pastry_node)
            
            self.nodes[node_name] = node
            print(f"Added node '{node_name}' at 127.0.0.1:{port}")
            
            if self.network_type == 'chord':
                hex_id = node.chord_node.hasher.get_hex_id(node.chord_node.id)
                print(f"   Node ID: {hex_id[:16]}...")
            else:
                print(f"   Node ID: {node.pastry_node.hex_id[:16]}...")
                
        except Exception as e:
            print(f"Error adding node: {e}")
    
    def do_remove_node(self, arg):
        node_name = arg.strip()
        
        if not node_name:
            print("Error: Please specify a node name")
            return
        
        if node_name not in self.nodes:
            print(f"Error: Node '{node_name}' not found")
            return
        
        try:
            node = self.nodes[node_name]
            
            if self.network_type == 'chord':
                node.chord_node.leave()
            else:
                node.pastry_node.leave()
            
            node.stop()
            del self.nodes[node_name]
            
            print(f"Removed node '{node_name}'")
            
        except Exception as e:
            print(f"Error removing node: {e}")
    
    def do_insert(self, arg):
        args = arg.split(maxsplit=2)
        
        if len(args) < 3:
            print("Error: Usage: insert <node_name> <key> <value>")
            return
        
        node_name, key, value = args
        
        if node_name not in self.nodes:
            print(f"Error: Node '{node_name}' not found")
            return
        
        try:
            node = self.nodes[node_name]
            
            if self.network_type == 'chord':
                node.insert(key, value)
                print(f"Inserted '{key}' = '{value}' via {node_name}")
            else:
                success, hops = node.insert(key, value)
                if success:
                    print(f"Inserted '{key}' = '{value}' via {node_name} ({hops} hops)")
                else:
                    print(f"Failed to insert '{key}'")
                    
        except Exception as e:
            print(f"Error inserting key: {e}")
    
    def do_lookup(self, arg):
        args = arg.split()
        
        if len(args) < 2:
            print("Error: Usage: lookup <node_name> <key>")
            return
        
        node_name, key = args[0], args[1]
        
        if node_name not in self.nodes:
            print(f"Error: Node '{node_name}' not found")
            return
        
        try:
            node = self.nodes[node_name]
            
            if self.network_type == 'chord':
                value = node.lookup(key)
                if value is not None:
                    print(f"Found: '{key}' = '{value}'")
                else:
                    print(f"Key '{key}' not found")
            else:
                value, hops = node.lookup(key)
                if value is not None:
                    print(f"Found: '{key}' = '{value}' ({hops} hops)")
                else:
                    print(f"Key '{key}' not found ({hops} hops)")
                    
        except Exception as e:
            print(f"Error looking up key: {e}")
    
    def do_delete(self, arg):
        args = arg.split()
        
        if len(args) < 2:
            print("Error: Usage: delete <node_name> <key>")
            return
        
        node_name, key = args[0], args[1]
        
        if node_name not in self.nodes:
            print(f"Error: Node '{node_name}' not found")
            return
        
        try:
            node = self.nodes[node_name]
            
            if self.network_type == 'chord':
                result = node.delete(key)
                if result:
                    print(f"Deleted key '{key}'")
                else:
                    print(f"Key '{key}' not found")
            else:
                success, hops = node.delete(key)
                if success:
                    print(f"Deleted key '{key}' ({hops} hops)")
                else:
                    print(f"Key '{key}' not found ({hops} hops)")
                    
        except Exception as e:
            print(f"Error deleting key: {e}")
    
    def do_list_nodes(self, arg):
        if not self.nodes:
            print("No nodes in the network")
            return
        
        print(f"\n{'Node Name':<15} {'Address':<20} {'Node ID':<20}")
        print("=" * 60)
        
        for name, node in self.nodes.items():
            address = f"{node.listen_ip}:{node.listen_port}"
            
            if self.network_type == 'chord':
                hex_id = node.chord_node.hasher.get_hex_id(node.chord_node.id)
                node_id = hex_id[:16] + "..."
            else:
                node_id = node.pastry_node.hex_id[:16] + "..."
            
            print(f"{name:<15} {address:<20} {node_id:<20}")
        
        print()
    
    def do_node_info(self, arg):
        node_name = arg.strip()
        
        if not node_name:
            print("Error: Please specify a node name")
            return
        
        if node_name not in self.nodes:
            print(f"Error: Node '{node_name}' not found")
            return
        
        node = self.nodes[node_name]
        
        print(f"\n{'='*60}")
        print(f"Node Information: {node_name}")
        print(f"{'='*60}")
        print(f"Address:     {node.listen_ip}:{node.listen_port}")
        
        if self.network_type == 'chord':
            chord_node = node.chord_node
            hex_id = chord_node.hasher.get_hex_id(chord_node.id)
            print(f"Node ID:     {hex_id}")
            print(f"Successor:   {chord_node.successor.address if chord_node.successor else 'None'}")
            print(f"Predecessor: {chord_node.predecessor.address if chord_node.predecessor else 'None'}")
            print(f"Data items:  {len(chord_node.data)}")
            
            if chord_node.data:
                print(f"\nStored Keys:")
                for key in list(chord_node.data.keys())[:10]:
                    print(f"  - {key}")
                if len(chord_node.data) > 10:
                    print(f"  ... and {len(chord_node.data) - 10} more")
        else:
            pastry_node = node.pastry_node
            print(f"Node ID:     {pastry_node.hex_id}")
            print(f"Data items:  {len(pastry_node.data)}")
            
            leaf_set = pastry_node.get_leaf_set()
            print(f"Leaf Set:    {len(leaf_set)} nodes")
            
            if pastry_node.data:
                print(f"\nStored Keys:")
                for key in list(pastry_node.data.keys())[:10]:
                    print(f"  - {key}")
                if len(pastry_node.data) > 10:
                    print(f"  ... and {len(pastry_node.data) - 10} more")
        
        print(f"{'='*60}\n")
    
    def do_status(self, arg):
        if self.network_type is None:
            print("Network not initialized")
            return
        
        print(f"\n{'='*60}")
        print(f"Network Status")
        print(f"{'='*60}")
        print(f"Type:        {self.network_type.upper()}")
        print(f"Nodes:       {len(self.nodes)}")
        
        total_keys = 0
        for node in self.nodes.values():
            if self.network_type == 'chord':
                total_keys += len(node.chord_node.data)
            else:
                total_keys += len(node.pastry_node.data)
        
        print(f"Total Keys:  {total_keys}")
        print(f"{'='*60}\n")
    
    def do_batch_insert(self, arg):
        args = arg.split()
        
        if len(args) < 2:
            print("Error: Usage: batch_insert <node_name> <num_items>")
            return
        
        node_name = args[0]
        try:
            num_items = int(args[1])
        except ValueError:
            print("Error: num_items must be an integer")
            return
        
        if node_name not in self.nodes:
            print(f"Error: Node '{node_name}' not found")
            return
        
        node = self.nodes[node_name]
        
        print(f"Inserting {num_items} items...")
        start_time = time.time()
        
        for i in range(num_items):
            key = f"key_{i}"
            value = f"value_{i}"
            
            try:
                if self.network_type == 'chord':
                    node.insert(key, value)
                else:
                    node.insert(key, value)
            except Exception as e:
                print(f"Error inserting {key}: {e}")
        
        elapsed = time.time() - start_time
        print(f"Inserted {num_items} items in {elapsed:.2f} seconds")
        print(f"   Average: {num_items/elapsed:.2f} inserts/sec")
    
    def do_shutdown(self, arg):
        if not self.nodes:
            print("No nodes to shutdown")
            self.network_type = None
            return
        
        print("Shutting down all nodes...")
        
        for name, node in list(self.nodes.items()):
            try:
                if self.network_type == 'chord':
                    node.chord_node.leave()
                else:
                    node.pastry_node.leave()
                
                node.stop()
                print(f"Stopped {name}")
            except Exception as e:
                print(f"Error stopping {name}: {e}")
        
        self.nodes.clear()
        self.network_type = None
        self.next_port = self.base_port
        print("Network shutdown complete")
    
    def do_exit(self, arg):
        if self.nodes:
            response = input("There are active nodes. Shutdown before exiting? (yes/no): ").strip().lower()
            if response == 'yes':
                self.do_shutdown('')
        
        print("Goodbye!")
        return True
    
    def do_quit(self, arg):
        return self.do_exit(arg)
    
    def do_clear(self, arg):
        import os
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def emptyline(self):
        pass
    
    def default(self, line):
        print(f"Unknown command: {line}")
        print("Type 'help' for a list of commands")

def main():
    cli = DHTCLI()
    try:
        cli.cmdloop()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        cli.do_shutdown('')
        print("Goodbye!")
    except Exception as e:
        print(f"\nFatal error: {e}")
        cli.do_shutdown('')

if __name__ == '__main__':
    main()
