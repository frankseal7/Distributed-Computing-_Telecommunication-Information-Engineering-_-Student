import simpy
import random
import time
from typing import Dict, List, Tuple
from dataclasses import dataclass
from enum import Enum
import statistics

class NodeState(Enum):
    ACTIVE = "active"
    OVERLOADED = "overloaded"
    FAILED = "failed"
    MIGRATING = "migrating"

@dataclass
class Node:
    id: str
    cpu_capacity: int
    memory_capacity: int
    env: simpy.Environment
    processes: List['Process'] = None
    state: NodeState = NodeState.ACTIVE
    
    def __post_init__(self):
        self.processes = []
        self.cpu_used = 0
        self.memory_used = 0
        self.load_history = []
    
    def can_host(self, process: 'Process') -> bool:
        return (self.cpu_used + process.cpu_req <= self.cpu_capacity and
                self.memory_used + process.memory_req <= self.memory_capacity and
                self.state == NodeState.ACTIVE)
    
    def add_process(self, process: 'Process'):
        if self.can_host(process):
            self.processes.append(process)
            self.cpu_used += process.cpu_req
            self.memory_used += process.memory_req
            process.host = self
            return True
        return False
    
    def remove_process(self, process: 'Process'):
        if process in self.processes:
            self.processes.remove(process)
            self.cpu_used -= process.cpu_req
            self.memory_used -= process.memory_req
    
    def get_load(self) -> float:
        cpu_load = self.cpu_used / self.cpu_capacity if self.cpu_capacity > 0 else 0
        mem_load = self.memory_used / self.memory_capacity if self.memory_capacity > 0 else 0
        return (cpu_load + mem_load) / 2
    
    def update_load_history(self):
        self.load_history.append(self.get_load())
        if len(self.load_history) > 100:
            self.load_history.pop(0)

@dataclass
class Process:
    id: str
    cpu_req: int
    memory_req: int
    duration: float
    host: Node = None
    start_time: float = None
    
    def execute(self, env: simpy.Environment):
        self.start_time = env.now
        yield env.timeout(self.duration)
        if self.host:
            self.host.remove_process(self)

class LoadBalancer:
    def __init__(self, env: simpy.Environment, nodes: List[Node]):
        self.env = env
        self.nodes = {node.id: node for node in nodes}
        self.process_queue = []
        self.migration_history = []
        self.failure_injection_active = False
        
    def place_process(self, process: Process) -> bool:
        # Try to find suitable node
        suitable_nodes = [n for n in self.nodes.values() if n.can_host(process)]
        
        if not suitable_nodes:
            print(f"No suitable node for process {process.id}")
            return False
        
        # Strategy 1: Least loaded node
        best_node = min(suitable_nodes, key=lambda n: n.get_load())
        
        # Strategy 2: Consider network delay (simplified)
        if len(suitable_nodes) > 1:
            # Prefer nodes with lower estimated latency
            latencies = {n.id: random.uniform(1, 50) for n in suitable_nodes}
            best_node = min(suitable_nodes, key=lambda n: latencies[n.id])
        
        success = best_node.add_process(process)
        if success:
            # Start process execution
            self.env.process(process.execute(self.env))
            print(f"Placed process {process.id} on node {best_node.id}")
            return True
        return False
    
    def balance_load(self):
        while True:
            yield self.env.timeout(5)  # Check every 5 time units
            
            # Find overloaded nodes
            overloaded = [n for n in self.nodes.values() 
                         if n.get_load() > 0.8 and n.state == NodeState.ACTIVE]
            
            for node in overloaded:
                print(f"Node {node.id} overloaded (load: {node.get_load():.2f})")
                node.state = NodeState.OVERLOADED
                
                # Migrate some processes
                if node.processes:
                    process_to_migrate = random.choice(node.processes)
                    self.migrate_process(process_to_migrate)
            
            # Find underloaded nodes that could take more processes
            underloaded = [n for n in self.nodes.values() 
                          if n.get_load() < 0.3 and n.state == NodeState.ACTIVE]
            
            # Update load history for all nodes
            for node in self.nodes.values():
                node.update_load_history()
    
    def migrate_process(self, process: Process):
        source = process.host
        if not source:
            return
        
        source.remove_process(process)
        
        # Find new host
        potential_hosts = [n for n in self.nodes.values() 
                          if n.id != source.id and n.can_host(process)]
        
        if potential_hosts:
            # Choose host with lowest load
            new_host = min(potential_hosts, key=lambda n: n.get_load())
            
            # Simulate migration delay
            migration_delay = random.uniform(10, 100) / 1000  # 10-100ms
            yield self.env.timeout(migration_delay)
            
            success = new_host.add_process(process)
            if success:
                self.migration_history.append({
                    'process': process.id,
                    'from': source.id,
                    'to': new_host.id,
                    'time': self.env.now,
                    'delay': migration_delay
                })
                print(f"Migrated process {process.id} from {source.id} to {new_host.id}")
                source.state = NodeState.ACTIVE
            else:
                # Return to source if migration fails
                source.add_process(process)
        else:
            # No suitable host found, return to source
            source.add_process(process)
    
    def inject_failure(self, node_id: str, duration: float = 30):
        node = self.nodes.get(node_id)
        if node and node.state == NodeState.ACTIVE:
            node.state = NodeState.FAILED
            print(f"Injected failure on node {node_id}")
            
            # Evacuate processes from failed node
            for process in node.processes[:]:
                self.migrate_process(process)
            
            # Recovery after duration
            yield self.env.timeout(duration)
            node.state = NodeState.ACTIVE
            print(f"Node {node_id} recovered")
    
    def simulate_network_partition(self, partition_groups: List[List[str]], duration: float = 20):
        """Simulate network partition where groups can't communicate"""
        print(f"Network partition for {duration} time units")
        
        # Store original state
        original_nodes = {id: node.state for id, node in self.nodes.items()}
        
        # Mark nodes in different partitions as unreachable
        for group in partition_groups:
            for node_id in group:
                node = self.nodes.get(node_id)
                if node:
                    node.state = NodeState.FAILED
        
        yield self.env.timeout(duration)
        
        # Restore
        for node_id, state in original_nodes.items():
            node = self.nodes.get(node_id)
            if node:
                node.state = state
        
        print("Network partition resolved")

def main_simulation():
    env = simpy.Environment()
    
    # Create nodes with different capacities
    nodes = [
        Node("edge1", cpu_capacity=4, memory_capacity=8, env=env),
        Node("edge2", cpu_capacity=4, memory_capacity=8, env=env),
        Node("core1", cpu_capacity=16, memory_capacity=32, env=env),
        Node("core2", cpu_capacity=16, memory_capacity=32, env=env),
        Node("cloud1", cpu_capacity=64, memory_capacity=128, env=env),
    ]
    
    balancer = LoadBalancer(env, nodes)
    
    # Start load balancing
    env.process(balancer.balance_load())
    
    # Generate processes
    def process_generator():
        process_id = 0
        while True:
            # Random arrival rate
            yield env.timeout(random.expovariate(1.0))
            
            # Create process with random requirements
            process = Process(
                id=f"proc_{process_id}",
                cpu_req=random.randint(1, 4),
                memory_req=random.randint(1, 4),
                duration=random.uniform(10, 50)
            )
            
            balancer.place_process(process)
            process_id += 1
    
    env.process(process_generator())
    
    # Inject failures periodically
    def failure_generator():
        while True:
            yield env.timeout(random.uniform(30, 60))
            if random.random() < 0.3:  # 30% chance
                node_to_fail = random.choice(list(balancer.nodes.keys()))
                env.process(balancer.inject_failure(node_to_fail, duration=15))
    
    env.process(failure_generator())
    
    # Run simulation
    print("Starting load balancing simulation...")
    runtime = 300  # Simulate for 300 time units
    env.run(until=runtime)
    
    # Print statistics
    print("\n=== Simulation Results ===")
    for node in nodes:
        if node.load_history:
            avg_load = statistics.mean(node.load_history)
            print(f"Node {node.id}: Avg load = {avg_load:.2f}, "
                  f"Max load = {max(node.load_history):.2f}, "
                  f"Migrations handled = {len([m for m in balancer.migration_history if m['to'] == node.id])}")
    
    print(f"\nTotal migrations: {len(balancer.migration_history)}")
    if balancer.migration_history:
        avg_migration_time = statistics.mean([m['delay'] for m in balancer.migration_history])
        print(f"Average migration delay: {avg_migration_time:.3f}")

if __name__ == "__main__":
    main_simulation()