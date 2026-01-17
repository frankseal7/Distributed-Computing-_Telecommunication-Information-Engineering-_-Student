import random
import time
import threading
from typing import Dict, List, Set, Optional
from enum import Enum
from dataclasses import dataclass
from collections import defaultdict
import heapq

class NodeState(Enum):
    PRIMARY = "primary"
    SECONDARY = "secondary"
    FAILED = "failed"
    RECOVERING = "recovering"

class FailureType(Enum):
    CRASH = "crash"
    NETWORK_PARTITION = "network_partition"
    BYZANTINE = "byzantine"
    CORRELATED = "correlated"

@dataclass
class Node:
    id: str
    state: NodeState
    data: Dict[str, any]
    replicas: List[str]  # IDs of replica nodes
    last_heartbeat: float = 0
    failover_priority: int = 0  # Lower = higher priority
    
    def __post_init__(self):
        self.lock = threading.Lock()
        self.heartbeat_interval = 1.0  # seconds
        
    def update_heartbeat(self):
        with self.lock:
            self.last_heartbeat = time.time()
    
    def is_alive(self, timeout: float = 2.0) -> bool:
        with self.lock:
            return time.time() - self.last_heartbeat <= timeout

@dataclass
class Service:
    id: str
    primary_node: str
    replica_nodes: List[str]
    state: Dict[str, any]
    
    def get_active_replicas(self, node_map: Dict[str, Node]) -> List[str]:
        """Get list of replica nodes that are alive"""
        alive = []
        for node_id in [self.primary_node] + self.replica_nodes:
            node = node_map.get(node_id)
            if node and node.state != NodeState.FAILED:
                alive.append(node_id)
        return alive
    
    def quorum_available(self, node_map: Dict[str, Node]) -> bool:
        """Check if quorum of replicas is available"""
        alive_nodes = self.get_active_replicas(node_map)
        return len(alive_nodes) > (len(self.replica_nodes) + 1) // 2

class RedundancyManager:
    def __init__(self):
        self.nodes: Dict[str, Node] = {}
        self.services: Dict[str, Service] = {}
        self.node_groups: Dict[str, List[str]] = {}  # Groups for correlated failures
        self.network_partitions: List[Set[str]] = []
        self.failure_log = []
        self.lock = threading.RLock()
        
    def add_node(self, node_id: str, replicas: List[str], 
                 group: str = "default", failover_priority: int = 0):
        with self.lock:
            self.nodes[node_id] = Node(
                id=node_id,
                state=NodeState.PRIMARY if failover_priority == 0 else NodeState.SECONDARY,
                data={},
                replicas=replicas,
                failover_priority=failover_priority
            )
            
            if group not in self.node_groups:
                self.node_groups[group] = []
            self.node_groups[group].append(node_id)
    
    def add_service(self, service_id: str, primary_node: str, replica_nodes: List[str]):
        with self.lock:
            self.services[service_id] = Service(
                id=service_id,
                primary_node=primary_node,
                replica_nodes=replica_nodes,
                state={}
            )
    
    def replicate_data(self, node_id: str, key: str, value: any) -> bool:
        """Replicate data to all replicas with consistency"""
        node = self.nodes.get(node_id)
        if not node or node.state == NodeState.FAILED:
            return False
        
        # Update primary
        node.data[key] = value
        
        # Replicate to secondaries
        success_count = 1  # Primary updated successfully
        
        for replica_id in node.replicas:
            replica = self.nodes.get(replica_id)
            if replica and replica.state != NodeState.FAILED:
                # Simulate network delay
                time.sleep(random.uniform(0.001, 0.01))
                replica.data[key] = value
                success_count += 1
        
        # Check if we have quorum
        required_quorum = len(node.replicas) // 2 + 1
        return success_count >= required_quorum
    
    def automated_failover(self, failed_node_id: str):
        """Automatically failover services from failed node"""
        with self.lock:
            failed_node = self.nodes.get(failed_node_id)
            if not failed_node:
                return
            
            failed_node.state = NodeState.FAILED
            print(f"Node {failed_node_id} marked as failed. Initiating failover...")
            
            # Find services affected by this failure
            affected_services = []
            for service in self.services.values():
                if failed_node_id == service.primary_node or failed_node_id in service.replica_nodes:
                    affected_services.append(service)
            
            for service in affected_services:
                self.failover_service(service, failed_node_id)
    
    def failover_service(self, service: Service, failed_node_id: str):
        """Failover a specific service"""
        with self.lock:
            # If primary failed, promote a replica
            if failed_node_id == service.primary_node:
                # Find best candidate for promotion
                candidates = []
                for replica_id in service.replica_nodes:
                    node = self.nodes.get(replica_id)
                    if node and node.state != NodeState.FAILED:
                        candidates.append((node.failover_priority, replica_id))
                
                if candidates:
                    # Select replica with highest priority (lowest number)
                    candidates.sort()
                    new_primary_id = candidates[0][1]
                    
                    # Update service
                    service.primary_node = new_primary_id
                    service.replica_nodes = [n for n in service.replica_nodes 
                                           if n != new_primary_id]
                    
                    print(f"Service {service.id}: Promoted node {new_primary_id} to primary")
                    
                    # Re-replicate data to new replicas if needed
                    self.rebalance_replicas(service)
                else:
                    print(f"Service {service.id}: No available replicas for failover!")
            
            # If replica failed, replace it
            elif failed_node_id in service.replica_nodes:
                service.replica_nodes = [n for n in service.replica_nodes 
                                       if n != failed_node_id]
                
                # Find replacement replica if we're below desired replication factor
                desired_replicas = 3
                if len(service.replica_nodes) < desired_replicas:
                    available_nodes = [nid for nid, node in self.nodes.items()
                                     if node.state != NodeState.FAILED and
                                     nid != service.primary_node and
                                     nid not in service.replica_nodes]
                    
                    if available_nodes:
                        new_replica = random.choice(available_nodes)
                        service.replica_nodes.append(new_replica)
                        print(f"Service {service.id}: Added node {new_replica} as new replica")
                        
                        # Sync data to new replica
                        self.sync_node_data(service.primary_node, new_replica)
    
    def sync_node_data(self, source_id: str, target_id: str):
        """Sync data from source node to target node"""
        source = self.nodes.get(source_id)
        target = self.nodes.get(target_id)
        
        if source and target:
            target.data = source.data.copy()
            print(f"Synced data from {source_id} to {target_id}")
    
    def rebalance_replicas(self, service: Service):
        """Ensure service has enough healthy replicas"""
        desired_replicas = 3
        current_replicas = len(service.replica_nodes)
        
        if current_replicas < desired_replicas:
            needed = desired_replicas - current_replicas
            
            # Find available nodes
            available = [nid for nid, node in self.nodes.items()
                        if node.state != NodeState.FAILED and
                        nid != service.primary_node and
                        nid not in service.replica_nodes]
            
            for _ in range(min(needed, len(available))):
                new_replica = available.pop(0)
                service.replica_nodes.append(new_replica)
                
                # Sync data
                self.sync_node_data(service.primary_node, new_replica)
                print(f"Service {service.id}: Added {new_replica} as additional replica")
    
    def inject_failure(self, failure_type: FailureType, 
                      target: Optional[str] = None,
                      duration: float = 30.0):
        """Inject various types of failures"""
        
        def failure_thread():
            print(f"Injecting {failure_type.value} failure...")
            
            if failure_type == FailureType.CRASH:
                # Crash a single node
                if not target:
                    target_node = random.choice(list(self.nodes.keys()))
                else:
                    target_node = target
                
                self.automated_failover(target_node)
                
                # Recovery after duration
                time.sleep(duration)
                self.recover_node(target_node)
            
            elif failure_type == FailureType.NETWORK_PARTITION:
                # Create network partition
                all_nodes = list(self.nodes.keys())
                split = len(all_nodes) // 2
                group1 = set(all_nodes[:split])
                group2 = set(all_nodes[split:])
                
                self.network_partitions = [group1, group2]
                print(f"Network partition created: {group1} | {group2}")
                
                # Simulate partition
                time.sleep(duration)
                self.network_partitions = []
                print("Network partition resolved")
            
            elif failure_type == FailureType.CORRELATED:
                # Correlated failure in a group
                if self.node_groups:
                    group_name = random.choice(list(self.node_groups.keys()))
                    nodes_in_group = self.node_groups[group_name]
                    
                    print(f"Correlated failure in group {group_name}: {nodes_in_group}")
                    
                    for node_id in nodes_in_group:
                        self.automated_failover(node_id)
                    
                    time.sleep(duration)
                    
                    for node_id in nodes_in_group:
                        self.recover_node(node_id)
            
            elif failure_type == FailureType.BYZANTINE:
                # Byzantine failure - node behaves arbitrarily
                if not target:
                    target_node = random.choice(list(self.nodes.keys()))
                else:
                    target_node = target
                
                print(f"Byzantine failure on node {target_node}")
                # In real system, would need BFT consensus to handle
                # Here we just mark as failed
                self.automated_failover(target_node)
                
                time.sleep(duration)
                self.recover_node(target_node)
        
        threading.Thread(target=failure_thread, daemon=True).start()
    
    def recover_node(self, node_id: str):
        """Recover a failed node"""
        with self.lock:
            node = self.nodes.get(node_id)
            if node:
                node.state = NodeState.RECOVERING
                print(f"Node {node_id} recovering...")
                
                # Find a healthy node to sync from
                for service in self.services.values():
                    if node_id in service.replica_nodes or node_id == service.primary_node:
                        # Sync from primary
                        if service.primary_node != node_id:
                            self.sync_node_data(service.primary_node, node_id)
                
                node.state = NodeState.SECONDARY
                print(f"Node {node_id} recovered and ready")
    
    def monitor_nodes(self):
        """Continuously monitor node health"""
        while True:
            time.sleep(1)
            with self.lock:
                for node in self.nodes.values():
                    if node.state != NodeState.FAILED:
                        node.update_heartbeat()
                        
                        # Check if node appears dead
                        if not node.is_alive():
                            print(f"Node {node.id} appears dead (no heartbeat)")
                            self.automated_failover(node.id)
    
    def simulate_multi_node_failure(self, num_failures: int = 3):
        """Simulate simultaneous multiple node failures"""
        print(f"Simulating {num_failures} simultaneous node failures")
        
        all_nodes = list(self.nodes.keys())
        if len(all_nodes) < num_failures:
            num_failures = len(all_nodes)
        
        failed_nodes = random.sample(all_nodes, num_failures)
        
        # Fail all nodes simultaneously
        for node_id in failed_nodes:
            self.automated_failover(node_id)
        
        print(f"Failed nodes: {failed_nodes}")
        
        # Wait and recover
        time.sleep(20)
        
        for node_id in failed_nodes:
            self.recover_node(node_id)
        
        print("All nodes recovered from multi-node failure")

def main():
    # Create redundancy manager
    manager = RedundancyManager()
    
    # Create nodes with replication relationships
    # Group nodes for correlated failures
    manager.add_node("node1", replicas=["node2", "node3"], group="datacenter1", failover_priority=0)
    manager.add_node("node2", replicas=["node1", "node3"], group="datacenter1", failover_priority=1)
    manager.add_node("node3", replicas=["node1", "node2"], group="datacenter2", failover_priority=2)
    manager.add_node("node4", replicas=["node5", "node6"], group="datacenter2", failover_priority=0)
    manager.add_node("node5", replicas=["node4", "node6"], group="datacenter3", failover_priority=1)
    manager.add_node("node6", replicas=["node4", "node5"], group="datacenter3", failover_priority=2)
    
    # Create services with replication
    manager.add_service("service1", "node1", ["node2", "node3"])
    manager.add_service("service2", "node4", ["node5", "node6"])
    manager.add_service("service3", "node2", ["node1", "node4"])
    
    # Start monitoring
    monitor_thread = threading.Thread(target=manager.monitor_nodes, daemon=True)
    monitor_thread.start()
    
    # Simulate some data replication
    print("Testing data replication...")
    manager.replicate_data("node1", "key1", "value1")
    manager.replicate_data("node4", "key2", "value2")
    
    # Run various failure scenarios
    print("\n=== Running Failure Scenarios ===")
    
    # 1. Single node crash
    time.sleep(2)
    manager.inject_failure(FailureType.CRASH, "node2", duration=15)
    
    # 2. Network partition
    time.sleep(20)
    manager.inject_failure(FailureType.NETWORK_PARTITION, duration=20)
    
    # 3. Multi-node failure
    time.sleep(25)
    manager.simulate_multi_node_failure(3)
    
    # 4. Correlated failure
    time.sleep(25)
    manager.inject_failure(FailureType.CORRELATED, duration=15)
    
    # 5. Byzantine failure
    time.sleep(20)
    manager.inject_failure(FailureType.BYZANTINE, "node3", duration=15)
    
    # Keep running
    print("\nSimulation running for 60 seconds...")
    time.sleep(60)
    print("Simulation complete.")

if __name__ == "__main__":
    main()