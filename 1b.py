import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.io.*;
import java.net.*;

class DistributedSystem {
    
    // Node class
    static class Node {
        String id;
        String tier; // "edge", "core", "cloud"
        int cpu;
        int memory;
        boolean active = true;
        Queue<Message> messageQueue = new PriorityQueue<>(
            Comparator.comparingInt(Message::getPriority).reversed()
        );
        Map<String, Service> services = new ConcurrentHashMap<>();
        Map<String, Transaction> activeTxns = new ConcurrentHashMap<>();
        
        Node(String id, String tier, int cpu, int memory) {
            this.id = id;
            this.tier = tier;
            this.cpu = cpu;
            this.memory = memory;
        }
        
        void sendRPC(Node dest, String serviceName, Object params) {
            Message msg = new Message(
                this.id, dest.id, "RPC",
                new RPCRequest(serviceName, params),
                System.currentTimeMillis(),
                5 // medium priority
            );
            Network.send(msg);
        }
        
        void handleMessage(Message msg) {
            if (!active) return;
            
            switch (msg.type) {
                case "RPC":
                    handleRPC((RPCRequest) msg.content);
                    break;
                case "TXN_START":
                    startTransaction((Transaction) msg.content);
                    break;
                case "TXN_PREPARE":
                    prepareTransaction((Transaction) msg.content);
                    break;
                case "TXN_COMMIT":
                    commitTransaction((String) msg.content);
                    break;
                case "RECOVERY":
                    recover();
                    break;
            }
        }
        
        void handleRPC(RPCRequest req) {
            Service service = services.get(req.serviceName);
            if (service != null) {
                service.execute(req.params);
            }
        }
        
        void startTransaction(Transaction txn) {
            activeTxns.put(txn.id, txn);
            // Send prepare to all participants
            for (String nodeId : txn.participants) {
                Message prepMsg = new Message(
                    this.id, nodeId, "TXN_PREPARE",
                    txn, System.currentTimeMillis(), 10
                );
                Network.send(prepMsg);
            }
        }
        
        void prepareTransaction(Transaction txn) {
            // Simulate lock acquisition
            boolean success = acquireLocks(txn);
            Message vote = new Message(
                this.id, txn.coordinator, "VOTE",
                success ? "YES" : "NO",
                System.currentTimeMillis(), 10
            );
            Network.send(vote);
        }
        
        boolean acquireLocks(Transaction txn) {
            // Simple lock acquisition simulation
            Random rand = new Random();
            return rand.nextDouble() > 0.1; // 10% failure rate
        }
        
        void commitTransaction(String txnId) {
            Transaction txn = activeTxns.get(txnId);
            if (txn != null) {
                // Commit changes
                activeTxns.remove(txnId);
            }
        }
        
        void recover() {
            // Recovery from checkpoint
            System.out.println("Node " + id + " recovering...");
            active = true;
        }
    }
    
    // Service class
    static class Service {
        String name;
        Node host;
        
        Service(String name, Node host) {
            this.name = name;
            this.host = host;
            host.services.put(name, this);
        }
        
        void execute(Object params) {
            // Simulate service execution
            System.out.println("Service " + name + " executing on " + host.id);
        }
    }
    
    // Transaction class
    static class Transaction {
        String id;
        String coordinator;
        List<String> participants;
        Map<String, Object> operations;
        
        Transaction(String coordinator, List<String> participants) {
            this.id = UUID.randomUUID().toString();
            this.coordinator = coordinator;
            this.participants = participants;
            this.operations = new HashMap<>();
        }
    }
    
    // Message class
    static class Message {
        String src;
        String dest;
        String type;
        Object content;
        long timestamp;
        int priority; // higher = more important
        
        Message(String src, String dest, String type, 
                Object content, long timestamp, int priority) {
            this.src = src;
            this.dest = dest;
            this.type = type;
            this.content = content;
            this.timestamp = timestamp;
            this.priority = priority;
        }
        
        int getPriority() { return priority; }
    }
    
    // RPC Request
    static class RPCRequest {
        String serviceName;
        Object params;
        
        RPCRequest(String serviceName, Object params) {
            this.serviceName = serviceName;
            this.params = params;
        }
    }
    
    // Network simulation
    static class Network {
        static Map<String, Node> nodes = new ConcurrentHashMap<>();
        static Random rand = new Random();
        
        static void send(Message msg) {
            // Simulate network delay (10-100ms)
            int delay = 10 + rand.nextInt(91);
            
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.schedule(() -> {
                Node dest = nodes.get(msg.dest);
                if (dest != null && dest.active) {
                    // Simulate packet loss (2% chance)
                    if (rand.nextDouble() > 0.02) {
                        dest.messageQueue.add(msg);
                    }
                }
            }, delay, TimeUnit.MILLISECONDS);
            scheduler.shutdown();
        }
        
        static void injectFailure(String nodeId) {
            Node node = nodes.get(nodeId);
            if (node != null) {
                node.active = false;
                System.out.println("Node " + nodeId + " failed!");
            }
        }
    }
    
    // Main simulation
    public static void main(String[] args) throws Exception {
        // Create nodes
        Node edge1 = new Node("edge1", "edge", 4, 8192);
        Node edge2 = new Node("edge2", "edge", 4, 8192);
        Node core1 = new Node("core1", "core", 16, 32768);
        Node cloud1 = new Node("cloud1", "cloud", 32, 131072);
        
        Network.nodes.put("edge1", edge1);
        Network.nodes.put("edge2", edge2);
        Network.nodes.put("core1", core1);
        Network.nodes.put("cloud1", cloud1);
        
        // Create services
        new Service("edgeProcessing", edge1);
        new Service("dataAggregation", core1);
        new Service("analytics", cloud1);
        
        // Start message processors for each node
        for (Node node : Network.nodes.values()) {
            new Thread(() -> {
                while (true) {
                    try {
                        Message msg = node.messageQueue.poll();
                        if (msg != null) {
                            node.handleMessage(msg);
                        }
                        Thread.sleep(10);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
        
        // Simulate some activity
        Thread.sleep(1000);
        
        // Send RPC
        edge1.sendRPC(core1, "dataAggregation", "process_data");
        
        // Start transaction
        Transaction txn = new Transaction("core1", 
            Arrays.asList("edge1", "edge2", "cloud1"));
        Message txnMsg = new Message("client", "core1", "TXN_START",
            txn, System.currentTimeMillis(), 10);
        Network.send(txnMsg);
        
        // Inject failure
        Thread.sleep(2000);
        Network.injectFailure("edge1");
        
        // Recovery
        Thread.sleep(1000);
        Message recoveryMsg = new Message("coordinator", "edge1", "RECOVERY",
            null, System.currentTimeMillis(), 10);
        Network.send(recoveryMsg);
        
        // Keep running
        Thread.sleep(5000);
        System.out.println("Simulation complete.");
    }
}