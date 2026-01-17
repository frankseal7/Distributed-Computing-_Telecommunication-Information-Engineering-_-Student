import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

class DistributedDeadlockDetector {
    
    static class Resource {
        String id;
        Lock lock = new ReentrantLock();
        Transaction owner = null;
        
        Resource(String id) {
            this.id = id;
        }
        
        synchronized boolean acquire(Transaction txn, long timeoutMs) {
            long start = System.currentTimeMillis();
            while (owner != null && owner != txn) {
                if (System.currentTimeMillis() - start > timeoutMs) {
                    return false;
                }
                try {
                    wait(timeoutMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            owner = txn;
            return true;
        }
        
        synchronized void release(Transaction txn) {
            if (owner == txn) {
                owner = null;
                notifyAll();
            }
        }
    }
    
    static class Transaction {
        String id;
        List<Resource> resources = new ArrayList<>();
        long startTime;
        boolean aborted = false;
        
        Transaction(String id) {
            this.id = id;
            this.startTime = System.currentTimeMillis();
        }
        
        boolean acquireResources(List<Resource> required, long timeoutMs) {
            // Sort resources to prevent deadlock (resource ordering)
            required.sort(Comparator.comparing(r -> r.id));
            
            for (Resource res : required) {
                if (!res.acquire(this, timeoutMs)) {
                    // Failed to acquire, release all acquired resources
                    for (Resource acquired : resources) {
                        acquired.release(this);
                    }
                    resources.clear();
                    return false;
                }
                resources.add(res);
            }
            return true;
        }
        
        void releaseAll() {
            for (Resource res : resources) {
                res.release(this);
            }
            resources.clear();
        }
        
        void abort() {
            aborted = true;
            releaseAll();
            System.out.println("Transaction " + id + " aborted");
        }
        
        void commit() {
            if (!aborted) {
                System.out.println("Transaction " + id + " committed");
                releaseAll();
            }
        }
    }
    
    static class DeadlockDetector implements Runnable {
        Map<String, Transaction> transactions = new ConcurrentHashMap<>();
        Map<String, Set<String>> waitForGraph = new ConcurrentHashMap<>();
        long detectionInterval = 5000; // 5 seconds
        
        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(detectionInterval);
                    detectDeadlocks();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        
        void addTransaction(Transaction txn) {
            transactions.put(txn.id, txn);
            waitForGraph.putIfAbsent(txn.id, ConcurrentHashMap.newKeySet());
        }
        
        void addWaitFor(String waiter, String holder) {
            waitForGraph.computeIfAbsent(waiter, k -> ConcurrentHashMap.newKeySet()).add(holder);
        }
        
        void removeTransaction(String txnId) {
            transactions.remove(txnId);
            waitForGraph.remove(txnId);
            // Remove references from other transactions
            for (Set<String> waits : waitForGraph.values()) {
                waits.remove(txnId);
            }
        }
        
        void detectDeadlocks() {
            System.out.println("Running deadlock detection...");
            
            // Build wait-for graph
            rebuildWaitForGraph();
            
            // Detect cycles using DFS
            Set<String> visited = new HashSet<>();
            Set<String> recursionStack = new HashSet<>();
            
            for (String node : waitForGraph.keySet()) {
                if (!visited.contains(node)) {
                    if (hasCycle(node, visited, recursionStack, new ArrayList<>())) {
                        // Cycle found, need to resolve
                        return;
                    }
                }
            }
            System.out.println("No deadlocks detected.");
        }
        
        boolean hasCycle(String node, Set<String> visited, 
                        Set<String> recursionStack, List<String> path) {
            visited.add(node);
            recursionStack.add(node);
            path.add(node);
            
            Set<String> neighbors = waitForGraph.getOrDefault(node, Collections.emptySet());
            for (String neighbor : neighbors) {
                if (!visited.contains(neighbor)) {
                    if (hasCycle(neighbor, visited, recursionStack, path)) {
                        return true;
                    }
                } else if (recursionStack.contains(neighbor)) {
                    // Cycle detected
                    System.out.println("Deadlock detected! Cycle: " + path);
                    resolveDeadlock(path);
                    return true;
                }
            }
            
            recursionStack.remove(node);
            path.remove(path.size() - 1);
            return false;
        }
        
        void resolveDeadlock(List<String> cycle) {
            if (cycle.isEmpty()) return;
            
            // Strategy: abort youngest transaction (latest start time)
            String toAbort = cycle.get(0);
            long youngestTime = transactions.get(toAbort).startTime;
            
            for (String txnId : cycle) {
                Transaction txn = transactions.get(txnId);
                if (txn != null && txn.startTime > youngestTime) {
                    youngestTime = txn.startTime;
                    toAbort = txnId;
                }
            }
            
            System.out.println("Aborting transaction " + toAbort + " to resolve deadlock");
            Transaction txn = transactions.get(toAbort);
            if (txn != null) {
                txn.abort();
                removeTransaction(toAbort);
            }
        }
        
        void rebuildWaitForGraph() {
            // Clear current graph
            for (Set<String> waits : waitForGraph.values()) {
                waits.clear();
            }
            
            // Rebuild based on current resource ownership
            // In a real system, this would come from lock managers
            synchronized (Resource.class) {
                // This is a simplified version
                for (Transaction txn1 : transactions.values()) {
                    for (Transaction txn2 : transactions.values()) {
                        if (!txn1.id.equals(txn2.id)) {
                            // Check if txn1 is waiting for resources held by txn2
                            boolean isWaiting = false;
                            // Simplified: random wait-for relationship for simulation
                            if (Math.random() < 0.3) {
                                addWaitFor(txn1.id, txn2.id);
                                isWaiting = true;
                            }
                            
                            if (isWaiting) {
                                System.out.println(txn1.id + " waiting for " + txn2.id);
                            }
                        }
                    }
                }
            }
        }
    }
    
    static class TransactionSimulator implements Runnable {
        DeadlockDetector detector;
        Random random = new Random();
        int transactionCount = 0;
        
        TransactionSimulator(DeadlockDetector detector) {
            this.detector = detector;
        }
        
        @Override
        public void run() {
            while (true) {
                try {
                    // Simulate transaction arrival
                    Thread.sleep(random.nextInt(3000) + 1000);
                    
                    // Create new transaction
                    Transaction txn = new Transaction("TXN-" + (++transactionCount));
                    detector.addTransaction(txn);
                    
                    System.out.println("Started transaction: " + txn.id);
                    
                    // Simulate transaction work
                    Thread transactionThread = new Thread(() -> {
                        try {
                            // Simulate resource acquisition
                            List<Resource> resources = new ArrayList<>();
                            int numResources = random.nextInt(3) + 2;
                            for (int i = 0; i < numResources; i++) {
                                resources.add(new Resource("RES-" + random.nextInt(10)));
                            }
                            
                            boolean acquired = txn.acquireResources(resources, 5000);
                            if (acquired) {
                                // Hold resources for some time
                                Thread.sleep(random.nextInt(4000) + 1000);
                                txn.commit();
                            } else {
                                System.out.println("Transaction " + txn.id + " failed to acquire resources");
                                txn.abort();
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            txn.abort();
                        } finally {
                            detector.removeTransaction(txn.id);
                        }
                    });
                    transactionThread.start();
                    
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }
    
    public static void main(String[] args) {
        DeadlockDetector detector = new DeadlockDetector();
        
        // Start deadlock detector
        Thread detectorThread = new Thread(detector);
        detectorThread.setDaemon(true);
        detectorThread.start();
        
        // Start transaction simulator
        TransactionSimulator simulator = new TransactionSimulator(detector);
        Thread simulatorThread = new Thread(simulator);
        simulatorThread.setDaemon(true);
        simulatorThread.start();
        
        // Simulate timeout-based recovery
        ScheduledExecutorService timeoutChecker = Executors.newScheduledThreadPool(1);
        timeoutChecker.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            for (Transaction txn : detector.transactions.values()) {
                if (!txn.aborted && (now - txn.startTime) > 10000) { // 10 second timeout
                    System.out.println("Transaction " + txn.id + " timed out");
                    txn.abort();
                    detector.removeTransaction(txn.id);
                }
            }
        }, 1, 1, TimeUnit.SECONDS);
        
        // Run for 60 seconds
        try {
            Thread.sleep(60000);
            System.out.println("Simulation complete.");
            timeoutChecker.shutdown();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}