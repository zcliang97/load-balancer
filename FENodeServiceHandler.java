import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransport;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;

public class FENodeServiceHandler extends AbstractBcryptServiceHandler {
    // Hyperthreading doubles the number of cores -> (2 * 8) * 2 = 32
    final int MAX_NUM_THREADS = 16;

    // workerMap represents the BENodes which are still functional
    final private ConcurrentHashMap<String, NodeInfo> workerMap = new ConcurrentHashMap<>(4);
    // Need a thread-safe Prioritiy Queue sorted on its weight for round robin. Max # of workers = 4
    // Priority Queue just helps us select which node should be used next
    final private PriorityBlockingQueue<NodeInfo> workerQueue = new PriorityBlockingQueue<>(4, Comparator.comparing(NodeInfo::getWorkLoad));
    final private ExecutorService es = Executors.newFixedThreadPool(MAX_NUM_THREADS);

    // Delegate RPC to BENode if available, else process it.
    @Override
    public List<String> hashPassword(
            List<String> passwordList,
            short logRounds
    ) throws IllegalArgument, org.apache.thrift.TException {
        System.out.println("FENode Hashing: " + passwordList);
        // Input checks
        if (passwordList.size() == 0) {
            throw new IllegalArgument("Password list cannot be empty");
        }

        if (logRounds < 4 || logRounds > 30) {
            throw new IllegalArgument("LogRound is not between 4 and 30 ");
        }

        // Refresh Priority Queue
        FENodeServiceHandler.this.buildPriorityQueue();

        // Run Weighted Round Robin if the passwordList size is below 8. Split equally if load is high
        CountDownLatch latch;
        int size = passwordList.size();

        List<Future<List<String>>> futures = new ArrayList<>(size);
        // If size is small, no need to split, just send it to the least busy node
        if (size <= MAX_NUM_THREADS) {
            latch = new CountDownLatch(1);
            futures.add(es.submit(new HashDelegateTask(passwordList, logRounds, latch)));
        } else {
            // If large numbers, split by # of BENodes and delegate to least nodes with workload
            int numThreads = workerMap.size();
            int chunkSize = size / numThreads;
            latch = new CountDownLatch(numThreads);

            for (int i = 0; i < numThreads; i++) {
                int start = chunkSize * i;
                int end = (i != numThreads - 1) ? (start + chunkSize) : size;
                futures.add(es.submit(new HashDelegateTask(passwordList.subList(start, end), logRounds, latch)));
            }
        }

        // Synchronize the delegation threads. Each thread has a sublist, just merge them
        List<String> result = new ArrayList<>(size);
        try {
            latch.await();
            for (Future<List<String>> future: futures) {
                result.addAll(future.get());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    // Delegate RPC to BENode if available, else process it.
    @Override
    public List<Boolean> checkPassword(
            List<String> passwordList,
            List<String> hashList
    ) throws IllegalArgument, org.apache.thrift.TException {
        // Input checks
        if (passwordList.isEmpty() || hashList.isEmpty()) {
            throw new IllegalArgument("Input lists cannot be empty");
        }

        if (passwordList.size() != hashList.size()) {
            throw new IllegalArgument("Both lists must be of equal size");
        }

        // Refresh Priority Queue
        FENodeServiceHandler.this.buildPriorityQueue();

        // Run Weighted Round Robin if the passwordList size is below 8. Split equally if load is high
        CountDownLatch latch;
        int size = passwordList.size();

        List<Future<List<Boolean>>> futures = new ArrayList<>(size);
        // If size is small, no need to split, just send it to the least busy node
        if (size <= MAX_NUM_THREADS) {
            latch = new CountDownLatch(1);
            futures.add(es.submit(new CheckHashDelegateTask(passwordList, hashList, latch)));
        } else {
            // If large numbers, split by # of BENodes and delegate to least nodes with workload
            int numThreads = workerMap.size();
            int chunkSize = size / numThreads;
            latch = new CountDownLatch(numThreads);

            for (int i = 0; i < numThreads; i++) {
                int start = chunkSize * i;
                int end = (i != numThreads - 1) ? (start + chunkSize) : size;
                futures.add(es.submit(new CheckHashDelegateTask(passwordList.subList(start, end), hashList.subList(start, end), latch)));
            }
        }

        // Synchronize the delegation threads. Each thread has a sublist, just merge them
        List<Boolean> result = new ArrayList<>(size);
        try {
            latch.await();
            for (Future<List<Boolean>> future: futures) {
                result.addAll(future.get());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public boolean heartBeat(String hostname, int port) {
        // Create new NodeInfo so that we can cache the connection
        try {
            NodeInfo newNode = new NodeInfo(hostname, port);
            workerMap.put(newNode.getNodeID(), newNode);
            FENodeServiceHandler.this.buildPriorityQueue();
            System.out.println("Registered Node: " + newNode.getNodeID());
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    // Given a client NodeInfo, make a RPC request for hashing
    private class HashDelegateTask implements Callable<List<String>> {
        private List<String> _passwords;
        private short _logRounds;
        private CountDownLatch _latch;

        public HashDelegateTask(List<String> passwords, short logRounds, CountDownLatch latch) {
            _passwords = passwords;
            _logRounds = logRounds;
            _latch = latch;
        }

        @Override
        public List<String> call() {
            try {
                List<String> hash = this.delegateHash();
                return hash;
            } finally {
                _latch.countDown();
            }
        }

        private List<String> delegateHash() {
            // If all nodes failed or if no BENodes
            if (workerMap.isEmpty()) {
                System.out.println("Hash: BENodes not found. Processing............");
                try {
                    return FENodeServiceHandler.super.hashPassword(this._passwords, this._logRounds);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (workerQueue.isEmpty()) {
                FENodeServiceHandler.this.buildPriorityQueue();
            }

            // Update weight in workerMap and refresh queue to inform other threads
            NodeInfo beNodeInfo = workerQueue.poll(); // Returns null if queue is empty
            beNodeInfo.addWorkload(this._passwords.size());
            FENodeServiceHandler.this.buildPriorityQueue();
            System.out.println("Hash: Delegating to " + beNodeInfo.getNodeID() + " Workload: " + beNodeInfo.getWorkLoad());

            boolean error = false;
            List<String> result = new ArrayList<>();
            TTransport transport = null;
            // Establish connection with BENode and invoke RPC
            try {
                // Must create new thrift client for each new thread
                transport = new TFramedTransport(beNodeInfo.getSocket());
                TProtocol protocol = new TBinaryProtocol(transport);
                BcryptService.Client beNodeClient = new BcryptService.Client(protocol);
                if (!transport.isOpen()) {
                    transport.open();
                }
                result = beNodeClient.hashPassword(this._passwords, this._logRounds);
            } catch (Exception e) {
                error = true;
                e.printStackTrace();
                // BENode failed + not valid
                workerMap.remove(beNodeInfo.getNodeID());
                FENodeServiceHandler.this.buildPriorityQueue();
            } finally {
                beNodeInfo.removeWorkload(this._passwords.size());
                FENodeServiceHandler.this.buildPriorityQueue();
                if (transport != null && transport.isOpen()) {
                    transport.close();
                }
            }

            // Retry with the next best node
            if (error) {
                return this.delegateHash();
            }
            return result;
        }
    }

    // Given a client NodeInfo, make a RPC request for hashing
    private class CheckHashDelegateTask implements Callable<List<Boolean>> {
        private List<String> _passwordList;
        private List<String> _hashList;
        private CountDownLatch _latch;

        public CheckHashDelegateTask(List<String> passwordList, List<String> hashList, CountDownLatch latch) {
            _passwordList = passwordList;
            _hashList = hashList;
            _latch = latch;
        }

        @Override
        public List<Boolean> call() {
            try {
                List<Boolean> checkList = this.delegateCheckHash();
                return checkList;
            } finally {
                _latch.countDown();
            }
        }

        private List<Boolean> delegateCheckHash() {
            // If all nodes failed or if no BENodes
            if (workerMap.isEmpty()) {
                System.out.println("CheckHash: BENodes not found. Processing............");
                try {
                    return FENodeServiceHandler.super.checkPassword(this._passwordList, this._hashList);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (workerQueue.isEmpty()) {
                FENodeServiceHandler.this.buildPriorityQueue();
            }

            // Update weight in workerMap and refresh queue to inform other threads
            NodeInfo beNodeInfo = workerQueue.poll();

            // PW List has same size as HashList
            beNodeInfo.addWorkload(this._passwordList.size() * 2);
            FENodeServiceHandler.this.buildPriorityQueue();
            System.out.println("CheckHash: Delegating to " + beNodeInfo.getNodeID() + " Workload: " + beNodeInfo.getWorkLoad());

            boolean error = false;
            List<Boolean> result = new ArrayList<>();
            TTransport transport = null;
            try {
                // RPC to BENode: must create new thrift client for each new thread
                transport = new TFramedTransport(beNodeInfo.getSocket());
                TProtocol protocol = new TBinaryProtocol(transport);
                BcryptService.Client beNodeClient = new BcryptService.Client(protocol);
                if (!transport.isOpen()) {
                    transport.open();
                }
                result = beNodeClient.checkPassword(this._passwordList, this._hashList);
            } catch (Exception e) {
                error = true;
                e.printStackTrace();
                // BENode failed + not valid
                workerMap.remove(beNodeInfo.getNodeID());
                FENodeServiceHandler.this.buildPriorityQueue();
            } finally {
                // Cleanup
                beNodeInfo.removeWorkload(this._passwordList.size() * 2);
                FENodeServiceHandler.this.buildPriorityQueue();
                if (transport != null && transport.isOpen()) {
                    transport.close();
                }
            }

            // Retry with the next best node
            if (error) {
                return this.delegateCheckHash();
            }
            return result;
        }
    }

    // This basically reorders the nodes with updated weights
    private synchronized void buildPriorityQueue() {
        workerQueue.clear();
        workerQueue.addAll(workerMap.values());
    }
}
