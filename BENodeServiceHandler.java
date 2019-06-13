import org.mindrot.jbcrypt.BCrypt;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BENodeServiceHandler extends AbstractBcryptServiceHandler {
    static int NUM_THREADS_PER_CORE = 8;
    // Eceubuntu2 has 8 threads per core and there are 2 cores
    static ExecutorService es = Executors.newFixedThreadPool(NUM_THREADS_PER_CORE);

    @Override
    public List<String> hashPassword(
            List<String> passwordList,
            short logRounds
    ) throws IllegalArgument, org.apache.thrift.TException {
        String mode = passwordList.size() <= NUM_THREADS_PER_CORE ? "Single-Thread" : "Multi-Thread";
        System.out.println("BENode hashing: " + passwordList + " with " + mode);
        if (passwordList.size() <= NUM_THREADS_PER_CORE) {
            return super.hashPassword(passwordList, logRounds);
        } else {
            int listSize = passwordList.size();
            int numThreads = Math.min(listSize, NUM_THREADS_PER_CORE);
            int chunkSize = listSize / numThreads;
            CountDownLatch latch = new CountDownLatch(numThreads);

            final List<String> result = new ArrayList<>(listSize);
            for (int i = 0; i < listSize; i++) {
                result.add(null);
            }

            for(int i = 0; i < numThreads; i++) {
                int start = chunkSize * i;
                int end = (i != numThreads - 1) ? (start + chunkSize) : listSize;
                this.es.execute(() -> {
                    System.out.println("Hash Password " + " | Thread ID: " + Thread.currentThread().getId() + " | Start: " + start + " | End: " + end);
                    for (int j = start; j < end; j++) {
                        String hash = BCrypt.hashpw(passwordList.get(j), BCrypt.gensalt(logRounds));
                        result.set(j, hash);
                    }
                    latch.countDown();
                });
            }

            // Wait for result array to populate by threads
            try {
                latch.await();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return result;
        }
    }

    @Override
    public List<Boolean> checkPassword(
            List<String> passwordList,
            List<String> hashList
    ) throws IllegalArgument, org.apache.thrift.TException {
        System.out.println("BENode checkPassword" + passwordList);
        if (passwordList.size() <= NUM_THREADS_PER_CORE) {
            return super.checkPassword(passwordList, hashList);
        } else {
            int listSize = passwordList.size();
            int numThreads = Math.min(passwordList.size(), NUM_THREADS_PER_CORE);
            int chunkSize = passwordList.size() / numThreads;
            CountDownLatch latch = new CountDownLatch(numThreads);

            List<Boolean> result = new ArrayList<>(listSize);
            for (int i = 0; i < listSize; i++) {
                result.add(false);
            }

            for(int i = 0; i < numThreads; i++) {
                int start = chunkSize * i;
                int end = (i != numThreads - 1) ? (start + chunkSize) : listSize;
                es.execute(() -> {
                    System.out.println("Check Password " + " | Thread ID: " + Thread.currentThread().getId() + " | Start: " + start + " | End: " + end);
                    for (int j = start; j < end; j++) {
                        result.set(j, BCrypt.checkpw(passwordList.get(j), hashList.get(j)));
                    }
                    latch.countDown();
                });
            }

            // Wait for result array to populate by threads
            try {
                latch.await();
            } catch (Exception e) {
                e.printStackTrace();
            }

            return result;
        }
    }

    public boolean heartBeat(String hostname, int port) {
        return true;
    }
}
