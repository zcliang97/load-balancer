import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.ArrayList;
import java.util.Deque;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.concurrent.*;

public class ClientF {

    private static final int WORKER_THREADS = 8;
    private static final int WORKER_SIZE = 128;

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: java ClientF FE_host FE_port");
            System.exit(-1);
        }

        final String hostFE = args[0];
        final int portFE = Integer.parseInt(args[1]);

        final ExecutorService executorService = Executors.newFixedThreadPool(WORKER_THREADS);
        final CyclicBarrier startCyclicBarrier = new CyclicBarrier(WORKER_THREADS);
        final CountDownLatch endCountDownLatch = new CountDownLatch(WORKER_THREADS);
        final Deque<Double> throughputs = new ConcurrentLinkedDeque<>();

        final List<String> password = new ArrayList<>(WORKER_SIZE);
        for (int index = 0; index < WORKER_SIZE; index++) {
            password.add("nfr5DjahzLlOZ4B1VPX2iHsthn1U1lFYQqJvBDB3ksXw2kBK4qnrxOzSItmlGhKH78nED0Djvzvm3LL0ds9sl5yTsOKCh1DJhz0qZF378gjQID3RfiPWw4l4FowrGMGiNNUyBXh3lq6dK6HwvKyCESjkhII0mrTf31KSti1JVSM2rrCcwK2zObU7YyAC1HWLSjRagcEZkmVwTPrYQd9JB8BaqQJYtQduF1nz2Afw7aZucF9aa3puwmZ7yKK3Ep0eADIgoiFHJ84p2c03q7qQscQid3gH0EH10fzb9ppi2KCEVTuFjgifRQVEFNEXtjBKmrZ3UvBvCPfiZnIj88ULUQqlZjaR2JUK9r9fHi8tIfLv5uZdDQkYB74RxLkwYHSJ8c6LcUcQqtZdW0sfqAGp7IUH9WvmwZpHzOCyLNn9u3YVptpoDbVi6cj3jG5VguHZ6yU0wb6nOTUE07mKFHh8gdH7phaciIFTpaI4gdN6qmTErXmMAzT7J9798cjP643EJDETK6H7TpM3v2jFO754eP03KmYTrBXa66LOFunVvdLogKZCqqFUiPIAhPXgaC9Pjfwmhx81y2JyBVtBYQFB9Q4hCPuCuYr1XCevrdRCcjhv7ujqgU0xT16ABOiwYyNSQqjh2PekzcyjG5lYGiMkpJJRZzHlQs318VmZL477UBvosKuhOvljBSMBXsoxcRdxD5Zu22QCQmYMQKb4uvHiYxnjqBE4BOzCI754PuAiXemBmYKmocM9epnFP8oJWa08VtjA4vgdDs8AtdKZmPU3gnY7IqPwDsTxe1jH9nJ7zUACydHGA6ISeYpOMZAh6nkqia9dXMLuS3Dl7oMPyEMi1tjsmLIqQzSD2LTV9mTT38YEbEYEecKN0PNS4RaSEq3Fd8326m46x3QYOyzedWJfhmVbohpElkIc04woM63mBykaIP2fxl4ziBIhS0TWTv7Ro5bT6UbfYtsGeb0XoUdf404Yt5GQiK201kOd9fINwTsW2ndWFekW5PXyfaLfbuMa");
        }

        final long startTime = System.currentTimeMillis();
        for (int counter = 1; counter <= WORKER_THREADS; counter++) {
            executorService.submit(() -> {
                try {
                    final TSocket clientSocket = new TSocket(hostFE, portFE);
                    final TTransport clientTransport = new TFramedTransport(clientSocket);
                    final TProtocol clientProtocol = new TBinaryProtocol(clientTransport);
                    final BcryptService.Client client = new BcryptService.Client(clientProtocol);
                    clientTransport.open();
                    startCyclicBarrier.await();
                    final long localStartTime = System.currentTimeMillis();
                    final List<String> hash = client.hashPassword(password, (short) 10);
                    final List<Boolean> results = client.checkPassword(password, hash);
                    for (Boolean isCorrect : results) {
                        if (!isCorrect) {
                            throw new IllegalStateException("Failed!");
                        }
                    }
                    final long localEndTime = System.currentTimeMillis();
                    final long localDuration = localEndTime - localStartTime;
                    throughputs.addLast((double) WORKER_SIZE / localDuration * 1000);
                    endCountDownLatch.countDown();
                    clientTransport.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        try {
            endCountDownLatch.await();
            final long endTime = System.currentTimeMillis();
            final long duration = endTime - startTime;
            System.out.println(String.format("Duration: %d ms", duration));
            final DoubleSummaryStatistics throughputStatistics = throughputs.stream()
                    .mapToDouble(Double::doubleValue)
                    .summaryStatistics();
            System.out.println(String.format("Average Throughput: %f h/s", throughputStatistics.getAverage()));
            System.out.println(String.format("Minimum Throughput: %f h/s", throughputStatistics.getMin()));
            System.out.println(String.format("Maximum Throughput: %f h/s", throughputStatistics.getMax()));
        } catch (InterruptedException e) {
            // Do Nothing.
        }

        executorService.shutdown();
    }

}