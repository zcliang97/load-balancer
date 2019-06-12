import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;

public class ClientB {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: java ClientB FE_host FE_port");
            System.exit(-1);
        }

        final String hostFE = args[0];
        final int portFE = Integer.parseInt(args[1]);

        final ExecutorService executorService = Executors.newFixedThreadPool(16);
        final CyclicBarrier startCyclicBarrier = new CyclicBarrier(16);
        final CountDownLatch endCountDownLatch = new CountDownLatch(16);

        final long startTime = System.currentTimeMillis();
        for (int counter = 1; counter <= 16; counter++) {
            executorService.submit(() -> {
                try {
                    final TSocket clientSocket = new TSocket(hostFE, portFE);
                    final TTransport clientTransport = new TFramedTransport(clientSocket);
                    final TProtocol clientProtocol = new TBinaryProtocol(clientTransport);
                    final BcryptService.Client client = new BcryptService.Client(clientProtocol);
                    clientTransport.open();
                    startCyclicBarrier.await();
                    client.hashPassword(Collections.singletonList(UUID.randomUUID().toString()), (short) 10);
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
            System.out.println(String.format("Duration: %d", endTime - startTime));
        } catch (InterruptedException e) {
            // Do Nothing.
        }

        executorService.shutdown();
    }

}