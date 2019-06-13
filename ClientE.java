import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ClientE {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: java ClientE FE_host FE_port");
            System.exit(-1);
        }

        final String hostFE = args[0];
        final int portFE = Integer.parseInt(args[1]);

        final ExecutorService executorService = Executors.newFixedThreadPool(4);
        final CyclicBarrier startCyclicBarrier = new CyclicBarrier(4);
        final CountDownLatch endCountDownLatch = new CountDownLatch(4);

        final List<String> password = new ArrayList<>(4);
        for (int index = 0; index < 25; index++) {
            password.add(UUID.randomUUID().toString());
        }

        final long startTime = System.currentTimeMillis();
        for (int counter = 1; counter <= 4; counter++) {
            executorService.submit(() -> {
                try {
                    final TSocket clientSocket = new TSocket(hostFE, portFE);
                    final TTransport clientTransport = new TFramedTransport(clientSocket);
                    final TProtocol clientProtocol = new TBinaryProtocol(clientTransport);
                    final BcryptService.Client client = new BcryptService.Client(clientProtocol);
                    clientTransport.open();
                    startCyclicBarrier.await();
                    System.out.println("Sending Passwords: " + password);
                    final List<String> hash = client.hashPassword(password, (short) 10);
                    final List<Boolean> results = client.checkPassword(password, hash);
                    System.out.println(results);
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