import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ClientA {

    public static void main(String[] args) throws TException {
        if (args.length != 2) {
            System.err.println("Usage: java ClientA FE_host FE_port");
            System.exit(-1);
        }

        final String hostFE = args[0];
        final int portFE = Integer.parseInt(args[1]);

        final TSocket clientSocket = new TSocket(hostFE, portFE);
        final TTransport clientTransport = new TFramedTransport(clientSocket);
        final TProtocol clientProtocol = new TBinaryProtocol(clientTransport);
        final BcryptService.Client client = new BcryptService.Client(clientProtocol);
        clientTransport.open();

        final List<String> password = new ArrayList<>(16);
        for (int index = 0; index < 16; index++) {
            password.add(UUID.randomUUID().toString());
        }

        final long startTime = System.currentTimeMillis();
        client.hashPassword(password, (short) 10);
        final long endTime = System.currentTimeMillis();
        System.out.println(String.format("Duration: %d", endTime - startTime));

        clientTransport.close();
    }

}