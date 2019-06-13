import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ClientG {

    public static void main(String[] args) throws TException {
        if (args.length != 2) {
            System.err.println("Usage: java ClientG FE_host FE_port");
            System.exit(-1);
        }

        final String hostFE = args[0];
        final int portFE = Integer.parseInt(args[1]);

        final TSocket clientSocket = new TSocket(hostFE, portFE);
        final TTransport clientTransport = new TFramedTransport(clientSocket);
        final TProtocol clientProtocol = new TBinaryProtocol(clientTransport);
        final BcryptService.Client client = new BcryptService.Client(clientProtocol);
        clientTransport.open();

        try {
            client.hashPassword(Collections.emptyList(), (short) 10);
            System.out.println("Test 1: Failed");
        } catch (IllegalArgument e) {
            System.out.println("Test 1: Passed");
        }

        try {
            client.hashPassword(Collections.singletonList("Hello"), (short) 31);
            System.out.println("Test 2: Failed");
        } catch (IllegalArgument e) {
            System.out.println("Test 2: Passed");
        }

        try {
            client.hashPassword(Collections.singletonList(""), (short) 10);
            System.out.println("Test 3: Passed");
        } catch (Exception e) {
            System.out.println("Test 3: Failed");
        }

        try {
            client.checkPassword(Collections.emptyList(), Collections.emptyList());
            System.out.println("Test 4: Failed");
        } catch (IllegalArgument e) {
            System.out.println("Test 4: Passed");
        }

        try {
            client.checkPassword(Collections.emptyList(), Collections.singletonList("Hello"));
            System.out.println("Test 5: Failed");
        } catch (IllegalArgument e) {
            System.out.println("Test 5: Passed");
        }

        try {
            client.checkPassword(Collections.singletonList("Hello"), Collections.emptyList());
            System.out.println("Test 6: Failed");
        } catch (IllegalArgument e) {
            System.out.println("Test 6: Passed");
        }

        try {
            final List<String> currentPasswords = new ArrayList<>();
            currentPasswords.add("Hello");
            currentPasswords.add("World");
            final List<String> currentHash = client.hashPassword(currentPasswords, (short) 10);
            currentHash.remove(0);
            client.checkPassword(currentPasswords, currentHash);
            System.out.println("Test 6: Failed");
        } catch (IllegalArgument e) {
            System.out.println("Test 6: Passed");
        }

        try {
            final List<String> currentPasswords = new ArrayList<>();
            currentPasswords.add("Hello");
            currentPasswords.add("World");
            final List<String> currentHash = client.hashPassword(currentPasswords, (short) 10);
            currentHash.set(0, "Hello");
            client.checkPassword(currentPasswords, currentHash);
            System.out.println("Test 7: Passed");
        } catch (IllegalArgument e) {
            System.out.println("Test 7: Failed");
        }

        clientTransport.close();
    }

}