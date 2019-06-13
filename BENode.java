import java.net.InetAddress;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.*;


public class BENode {
    static Logger log;

    public static void main(String [] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: java BENode FE_host FE_port BE_port");
            System.exit(-1);
        }

        // initialize log4j
        BasicConfigurator.configure();
        log = Logger.getLogger(BENode.class.getName());

        String hostFE = args[0];
        int portFE = Integer.parseInt(args[1]);
        int portBE = Integer.parseInt(args[2]);
        log.info("Launching BE node on port " + portBE + " at host " + getHostName());

        try {
            // Launch thrift server.
            // Handles Hash and check Pass requests
            BcryptService.Processor processor =
                    new BcryptService.Processor<BcryptService.Iface>(new BENodeServiceHandler());
            TNonblockingServerSocket socket = new TNonblockingServerSocket(portBE);
            THsHaServer.Args sargs = new THsHaServer.Args(socket);
            sargs.protocolFactory(new TBinaryProtocol.Factory());
            sargs.transportFactory(new TFramedTransport.Factory());
            sargs.processorFactory(new TProcessorFactory(processor));
            sargs.maxWorkerThreads(64);
            THsHaServer server = new THsHaServer(sargs);

            Thread feNodeCheck = new Thread(() -> {
                while(!server.isServing()) {
                    System.out.println("Starting BENode Server");
                }

                // Register BE Node to FENode we want this to block the current BENode because useless without FENode
                TSocket sock = new TSocket(hostFE, portFE);
                TTransport transport = new TFramedTransport(sock);
                TProtocol protocol = new TBinaryProtocol(transport);
                BcryptService.Client FENode = new BcryptService.Client(protocol);
                boolean connected = false;

                while(!connected) {
                    System.out.println("Attempting to find FENode");
                    try {
                        transport.open();
                        FENode.heartBeat(BENode.getHostName(), portBE);
                        transport.close();
                        connected = true;
                        System.out.println("FENode Connected");
                    } catch (Exception e) {
                        System.out.println("Cannot find FENode, Retrying");
                    } finally {
                        if(transport.isOpen()) {
                            transport.close();
                        }
                    }
                }
            });

            feNodeCheck.start();
            server.serve();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    static String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "localhost";
        }
    }
}
