import org.apache.thrift.transport.TSocket;

import java.util.concurrent.atomic.AtomicInteger;

public class NodeInfo {
    private String _id;
    private String _hostname;
    private int _port;
    private int _workload;

    public NodeInfo(String hostname, int port) {
        this._id = hostname + Integer.toString(port);
        this._hostname = hostname;
        this._port = port;
        this._workload = 0;
    }

    public String getNodeID() {
        return this._id;
    }

    public TSocket getSocket() {
        return new TSocket(this._hostname, this._port);
    }

    public synchronized int getWorkLoad() {
        return this._workload;
    }

    public synchronized void addWorkload(int workload) {
        this._workload = this.getWorkLoad() + workload;
    }

    public synchronized void removeWorkload(int workload) {
        this._workload =  Math.max(0, this.getWorkLoad() - workload);
    }
}
