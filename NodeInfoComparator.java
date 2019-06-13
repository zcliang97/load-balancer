import java.util.Comparator;

public class NodeInfoComparator implements Comparator<NodeInfo> {
    public int compare(NodeInfo n1, NodeInfo n2) {
        return n1.getWorkLoad() - n2.getWorkLoad();
    }
}
