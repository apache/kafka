package kafka.common;

/**
 * Information about a Kafka node
 */
public class Node {

    private final int id;
    private final String host;
    private final int port;

    public Node(int id, String host, int port) {
        super();
        this.id = id;
        this.host = host;
        this.port = port;
    }

    /**
     * The node id of this node
     */
    public int id() {
        return id;
    }

    /**
     * The host name for this node
     */
    public String host() {
        return host;
    }

    /**
     * The port for this node
     */
    public int port() {
        return port;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((host == null) ? 0 : host.hashCode());
        result = prime * result + id;
        result = prime * result + port;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Node other = (Node) obj;
        if (host == null) {
            if (other.host != null)
                return false;
        } else if (!host.equals(other.host))
            return false;
        if (id != other.id)
            return false;
        if (port != other.port)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "Node(" + id + ", " + host + ", " + port + ")";
    }

}
