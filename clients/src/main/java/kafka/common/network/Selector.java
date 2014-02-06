package kafka.common.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kafka.common.KafkaException;

/**
 * A selector interface for doing non-blocking multi-connection network I/O.
 * <p>
 * This class works with {@link NetworkSend} and {@link NetworkReceive} to transmit size-delimited network requests and
 * responses.
 * <p>
 * A connection can be added to the selector associated with an integer id by doing
 * 
 * <pre>
 * selector.connect(42, new InetSocketAddress(&quot;google.com&quot;, server.port), 64000, 64000);
 * </pre>
 * 
 * The connect call does not block on the creation of the TCP connection, so the connect method only begins initiating
 * the connection. The successful invocation of this method does not mean a valid connection has been established.
 * 
 * Sending requests, receiving responses, processing connection completions, and disconnections on the existing
 * connections are all done using the <code>poll()</code> call.
 * 
 * <pre>
 * List&lt;NetworkRequest&gt; requestsToSend = Arrays.asList(new NetworkRequest(0, myBytes), new NetworkRequest(1, myOtherBytes));
 * selector.poll(TIMEOUT_MS, requestsToSend);
 * </pre>
 * 
 * The selector maintains several lists that are reset by each call to <code>poll()</code> which are available via
 * various getters. These are reset by each call to <code>poll()</code>.
 * 
 * This class is not thread safe!
 */
public class Selector implements Selectable {

    private final java.nio.channels.Selector selector;
    private final Map<Integer, SelectionKey> keys;
    private final List<NetworkSend> completedSends;
    private final List<NetworkReceive> completedReceives;
    private final List<Integer> disconnected;
    private final List<Integer> connected;

    /**
     * Create a new selector
     */
    public Selector() {
        try {
            this.selector = java.nio.channels.Selector.open();
        } catch (IOException e) {
            throw new KafkaException(e);
        }
        this.keys = new HashMap<Integer, SelectionKey>();
        this.completedSends = new ArrayList<NetworkSend>();
        this.completedReceives = new ArrayList<NetworkReceive>();
        this.connected = new ArrayList<Integer>();
        this.disconnected = new ArrayList<Integer>();
    }

    /**
     * Begin connecting to the given address and add the connection to this selector associated with the given id
     * number.
     * <p>
     * Note that this call only initiates the connection, which will be completed on a future {@link #poll(long, List)}
     * call. Check {@link #connected()} to see which (if any) connections have completed after a given poll call.
     * @param id The id for the new connection
     * @param address The address to connect to
     * @param sendBufferSize The send buffer for the new connection
     * @param receiveBufferSize The receive buffer for the new connection
     * @throws IllegalStateException if there is already a connection for that id
     * @throws UnresolvedAddressException if DNS resolution fails on the hostname
     */
    @Override
    public void connect(int id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException {
        SocketChannel channel = SocketChannel.open();
        channel.configureBlocking(false);
        Socket socket = channel.socket();
        socket.setKeepAlive(true);
        socket.setSendBufferSize(sendBufferSize);
        socket.setReceiveBufferSize(receiveBufferSize);
        socket.setTcpNoDelay(true);
        try {
            channel.connect(address);
        } catch (UnresolvedAddressException e) {
            channel.close();
            throw e;
        }
        SelectionKey key = channel.register(this.selector, SelectionKey.OP_CONNECT);
        key.attach(new Transmissions(id));
        if (this.keys.containsKey(key))
            throw new IllegalStateException("There is already a connection for id " + id);
        this.keys.put(id, key);
    }

    /**
     * Disconnect any connections for the given id (if there are any). The disconnection is asynchronous and will not be
     * processed until the next {@link #poll(long, List) poll()} call.
     */
    @Override
    public void disconnect(int id) {
        SelectionKey key = this.keys.get(id);
        if (key != null)
            key.cancel();
    }

    /**
     * Interrupt the selector if it is blocked waiting to do I/O.
     */
    @Override
    public void wakeup() {
        this.selector.wakeup();
    }

    /**
     * Close this selector and all associated connections
     */
    @Override
    public void close() {
        for (SelectionKey key : this.selector.keys()) {
            try {
                close(key);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            this.selector.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Do whatever I/O can be done on each connection without blocking. This includes completing connections, completing
     * disconnections, initiating new sends, or making progress on in-progress sends or receives.
     * <p>
     * The provided network sends will be started.
     * 
     * When this call is completed the user can check for completed sends, receives, connections or disconnects using
     * {@link #completedSends()}, {@link #completedReceives()}, {@link #connected()}, {@link #disconnected()}. These
     * lists will be cleared at the beginning of each {@link #poll(long, List)} call and repopulated by the call if any
     * completed I/O.
     * 
     * @param timeout The amount of time to wait, in milliseconds. If negative, wait indefinitely.
     * @param sends The list of new sends to begin
     * 
     * @throws IllegalStateException If a send is given for which we have no existing connection or for which there is
     *         already an in-progress send
     */
    @Override
    public void poll(long timeout, List<NetworkSend> sends) throws IOException {
        clear();

        /* register for write interest on any new sends */
        for (NetworkSend send : sends) {
            SelectionKey key = keyForId(send.destination());
            Transmissions transmissions = transmissions(key);
            if (transmissions.hasSend())
                throw new IllegalStateException("Attempt to begin a send operation with prior send operation still in progress.");
            transmissions.send = send;
            try {
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            } catch (CancelledKeyException e) {
                close(key);
            }
        }

        /* check ready keys */
        int readyKeys = select(timeout);
        if (readyKeys > 0) {
            Set<SelectionKey> keys = this.selector.selectedKeys();
            Iterator<SelectionKey> iter = keys.iterator();
            while (iter.hasNext()) {
                SelectionKey key = iter.next();
                iter.remove();

                Transmissions transmissions = transmissions(key);
                SocketChannel channel = channel(key);
                try {
                    /*
                     * complete any connections that have finished their handshake
                     */
                    if (key.isConnectable()) {
                        channel.finishConnect();
                        key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
                        this.connected.add(transmissions.id);
                    }

                    /* read from any connections that have readable data */
                    if (key.isReadable()) {
                        if (!transmissions.hasReceive())
                            transmissions.receive = new NetworkReceive(transmissions.id);
                        transmissions.receive.readFrom(channel);
                        if (transmissions.receive.complete()) {
                            transmissions.receive.payload().rewind();
                            this.completedReceives.add(transmissions.receive);
                            transmissions.clearReceive();
                        }
                    }

                    /*
                     * write to any sockets that have space in their buffer and for which we have data
                     */
                    if (key.isWritable()) {
                        transmissions.send.writeTo(channel);
                        if (transmissions.send.remaining() <= 0) {
                            this.completedSends.add(transmissions.send);
                            transmissions.clearSend();
                            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                        }
                    }

                    /* cancel any defunct sockets */
                    if (!key.isValid())
                        close(key);
                } catch (IOException e) {
                    e.printStackTrace();
                    close(key);
                }
            }
        }
    }

    @Override
    public List<NetworkSend> completedSends() {
        return this.completedSends;
    }

    @Override
    public List<NetworkReceive> completedReceives() {
        return this.completedReceives;
    }

    @Override
    public List<Integer> disconnected() {
        return this.disconnected;
    }

    @Override
    public List<Integer> connected() {
        return this.connected;
    }

    /**
     * Clear the results from the prior poll
     */
    private void clear() {
        this.completedSends.clear();
        this.completedReceives.clear();
        this.connected.clear();
        this.disconnected.clear();
    }

    /**
     * Check for data, waiting up to the given timeout.
     * 
     * @param ms Length of time to wait, in milliseconds. If negative, wait indefinitely.
     * @return The number of keys ready
     * @throws IOException
     */
    private int select(long ms) throws IOException {
        if (ms == 0L)
            return this.selector.selectNow();
        else if (ms < 0L)
            return this.selector.select();
        else
            return this.selector.select(ms);
    }

    /**
     * Begin closing this connection
     */
    private void close(SelectionKey key) throws IOException {
        SocketChannel channel = channel(key);
        Transmissions trans = transmissions(key);
        if (trans != null)
            this.disconnected.add(trans.id);
        key.attach(null);
        key.cancel();
        channel.socket().close();
        channel.close();
    }

    /**
     * Get the selection key associated with this numeric id
     */
    private SelectionKey keyForId(int id) {
        SelectionKey key = this.keys.get(id);
        if (key == null)
            throw new IllegalStateException("Attempt to write to socket for which there is no open connection.");
        return key;
    }

    /**
     * Get the transmissions for the given connection
     */
    private Transmissions transmissions(SelectionKey key) {
        return (Transmissions) key.attachment();
    }

    /**
     * Get the socket channel associated with this selection key
     */
    private SocketChannel channel(SelectionKey key) {
        return (SocketChannel) key.channel();
    }

    /**
     * The id and in-progress send and receive associated with a connection
     */
    private static class Transmissions {
        public int id;
        public NetworkSend send;
        public NetworkReceive receive;

        public Transmissions(int id) {
            this.id = id;
        }

        public boolean hasSend() {
            return this.send != null;
        }

        public void clearSend() {
            this.send = null;
        }

        public boolean hasReceive() {
            return this.receive != null;
        }

        public void clearReceive() {
            this.receive = null;
        }
    }

}
