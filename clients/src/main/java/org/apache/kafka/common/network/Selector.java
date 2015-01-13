/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.network;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger log = LoggerFactory.getLogger(Selector.class);

    private final java.nio.channels.Selector selector;
    private final Map<Integer, SelectionKey> keys;
    private final List<NetworkSend> completedSends;
    private final List<NetworkReceive> completedReceives;
    private final List<Integer> disconnected;
    private final List<Integer> connected;
    private final Time time;
    private final SelectorMetrics sensors;
    private final String metricGrpPrefix;
    private final Map<String, String> metricTags;

    /**
     * Create a new selector
     */
    public Selector(Metrics metrics, Time time , String metricGrpPrefix , Map<String, String> metricTags) {
        try {
            this.selector = java.nio.channels.Selector.open();
        } catch (IOException e) {
            throw new KafkaException(e);
        }
        this.time = time;
        this.metricGrpPrefix = metricGrpPrefix;
        this.metricTags = metricTags;
        this.keys = new HashMap<Integer, SelectionKey>();
        this.completedSends = new ArrayList<NetworkSend>();
        this.completedReceives = new ArrayList<NetworkReceive>();
        this.connected = new ArrayList<Integer>();
        this.disconnected = new ArrayList<Integer>();
        this.sensors = new SelectorMetrics(metrics);
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
     * @throws IOException if DNS resolution fails on the hostname or if the broker is down
     */
    @Override
    public void connect(int id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException {
        if (this.keys.containsKey(id))
            throw new IllegalStateException("There is already a connection for id " + id);

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
            throw new IOException("Can't resolve address: " + address, e);
        } catch (IOException e) {
            channel.close();
            throw e;
        }
        SelectionKey key = channel.register(this.selector, SelectionKey.OP_CONNECT);
        key.attach(new Transmissions(id));
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
        for (SelectionKey key : this.selector.keys())
            close(key);
        try {
            this.selector.close();
        } catch (IOException e) {
            log.error("Exception closing selector:", e);
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
        long startSelect = time.nanoseconds();
        int readyKeys = select(timeout);
        long endSelect = time.nanoseconds();
        this.sensors.selectTime.record(endSelect - startSelect, time.milliseconds());

        if (readyKeys > 0) {
            Set<SelectionKey> keys = this.selector.selectedKeys();
            Iterator<SelectionKey> iter = keys.iterator();
            while (iter.hasNext()) {
                SelectionKey key = iter.next();
                iter.remove();

                Transmissions transmissions = transmissions(key);
                SocketChannel channel = channel(key);

                // register all per-broker metrics at once
                sensors.maybeRegisterNodeMetrics(transmissions.id);

                try {
                    /* complete any connections that have finished their handshake */
                    if (key.isConnectable()) {
                        channel.finishConnect();
                        key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
                        this.connected.add(transmissions.id);
                        this.sensors.connectionCreated.record();
                    }

                    /* read from any connections that have readable data */
                    if (key.isReadable()) {
                        if (!transmissions.hasReceive())
                            transmissions.receive = new NetworkReceive(transmissions.id);
                        transmissions.receive.readFrom(channel);
                        if (transmissions.receive.complete()) {
                            transmissions.receive.payload().rewind();
                            this.completedReceives.add(transmissions.receive);
                            this.sensors.recordBytesReceived(transmissions.id, transmissions.receive.payload().limit());
                            transmissions.clearReceive();
                        }
                    }

                    /* write to any sockets that have space in their buffer and for which we have data */
                    if (key.isWritable()) {
                        transmissions.send.writeTo(channel);
                        if (transmissions.send.remaining() <= 0) {
                            this.completedSends.add(transmissions.send);
                            this.sensors.recordBytesSent(transmissions.id, transmissions.send.size());
                            transmissions.clearSend();
                            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                        }
                    }

                    /* cancel any defunct sockets */
                    if (!key.isValid())
                        close(key);
                } catch (IOException e) {
                    InetAddress remoteAddress = null;
                    Socket socket = channel.socket();
                    if (socket != null)
                        remoteAddress = socket.getInetAddress();
                    log.warn("Error in I/O with {}", remoteAddress , e);
                    close(key);
                }
            }
        }
        long endIo = time.nanoseconds();
        this.sensors.ioTime.record(endIo - endSelect, time.milliseconds());
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
    private void close(SelectionKey key) {
        SocketChannel channel = channel(key);
        Transmissions trans = transmissions(key);
        if (trans != null) {
            this.disconnected.add(trans.id);
            this.keys.remove(trans.id);
            trans.clearReceive();
            trans.clearSend();
        }
        key.attach(null);
        key.cancel();
        try {
            channel.socket().close();
            channel.close();
        } catch (IOException e) {
            log.error("Exception closing connection to node {}:", trans.id, e);
        }
        this.sensors.connectionClosed.record();
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

    private class SelectorMetrics {
        private final Metrics metrics;
        public final Sensor connectionClosed;
        public final Sensor connectionCreated;
        public final Sensor bytesTransferred;
        public final Sensor bytesSent;
        public final Sensor bytesReceived;
        public final Sensor selectTime;
        public final Sensor ioTime;

        public SelectorMetrics(Metrics metrics) {
            this.metrics = metrics;
            String metricGrpName = metricGrpPrefix + "-metrics";

            this.connectionClosed = this.metrics.sensor("connections-closed");
            MetricName metricName = new MetricName("connection-close-rate", metricGrpName, "Connections closed per second in the window.", metricTags);
            this.connectionClosed.add(metricName, new Rate());

            this.connectionCreated = this.metrics.sensor("connections-created");
            metricName = new MetricName("connection-creation-rate", metricGrpName, "New connections established per second in the window.", metricTags);
            this.connectionCreated.add(metricName, new Rate());

            this.bytesTransferred = this.metrics.sensor("bytes-sent-received");
            metricName = new MetricName("network-io-rate", metricGrpName, "The average number of network operations (reads or writes) on all connections per second.", metricTags);
            bytesTransferred.add(metricName, new Rate(new Count()));

            this.bytesSent = this.metrics.sensor("bytes-sent", bytesTransferred);
            metricName = new MetricName("outgoing-byte-rate", metricGrpName, "The average number of outgoing bytes sent per second to all servers.", metricTags);
            this.bytesSent.add(metricName, new Rate());
            metricName = new MetricName("request-rate", metricGrpName, "The average number of requests sent per second.", metricTags);
            this.bytesSent.add(metricName, new Rate(new Count()));
            metricName = new MetricName("request-size-avg", metricGrpName, "The average size of all requests in the window..", metricTags);
            this.bytesSent.add(metricName, new Avg());
            metricName = new MetricName("request-size-max", metricGrpName, "The maximum size of any request sent in the window.", metricTags);
            this.bytesSent.add(metricName, new Max());

            this.bytesReceived = this.metrics.sensor("bytes-received", bytesTransferred);
            metricName = new MetricName("incoming-byte-rate", metricGrpName, "Bytes/second read off all sockets", metricTags);
            this.bytesReceived.add(metricName, new Rate());
            metricName = new MetricName("response-rate", metricGrpName, "Responses received sent per second.", metricTags);
            this.bytesReceived.add(metricName, new Rate(new Count()));

            this.selectTime = this.metrics.sensor("select-time");
            metricName = new MetricName("select-rate", metricGrpName, "Number of times the I/O layer checked for new I/O to perform per second", metricTags);
            this.selectTime.add(metricName, new Rate(new Count()));
            metricName = new MetricName("io-wait-time-ns-avg", metricGrpName, "The average length of time the I/O thread spent waiting for a socket ready for reads or writes in nanoseconds.", metricTags);
            this.selectTime.add(metricName, new Avg());
            metricName = new MetricName("io-wait-ratio", metricGrpName, "The fraction of time the I/O thread spent waiting.", metricTags);
            this.selectTime.add(metricName, new Rate(TimeUnit.NANOSECONDS));

            this.ioTime = this.metrics.sensor("io-time");
            metricName = new MetricName("io-time-ns-avg", metricGrpName, "The average length of time for I/O per select call in nanoseconds.", metricTags);
            this.ioTime.add(metricName, new Avg());
            metricName = new MetricName("io-ratio", metricGrpName, "The fraction of time the I/O thread spent doing I/O", metricTags);
            this.ioTime.add(metricName, new Rate(TimeUnit.NANOSECONDS));

            metricName = new MetricName("connection-count", metricGrpName, "The current number of active connections.", metricTags);
            this.metrics.addMetric(metricName, new Measurable() {
                public double measure(MetricConfig config, long now) {
                    return keys.size();
                }
            });
        }

        public void maybeRegisterNodeMetrics(int node) {
            if (node >= 0) {
                // if one sensor of the metrics has been registered for the node,
                // then all other sensors should have been registered; and vice versa
                String nodeRequestName = "node-" + node + ".bytes-sent";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest == null) {
                    String metricGrpName = metricGrpPrefix + "-node-metrics";

                    Map<String, String> tags = new LinkedHashMap<String, String>(metricTags);
                    tags.put("node-id", "node-"+node);

                    nodeRequest = this.metrics.sensor(nodeRequestName);
                    MetricName metricName = new MetricName("outgoing-byte-rate", metricGrpName, tags);
                    nodeRequest.add(metricName, new Rate());
                    metricName = new MetricName("request-rate", metricGrpName, "The average number of requests sent per second.", tags);
                    nodeRequest.add(metricName, new Rate(new Count()));
                    metricName = new MetricName("request-size-avg", metricGrpName, "The average size of all requests in the window..", tags);
                    nodeRequest.add(metricName, new Avg());
                    metricName = new MetricName("request-size-max", metricGrpName, "The maximum size of any request sent in the window.", tags);
                    nodeRequest.add(metricName, new Max());

                    String nodeResponseName = "node-" + node + ".bytes-received";
                    Sensor nodeResponse = this.metrics.sensor(nodeResponseName);
                    metricName = new MetricName("incoming-byte-rate", metricGrpName, tags);
                    nodeResponse.add(metricName, new Rate());
                    metricName = new MetricName("response-rate", metricGrpName, "The average number of responses received per second.", tags);
                    nodeResponse.add(metricName, new Rate(new Count()));

                    String nodeTimeName = "node-" + node + ".latency";
                    Sensor nodeRequestTime = this.metrics.sensor(nodeTimeName);
                    metricName = new MetricName("request-latency-avg", metricGrpName, tags);
                    nodeRequestTime.add(metricName, new Avg());
                    metricName = new MetricName("request-latency-max", metricGrpName, tags);
                    nodeRequestTime.add(metricName, new Max());
                }
            }
        }

        public void recordBytesSent(int node, int bytes) {
            long now = time.milliseconds();
            this.bytesSent.record(bytes, now);
            if (node >= 0) {
                String nodeRequestName = "node-" + node + ".bytes-sent";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest != null)
                    nodeRequest.record(bytes, now);
            }
        }

        public void recordBytesReceived(int node, int bytes) {
            long now = time.milliseconds();
            this.bytesReceived.record(bytes, now);
            if (node >= 0) {
                String nodeRequestName = "node-" + node + ".bytes-received";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest != null)
                    nodeRequest.record(bytes, now);
            }
        }
    }

}
