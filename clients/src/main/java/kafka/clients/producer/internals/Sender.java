package kafka.clients.producer.internals;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kafka.common.Cluster;
import kafka.common.Node;
import kafka.common.TopicPartition;
import kafka.common.errors.NetworkException;
import kafka.common.network.NetworkReceive;
import kafka.common.network.NetworkSend;
import kafka.common.network.Selectable;
import kafka.common.protocol.ApiKeys;
import kafka.common.protocol.Errors;
import kafka.common.protocol.ProtoUtils;
import kafka.common.protocol.types.Struct;
import kafka.common.requests.RequestHeader;
import kafka.common.requests.RequestSend;
import kafka.common.requests.ResponseHeader;
import kafka.common.utils.Time;

/**
 * The background thread that handles the sending of produce requests to the Kafka cluster. This thread makes metadata
 * requests to renew its view of the cluster and then sends produce requests to the appropriate nodes.
 */
public class Sender implements Runnable {

    private final Map<Integer, NodeState> nodeState;
    private final RecordAccumulator accumulator;
    private final Selectable selector;
    private final String clientId;
    private final int maxRequestSize;
    private final long reconnectBackoffMs;
    private final short acks;
    private final int requestTimeout;
    private final InFlightRequests inFlightRequests;
    private final Metadata metadata;
    private final Time time;
    private int correlation;
    private boolean metadataFetchInProgress;
    private volatile boolean running;

    public Sender(Selectable selector,
                  Metadata metadata,
                  RecordAccumulator accumulator,
                  String clientId,
                  int maxRequestSize,
                  long reconnectBackoffMs,
                  short acks,
                  int requestTimeout,
                  Time time) {
        this.nodeState = new HashMap<Integer, NodeState>();
        this.accumulator = accumulator;
        this.selector = selector;
        this.maxRequestSize = maxRequestSize;
        this.reconnectBackoffMs = reconnectBackoffMs;
        this.metadata = metadata;
        this.clientId = clientId;
        this.running = true;
        this.requestTimeout = requestTimeout;
        this.acks = acks;
        this.inFlightRequests = new InFlightRequests();
        this.correlation = 0;
        this.metadataFetchInProgress = false;
        this.time = time;
    }

    /**
     * The main run loop for the sender thread
     */
    public void run() {
        // main loop, runs until close is called
        while (running) {
            try {
                run(time.milliseconds());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // send anything left in the accumulator
        int unsent = 0;
        do {
            try {
                unsent = run(time.milliseconds());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } while (unsent > 0);

        // close all the connections
        this.selector.close();
    }

    /**
     * Run a single iteration of sending
     * 
     * @param now The current time
     * @return The total number of topic/partitions that had data ready (regardless of what we actually sent)
     */
    public int run(long now) {
        Cluster cluster = metadata.fetch();
        // get the list of partitions with data ready to send
        List<TopicPartition> ready = this.accumulator.ready(now);

        // prune the list of ready topics to eliminate any that we aren't ready to send yet
        List<TopicPartition> sendable = processReadyPartitions(cluster, ready, now);

        // should we update our metadata?
        List<NetworkSend> sends = new ArrayList<NetworkSend>(sendable.size());
        InFlightRequest metadataReq = maybeMetadataRequest(cluster, now);
        if (metadataReq != null) {
            sends.add(metadataReq.request);
            this.inFlightRequests.add(metadataReq);
        }

        // create produce requests
        List<RecordBatch> batches = this.accumulator.drain(sendable, this.maxRequestSize);
        List<InFlightRequest> requests = collate(cluster, batches);
        for (int i = 0; i < requests.size(); i++) {
            InFlightRequest request = requests.get(i);
            this.inFlightRequests.add(request);
            sends.add(request.request);
        }

        // do the I/O
        try {
            this.selector.poll(5L, sends);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // handle responses, connections, and disconnections
        handleSends(this.selector.completedSends());
        handleResponses(this.selector.completedReceives(), now);
        handleDisconnects(this.selector.disconnected());
        handleConnects(this.selector.connected());

        return ready.size();
    }

    private InFlightRequest maybeMetadataRequest(Cluster cluster, long now) {
        if (this.metadataFetchInProgress || !metadata.needsUpdate(now))
            return null;
        Node node = cluster.nextNode();
        NodeState state = nodeState.get(node.id());
        if (state == null || (state.state == ConnectionState.DISCONNECTED && now - state.lastConnectAttempt > this.reconnectBackoffMs)) {
            // we don't have a connection to this node right now, make one
            initiateConnect(node, now);
            return null;
        } else if (state.state == ConnectionState.CONNECTED) {
            this.metadataFetchInProgress = true;
            return metadataRequest(node.id(), metadata.topics());
        } else {
            return null;
        }
    }

    /**
     * Start closing the sender (won't actually complete until all data is sent out)
     */
    public void initiateClose() {
        this.running = false;
        this.accumulator.close();
    }

    /**
     * Process the set of topic-partitions with data ready to send. If we have a connection to the appropriate node, add
     * it to the returned set. For any partitions we have no connection to either make one, fetch the appropriate
     * metdata to be able to do so
     */
    private List<TopicPartition> processReadyPartitions(Cluster cluster, List<TopicPartition> ready, long now) {
        List<TopicPartition> sendable = new ArrayList<TopicPartition>(ready.size());
        for (TopicPartition tp : ready) {
            Node node = cluster.leaderFor(tp);
            if (node == null) {
                // we don't know about this topic/partition or it has no leader, re-fetch metadata
                metadata.forceUpdate();
            } else {
                NodeState state = nodeState.get(node.id());
                // TODO: encapsulate this logic somehow
                if (state == null || (state.state == ConnectionState.DISCONNECTED && now - state.lastConnectAttempt > this.reconnectBackoffMs)) {
                    // we don't have a connection to this node right now, make one
                    initiateConnect(node, now);
                } else if (state.state == ConnectionState.CONNECTED && inFlightRequests.canSendMore(node.id())) {
                    sendable.add(tp);
                }
            }
        }
        return sendable;
    }

    /**
     * Initiate a connection to the given node
     */
    private void initiateConnect(Node node, long now) {
        try {
            selector.connect(node.id(), new InetSocketAddress(node.host(), node.port()), 64 * 1024 * 1024, 64 * 1024 * 1024); // TODO
                                                                                                                              // socket
                                                                                                                              // buffers
            nodeState.put(node.id(), new NodeState(ConnectionState.CONNECTING, now));
        } catch (IOException e) {
            /* attempt failed, we'll try again after the backoff */
            nodeState.put(node.id(), new NodeState(ConnectionState.DISCONNECTED, now));
            /* maybe the problem is our metadata, update it */
            metadata.forceUpdate();
        }
    }

    /**
     * Handle any closed connections
     */
    private void handleDisconnects(List<Integer> disconnects) {
        for (int node : disconnects) {
            for (InFlightRequest request : this.inFlightRequests.clearAll(node)) {
                if (request.batches != null) {
                    for (RecordBatch batch : request.batches.values())
                        batch.done(-1L, new NetworkException("The server disconnected unexpectedly without sending a response."));
                    this.accumulator.deallocate(request.batches.values());
                }
                NodeState state = this.nodeState.get(request.request.destination());
                if (state != null)
                    state.state = ConnectionState.DISCONNECTED;
            }
        }
    }

    /**
     * Record any connections that completed in our node state
     */
    private void handleConnects(List<Integer> connects) {
        for (Integer id : connects)
            this.nodeState.get(id).state = ConnectionState.CONNECTED;
    }

    /**
     * Process completed sends
     */
    public void handleSends(List<NetworkSend> sends) {
        /* if acks = 0 then the request is satisfied once sent */
        for (NetworkSend send : sends) {
            Deque<InFlightRequest> requests = this.inFlightRequests.requestQueue(send.destination());
            InFlightRequest request = requests.peekFirst();
            if (!request.expectResponse) {
                requests.pollFirst();
                if (request.request.header().apiKey() == ApiKeys.PRODUCE.id) {
                    for (RecordBatch batch : request.batches.values())
                        batch.done(-1L, Errors.NONE.exception());
                    this.accumulator.deallocate(request.batches.values());
                }
            }
        }
    }

    /**
     * Handle responses from the server
     */
    private void handleResponses(List<NetworkReceive> receives, long now) {
        for (NetworkReceive receive : receives) {
            int source = receive.source();
            InFlightRequest req = inFlightRequests.nextCompleted(source);
            ResponseHeader header = ResponseHeader.parse(receive.payload());
            short apiKey = req.request.header().apiKey();
            Struct body = (Struct) ProtoUtils.currentResponseSchema(apiKey).read(receive.payload());
            correlate(req.request.header(), header);
            if (req.request.header().apiKey() == ApiKeys.PRODUCE.id)
                handleProduceResponse(req, body);
            else if (req.request.header().apiKey() == ApiKeys.METADATA.id)
                handleMetadataResponse(body, now);
            else
                throw new IllegalStateException("Unexpected response type: " + req.request.header().apiKey());
        }
    }

    private void handleMetadataResponse(Struct body, long now) {
        this.metadataFetchInProgress = false;
        this.metadata.update(ProtoUtils.parseMetadataResponse(body), now);
    }

    /**
     * Handle a produce response
     */
    private void handleProduceResponse(InFlightRequest request, Struct response) {
        for (Object topicResponse : (Object[]) response.get("responses")) {
            Struct topicRespStruct = (Struct) topicResponse;
            String topic = (String) topicRespStruct.get("topic");
            for (Object partResponse : (Object[]) topicRespStruct.get("partition_responses")) {
                Struct partRespStruct = (Struct) partResponse;
                int partition = (Integer) partRespStruct.get("partition");
                short errorCode = (Short) partRespStruct.get("error_code");
                long offset = (Long) partRespStruct.get("base_offset");
                RecordBatch batch = request.batches.get(new TopicPartition(topic, partition));
                batch.done(offset, Errors.forCode(errorCode).exception());
            }
        }
        this.accumulator.deallocate(request.batches.values());
    }

    /**
     * Validate that the response corresponds to the request we expect or else explode
     */
    private void correlate(RequestHeader requestHeader, ResponseHeader responseHeader) {
        if (requestHeader.correlationId() != responseHeader.correlationId())
            throw new IllegalStateException("Correlation id for response (" + responseHeader.correlationId()
                                            + ") does not match request ("
                                            + requestHeader.correlationId()
                                            + ")");
    }

    /**
     * Create a metadata request for the given topics
     */
    private InFlightRequest metadataRequest(int node, Set<String> topics) {
        String[] ts = new String[topics.size()];
        topics.toArray(ts);
        Struct body = new Struct(ProtoUtils.currentRequestSchema(ApiKeys.METADATA.id));
        body.set("topics", topics.toArray());
        RequestSend send = new RequestSend(node, new RequestHeader(ApiKeys.METADATA.id, clientId, correlation++), body);
        return new InFlightRequest(true, send, null);
    }

    /**
     * Collate the record batches into a list of produce requests on a per-node basis
     */
    private List<InFlightRequest> collate(Cluster cluster, List<RecordBatch> batches) {
        Map<Integer, List<RecordBatch>> collated = new HashMap<Integer, List<RecordBatch>>();
        for (RecordBatch batch : batches) {
            Node node = cluster.leaderFor(batch.topicPartition);
            List<RecordBatch> found = collated.get(node.id());
            if (found == null) {
                found = new ArrayList<RecordBatch>();
                collated.put(node.id(), found);
            }
            found.add(batch);
        }
        List<InFlightRequest> requests = new ArrayList<InFlightRequest>(collated.size());
        for (Map.Entry<Integer, List<RecordBatch>> entry : collated.entrySet())
            requests.add(produceRequest(entry.getKey(), acks, requestTimeout, entry.getValue()));
        return requests;
    }

    /**
     * Create a produce request from the given record batches
     */
    private InFlightRequest produceRequest(int destination, short acks, int timeout, List<RecordBatch> batches) {
        Map<TopicPartition, RecordBatch> batchesByPartition = new HashMap<TopicPartition, RecordBatch>();
        Map<String, List<RecordBatch>> batchesByTopic = new HashMap<String, List<RecordBatch>>();
        for (RecordBatch batch : batches) {
            batchesByPartition.put(batch.topicPartition, batch);
            List<RecordBatch> found = batchesByTopic.get(batch.topicPartition.topic());
            if (found == null) {
                found = new ArrayList<RecordBatch>();
                batchesByTopic.put(batch.topicPartition.topic(), found);
            }
            found.add(batch);
        }
        Struct produce = new Struct(ProtoUtils.currentRequestSchema(ApiKeys.PRODUCE.id));
        produce.set("acks", acks);
        produce.set("timeout", timeout);
        List<Struct> topicDatas = new ArrayList<Struct>(batchesByTopic.size());
        for (Map.Entry<String, List<RecordBatch>> entry : batchesByTopic.entrySet()) {
            Struct topicData = produce.instance("topic_data");
            topicData.set("topic", entry.getKey());
            List<RecordBatch> parts = entry.getValue();
            Object[] partitionData = new Object[parts.size()];
            for (int i = 0; i < parts.size(); i++) {
                ByteBuffer buffer = parts.get(i).records.buffer();
                buffer.flip();
                Struct part = topicData.instance("data")
                                       .set("partition", parts.get(i).topicPartition.partition())
                                       .set("message_set", buffer);
                partitionData[i] = part;
            }
            topicData.set("data", partitionData);
            topicDatas.add(topicData);
        }
        produce.set("topic_data", topicDatas.toArray());

        RequestHeader header = new RequestHeader(ApiKeys.PRODUCE.id, clientId, correlation++);
        RequestSend send = new RequestSend(destination, header, produce);
        return new InFlightRequest(acks != 0, send, batchesByPartition);
    }

    /**
     * Wake up the selector associated with this send thread
     */
    public void wakeup() {
        this.selector.wakeup();
    }

    /**
     * The states of a node connection
     */
    private static enum ConnectionState {
        DISCONNECTED, CONNECTING, CONNECTED
    }

    /**
     * The state of a node
     */
    private static final class NodeState {
        private ConnectionState state;
        private long lastConnectAttempt;

        public NodeState(ConnectionState state, long lastConnectAttempt) {
            this.state = state;
            this.lastConnectAttempt = lastConnectAttempt;
        }

        public String toString() {
            return "NodeState(" + state + ", " + lastConnectAttempt + ")";
        }
    }

    /**
     * An request that hasn't been fully processed yet
     */
    private static final class InFlightRequest {
        public boolean expectResponse;
        public Map<TopicPartition, RecordBatch> batches;
        public RequestSend request;

        /**
         * @param expectResponse Should we expect a response message or is this request complete once it is sent?
         * @param request The request
         * @param batches The record batches contained in the request if it is a produce request
         */
        public InFlightRequest(boolean expectResponse, RequestSend request, Map<TopicPartition, RecordBatch> batches) {
            this.batches = batches;
            this.request = request;
            this.expectResponse = expectResponse;
        }
    }

    /**
     * A set of outstanding request queues for each node that have not yet received responses
     */
    private static final class InFlightRequests {
        private final Map<Integer, Deque<InFlightRequest>> requests = new HashMap<Integer, Deque<InFlightRequest>>();

        /**
         * Add the given request to the queue for the node it was directed to
         */
        public void add(InFlightRequest request) {
            Deque<InFlightRequest> reqs = this.requests.get(request.request.destination());
            if (reqs == null) {
                reqs = new ArrayDeque<InFlightRequest>();
                this.requests.put(request.request.destination(), reqs);
            }
            reqs.addFirst(request);
        }

        public Deque<InFlightRequest> requestQueue(int node) {
            Deque<InFlightRequest> reqs = requests.get(node);
            if (reqs == null || reqs.isEmpty())
                throw new IllegalStateException("Response from server for which there are no in-flight requests.");
            return reqs;
        }

        /**
         * Get the oldest request (the one that that will be completed next) for the given node
         */
        public InFlightRequest nextCompleted(int node) {
            return requestQueue(node).pollLast();
        }

        /**
         * Can we send more requests to this node?
         * 
         * @param node Node in question
         * @return true iff we have no requests still being sent to the given node
         */
        public boolean canSendMore(int node) {
            Deque<InFlightRequest> queue = requests.get(node);
            return queue == null || queue.isEmpty() || queue.peekFirst().request.complete();
        }

        /**
         * Clear out all the in-flight requests for the given node and return them
         * 
         * @param node The node
         * @return All the in-flight requests for that node that have been removed
         */
        public Iterable<InFlightRequest> clearAll(int node) {
            Deque<InFlightRequest> reqs = requests.get(node);
            if (reqs == null) {
                return Collections.emptyList();
            } else {
                return requests.remove(node);
            }
        }
    }

}
