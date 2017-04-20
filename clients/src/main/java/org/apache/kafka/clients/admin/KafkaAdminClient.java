/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.BrokerNotAvailableException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DeleteTopicsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.common.utils.Utils.closeQuietly;

/**
 * An administrative client for Kafka which supports managing and inspecting topics, brokers,
 * and configurations.
 */
public class KafkaAdminClient extends AdminClient {
    private static final Logger log = LoggerFactory.getLogger(KafkaAdminClient.class);

    /**
     * The maximum number of times to retry a call before failing it.
     */
    private static final int MAX_CALL_RETRIES = 5;

    /**
     * The next integer to use to name a KafkaAdminClient which the user hasn't specified an explicit name for.
     */
    private static final AtomicInteger ADMIN_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

    /**
     * The default timeout to use for an operation.
     */
    private final int defaultTimeoutMs;

    /**
     * The name of this AdminClient instance.
     */
    private final String clientId;

    /**
     * Provides the time.
     */
    private final Time time;

    /**
     * The cluster metadata used by the KafkaClient.
     */
    private final Metadata metadata;

    /**
     * The metrics for this KafkaAdminClient.
     */
    private final Metrics metrics;

    /**
     * The network client to use.
     */
    private final KafkaClient client;

    /**
     * The runnable used in the service thread for this admin client.
     */
    private final AdminClientRunnable runnable;

    /**
     * The network service thread for this admin client.
     */
    private final Thread thread;

    /**
     * True if this client is closed.
     */
    private volatile boolean closed = false;

    /**
     * Get or create a list value from a map.
     *
     * @param map   The map to get or create the element from.
     * @param key   The key.
     * @param <K>   The key type.
     * @param <V>   The value type.
     * @return      The list value.
     */
    static <K, V> List<V> getOrCreateListValue(Map<K, List<V>> map, K key) {
        List<V> list = map.get(key);
        if (list != null)
            return list;
        list = new LinkedList<V>();
        map.put(key, list);
        return list;
    }

    /**
     * Send an exception to every element in a collection of KafkaFutureImpls.
     *
     * @param futures   The collection of KafkaFutureImpl objects.
     * @param exc       The exception
     * @param <T>       The KafkaFutureImpl result type.
     */
    private static <T> void completeAllExceptionally(Collection<KafkaFutureImpl<T>> futures, Throwable exc) {
        for (KafkaFutureImpl<?> future : futures) {
            future.completeExceptionally(exc);
        }
    }

    /**
     * Get the current time remaining before a deadline as an integer.
     *
     * @param now           The current time in milliseconds.
     * @param deadlineMs    The deadline time in milliseconds.
     * @return              The time delta in milliseconds.
     */
    static int calcTimeoutMsRemainingAsInt(long now, long deadlineMs) {
        long deltaMs = deadlineMs - now;
        if (deltaMs > Integer.MAX_VALUE)
            deltaMs = Integer.MAX_VALUE;
        else if (deltaMs < Integer.MIN_VALUE)
            deltaMs = Integer.MIN_VALUE;
        return (int) deltaMs;
    }

    /**
     * Generate the client id based on the configuration.
     *
     * @param config    The configuration
     *
     * @return          The client id
     */
    static String generateClientId(AdminClientConfig config) {
        String clientId = config.getString(AdminClientConfig.CLIENT_ID_CONFIG);
        if (clientId.length() > 0)
            return clientId;
        return "adminclient-" + ADMIN_CLIENT_ID_SEQUENCE.getAndIncrement();
    }

    /**
     * Get the deadline for a particular call.
     *
     * @param optionTimeoutMs   The timeout option given by the user.
     *
     * @return                  The deadline in milliseconds.
     */
    private long calcDeadlineMs(Integer optionTimeoutMs) {
        if (optionTimeoutMs != null)
            return time.milliseconds() + Math.min(0, optionTimeoutMs);
        // If optionTimeoutMs is less than 0, we use the default timeout.
        return time.milliseconds() + defaultTimeoutMs;
    }

    /**
     * Pretty-print an exception.
     *
     * @param throwable     The exception.
     *
     * @return              A compact human-readable string.
     */
    static String prettyPrintException(Throwable throwable) {
        if (throwable == null)
            return "Null exception.";
        if (throwable.getMessage() != null) {
            return throwable.getClass().getSimpleName() + ": " + throwable.getMessage();
        }
        return throwable.getClass().getSimpleName();
    }

    static KafkaAdminClient create(AdminClientConfig config) {
        Metadata metadata = null;
        Metrics metrics = null;
        NetworkClient networkClient = null;
        Time time = Time.SYSTEM;
        String clientId = generateClientId(config);
        ChannelBuilder channelBuilder = null;
        Selector selector = null;
        ApiVersions apiVersions = new ApiVersions();

        try {
            metadata = new Metadata(config.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG),
                    config.getLong(AdminClientConfig.METADATA_MAX_AGE_CONFIG));
            metrics = new Metrics(new MetricConfig(), new LinkedList<MetricsReporter>(), time);
            String metricGrpPrefix = "admin-client";
            channelBuilder = ClientUtils.createChannelBuilder(config);
            selector = new Selector(config.getLong(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG),
                    metrics, time, metricGrpPrefix, channelBuilder);
            networkClient = new NetworkClient(
                selector,
                metadata,
                clientId,
                100,
                config.getLong(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG),
                config.getInt(AdminClientConfig.SEND_BUFFER_CONFIG),
                config.getInt(AdminClientConfig.RECEIVE_BUFFER_CONFIG),
                config.getInt(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG),
                time,
                true,
                apiVersions);
            channelBuilder = null;
            return new KafkaAdminClient(config, clientId, time, metadata, metrics, networkClient);
        } catch (Throwable exc) {
            closeQuietly(metrics, "Metrics");
            closeQuietly(networkClient, "NetworkClient");
            closeQuietly(selector, "Selector");
            closeQuietly(channelBuilder, "ChannelBuilder");
            throw new KafkaException("Failed create new KafkaAdminClient", exc);
        }
    }

    static KafkaAdminClient create(AdminClientConfig config, KafkaClient client, Metadata metadata) {
        Metrics metrics = null;
        Time time = Time.SYSTEM;
        String clientId = generateClientId(config);

        try {
            metrics = new Metrics(new MetricConfig(), new LinkedList<MetricsReporter>(), time);
            return new KafkaAdminClient(config, clientId, time, metadata, metrics, client);
        } catch (Throwable exc) {
            closeQuietly(metrics, "Metrics");
            throw new KafkaException("Failed create new KafkaAdminClient", exc);
        }
    }

    private KafkaAdminClient(AdminClientConfig config, String clientId, Time time, Metadata metadata,
                     Metrics metrics, KafkaClient client) {
        this.defaultTimeoutMs = config.getInt(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG);
        this.clientId = clientId;
        this.time = time;
        this.metadata = metadata;
        List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(
            config.getList(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
        this.metadata.update(Cluster.bootstrap(addresses), Collections.<String>emptySet(), time.milliseconds());
        this.metrics = metrics;
        this.client = client;
        this.runnable = new AdminClientRunnable();
        this.thread = new Thread(runnable, "AdminClientThread(" + clientId + ")");
        config.logUnused();
        log.debug("Created Kafka admin client {}", this.clientId);
        thread.start();
    }

    @Override
    public void close() {
        closed = true;
        client.wakeup(); // Wake the thread, if it is blocked inside poll().
        try {
            // Wait for the thread to be joined.
            thread.join();
            log.debug("{}: closed.", clientId);
        } catch (InterruptedException e) {
            log.debug("{}: interrupted while joining I/O thread", clientId, e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * An interface for providing a node for a call.
     */
    private interface NodeProvider {
        Node provide();
    }

    /**
     * Provides a constant node which is known at construction time.
     */
    private static class ConstantAdminNodeProvider implements NodeProvider {
        private final Node node;

        ConstantAdminNodeProvider(Node node) {
            this.node = node;
        }

        @Override
        public Node provide() {
            return node;
        }
    }

    /**
     * Provides the controller node.
     */
    private class ControllerNodeProvider implements NodeProvider {
        @Override
        public Node provide() {
            return metadata.fetch().controller();
        }
    }

    /**
     * Provides the least loaded node.
     */
    private class LeastLoadedNodeProvider implements NodeProvider {
        @Override
        public Node provide() {
            return client.leastLoadedNode(time.milliseconds());
        }
    }

    private abstract class Call {
        private final String callName;
        private final long deadlineMs;
        private final NodeProvider nodeProvider;
        private int tries = 0;

        Call(String callName, long deadlineMs, NodeProvider nodeProvider) {
            this.callName = callName;
            this.deadlineMs = deadlineMs;
            this.nodeProvider = nodeProvider;
        }

        final void fail(long now, Throwable throwable) {
            // If this is an UnsupportedVersionException that we can retry, do so.
            if ((throwable instanceof UnsupportedVersionException) &&
                     handleUnsupportedVersionException((UnsupportedVersionException)throwable)) {
                log.trace("{} attempting protocol downgrade.", callName);
                runnable.call(this);
                return;
            }

            // If the exception is not retryable, fail immediately.
            if (!(throwable instanceof RetriableException)) {
                log.debug("{} failed: {}", callName, prettyPrintException(throwable));
                handleFailure(throwable);
                return;
            }
            // Retriable exceptions can be retried a few times.
            tries++;
            if ((tries > MAX_CALL_RETRIES) || calcTimeoutMsRemainingAsInt(now, deadlineMs) < 0) {
                log.debug("{} failed after {} attempt{}: {}",
                    callName, tries, tries == 1 ? "" : "s", prettyPrintException(throwable));
                handleFailure(throwable);
                return;
            }
            log.debug("{} failed: {}.  Beginning retry #{}",
                callName, prettyPrintException(throwable), tries);
            runnable.call(this);
        }

        /**
         * Create an AbstractRequest.Builder for this Call.
         *
         * @param timeoutMs The timeout in milliseconds.
         *
         * @return          The AbstractRequest builder.
         */
        abstract AbstractRequest.Builder createRequest(int timeoutMs);

        /**
         * Process the call response.
         *
         * @param abstractResponse  The AbstractResponse.
         *
         * @return                  True if the response has been processed; false to re-submit the request.
         */
        abstract void handleResponse(AbstractResponse abstractResponse);

        /**
         * Handle a failure.  This will only be called if the failure exception was not
         * retryable, or if we hit a timeout.
         *
         * @param throwable     The exception.
         */
        abstract void handleFailure(Throwable throwable);

        /**
         * Handle an UnsupportedVersionException.
         *
         * @param exception     The exception.
         *
         * @return              True if the exception can be handled; false otherwise.
         */
        boolean handleUnsupportedVersionException(UnsupportedVersionException exception) {
            return false;
        }

        @Override
        public String toString() {
            return "Call(callName=" + callName + ", deadlineMs=" + deadlineMs + ")";
        }
    }

    private final class AdminClientRunnable implements Runnable {
        /**
         * Pending calls.  Protected by the object monitor.
         */
        private List<Call> newCalls = new LinkedList<>();

        /**
         * Check if the AdminClient metadata is ready.
         * We need to know who the controller is, and have a non-empty view of the cluster.
         *
         * @param prevMetadataVersion       The previous metadata version which wasn't usable.
         * @return                          null if the metadata is usable; the current metadata
         *                                  version otherwise
         */
        private Integer checkMetadataReady(Integer prevMetadataVersion) {
            if (prevMetadataVersion != null) {
                if (prevMetadataVersion == metadata.version())
                    return prevMetadataVersion;
            }
            Cluster cluster = metadata.fetch();
            if (cluster.nodes().isEmpty()) {
                log.trace("{}: metadata is not ready yet.  No cluster nodes found.", clientId);
                return metadata.requestUpdate();
            }
            if (cluster.controller() == null) {
                log.trace("{}: metadata is not ready yet.  No controller found.", clientId);
                return metadata.requestUpdate();
            }
            if (prevMetadataVersion != null) {
                log.trace("{}: metadata is now ready.", clientId);
            }
            return null;
        }

        /**
         * Time out a list of calls.
         *
         * @param now       The current time in milliseconds.
         * @param calls     The collection of calls.  Must be sorted from oldest to newest.
         */
        private int timeoutCalls(long now, Collection<Call> calls) {
            int numTimedOut = 0;
            for (Iterator<Call> iter = calls.iterator(); iter.hasNext(); ) {
                Call call = iter.next();
                if (calcTimeoutMsRemainingAsInt(now, call.deadlineMs) < 0) {
                    call.fail(now, new TimeoutException());
                    iter.remove();
                    numTimedOut++;
                }
            }
            return numTimedOut;
        }

        /**
         * Time out the elements in the newRpcs list which are expired.
         *
         * @param now       The current time in milliseconds.
         */
        private synchronized void timeoutNewCalls(long now) {
            int numTimedOut = timeoutCalls(now, newCalls);
            if (numTimedOut > 0) {
                log.debug("{}: timed out {} new calls.", clientId, numTimedOut);
            }
        }

        /**
         * Time out calls which have been assigned to nodes.
         *
         * @param now           The current time in milliseconds.
         * @param callsToSend   A map of nodes to the calls they need to handle.
         */
        private void timeoutCallsToSend(long now, Map<Node, List<Call>> callsToSend) {
            int numTimedOut = 0;
            for (List<Call> callList : callsToSend.values()) {
                numTimedOut += timeoutCalls(now, callList);
            }
            if (numTimedOut > 0)
                log.debug("{}: timed out {} call(s) with assigned nodes.", clientId, numTimedOut);
        }

        /**
         * Choose nodes for the RPCs in the newRpcs list.
         *
         * This function holds the lock for the minimum amount of time, to avoid blocking
         * users of AdminClient who will also take the lock to add new RPC requests.
         *
         * @param now           The current time in milliseconds.
         * @param callsToSend   A map of nodes to the calls they need to handle.
         *
         * @return              The new RPCs we need to process.
         */
        private void chooseNodesForNewCalls(long now, Map<Node, List<Call>> callsToSend) {
            List<Call> newCallsToAdd = null;
            synchronized (this) {
                if (newCalls.isEmpty()) {
                    return;
                }
                newCallsToAdd = newCalls;
                newCalls = new LinkedList<>();
            }
            for (Call call : newCallsToAdd) {
                chooseNodeForNewCall(now, callsToSend, call);
            }
        }

        /**
         * Choose a node for a new call.
         *
         * @param now           The current time in milliseconds.
         * @param callsToSend   A map of nodes to the RPCs they need to handle.
         * @param call          The call.
         */
        private void chooseNodeForNewCall(long now, Map<Node, List<Call>> callsToSend, Call call) {
            Node node = call.nodeProvider.provide();
            if (node == null) {
                call.fail(now, new BrokerNotAvailableException(
                    String.format("Error choosing node for %s: no node found.", call.callName)));
                return;
            }
            log.trace("{}: assigned {} to {}", clientId, call, node);
            getOrCreateListValue(callsToSend, node).add(call);
        }

        /**
         * Send the RPCs which are ready.
         *
         * @param now                   The current time in milliseconds.
         * @param callsToSend           The calls to send, by node.
         * @param correlationIdToRpc    A map of correlation IDs to RPCs.
         * @param callsInFlight         A map of nodes to the RPCs they have in flight.
         *
         * @return                      The minimum timeout we need for poll().
         */
        private long sendEligibleRpcs(long now, Map<Node, List<Call>> callsToSend,
                         Map<Integer, Call> correlationIdToRpc, Map<String, List<Call>> callsInFlight) {
            long pollTimeout = Long.MAX_VALUE;
            for (Iterator<Map.Entry<Node, List<Call>>> iter = callsToSend.entrySet().iterator();
                     iter.hasNext(); ) {
                Map.Entry<Node, List<Call>> entry = iter.next();
                List<Call> calls = entry.getValue();
                if (calls.isEmpty()) {
                    iter.remove();
                    continue;
                }
                Node node = entry.getKey();
                if (!client.ready(node, now)) {
                    long nodeTimeout = client.connectionDelay(node, now);
                    pollTimeout = Math.min(pollTimeout, nodeTimeout);
                    log.trace("{}: client is not ready to send to {}.  Must delay {} ms", clientId, node, nodeTimeout);
                    continue;
                }
                Call call = calls.remove(0);
                int timeoutMs = calcTimeoutMsRemainingAsInt(now, call.deadlineMs);
                AbstractRequest.Builder<?> requestBuilder = null;
                try {
                    requestBuilder = call.createRequest(timeoutMs);
                } catch (Throwable throwable) {
                    call.fail(now, new KafkaException(String.format(
                        "Internal error sending %s to %s.", call.callName, node)));
                    continue;
                }
                ClientRequest clientRequest = client.newClientRequest(node.idString(), requestBuilder, now, true);
                log.trace("{}: sending {} to {}. correlationId={}", clientId, requestBuilder, node,
                    clientRequest.correlationId());
                client.send(clientRequest, now);
                getOrCreateListValue(callsInFlight, node.idString()).add(call);
                correlationIdToRpc.put(clientRequest.correlationId(), call);
            }
            return pollTimeout;
        }

        /**
         * Time out expired RPCs that are in flight.
         *
         * RPCs that are in flight may have been partially or completely sent over the wire.  They may
         * even be in the process of being processed by the remote server.  At the moment, our only option
         * to time them out is to close the entire connection.
         *
         * @param now                   The current time in milliseconds.
         * @param callsInFlight         A map of nodes to the RPCs they have in flight.
         */
        private void timeoutRpcsInFlight(long now, Map<String, List<Call>> callsInFlight) {
            int numTimedOut = 0;
            for (Map.Entry<String, List<Call>> entry : callsInFlight.entrySet()) {
                List<Call> contexts = entry.getValue();
                if (contexts.isEmpty())
                    continue;
                String nodeId = entry.getKey();
                // We assume that the first element in the list is the earliest.  So it should be the
                // only one we need to check the timeout for.
                Call call = contexts.get(0);
                if (calcTimeoutMsRemainingAsInt(now, call.deadlineMs) < 0) {
                    log.debug("{}: Closing connection to {} to time out {}", clientId, nodeId, call);
                    client.close(nodeId);
                    numTimedOut++;
                    // We don't remove anything from the callsInFlight data structure.  Because the connection
                    // has been closed, the RPCs should be returned by the next client#poll(),
                    // and handled at that point.
                }
            }
            if (numTimedOut > 0)
                log.debug("{}: timed out {} call(s) in flight.", clientId, numTimedOut);
        }

        /**
         * Handle responses from the server.
         *
         * @param now                   The current time in milliseconds.
         * @param responses             The latest responses from KafkaClient.
         * @param correlationIdToRpc    A map of correlation IDs to RPCs.
         * @param callsInFlight         A map of nodes to the RPCs they have in flight.
        **/
        private void handleResponses(long now, List<ClientResponse> responses, Map<String, List<Call>> callsInFlight,
                Map<Integer, Call> correlationIdToRpc) {
            for (ClientResponse response : responses) {
                int correlationId = response.requestHeader().correlationId();

                Call call = correlationIdToRpc.get(correlationId);
                if (call == null) {
                    // If the server returns information about a correlation ID we didn't use yet,
                    // an internal server error has occurred.  Close the connection and log an error message.
                    log.error("Internal server error on {}: server returned information about unknown " +
                        "correlation ID {}", response.destination(), correlationId);
                    client.close(response.destination());
                    continue;
                }

                // Stop tracking this call.
                correlationIdToRpc.remove(correlationId);
                getOrCreateListValue(callsInFlight, response.requestHeader().clientId()).remove(call);

                // Handle the result of the call.  This may involve retrying the call, if we got a
                // retryible exception.
                if (response.versionMismatch() != null) {
                    call.fail(now, response.versionMismatch());
                } else if (response.wasDisconnected()) {
                    call.fail(now, new DisconnectException(String.format(
                        "Cancelled %s request with correlation id %s due to node %s being disconnected",
                        call.callName, correlationId, response.destination())));
                } else {
                    try {
                        call.handleResponse(response.responseBody());
                        if (log.isTraceEnabled())
                            log.trace("{}: {} got response {}", clientId, call, response.responseBody());
                    } catch (Throwable t) {
                        if (log.isTraceEnabled())
                            log.trace("{}: {} handleResponse failed with {}", clientId, call, prettyPrintException(t));
                        call.fail(now, t);
                    }
                }
            }
        }

        @Override
        public void run() {
            /**
             * Maps nodes to calls that we want to send.
             */
            Map<Node, List<Call>> callsToSend = new HashMap<>();

            /**
             * Maps node ID strings to calls that have been sent.
             */
            Map<String, List<Call>> callsInFlight = new HashMap<>();

            /**
             * Maps correlation IDs to calls that have been sent.
             */
            Map<Integer, Call> correlationIdToRpc = new HashMap<>();

            /**
             * The previous metadata version which wasn't usable, or null if there is none.
             */
            Integer prevMetadataVersion = null;

            long now = time.milliseconds();
            log.trace("{} thread starting", clientId);
            while (true) {
                // Check if the AdminClient is shutting down.
                if (closed)
                    break;

                // Handle timeouts.
                timeoutNewCalls(now);
                timeoutCallsToSend(now, callsToSend);
                timeoutRpcsInFlight(now, callsInFlight);

                // Handle new RPCs and metadata update requests.
                prevMetadataVersion = checkMetadataReady(prevMetadataVersion);
                long pollTimeout = 1200000;
                if (prevMetadataVersion == null) {
                    chooseNodesForNewCalls(now, callsToSend);
                    pollTimeout = Math.min(pollTimeout,
                        sendEligibleRpcs(now, callsToSend, correlationIdToRpc, callsInFlight));
                }

                // Wait for network responses.
                log.trace("{}: entering KafkaClient#poll(timeout={})", clientId, pollTimeout);
                List<ClientResponse> responses = client.poll(pollTimeout, now);
                log.trace("{}: KafkaClient#poll retrieved {} response(s)", clientId, responses.size());

                // Update the current time and handle the latest responses.
                now = time.milliseconds();
                handleResponses(now, responses, callsInFlight, correlationIdToRpc);
            }
            int numTimedOut = 0;
            synchronized (this) {
                numTimedOut += timeoutCalls(Long.MAX_VALUE, newCalls);
            }
            numTimedOut += timeoutCalls(Long.MAX_VALUE, correlationIdToRpc.values());
            if (numTimedOut > 0) {
                log.debug("{}: timed out {} remaining operations.", clientId, numTimedOut);
            }
            closeQuietly(client, "KafkaClient");
            closeQuietly(metrics, "Metrics");
            log.debug("{}: exiting AdminClientRunnable thread.", clientId);
        }

        void call(Call call) {
            log.debug("{}: queueing {}", clientId, call);
            synchronized (this) {
                newCalls.add(call);
            }
            client.wakeup();
        }
    }

    @Override
    public CreateTopicResults createTopics(final Collection<NewTopic> newTopics,
                                           final CreateTopicsOptions options) {
        final Map<String, KafkaFutureImpl<Void>> topicFutures = new HashMap<>(newTopics.size());
        for (NewTopic newTopic : newTopics) {
            topicFutures.put(newTopic.name(), new KafkaFutureImpl<Void>());
        }
        final Map<String, CreateTopicsRequest.TopicDetails> topicsMap = new HashMap<>(newTopics.size());
        for (NewTopic newTopic : newTopics) {
            topicsMap.put(newTopic.name(), newTopic.convertToTopicDetails());
        }
        runnable.call(new Call("createTopics", calcDeadlineMs(options.timeoutMs()),
            new ControllerNodeProvider()) {

            @Override
            public AbstractRequest.Builder createRequest(int timeoutMs) {
                return new CreateTopicsRequest.Builder(topicsMap, timeoutMs, options.validateOnly());
            }

            @Override
            public void handleResponse(AbstractResponse abstractResponse) {
                CreateTopicsResponse response = (CreateTopicsResponse) abstractResponse;
                // Handle server responses for particular topics.
                for (Map.Entry<String, CreateTopicsResponse.Error> entry : response.errors().entrySet()) {
                    KafkaFutureImpl<Void> future = topicFutures.get(entry.getKey());
                    if (future == null) {
                        log.warn("Server response mentioned unknown topic {}", entry.getKey());
                    } else {
                        ApiException exception = entry.getValue().exception();
                        if (exception != null) {
                            future.completeExceptionally(exception);
                        } else {
                            future.complete(null);
                        }
                    }
                }
                // The server should send back a response for every topic.  But do a sanity check anyway.
                for (Map.Entry<String, KafkaFutureImpl<Void>> entry : topicFutures.entrySet()) {
                    KafkaFutureImpl<Void> future = entry.getValue();
                    if (!future.isDone()) {
                        future.completeExceptionally(new ApiException("The server response did not " +
                            "contain a reference to node " + entry.getKey()));
                    }
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(topicFutures.values(), throwable);
            }
        });
        return new CreateTopicResults(new HashMap<String, KafkaFuture<Void>>(topicFutures));
    }

    @Override
    public DeleteTopicResults deleteTopics(final Collection<String> topicNames,
                                           DeleteTopicsOptions options) {
        final Map<String, KafkaFutureImpl<Void>> topicFutures = new HashMap<>(topicNames.size());
        for (String topicName : topicNames) {
            topicFutures.put(topicName, new KafkaFutureImpl<Void>());
        }
        runnable.call(new Call("deleteTopics", calcDeadlineMs(options.timeoutMs()),
            new ControllerNodeProvider()) {

            @Override
            AbstractRequest.Builder createRequest(int timeoutMs) {
                return new DeleteTopicsRequest.Builder(new HashSet<String>(topicNames), timeoutMs);
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                DeleteTopicsResponse response = (DeleteTopicsResponse) abstractResponse;
                // Handle server responses for particular topics.
                for (Map.Entry<String, Errors> entry : response.errors().entrySet()) {
                    KafkaFutureImpl<Void> future = topicFutures.get(entry.getKey());
                    if (future == null) {
                        log.warn("Server response mentioned unknown topic {}", entry.getKey());
                    } else {
                        ApiException exception = entry.getValue().exception();
                        if (exception != null) {
                            future.completeExceptionally(exception);
                        } else {
                            future.complete(null);
                        }
                    }
                }
                // The server should send back a response for every topic.  But do a sanity check anyway.
                for (Map.Entry<String, KafkaFutureImpl<Void>> entry : topicFutures.entrySet()) {
                    KafkaFutureImpl<Void> future = entry.getValue();
                    if (!future.isDone()) {
                        future.completeExceptionally(new ApiException("The server response did not " +
                            "contain a reference to node " + entry.getKey()));
                    }
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(topicFutures.values(), throwable);
            }
        });
        return new DeleteTopicResults(new HashMap<String, KafkaFuture<Void>>(topicFutures));
    }

    @Override
    public ListTopicsResults listTopics(final ListTopicsOptions options) {
        final KafkaFutureImpl<Map<String, TopicListing>> topicListingFuture = new KafkaFutureImpl<>();
        runnable.call(new Call("listTopics", calcDeadlineMs(options.timeoutMs()), new LeastLoadedNodeProvider()) {
            @Override
            AbstractRequest.Builder createRequest(int timeoutMs) {
                return MetadataRequest.Builder.allTopics();
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                MetadataResponse response = (MetadataResponse) abstractResponse;
                Cluster cluster = response.cluster();
                Map<String, TopicListing> topicListing = new HashMap<>();
                for (String topicName : cluster.topics()) {
                    boolean internal = cluster.internalTopics().contains(topicName);
                    if (internal == false || options.listInternal())
                        topicListing.put(topicName, new TopicListing(topicName, internal));
                }
                topicListingFuture.complete(topicListing);
            }

            @Override
            void handleFailure(Throwable throwable) {
                topicListingFuture.completeExceptionally(throwable);
            }
        });
        return new ListTopicsResults(topicListingFuture);
    }

    @Override
    public DescribeTopicsResults describeTopics(final Collection<String> topicNames, DescribeTopicsOptions options) {
        final Map<String, KafkaFutureImpl<TopicDescription>> topicFutures = new HashMap<>(topicNames.size());
        for (String topicName : topicNames) {
            topicFutures.put(topicName, new KafkaFutureImpl<TopicDescription>());
        }
        runnable.call(new Call("describeTopics", calcDeadlineMs(options.timeoutMs()),
            new ControllerNodeProvider()) {

            @Override
            AbstractRequest.Builder createRequest(int timeoutMs) {
                return new DeleteTopicsRequest.Builder(new HashSet<String>(topicNames), timeoutMs);
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                MetadataResponse response = (MetadataResponse) abstractResponse;
                // Handle server responses for particular topics.
                for (Map.Entry<String, KafkaFutureImpl<TopicDescription>> entry : topicFutures.entrySet()) {
                    String topicName = entry.getKey();
                    KafkaFutureImpl<TopicDescription> future = entry.getValue();
                    Errors topicError = response.errors().get(topicName);
                    if (topicError != null) {
                        future.completeExceptionally(topicError.exception());
                        continue;
                    }
                    Cluster cluster = response.cluster();
                    if (!cluster.topics().contains(topicName)) {
                        future.completeExceptionally(new InvalidTopicException("Topic " + topicName + " not found."));
                        continue;
                    }
                    boolean isInternal = cluster.internalTopics().contains(topicName);
                    TreeMap<Integer, TopicPartitionInfo> partitions = new TreeMap<>();
                    List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topicName);
                    for (PartitionInfo partitionInfo : partitionInfos) {
                        TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(
                            partitionInfo.partition(), partitionInfo.leader(), Arrays.asList(partitionInfo.replicas()),
                            Arrays.asList(partitionInfo.inSyncReplicas()));
                        partitions.put(partitionInfo.partition(), topicPartitionInfo);
                    }
                    TopicDescription topicDescription = new TopicDescription(topicName, isInternal, partitions);
                    future.complete(topicDescription);
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(topicFutures.values(), throwable);
            }
        });
        return new DescribeTopicsResults(new HashMap<String, KafkaFuture<TopicDescription>>(topicFutures));
    }

    @Override
    public DescribeClusterResults describeCluster(DescribeClusterOptions options) {
        final KafkaFutureImpl<Collection<Node>> describeClusterFuture = new KafkaFutureImpl<>();
        runnable.call(new Call("listNodes", calcDeadlineMs(options.timeoutMs()), new LeastLoadedNodeProvider()) {
            @Override
            AbstractRequest.Builder createRequest(int timeoutMs) {
                return new MetadataRequest.Builder(Collections.<String>emptyList());
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                MetadataResponse response = (MetadataResponse) abstractResponse;
                describeClusterFuture.complete(response.brokers());
            }

            @Override
            void handleFailure(Throwable throwable) {
                describeClusterFuture.completeExceptionally(throwable);
            }
        });
        return new DescribeClusterResults(describeClusterFuture);
    }

    @Override
    public ApiVersionsResults apiVersions(Collection<Node> nodes, ApiVersionsOptions options) {
        final long deadlineMs = calcDeadlineMs(options.timeoutMs());
        Map<Node, KafkaFuture<NodeApiVersions>> nodeFutures = new HashMap<>();
        for (final Node node : nodes) {
            final KafkaFutureImpl<NodeApiVersions> nodeFuture = new KafkaFutureImpl<>();
            nodeFutures.put(node, nodeFuture);
            runnable.call(new Call("apiVersions", deadlineMs, new ConstantAdminNodeProvider(node)) {
                    @Override
                    public AbstractRequest.Builder createRequest(int timeoutMs) {
                        return new ApiVersionsRequest.Builder();
                    }

                    @Override
                    public void handleResponse(AbstractResponse abstractResponse) {
                        ApiVersionsResponse response = (ApiVersionsResponse) abstractResponse;
                        nodeFuture.complete(new NodeApiVersions(response.apiVersions()));
                    }

                    @Override
                    public void handleFailure(Throwable throwable) {
                        nodeFuture.completeExceptionally(throwable);
                    }
                });
        }
        return new ApiVersionsResults(nodeFutures);

    }
}
