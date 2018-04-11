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
import org.apache.kafka.clients.admin.DeleteAclsResult.FilterResult;
import org.apache.kafka.clients.admin.DeleteAclsResult.FilterResults;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.BrokerNotAvailableException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AlterConfigsRequest;
import org.apache.kafka.common.requests.AlterConfigsResponse;
import org.apache.kafka.common.requests.AlterReplicaLogDirsRequest;
import org.apache.kafka.common.requests.AlterReplicaLogDirsResponse;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.CreateAclsRequest;
import org.apache.kafka.common.requests.CreateAclsRequest.AclCreation;
import org.apache.kafka.common.requests.CreateAclsResponse;
import org.apache.kafka.common.requests.CreateAclsResponse.AclCreationResponse;
import org.apache.kafka.common.requests.CreateDelegationTokenRequest;
import org.apache.kafka.common.requests.CreateDelegationTokenResponse;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreatePartitionsResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteAclsRequest;
import org.apache.kafka.common.requests.DeleteAclsResponse;
import org.apache.kafka.common.requests.DeleteAclsResponse.AclDeletionResult;
import org.apache.kafka.common.requests.DeleteAclsResponse.AclFilterResponse;
import org.apache.kafka.common.requests.DeleteRecordsRequest;
import org.apache.kafka.common.requests.DeleteRecordsResponse;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DeleteTopicsResponse;
import org.apache.kafka.common.requests.DescribeAclsRequest;
import org.apache.kafka.common.requests.DescribeAclsResponse;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.requests.DescribeDelegationTokenRequest;
import org.apache.kafka.common.requests.DescribeDelegationTokenResponse;
import org.apache.kafka.common.requests.DescribeLogDirsRequest;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.apache.kafka.common.requests.ExpireDelegationTokenRequest;
import org.apache.kafka.common.requests.ExpireDelegationTokenResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RenewDelegationTokenRequest;
import org.apache.kafka.common.requests.RenewDelegationTokenResponse;
import org.apache.kafka.common.requests.Resource;
import org.apache.kafka.common.requests.ResourceType;
import org.apache.kafka.common.security.token.delegation.DelegationToken;
import org.apache.kafka.common.security.token.delegation.TokenInformation;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.kafka.common.utils.Utils.closeQuietly;

/**
 * The default implementation of {@link AdminClient}. An instance of this class is created by invoking one of the
 * {@code create()} methods in {@code AdminClient}. Users should not refer to this class directly.
 *
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class KafkaAdminClient extends AdminClient {

    /**
     * The next integer to use to name a KafkaAdminClient which the user hasn't specified an explicit name for.
     */
    private static final AtomicInteger ADMIN_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

    /**
     * The prefix to use for the JMX metrics for this class
     */
    private static final String JMX_PREFIX = "kafka.admin.client";

    /**
     * An invalid shutdown time which indicates that a shutdown has not yet been performed.
     */
    private static final long INVALID_SHUTDOWN_TIME = -1;

    private final Logger log;

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
     * During a close operation, this is the time at which we will time out all pending operations
     * and force the RPC thread to exit. If the admin client is not closing, this will be 0.
     */
    private final AtomicLong hardShutdownTimeMs = new AtomicLong(INVALID_SHUTDOWN_TIME);

    /**
     * A factory which creates TimeoutProcessors for the RPC thread.
     */
    private final TimeoutProcessorFactory timeoutProcessorFactory;

    private final int maxRetries;

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
        list = new LinkedList<>();
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
        if (!clientId.isEmpty())
            return clientId;
        return "adminclient-" + ADMIN_CLIENT_ID_SEQUENCE.getAndIncrement();
    }

    /**
     * Get the deadline for a particular call.
     *
     * @param now               The current time in milliseconds.
     * @param optionTimeoutMs   The timeout option given by the user.
     *
     * @return                  The deadline in milliseconds.
     */
    private long calcDeadlineMs(long now, Integer optionTimeoutMs) {
        if (optionTimeoutMs != null)
            return now + Math.max(0, optionTimeoutMs);
        return now + defaultTimeoutMs;
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

    static KafkaAdminClient createInternal(AdminClientConfig config, TimeoutProcessorFactory timeoutProcessorFactory) {
        Metrics metrics = null;
        NetworkClient networkClient = null;
        Time time = Time.SYSTEM;
        String clientId = generateClientId(config);
        ChannelBuilder channelBuilder = null;
        Selector selector = null;
        ApiVersions apiVersions = new ApiVersions();
        LogContext logContext = createLogContext(clientId);

        try {
            // Since we only request node information, it's safe to pass true for allowAutoTopicCreation (and it
            // simplifies communication with older brokers)
            Metadata metadata = new Metadata(config.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG),
                    config.getLong(AdminClientConfig.METADATA_MAX_AGE_CONFIG), true);
            List<MetricsReporter> reporters = config.getConfiguredInstances(AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG,
                MetricsReporter.class);
            Map<String, String> metricTags = Collections.singletonMap("client-id", clientId);
            MetricConfig metricConfig = new MetricConfig().samples(config.getInt(AdminClientConfig.METRICS_NUM_SAMPLES_CONFIG))
                .timeWindow(config.getLong(AdminClientConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                .recordLevel(Sensor.RecordingLevel.forName(config.getString(AdminClientConfig.METRICS_RECORDING_LEVEL_CONFIG)))
                .tags(metricTags);
            reporters.add(new JmxReporter(JMX_PREFIX));
            metrics = new Metrics(metricConfig, reporters, time);
            String metricGrpPrefix = "admin-client";
            channelBuilder = ClientUtils.createChannelBuilder(config);
            selector = new Selector(config.getLong(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG),
                    metrics, time, metricGrpPrefix, channelBuilder, logContext);
            networkClient = new NetworkClient(
                selector,
                metadata,
                clientId,
                1,
                config.getLong(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG),
                config.getLong(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                config.getInt(AdminClientConfig.SEND_BUFFER_CONFIG),
                config.getInt(AdminClientConfig.RECEIVE_BUFFER_CONFIG),
                (int) TimeUnit.HOURS.toMillis(1),
                time,
                true,
                apiVersions,
                logContext);
            return new KafkaAdminClient(config, clientId, time, metadata, metrics, networkClient,
                timeoutProcessorFactory, logContext);
        } catch (Throwable exc) {
            closeQuietly(metrics, "Metrics");
            closeQuietly(networkClient, "NetworkClient");
            closeQuietly(selector, "Selector");
            closeQuietly(channelBuilder, "ChannelBuilder");
            throw new KafkaException("Failed create new KafkaAdminClient", exc);
        }
    }

    static KafkaAdminClient createInternal(AdminClientConfig config, KafkaClient client, Metadata metadata, Time time) {
        Metrics metrics = null;
        String clientId = generateClientId(config);

        try {
            metrics = new Metrics(new MetricConfig(), new LinkedList<MetricsReporter>(), time);
            return new KafkaAdminClient(config, clientId, time, metadata, metrics, client, null,
                    createLogContext(clientId));
        } catch (Throwable exc) {
            closeQuietly(metrics, "Metrics");
            throw new KafkaException("Failed create new KafkaAdminClient", exc);
        }
    }

    private static LogContext createLogContext(String clientId) {
        return new LogContext("[AdminClient clientId=" + clientId + "] ");
    }

    private KafkaAdminClient(AdminClientConfig config, String clientId, Time time, Metadata metadata,
                     Metrics metrics, KafkaClient client, TimeoutProcessorFactory timeoutProcessorFactory,
                     LogContext logContext) {
        this.defaultTimeoutMs = config.getInt(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG);
        this.clientId = clientId;
        this.log = logContext.logger(KafkaAdminClient.class);
        this.time = time;
        this.metadata = metadata;
        List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(
            config.getList(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
        this.metadata.update(Cluster.bootstrap(addresses), Collections.<String>emptySet(), time.milliseconds());
        this.metrics = metrics;
        this.client = client;
        this.runnable = new AdminClientRunnable();
        String threadName = "kafka-admin-client-thread | " + clientId;
        this.thread = new KafkaThread(threadName, runnable, true);
        this.timeoutProcessorFactory = (timeoutProcessorFactory == null) ?
            new TimeoutProcessorFactory() : timeoutProcessorFactory;
        this.maxRetries = config.getInt(AdminClientConfig.RETRIES_CONFIG);
        config.logUnused();
        AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics);
        log.debug("Kafka admin client initialized");
        thread.start();
    }

    Time time() {
        return time;
    }

    @Override
    public void close(long duration, TimeUnit unit) {
        long waitTimeMs = unit.toMillis(duration);
        waitTimeMs = Math.min(TimeUnit.DAYS.toMillis(365), waitTimeMs); // Limit the timeout to a year.
        long now = time.milliseconds();
        long newHardShutdownTimeMs = now + waitTimeMs;
        long prev = INVALID_SHUTDOWN_TIME;
        while (true) {
            if (hardShutdownTimeMs.compareAndSet(prev, newHardShutdownTimeMs)) {
                if (prev == INVALID_SHUTDOWN_TIME) {
                    log.debug("Initiating close operation.");
                } else {
                    log.debug("Moving hard shutdown time forward.");
                }
                client.wakeup(); // Wake the thread, if it is blocked inside poll().
                break;
            }
            prev = hardShutdownTimeMs.get();
            if (prev < newHardShutdownTimeMs) {
                log.debug("Hard shutdown time is already earlier than requested.");
                newHardShutdownTimeMs = prev;
                break;
            }
        }
        if (log.isDebugEnabled()) {
            long deltaMs = Math.max(0, newHardShutdownTimeMs - time.milliseconds());
            log.debug("Waiting for the I/O thread to exit. Hard shutdown in {} ms.", deltaMs);
        }
        try {
            // Wait for the thread to be joined.
            thread.join();

            AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);

            log.debug("Kafka admin client closed.");
        } catch (InterruptedException e) {
            log.debug("Interrupted while joining I/O thread", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * An interface for providing a node for a call.
     */
    private interface NodeProvider {
        Node provide();
    }

    private class ConstantNodeIdProvider implements NodeProvider {
        private final int nodeId;

        ConstantNodeIdProvider(int nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public Node provide() {
            return metadata.fetch().nodeById(nodeId);
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

    abstract class Call {
        private final String callName;
        private final long deadlineMs;
        private final NodeProvider nodeProvider;
        private int tries = 0;
        private boolean aborted = false;

        Call(String callName, long deadlineMs, NodeProvider nodeProvider) {
            this.callName = callName;
            this.deadlineMs = deadlineMs;
            this.nodeProvider = nodeProvider;
        }

        /**
         * Handle a failure.
         *
         * Depending on what the exception is and how many times we have already tried, we may choose to
         * fail the Call, or retry it. It is important to print the stack traces here in some cases,
         * since they are not necessarily preserved in ApiVersionException objects.
         *
         * @param now           The current time in milliseconds.
         * @param throwable     The failure exception.
         */
        final void fail(long now, Throwable throwable) {
            if (aborted) {
                // If the call was aborted while in flight due to a timeout, deliver a
                // TimeoutException. In this case, we do not get any more retries - the call has
                // failed. We increment tries anyway in order to display an accurate log message.
                tries++;
                if (log.isDebugEnabled()) {
                    log.debug("{} aborted at {} after {} attempt(s)", this, now, tries,
                        new Exception(prettyPrintException(throwable)));
                }
                handleFailure(new TimeoutException("Aborted due to timeout."));
                return;
            }
            // If this is an UnsupportedVersionException that we can retry, do so. Note that a
            // protocol downgrade will not count against the total number of retries we get for
            // this RPC. That is why 'tries' is not incremented.
            if ((throwable instanceof UnsupportedVersionException) &&
                     handleUnsupportedVersionException((UnsupportedVersionException) throwable)) {
                log.trace("{} attempting protocol downgrade.", this);
                runnable.enqueue(this, now);
                return;
            }
            tries++;
            // If the call has timed out, fail.
            if (calcTimeoutMsRemainingAsInt(now, deadlineMs) < 0) {
                if (log.isDebugEnabled()) {
                    log.debug("{} timed out at {} after {} attempt(s)", this, now, tries,
                        new Exception(prettyPrintException(throwable)));
                }
                handleFailure(throwable);
                return;
            }
            // If the exception is not retryable, fail.
            if (!(throwable instanceof RetriableException)) {
                if (log.isDebugEnabled()) {
                    log.debug("{} failed with non-retriable exception after {} attempt(s)", this, tries,
                        new Exception(prettyPrintException(throwable)));
                }
                handleFailure(throwable);
                return;
            }
            // If we are out of retries, fail.
            if (tries > maxRetries) {
                if (log.isDebugEnabled()) {
                    log.debug("{} failed after {} attempt(s)", this, tries,
                        new Exception(prettyPrintException(throwable)));
                }
                handleFailure(throwable);
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("{} failed: {}. Beginning retry #{}",
                    this, prettyPrintException(throwable), tries);
            }
            runnable.enqueue(this, now);
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
         */
        abstract void handleResponse(AbstractResponse abstractResponse);

        /**
         * Handle a failure. This will only be called if the failure exception was not
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

    static class TimeoutProcessorFactory {
        TimeoutProcessor create(long now) {
            return new TimeoutProcessor(now);
        }
    }

    static class TimeoutProcessor {
        /**
         * The current time in milliseconds.
         */
        private final long now;

        /**
         * The number of milliseconds until the next timeout.
         */
        private int nextTimeoutMs;

        /**
         * Create a new timeout processor.
         *
         * @param now           The current time in milliseconds since the epoch.
         */
        TimeoutProcessor(long now) {
            this.now = now;
            this.nextTimeoutMs = Integer.MAX_VALUE;
        }

        /**
         * Check for calls which have timed out.
         * Timed out calls will be removed and failed.
         * The remaining milliseconds until the next timeout will be updated.
         *
         * @param calls         The collection of calls.
         *
         * @return              The number of calls which were timed out.
         */
        int handleTimeouts(Collection<Call> calls, String msg) {
            int numTimedOut = 0;
            for (Iterator<Call> iter = calls.iterator(); iter.hasNext(); ) {
                Call call = iter.next();
                int remainingMs = calcTimeoutMsRemainingAsInt(now, call.deadlineMs);
                if (remainingMs < 0) {
                    call.fail(now, new TimeoutException(msg));
                    iter.remove();
                    numTimedOut++;
                } else {
                    nextTimeoutMs = Math.min(nextTimeoutMs, remainingMs);
                }
            }
            return numTimedOut;
        }

        /**
         * Check whether a call should be timed out.
         * The remaining milliseconds until the next timeout will be updated.
         *
         * @param call      The call.
         *
         * @return          True if the call should be timed out.
         */
        boolean callHasExpired(Call call) {
            int remainingMs = calcTimeoutMsRemainingAsInt(now, call.deadlineMs);
            if (remainingMs < 0)
                return true;
            nextTimeoutMs = Math.min(nextTimeoutMs, remainingMs);
            return false;
        }

        int nextTimeoutMs() {
            return nextTimeoutMs;
        }
    }

    private final class AdminClientRunnable implements Runnable {
        /**
         * Pending calls. Protected by the object monitor.
         * This will be null only if the thread has shut down.
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
                log.trace("Metadata is not ready yet. No cluster nodes found.");
                return metadata.requestUpdate();
            }
            if (cluster.controller() == null) {
                log.trace("Metadata is not ready yet. No controller found.");
                return metadata.requestUpdate();
            }
            if (prevMetadataVersion != null) {
                log.trace("Metadata is now ready.");
            }
            return null;
        }

        /**
         * Time out the elements in the newCalls list which are expired.
         *
         * @param processor     The timeout processor.
         */
        private synchronized void timeoutNewCalls(TimeoutProcessor processor) {
            int numTimedOut = processor.handleTimeouts(newCalls,
                    "Timed out waiting for a node assignment.");
            if (numTimedOut > 0)
                log.debug("Timed out {} new calls.", numTimedOut);
        }

        /**
         * Time out calls which have been assigned to nodes.
         *
         * @param processor     The timeout processor.
         * @param callsToSend   A map of nodes to the calls they need to handle.
         */
        private void timeoutCallsToSend(TimeoutProcessor processor, Map<Node, List<Call>> callsToSend) {
            int numTimedOut = 0;
            for (List<Call> callList : callsToSend.values()) {
                numTimedOut += processor.handleTimeouts(callList,
                    "Timed out waiting to send the call.");
            }
            if (numTimedOut > 0)
                log.debug("Timed out {} call(s) with assigned nodes.", numTimedOut);
        }

        /**
         * Choose nodes for the calls in the callsToSend list.
         *
         * This function holds the lock for the minimum amount of time, to avoid blocking
         * users of AdminClient who will also take the lock to add new calls.
         *
         * @param now           The current time in milliseconds.
         * @param callsToSend   A map of nodes to the calls they need to handle.
         *
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
         * @param callsToSend   A map of nodes to the calls they need to handle.
         * @param call          The call.
         */
        private void chooseNodeForNewCall(long now, Map<Node, List<Call>> callsToSend, Call call) {
            Node node = call.nodeProvider.provide();
            if (node == null) {
                call.fail(now, new BrokerNotAvailableException(
                    String.format("Error choosing node for %s: no node found.", call.callName)));
                return;
            }
            log.trace("Assigned {} to {}", call, node);
            getOrCreateListValue(callsToSend, node).add(call);
        }

        /**
         * Send the calls which are ready.
         *
         * @param now                   The current time in milliseconds.
         * @param callsToSend           The calls to send, by node.
         * @param correlationIdToCalls  A map of correlation IDs to calls.
         * @param callsInFlight         A map of nodes to the calls they have in flight.
         *
         * @return                      The minimum timeout we need for poll().
         */
        private long sendEligibleCalls(long now, Map<Node, List<Call>> callsToSend,
                         Map<Integer, Call> correlationIdToCalls, Map<String, List<Call>> callsInFlight) {
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
                    log.trace("Client is not ready to send to {}. Must delay {} ms", node, nodeTimeout);
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
                log.trace("Sending {} to {}. correlationId={}", requestBuilder, node, clientRequest.correlationId());
                client.send(clientRequest, now);
                getOrCreateListValue(callsInFlight, node.idString()).add(call);
                correlationIdToCalls.put(clientRequest.correlationId(), call);
            }
            return pollTimeout;
        }

        /**
         * Time out expired calls that are in flight.
         *
         * Calls that are in flight may have been partially or completely sent over the wire. They may
         * even be in the process of being processed by the remote server. At the moment, our only option
         * to time them out is to close the entire connection.
         *
         * @param processor         The timeout processor.
         * @param callsInFlight     A map of nodes to the calls they have in flight.
         */
        private void timeoutCallsInFlight(TimeoutProcessor processor, Map<String, List<Call>> callsInFlight) {
            int numTimedOut = 0;
            for (Map.Entry<String, List<Call>> entry : callsInFlight.entrySet()) {
                List<Call> contexts = entry.getValue();
                if (contexts.isEmpty())
                    continue;
                String nodeId = entry.getKey();
                // We assume that the first element in the list is the earliest. So it should be the
                // only one we need to check the timeout for.
                Call call = contexts.get(0);
                if (processor.callHasExpired(call)) {
                    if (call.aborted) {
                        log.warn("Aborted call {} is still in callsInFlight.", call);
                    } else {
                        log.debug("Closing connection to {} to time out {}", nodeId, call);
                        call.aborted = true;
                        client.disconnect(nodeId);
                        numTimedOut++;
                        // We don't remove anything from the callsInFlight data structure. Because the connection
                        // has been closed, the calls should be returned by the next client#poll(),
                        // and handled at that point.
                    }
                }
            }
            if (numTimedOut > 0)
                log.debug("Timed out {} call(s) in flight.", numTimedOut);
        }

        /**
         * If an authentication exception is encountered with connection to any broker,
         * fail all pending requests.
         */
        private void handleAuthenticationException(long now, Map<Node, List<Call>> callsToSend) {
            AuthenticationException authenticationException = metadata.getAndClearAuthenticationException();
            if (authenticationException == null) {
                for (Node node : callsToSend.keySet()) {
                    authenticationException = client.authenticationException(node);
                    if (authenticationException != null)
                        break;
                }
            }
            if (authenticationException != null) {
                synchronized (this) {
                    failCalls(now, newCalls, authenticationException);
                }
                for (List<Call> calls : callsToSend.values()) {
                    failCalls(now, calls, authenticationException);
                }
                callsToSend.clear();
            }
        }

        private void failCalls(long now, List<Call> calls, AuthenticationException authenticationException) {
            for (Call call : calls) {
                call.fail(now, authenticationException);
            }
            calls.clear();
        }

        /**
         * Handle responses from the server.
         *
         * @param now                   The current time in milliseconds.
         * @param responses             The latest responses from KafkaClient.
         * @param correlationIdToCall   A map of correlation IDs to calls.
         * @param callsInFlight         A map of nodes to the calls they have in flight.
        **/
        private void handleResponses(long now, List<ClientResponse> responses, Map<String, List<Call>> callsInFlight,
                Map<Integer, Call> correlationIdToCall) {
            for (ClientResponse response : responses) {
                int correlationId = response.requestHeader().correlationId();

                Call call = correlationIdToCall.get(correlationId);
                if (call == null) {
                    // If the server returns information about a correlation ID we didn't use yet,
                    // an internal server error has occurred. Close the connection and log an error message.
                    log.error("Internal server error on {}: server returned information about unknown " +
                        "correlation ID {}, requestHeader = {}", response.destination(), correlationId,
                        response.requestHeader());
                    client.disconnect(response.destination());
                    continue;
                }

                // Stop tracking this call.
                correlationIdToCall.remove(correlationId);
                List<Call> calls = callsInFlight.get(response.destination());
                if ((calls == null) || (!calls.remove(call))) {
                    log.error("Internal server error on {}: ignoring call {} in correlationIdToCall " +
                        "that did not exist in callsInFlight", response.destination(), call);
                    continue;
                }

                // Handle the result of the call. This may involve retrying the call, if we got a
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
                            log.trace("{} got response {}", call,
                                    response.responseBody().toString(response.requestHeader().apiVersion()));
                    } catch (Throwable t) {
                        if (log.isTraceEnabled())
                            log.trace("{} handleResponse failed with {}", call, prettyPrintException(t));
                        call.fail(now, t);
                    }
                }
            }
        }

        private synchronized boolean threadShouldExit(long now, long curHardShutdownTimeMs,
                Map<Node, List<Call>> callsToSend, Map<Integer, Call> correlationIdToCalls) {
            if (newCalls.isEmpty() && callsToSend.isEmpty() && correlationIdToCalls.isEmpty()) {
                log.trace("All work has been completed, and the I/O thread is now exiting.");
                return true;
            }
            if (now > curHardShutdownTimeMs) {
                log.info("Forcing a hard I/O thread shutdown. Requests in progress will be aborted.");
                return true;
            }
            log.debug("Hard shutdown in {} ms.", curHardShutdownTimeMs - now);
            return false;
        }

        @Override
        public void run() {
            /*
             * Maps nodes to calls that we want to send.
             */
            Map<Node, List<Call>> callsToSend = new HashMap<>();

            /*
             * Maps node ID strings to calls that have been sent.
             */
            Map<String, List<Call>> callsInFlight = new HashMap<>();

            /*
             * Maps correlation IDs to calls that have been sent.
             */
            Map<Integer, Call> correlationIdToCalls = new HashMap<>();

            /*
             * The previous metadata version which wasn't usable, or null if there is none.
             */
            Integer prevMetadataVersion = null;

            long now = time.milliseconds();
            log.trace("Thread starting");
            while (true) {
                // Check if the AdminClient thread should shut down.
                long curHardShutdownTimeMs = hardShutdownTimeMs.get();
                if ((curHardShutdownTimeMs != INVALID_SHUTDOWN_TIME) &&
                        threadShouldExit(now, curHardShutdownTimeMs, callsToSend, correlationIdToCalls))
                    break;

                // Handle timeouts.
                TimeoutProcessor timeoutProcessor = timeoutProcessorFactory.create(now);
                timeoutNewCalls(timeoutProcessor);
                timeoutCallsToSend(timeoutProcessor, callsToSend);
                timeoutCallsInFlight(timeoutProcessor, callsInFlight);

                long pollTimeout = Math.min(1200000, timeoutProcessor.nextTimeoutMs());
                if (curHardShutdownTimeMs != INVALID_SHUTDOWN_TIME) {
                    pollTimeout = Math.min(pollTimeout, curHardShutdownTimeMs - now);
                }

                // Handle new calls and metadata update requests.
                prevMetadataVersion = checkMetadataReady(prevMetadataVersion);
                if (prevMetadataVersion == null) {
                    chooseNodesForNewCalls(now, callsToSend);
                    pollTimeout = Math.min(pollTimeout,
                        sendEligibleCalls(now, callsToSend, correlationIdToCalls, callsInFlight));
                }

                // Wait for network responses.
                log.trace("Entering KafkaClient#poll(timeout={})", pollTimeout);
                List<ClientResponse> responses = client.poll(pollTimeout, now);
                log.trace("KafkaClient#poll retrieved {} response(s)", responses.size());

                // Update the current time and handle the latest responses.
                now = time.milliseconds();
                handleAuthenticationException(now, callsToSend);
                handleResponses(now, responses, callsInFlight, correlationIdToCalls);
            }
            int numTimedOut = 0;
            TimeoutProcessor timeoutProcessor = new TimeoutProcessor(Long.MAX_VALUE);
            synchronized (this) {
                numTimedOut += timeoutProcessor.handleTimeouts(newCalls,
                        "The AdminClient thread has exited.");
                newCalls = null;
            }
            numTimedOut += timeoutProcessor.handleTimeouts(correlationIdToCalls.values(),
                    "The AdminClient thread has exited.");
            if (numTimedOut > 0) {
                log.debug("Timed out {} remaining operations.", numTimedOut);
            }
            closeQuietly(client, "KafkaClient");
            closeQuietly(metrics, "Metrics");
            log.debug("Exiting AdminClientRunnable thread.");
        }

        /**
         * Queue a call for sending.
         *
         * If the AdminClient thread has exited, this will fail. Otherwise, it will succeed (even
         * if the AdminClient is shutting down). This function should called when retrying an
         * existing call.
         *
         * @param call      The new call object.
         * @param now       The current time in milliseconds.
         */
        void enqueue(Call call, long now) {
            if (log.isDebugEnabled()) {
                log.debug("Queueing {} with a timeout {} ms from now.", call, call.deadlineMs - now);
            }
            boolean accepted = false;
            synchronized (this) {
                if (newCalls != null) {
                    newCalls.add(call);
                    accepted = true;
                }
            }
            if (accepted) {
                client.wakeup(); // wake the thread if it is in poll()
            } else {
                log.debug("The AdminClient thread has exited. Timing out {}.", call);
                call.fail(Long.MAX_VALUE, new TimeoutException("The AdminClient thread has exited."));
            }
        }

        /**
         * Initiate a new call.
         *
         * This will fail if the AdminClient is scheduled to shut down.
         *
         * @param call      The new call object.
         * @param now       The current time in milliseconds.
         */
        void call(Call call, long now) {
            if (hardShutdownTimeMs.get() != INVALID_SHUTDOWN_TIME) {
                log.debug("The AdminClient is not accepting new calls. Timing out {}.", call);
                call.fail(Long.MAX_VALUE, new TimeoutException("The AdminClient thread is not accepting new calls."));
            } else {
                enqueue(call, now);
            }
        }
    }

    /**
     * Returns true if a topic name cannot be represented in an RPC.  This function does NOT check
     * whether the name is too long, contains invalid characters, etc.  It is better to enforce
     * those policies on the server, so that they can be changed in the future if needed.
     */
    private static boolean topicNameIsUnrepresentable(String topicName) {
        return (topicName == null) || topicName.isEmpty();
    }

    @Override
    public CreateTopicsResult createTopics(final Collection<NewTopic> newTopics,
                                           final CreateTopicsOptions options) {
        final Map<String, KafkaFutureImpl<Void>> topicFutures = new HashMap<>(newTopics.size());
        final Map<String, CreateTopicsRequest.TopicDetails> topicsMap = new HashMap<>(newTopics.size());
        for (NewTopic newTopic : newTopics) {
            if (topicNameIsUnrepresentable(newTopic.name())) {
                KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new InvalidTopicException("The given topic name '" +
                    newTopic.name() + "' cannot be represented in a request."));
                topicFutures.put(newTopic.name(), future);
            } else if (!topicFutures.containsKey(newTopic.name())) {
                topicFutures.put(newTopic.name(), new KafkaFutureImpl<Void>());
                topicsMap.put(newTopic.name(), newTopic.convertToTopicDetails());
            }
        }
        final long now = time.milliseconds();
        Call call = new Call("createTopics", calcDeadlineMs(now, options.timeoutMs()),
            new ControllerNodeProvider()) {

            @Override
            public AbstractRequest.Builder createRequest(int timeoutMs) {
                return new CreateTopicsRequest.Builder(topicsMap, timeoutMs, options.shouldValidateOnly());
            }

            @Override
            public void handleResponse(AbstractResponse abstractResponse) {
                CreateTopicsResponse response = (CreateTopicsResponse) abstractResponse;
                // Handle server responses for particular topics.
                for (Map.Entry<String, ApiError> entry : response.errors().entrySet()) {
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
                // The server should send back a response for every topic. But do a sanity check anyway.
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
        };
        if (!topicsMap.isEmpty()) {
            runnable.call(call, now);
        }
        return new CreateTopicsResult(new HashMap<String, KafkaFuture<Void>>(topicFutures));
    }

    @Override
    public DeleteTopicsResult deleteTopics(Collection<String> topicNames,
                                           DeleteTopicsOptions options) {
        final Map<String, KafkaFutureImpl<Void>> topicFutures = new HashMap<>(topicNames.size());
        final List<String> validTopicNames = new ArrayList<>(topicNames.size());
        for (String topicName : topicNames) {
            if (topicNameIsUnrepresentable(topicName)) {
                KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new InvalidTopicException("The given topic name '" +
                    topicName + "' cannot be represented in a request."));
                topicFutures.put(topicName, future);
            } else if (!topicFutures.containsKey(topicName)) {
                topicFutures.put(topicName, new KafkaFutureImpl<Void>());
                validTopicNames.add(topicName);
            }
        }
        final long now = time.milliseconds();
        Call call = new Call("deleteTopics", calcDeadlineMs(now, options.timeoutMs()),
            new ControllerNodeProvider()) {

            @Override
            AbstractRequest.Builder createRequest(int timeoutMs) {
                return new DeleteTopicsRequest.Builder(new HashSet<>(validTopicNames), timeoutMs);
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
                // The server should send back a response for every topic. But do a sanity check anyway.
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
        };
        if (!validTopicNames.isEmpty()) {
            runnable.call(call, now);
        }
        return new DeleteTopicsResult(new HashMap<String, KafkaFuture<Void>>(topicFutures));
    }

    @Override
    public ListTopicsResult listTopics(final ListTopicsOptions options) {
        final KafkaFutureImpl<Map<String, TopicListing>> topicListingFuture = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        runnable.call(new Call("listTopics", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

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
                    if (!internal || options.shouldListInternal())
                        topicListing.put(topicName, new TopicListing(topicName, internal));
                }
                topicListingFuture.complete(topicListing);
            }

            @Override
            void handleFailure(Throwable throwable) {
                topicListingFuture.completeExceptionally(throwable);
            }
        }, now);
        return new ListTopicsResult(topicListingFuture);
    }

    @Override
    public DescribeTopicsResult describeTopics(final Collection<String> topicNames, DescribeTopicsOptions options) {
        final Map<String, KafkaFutureImpl<TopicDescription>> topicFutures = new HashMap<>(topicNames.size());
        final ArrayList<String> topicNamesList = new ArrayList<>();
        for (String topicName : topicNames) {
            if (topicNameIsUnrepresentable(topicName)) {
                KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<TopicDescription>();
                future.completeExceptionally(new InvalidTopicException("The given topic name '" +
                    topicName + "' cannot be represented in a request."));
                topicFutures.put(topicName, future);
            } else if (!topicFutures.containsKey(topicName)) {
                topicFutures.put(topicName, new KafkaFutureImpl<TopicDescription>());
                topicNamesList.add(topicName);
            }
        }
        final long now = time.milliseconds();
        Call call = new Call("describeTopics", calcDeadlineMs(now, options.timeoutMs()),
            new ControllerNodeProvider()) {

            private boolean supportsDisablingTopicCreation = true;

            @Override
            AbstractRequest.Builder createRequest(int timeoutMs) {
                if (supportsDisablingTopicCreation)
                    return new MetadataRequest.Builder(topicNamesList, false);
                else
                    return MetadataRequest.Builder.allTopics();
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                MetadataResponse response = (MetadataResponse) abstractResponse;
                // Handle server responses for particular topics.
                Cluster cluster = response.cluster();
                Map<String, Errors> errors = response.errors();
                for (Map.Entry<String, KafkaFutureImpl<TopicDescription>> entry : topicFutures.entrySet()) {
                    String topicName = entry.getKey();
                    KafkaFutureImpl<TopicDescription> future = entry.getValue();
                    Errors topicError = errors.get(topicName);
                    if (topicError != null) {
                        future.completeExceptionally(topicError.exception());
                        continue;
                    }
                    if (!cluster.topics().contains(topicName)) {
                        future.completeExceptionally(new InvalidTopicException("Topic " + topicName + " not found."));
                        continue;
                    }
                    boolean isInternal = cluster.internalTopics().contains(topicName);
                    List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topicName);
                    List<TopicPartitionInfo> partitions = new ArrayList<>(partitionInfos.size());
                    for (PartitionInfo partitionInfo : partitionInfos) {
                        TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(
                            partitionInfo.partition(), leader(partitionInfo), Arrays.asList(partitionInfo.replicas()),
                            Arrays.asList(partitionInfo.inSyncReplicas()));
                        partitions.add(topicPartitionInfo);
                    }
                    Collections.sort(partitions, new Comparator<TopicPartitionInfo>() {
                        @Override
                        public int compare(TopicPartitionInfo tp1, TopicPartitionInfo tp2) {
                            return Integer.compare(tp1.partition(), tp2.partition());
                        }
                    });
                    TopicDescription topicDescription = new TopicDescription(topicName, isInternal, partitions);
                    future.complete(topicDescription);
                }
            }

            private Node leader(PartitionInfo partitionInfo) {
                if (partitionInfo.leader() == null || partitionInfo.leader().id() == Node.noNode().id())
                    return null;
                return partitionInfo.leader();
            }

            @Override
            boolean handleUnsupportedVersionException(UnsupportedVersionException exception) {
                if (supportsDisablingTopicCreation) {
                    supportsDisablingTopicCreation = false;
                    return true;
                }
                return false;
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(topicFutures.values(), throwable);
            }
        };
        if (!topicNamesList.isEmpty()) {
            runnable.call(call, now);
        }
        return new DescribeTopicsResult(new HashMap<String, KafkaFuture<TopicDescription>>(topicFutures));
    }

    @Override
    public DescribeClusterResult describeCluster(DescribeClusterOptions options) {
        final KafkaFutureImpl<Collection<Node>> describeClusterFuture = new KafkaFutureImpl<>();
        final KafkaFutureImpl<Node> controllerFuture = new KafkaFutureImpl<>();
        final KafkaFutureImpl<String> clusterIdFuture = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        runnable.call(new Call("listNodes", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            @Override
            AbstractRequest.Builder createRequest(int timeoutMs) {
                // Since this only requests node information, it's safe to pass true for allowAutoTopicCreation (and it
                // simplifies communication with older brokers)
                return new MetadataRequest.Builder(Collections.<String>emptyList(), true);
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                MetadataResponse response = (MetadataResponse) abstractResponse;
                describeClusterFuture.complete(response.brokers());
                controllerFuture.complete(controller(response));
                clusterIdFuture.complete(response.clusterId());
            }

            private Node controller(MetadataResponse response) {
                if (response.controller() == null || response.controller().id() == MetadataResponse.NO_CONTROLLER_ID)
                    return null;
                return response.controller();
            }

            @Override
            void handleFailure(Throwable throwable) {
                describeClusterFuture.completeExceptionally(throwable);
                controllerFuture.completeExceptionally(throwable);
                clusterIdFuture.completeExceptionally(throwable);
            }
        }, now);

        return new DescribeClusterResult(describeClusterFuture, controllerFuture, clusterIdFuture);
    }

    @Override
    public DescribeAclsResult describeAcls(final AclBindingFilter filter, DescribeAclsOptions options) {
        final long now = time.milliseconds();
        final KafkaFutureImpl<Collection<AclBinding>> future = new KafkaFutureImpl<>();
        runnable.call(new Call("describeAcls", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            @Override
            AbstractRequest.Builder createRequest(int timeoutMs) {
                return new DescribeAclsRequest.Builder(filter);
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                DescribeAclsResponse response = (DescribeAclsResponse) abstractResponse;
                if (response.error().isFailure()) {
                    future.completeExceptionally(response.error().exception());
                } else {
                    future.complete(response.acls());
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                future.completeExceptionally(throwable);
            }
        }, now);
        return new DescribeAclsResult(future);
    }

    @Override
    public CreateAclsResult createAcls(Collection<AclBinding> acls, CreateAclsOptions options) {
        final long now = time.milliseconds();
        final Map<AclBinding, KafkaFutureImpl<Void>> futures = new HashMap<>();
        final List<AclCreation> aclCreations = new ArrayList<>();
        for (AclBinding acl : acls) {
            if (futures.get(acl) == null) {
                KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
                futures.put(acl, future);
                String indefinite = acl.toFilter().findIndefiniteField();
                if (indefinite == null) {
                    aclCreations.add(new AclCreation(acl));
                } else {
                    future.completeExceptionally(new InvalidRequestException("Invalid ACL creation: " +
                        indefinite));
                }
            }
        }
        runnable.call(new Call("createAcls", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            @Override
            AbstractRequest.Builder createRequest(int timeoutMs) {
                return new CreateAclsRequest.Builder(aclCreations);
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                CreateAclsResponse response = (CreateAclsResponse) abstractResponse;
                List<AclCreationResponse> responses = response.aclCreationResponses();
                Iterator<AclCreationResponse> iter = responses.iterator();
                for (AclCreation aclCreation : aclCreations) {
                    KafkaFutureImpl<Void> future = futures.get(aclCreation.acl());
                    if (!iter.hasNext()) {
                        future.completeExceptionally(new UnknownServerException(
                            "The broker reported no creation result for the given ACL."));
                    } else {
                        AclCreationResponse creation = iter.next();
                        if (creation.error().isFailure()) {
                            future.completeExceptionally(creation.error().exception());
                        } else {
                            future.complete(null);
                        }
                    }
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(futures.values(), throwable);
            }
        }, now);
        return new CreateAclsResult(new HashMap<AclBinding, KafkaFuture<Void>>(futures));
    }

    @Override
    public DeleteAclsResult deleteAcls(Collection<AclBindingFilter> filters, DeleteAclsOptions options) {
        final long now = time.milliseconds();
        final Map<AclBindingFilter, KafkaFutureImpl<FilterResults>> futures = new HashMap<>();
        final List<AclBindingFilter> filterList = new ArrayList<>();
        for (AclBindingFilter filter : filters) {
            if (futures.get(filter) == null) {
                filterList.add(filter);
                futures.put(filter, new KafkaFutureImpl<FilterResults>());
            }
        }
        runnable.call(new Call("deleteAcls", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            @Override
            AbstractRequest.Builder createRequest(int timeoutMs) {
                return new DeleteAclsRequest.Builder(filterList);
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                DeleteAclsResponse response = (DeleteAclsResponse) abstractResponse;
                List<AclFilterResponse> responses = response.responses();
                Iterator<AclFilterResponse> iter = responses.iterator();
                for (AclBindingFilter filter : filterList) {
                    KafkaFutureImpl<FilterResults> future = futures.get(filter);
                    if (!iter.hasNext()) {
                        future.completeExceptionally(new UnknownServerException(
                            "The broker reported no deletion result for the given filter."));
                    } else {
                        AclFilterResponse deletion = iter.next();
                        if (deletion.error().isFailure()) {
                            future.completeExceptionally(deletion.error().exception());
                        } else {
                            List<FilterResult> filterResults = new ArrayList<>();
                            for (AclDeletionResult deletionResult : deletion.deletions()) {
                                filterResults.add(new FilterResult(deletionResult.acl(), deletionResult.error().exception()));
                            }
                            future.complete(new FilterResults(filterResults));
                        }
                    }
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(futures.values(), throwable);
            }
        }, now);
        return new DeleteAclsResult(new HashMap<AclBindingFilter, KafkaFuture<FilterResults>>(futures));
    }

    @Override
    public DescribeConfigsResult describeConfigs(Collection<ConfigResource> configResources, final DescribeConfigsOptions options) {
        final Map<ConfigResource, KafkaFutureImpl<Config>> unifiedRequestFutures = new HashMap<>();
        final Map<ConfigResource, KafkaFutureImpl<Config>> brokerFutures = new HashMap<>(configResources.size());

        // The BROKER resources which we want to describe.  We must make a separate DescribeConfigs
        // request for every BROKER resource we want to describe.
        final Collection<Resource> brokerResources = new ArrayList<>();

        // The non-BROKER resources which we want to describe.  These resources can be described by a
        // single, unified DescribeConfigs request.
        final Collection<Resource> unifiedRequestResources = new ArrayList<>(configResources.size());

        for (ConfigResource resource : configResources) {
            if (resource.type() == ConfigResource.Type.BROKER && !resource.isDefault()) {
                brokerFutures.put(resource, new KafkaFutureImpl<Config>());
                brokerResources.add(configResourceToResource(resource));
            } else {
                unifiedRequestFutures.put(resource, new KafkaFutureImpl<Config>());
                unifiedRequestResources.add(configResourceToResource(resource));
            }
        }

        final long now = time.milliseconds();
        if (!unifiedRequestResources.isEmpty()) {
            runnable.call(new Call("describeConfigs", calcDeadlineMs(now, options.timeoutMs()),
                new LeastLoadedNodeProvider()) {

                @Override
                AbstractRequest.Builder createRequest(int timeoutMs) {
                    return new DescribeConfigsRequest.Builder(unifiedRequestResources)
                            .includeSynonyms(options.includeSynonyms());
                }

                @Override
                void handleResponse(AbstractResponse abstractResponse) {
                    DescribeConfigsResponse response = (DescribeConfigsResponse) abstractResponse;
                    for (Map.Entry<ConfigResource, KafkaFutureImpl<Config>> entry : unifiedRequestFutures.entrySet()) {
                        ConfigResource configResource = entry.getKey();
                        KafkaFutureImpl<Config> future = entry.getValue();
                        DescribeConfigsResponse.Config config = response.config(configResourceToResource(configResource));
                        if (config == null) {
                            future.completeExceptionally(new UnknownServerException(
                                "Malformed broker response: missing config for " + configResource));
                            continue;
                        }
                        if (config.error().isFailure()) {
                            future.completeExceptionally(config.error().exception());
                            continue;
                        }
                        List<ConfigEntry> configEntries = new ArrayList<>();
                        for (DescribeConfigsResponse.ConfigEntry configEntry : config.entries()) {
                            configEntries.add(new ConfigEntry(configEntry.name(),
                                    configEntry.value(), configSource(configEntry.source()),
                                    configEntry.isSensitive(), configEntry.isReadOnly(),
                                    configSynonyms(configEntry)));
                        }
                        future.complete(new Config(configEntries));
                    }
                }

                @Override
                void handleFailure(Throwable throwable) {
                    completeAllExceptionally(unifiedRequestFutures.values(), throwable);
                }
            }, now);
        }

        for (Map.Entry<ConfigResource, KafkaFutureImpl<Config>> entry : brokerFutures.entrySet()) {
            final KafkaFutureImpl<Config> brokerFuture = entry.getValue();
            final Resource resource = configResourceToResource(entry.getKey());
            final int nodeId = Integer.parseInt(resource.name());
            runnable.call(new Call("describeBrokerConfigs", calcDeadlineMs(now, options.timeoutMs()),
                    new ConstantNodeIdProvider(nodeId)) {

                @Override
                AbstractRequest.Builder createRequest(int timeoutMs) {
                    return new DescribeConfigsRequest.Builder(Collections.singleton(resource))
                            .includeSynonyms(options.includeSynonyms());
                }

                @Override
                void handleResponse(AbstractResponse abstractResponse) {
                    DescribeConfigsResponse response = (DescribeConfigsResponse) abstractResponse;
                    DescribeConfigsResponse.Config config = response.configs().get(resource);

                    if (config == null) {
                        brokerFuture.completeExceptionally(new UnknownServerException(
                            "Malformed broker response: missing config for " + resource));
                        return;
                    }
                    if (config.error().isFailure())
                        brokerFuture.completeExceptionally(config.error().exception());
                    else {
                        List<ConfigEntry> configEntries = new ArrayList<>();
                        for (DescribeConfigsResponse.ConfigEntry configEntry : config.entries()) {
                            configEntries.add(new ConfigEntry(configEntry.name(), configEntry.value(),
                                configSource(configEntry.source()), configEntry.isSensitive(), configEntry.isReadOnly(),
                                configSynonyms(configEntry)));
                        }
                        brokerFuture.complete(new Config(configEntries));
                    }
                }

                @Override
                void handleFailure(Throwable throwable) {
                    brokerFuture.completeExceptionally(throwable);
                }
            }, now);
        }
        final Map<ConfigResource, KafkaFuture<Config>> allFutures = new HashMap<>();
        allFutures.putAll(brokerFutures);
        allFutures.putAll(unifiedRequestFutures);
        return new DescribeConfigsResult(allFutures);
    }

    private Resource configResourceToResource(ConfigResource configResource) {
        ResourceType resourceType;
        switch (configResource.type()) {
            case TOPIC:
                resourceType = ResourceType.TOPIC;
                break;
            case BROKER:
                resourceType = ResourceType.BROKER;
                break;
            default:
                throw new IllegalArgumentException("Unexpected resource type " + configResource.type());
        }
        return new Resource(resourceType, configResource.name());
    }

    private List<ConfigEntry.ConfigSynonym> configSynonyms(DescribeConfigsResponse.ConfigEntry configEntry) {
        List<ConfigEntry.ConfigSynonym> synonyms = new ArrayList<>(configEntry.synonyms().size());
        for (DescribeConfigsResponse.ConfigSynonym synonym : configEntry.synonyms()) {
            synonyms.add(new ConfigEntry.ConfigSynonym(synonym.name(), synonym.value(), configSource(synonym.source())));
        }
        return synonyms;
    }

    private ConfigEntry.ConfigSource configSource(DescribeConfigsResponse.ConfigSource source) {
        ConfigEntry.ConfigSource configSource;
        switch (source) {
            case TOPIC_CONFIG:
                configSource = ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG;
                break;
            case DYNAMIC_BROKER_CONFIG:
                configSource = ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG;
                break;
            case DYNAMIC_DEFAULT_BROKER_CONFIG:
                configSource = ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG;
                break;
            case STATIC_BROKER_CONFIG:
                configSource = ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG;
                break;
            case DEFAULT_CONFIG:
                configSource = ConfigEntry.ConfigSource.DEFAULT_CONFIG;
                break;
            default:
                throw new IllegalArgumentException("Unexpected config source " + source);
        }
        return configSource;
    }

    @Override
    public AlterConfigsResult alterConfigs(Map<ConfigResource, Config> configs, final AlterConfigsOptions options) {
        final Map<ConfigResource, KafkaFutureImpl<Void>> allFutures = new HashMap<>();
        // We must make a separate AlterConfigs request for every BROKER resource we want to alter
        // and send the request to that specific broker. Other resources are grouped together into
        // a single request that may be sent to any broker.
        final Collection<ConfigResource> unifiedRequestResources = new ArrayList<>();

        for (ConfigResource resource : configs.keySet()) {
            if (resource.type() == ConfigResource.Type.BROKER && !resource.isDefault()) {
                NodeProvider nodeProvider = new ConstantNodeIdProvider(Integer.parseInt(resource.name()));
                allFutures.putAll(alterConfigs(configs, options, Collections.singleton(resource), nodeProvider));
            } else
                unifiedRequestResources.add(resource);
        }
        if (!unifiedRequestResources.isEmpty())
          allFutures.putAll(alterConfigs(configs, options, unifiedRequestResources, new LeastLoadedNodeProvider()));
        return new AlterConfigsResult(new HashMap<ConfigResource, KafkaFuture<Void>>(allFutures));
    }

    private Map<ConfigResource, KafkaFutureImpl<Void>> alterConfigs(Map<ConfigResource, Config> configs,
                                                                    final AlterConfigsOptions options,
                                                                    Collection<ConfigResource> resources,
                                                                    NodeProvider nodeProvider) {
        final Map<ConfigResource, KafkaFutureImpl<Void>> futures = new HashMap<>();
        final Map<Resource, AlterConfigsRequest.Config> requestMap = new HashMap<>(resources.size());
        for (ConfigResource resource : resources) {
            List<AlterConfigsRequest.ConfigEntry> configEntries = new ArrayList<>();
            for (ConfigEntry configEntry: configs.get(resource).entries())
                configEntries.add(new AlterConfigsRequest.ConfigEntry(configEntry.name(), configEntry.value()));
            requestMap.put(configResourceToResource(resource), new AlterConfigsRequest.Config(configEntries));
            futures.put(resource, new KafkaFutureImpl<Void>());
        }

        final long now = time.milliseconds();
        runnable.call(new Call("alterConfigs", calcDeadlineMs(now, options.timeoutMs()), nodeProvider) {

            @Override
            public AbstractRequest.Builder createRequest(int timeoutMs) {
                return new AlterConfigsRequest.Builder(requestMap, options.shouldValidateOnly());
            }

            @Override
            public void handleResponse(AbstractResponse abstractResponse) {
                AlterConfigsResponse response = (AlterConfigsResponse) abstractResponse;
                for (Map.Entry<ConfigResource, KafkaFutureImpl<Void>> entry : futures.entrySet()) {
                    KafkaFutureImpl<Void> future = entry.getValue();
                    ApiException exception = response.errors().get(configResourceToResource(entry.getKey())).exception();
                    if (exception != null) {
                        future.completeExceptionally(exception);
                    } else {
                        future.complete(null);
                    }
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(futures.values(), throwable);
            }
        }, now);
        return futures;
    }

    @Override
    public AlterReplicaLogDirsResult alterReplicaLogDirs(Map<TopicPartitionReplica, String> replicaAssignment, final AlterReplicaLogDirsOptions options) {
        final Map<TopicPartitionReplica, KafkaFutureImpl<Void>> futures = new HashMap<>(replicaAssignment.size());

        for (TopicPartitionReplica replica : replicaAssignment.keySet())
            futures.put(replica, new KafkaFutureImpl<Void>());

        Map<Integer, Map<TopicPartition, String>> replicaAssignmentByBroker = new HashMap<>();
        for (Map.Entry<TopicPartitionReplica, String> entry: replicaAssignment.entrySet()) {
            TopicPartitionReplica replica = entry.getKey();
            String logDir = entry.getValue();
            int brokerId = replica.brokerId();
            TopicPartition topicPartition = new TopicPartition(replica.topic(), replica.partition());
            if (!replicaAssignmentByBroker.containsKey(brokerId))
                replicaAssignmentByBroker.put(brokerId, new HashMap<TopicPartition, String>());
            replicaAssignmentByBroker.get(brokerId).put(topicPartition, logDir);
        }

        final long now = time.milliseconds();
        for (Map.Entry<Integer, Map<TopicPartition, String>> entry: replicaAssignmentByBroker.entrySet()) {
            final int brokerId = entry.getKey();
            final Map<TopicPartition, String> assignment = entry.getValue();

            runnable.call(new Call("alterReplicaLogDirs", calcDeadlineMs(now, options.timeoutMs()),
                new ConstantNodeIdProvider(brokerId)) {

                @Override
                public AbstractRequest.Builder createRequest(int timeoutMs) {
                    return new AlterReplicaLogDirsRequest.Builder(assignment);
                }

                @Override
                public void handleResponse(AbstractResponse abstractResponse) {
                    AlterReplicaLogDirsResponse response = (AlterReplicaLogDirsResponse) abstractResponse;
                    for (Map.Entry<TopicPartition, Errors> responseEntry: response.responses().entrySet()) {
                        TopicPartition tp = responseEntry.getKey();
                        Errors error = responseEntry.getValue();
                        TopicPartitionReplica replica = new TopicPartitionReplica(tp.topic(), tp.partition(), brokerId);
                        KafkaFutureImpl<Void> future = futures.get(replica);
                        if (future == null) {
                            handleFailure(new IllegalStateException(
                                "The partition " + tp + " in the response from broker " + brokerId + " is not in the request"));
                        } else if (error == Errors.NONE) {
                            future.complete(null);
                        } else {
                            future.completeExceptionally(error.exception());
                        }
                    }
                }
                @Override
                void handleFailure(Throwable throwable) {
                    completeAllExceptionally(futures.values(), throwable);
                }
            }, now);
        }

        return new AlterReplicaLogDirsResult(new HashMap<TopicPartitionReplica, KafkaFuture<Void>>(futures));
    }

    @Override
    public DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers, DescribeLogDirsOptions options) {
        final Map<Integer, KafkaFutureImpl<Map<String, DescribeLogDirsResponse.LogDirInfo>>> futures = new HashMap<>(brokers.size());

        for (Integer brokerId: brokers) {
            futures.put(brokerId, new KafkaFutureImpl<Map<String, DescribeLogDirsResponse.LogDirInfo>>());
        }

        final long now = time.milliseconds();
        for (final Integer brokerId: brokers) {
            runnable.call(new Call("describeLogDirs", calcDeadlineMs(now, options.timeoutMs()),
                new ConstantNodeIdProvider(brokerId)) {

                @Override
                public AbstractRequest.Builder createRequest(int timeoutMs) {
                    // Query selected partitions in all log directories
                    return new DescribeLogDirsRequest.Builder(null);
                }

                @Override
                public void handleResponse(AbstractResponse abstractResponse) {
                    DescribeLogDirsResponse response = (DescribeLogDirsResponse) abstractResponse;
                    KafkaFutureImpl<Map<String, DescribeLogDirsResponse.LogDirInfo>> future = futures.get(brokerId);
                    if (response.logDirInfos().size() > 0) {
                        future.complete(response.logDirInfos());
                    } else {
                        // response.logDirInfos() will be empty if and only if the user is not authorized to describe clsuter resource.
                        future.completeExceptionally(Errors.CLUSTER_AUTHORIZATION_FAILED.exception());
                    }
                }
                @Override
                void handleFailure(Throwable throwable) {
                    completeAllExceptionally(futures.values(), throwable);
                }
            }, now);
        }

        return new DescribeLogDirsResult(new HashMap<Integer, KafkaFuture<Map<String, DescribeLogDirsResponse.LogDirInfo>>>(futures));
    }

    @Override
    public DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> replicas, DescribeReplicaLogDirsOptions options) {
        final Map<TopicPartitionReplica, KafkaFutureImpl<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> futures = new HashMap<>(replicas.size());

        for (TopicPartitionReplica replica : replicas) {
            futures.put(replica, new KafkaFutureImpl<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>());
        }

        Map<Integer, Set<TopicPartition>> partitionsByBroker = new HashMap<>();

        for (TopicPartitionReplica replica: replicas) {
            if (!partitionsByBroker.containsKey(replica.brokerId()))
                partitionsByBroker.put(replica.brokerId(), new HashSet<TopicPartition>());
            partitionsByBroker.get(replica.brokerId()).add(new TopicPartition(replica.topic(), replica.partition()));
        }

        final long now = time.milliseconds();
        for (Map.Entry<Integer, Set<TopicPartition>> entry: partitionsByBroker.entrySet()) {
            final int brokerId = entry.getKey();
            final Set<TopicPartition> topicPartitions = entry.getValue();
            final Map<TopicPartition, ReplicaLogDirInfo> replicaDirInfoByPartition = new HashMap<>();
            for (TopicPartition topicPartition: topicPartitions)
                replicaDirInfoByPartition.put(topicPartition, new ReplicaLogDirInfo());

            runnable.call(new Call("describeReplicaLogDirs", calcDeadlineMs(now, options.timeoutMs()),
                new ConstantNodeIdProvider(brokerId)) {

                @Override
                public AbstractRequest.Builder createRequest(int timeoutMs) {
                    // Query selected partitions in all log directories
                    return new DescribeLogDirsRequest.Builder(topicPartitions);
                }

                @Override
                public void handleResponse(AbstractResponse abstractResponse) {
                    DescribeLogDirsResponse response = (DescribeLogDirsResponse) abstractResponse;
                    for (Map.Entry<String, DescribeLogDirsResponse.LogDirInfo> responseEntry: response.logDirInfos().entrySet()) {
                        String logDir = responseEntry.getKey();
                        DescribeLogDirsResponse.LogDirInfo logDirInfo = responseEntry.getValue();

                        // No replica info will be provided if the log directory is offline
                        if (logDirInfo.error == Errors.KAFKA_STORAGE_ERROR)
                            continue;
                        if (logDirInfo.error != Errors.NONE)
                            handleFailure(new IllegalStateException(
                                "The error " + logDirInfo.error + " for log directory " + logDir + " in the response from broker " + brokerId + " is illegal"));

                        for (Map.Entry<TopicPartition, DescribeLogDirsResponse.ReplicaInfo> replicaInfoEntry: logDirInfo.replicaInfos.entrySet()) {
                            TopicPartition tp = replicaInfoEntry.getKey();
                            DescribeLogDirsResponse.ReplicaInfo replicaInfo = replicaInfoEntry.getValue();
                            ReplicaLogDirInfo replicaLogDirInfo = replicaDirInfoByPartition.get(tp);
                            if (replicaLogDirInfo == null) {
                                handleFailure(new IllegalStateException(
                                    "The partition " + tp + " in the response from broker " + brokerId + " is not in the request"));
                            } else if (replicaInfo.isFuture) {
                                replicaDirInfoByPartition.put(tp, new ReplicaLogDirInfo(replicaLogDirInfo.getCurrentReplicaLogDir(),
                                                                                        replicaLogDirInfo.getCurrentReplicaOffsetLag(),
                                                                                        logDir,
                                                                                        replicaInfo.offsetLag));
                            } else {
                                replicaDirInfoByPartition.put(tp, new ReplicaLogDirInfo(logDir,
                                                                                        replicaInfo.offsetLag,
                                                                                        replicaLogDirInfo.getFutureReplicaLogDir(),
                                                                                        replicaLogDirInfo.getFutureReplicaOffsetLag()));
                            }
                        }
                    }

                    for (Map.Entry<TopicPartition, ReplicaLogDirInfo> entry: replicaDirInfoByPartition.entrySet()) {
                        TopicPartition tp = entry.getKey();
                        KafkaFutureImpl<ReplicaLogDirInfo> future = futures.get(new TopicPartitionReplica(tp.topic(), tp.partition(), brokerId));
                        future.complete(entry.getValue());
                    }
                }
                @Override
                void handleFailure(Throwable throwable) {
                    completeAllExceptionally(futures.values(), throwable);
                }
            }, now);
        }

        return new DescribeReplicaLogDirsResult(new HashMap<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>>(futures));
    }

    public CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions,
                                                   final CreatePartitionsOptions options) {
        final Map<String, KafkaFutureImpl<Void>> futures = new HashMap<>(newPartitions.size());
        for (String topic : newPartitions.keySet()) {
            futures.put(topic, new KafkaFutureImpl<Void>());
        }
        final Map<String, NewPartitions> requestMap = new HashMap<>(newPartitions);

        final long now = time.milliseconds();
        runnable.call(new Call("createPartitions", calcDeadlineMs(now, options.timeoutMs()),
                new ControllerNodeProvider()) {

            @Override
            public AbstractRequest.Builder createRequest(int timeoutMs) {
                return new CreatePartitionsRequest.Builder(requestMap, timeoutMs, options.validateOnly());
            }

            @Override
            public void handleResponse(AbstractResponse abstractResponse) {
                CreatePartitionsResponse response = (CreatePartitionsResponse) abstractResponse;
                for (Map.Entry<String, ApiError> result : response.errors().entrySet()) {
                    KafkaFutureImpl<Void> future = futures.get(result.getKey());
                    if (result.getValue().isSuccess()) {
                        future.complete(null);
                    } else {
                        future.completeExceptionally(result.getValue().exception());
                    }
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(futures.values(), throwable);
            }
        }, now);
        return new CreatePartitionsResult(new HashMap<String, KafkaFuture<Void>>(futures));
    }

    public DeleteRecordsResult deleteRecords(final Map<TopicPartition, RecordsToDelete> recordsToDelete,
                                             final DeleteRecordsOptions options) {

        // requests need to be sent to partitions leader nodes so ...
        // ... from the provided map it's needed to create more maps grouping topic/partition per leader

        final Map<TopicPartition, KafkaFutureImpl<DeletedRecords>> futures = new HashMap<>(recordsToDelete.size());
        for (TopicPartition topicPartition: recordsToDelete.keySet()) {
            futures.put(topicPartition, new KafkaFutureImpl<DeletedRecords>());
        }

        // preparing topics list for asking metadata about them
        final Set<String> topics = new HashSet<>();
        for (TopicPartition topicPartition: recordsToDelete.keySet()) {
            topics.add(topicPartition.topic());
        }

        final long nowMetadata = time.milliseconds();
        final long deadline = calcDeadlineMs(nowMetadata, options.timeoutMs());
        // asking for topics metadata for getting partitions leaders
        runnable.call(new Call("topicsMetadata", deadline,
                new LeastLoadedNodeProvider()) {

            @Override
            AbstractRequest.Builder createRequest(int timeoutMs) {
                return new MetadataRequest.Builder(new ArrayList<>(topics), false);
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                MetadataResponse response = (MetadataResponse) abstractResponse;

                Map<String, Errors> errors = response.errors();
                Cluster cluster = response.cluster();

                // completing futures for topics with errors
                for (Map.Entry<String, Errors> topicError: errors.entrySet()) {

                    for (Map.Entry<TopicPartition, KafkaFutureImpl<DeletedRecords>> future: futures.entrySet()) {
                        if (future.getKey().topic().equals(topicError.getKey())) {
                            future.getValue().completeExceptionally(topicError.getValue().exception());
                        }
                    }
                }

                // grouping topic partitions per leader
                Map<Node, Map<TopicPartition, Long>> leaders = new HashMap<>();
                for (Map.Entry<TopicPartition, RecordsToDelete> entry: recordsToDelete.entrySet()) {

                    // avoiding to send deletion request for topics with errors
                    if (!errors.containsKey(entry.getKey().topic())) {

                        Node node = cluster.leaderFor(entry.getKey());
                        if (node != null) {
                            if (!leaders.containsKey(node))
                                leaders.put(node, new HashMap<TopicPartition, Long>());
                            leaders.get(node).put(entry.getKey(), entry.getValue().beforeOffset());
                        } else {
                            KafkaFutureImpl<DeletedRecords> future = futures.get(entry.getKey());
                            future.completeExceptionally(Errors.LEADER_NOT_AVAILABLE.exception());
                        }
                    }
                }

                for (final Map.Entry<Node, Map<TopicPartition, Long>> entry: leaders.entrySet()) {

                    final long nowDelete = time.milliseconds();

                    final int brokerId = entry.getKey().id();

                    runnable.call(new Call("deleteRecords", deadline,
                            new ConstantNodeIdProvider(brokerId)) {

                        @Override
                        AbstractRequest.Builder createRequest(int timeoutMs) {
                            return new DeleteRecordsRequest.Builder(timeoutMs, entry.getValue());
                        }

                        @Override
                        void handleResponse(AbstractResponse abstractResponse) {
                            DeleteRecordsResponse response = (DeleteRecordsResponse) abstractResponse;
                            for (Map.Entry<TopicPartition, DeleteRecordsResponse.PartitionResponse> result: response.responses().entrySet()) {

                                KafkaFutureImpl<DeletedRecords> future = futures.get(result.getKey());
                                if (result.getValue().error == Errors.NONE) {
                                    future.complete(new DeletedRecords(result.getValue().lowWatermark));
                                } else {
                                    future.completeExceptionally(result.getValue().error.exception());
                                }
                            }
                        }

                        @Override
                        void handleFailure(Throwable throwable) {
                            completeAllExceptionally(futures.values(), throwable);
                        }
                    }, nowDelete);
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(futures.values(), throwable);
            }
        }, nowMetadata);

        return new DeleteRecordsResult(new HashMap<TopicPartition, KafkaFuture<DeletedRecords>>(futures));
    }

    @Override
    public CreateDelegationTokenResult createDelegationToken(final CreateDelegationTokenOptions options) {
        final KafkaFutureImpl<DelegationToken> delegationTokenFuture = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        runnable.call(new Call("createDelegationToken", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            @Override
            AbstractRequest.Builder createRequest(int timeoutMs) {
                return new CreateDelegationTokenRequest.Builder(options.renewers(), options.maxlifeTimeMs());
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                CreateDelegationTokenResponse response = (CreateDelegationTokenResponse) abstractResponse;
                if (response.hasError()) {
                    delegationTokenFuture.completeExceptionally(response.error().exception());
                } else {
                    TokenInformation tokenInfo =  new TokenInformation(response.tokenId(), response.owner(),
                        options.renewers(), response.issueTimestamp(), response.maxTimestamp(), response.expiryTimestamp());
                    DelegationToken token = new DelegationToken(tokenInfo, response.hmacBytes());
                    delegationTokenFuture.complete(token);
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                delegationTokenFuture.completeExceptionally(throwable);
            }
        }, now);

        return new CreateDelegationTokenResult(delegationTokenFuture);
    }

    @Override
    public RenewDelegationTokenResult renewDelegationToken(final byte[] hmac, final RenewDelegationTokenOptions options) {
        final KafkaFutureImpl<Long>  expiryTimeFuture = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        runnable.call(new Call("renewDelegationToken", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            @Override
            AbstractRequest.Builder createRequest(int timeoutMs) {
                return new RenewDelegationTokenRequest.Builder(hmac, options.renewTimePeriodMs());
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                RenewDelegationTokenResponse response = (RenewDelegationTokenResponse) abstractResponse;
                if (response.hasError()) {
                    expiryTimeFuture.completeExceptionally(response.error().exception());
                } else {
                    expiryTimeFuture.complete(response.expiryTimestamp());
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                expiryTimeFuture.completeExceptionally(throwable);
            }
        }, now);

        return new RenewDelegationTokenResult(expiryTimeFuture);
    }

    @Override
    public ExpireDelegationTokenResult expireDelegationToken(final byte[] hmac, final ExpireDelegationTokenOptions options) {
        final KafkaFutureImpl<Long>  expiryTimeFuture = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        runnable.call(new Call("expireDelegationToken", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            @Override
            AbstractRequest.Builder createRequest(int timeoutMs) {
                return new ExpireDelegationTokenRequest.Builder(hmac, options.expiryTimePeriodMs());
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                ExpireDelegationTokenResponse response = (ExpireDelegationTokenResponse) abstractResponse;
                if (response.hasError()) {
                    expiryTimeFuture.completeExceptionally(response.error().exception());
                } else {
                    expiryTimeFuture.complete(response.expiryTimestamp());
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                expiryTimeFuture.completeExceptionally(throwable);
            }
        }, now);

        return new ExpireDelegationTokenResult(expiryTimeFuture);
    }

    @Override
    public DescribeDelegationTokenResult describeDelegationToken(final DescribeDelegationTokenOptions options) {
        final KafkaFutureImpl<List<DelegationToken>>  tokensFuture = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        runnable.call(new Call("describeDelegationToken", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            @Override
            AbstractRequest.Builder createRequest(int timeoutMs) {
                return new DescribeDelegationTokenRequest.Builder(options.owners());
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                DescribeDelegationTokenResponse response = (DescribeDelegationTokenResponse) abstractResponse;
                if (response.hasError()) {
                    tokensFuture.completeExceptionally(response.error().exception());
                } else {
                    tokensFuture.complete(response.tokens());
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                tokensFuture.completeExceptionally(throwable);
            }
        }, now);

        return new DescribeDelegationTokenResult(tokensFuture);
    }
}
