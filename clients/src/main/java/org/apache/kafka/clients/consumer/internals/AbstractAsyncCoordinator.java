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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.io.Closeable;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * AbstractAsyncCoordinator implements group management for a single group member by interacting with
 * the coordinator. Group semantics are provided by extending this class.
 * See {@link DefaultAsyncCoordinator} for example usage.
 * */
public class AbstractAsyncCoordinator implements Closeable {
    private final Logger log;
    private final Time time;
    protected final ConsumerNetworkClient networkClient;
    protected final GroupRebalanceConfig rebalanceConfig;
    private final GroupCoordinatorMetrics sensors;
    private Node coordinator;
    private RequestFuture<Void> findCoordinatorFuture;
    private RuntimeException fatalFindCoordinatorException;
    protected Generation generation;
    private long lastRebalanceEndMs = -1L;
    private long lastTimeOfConnectionMs = -1L;

    public AbstractAsyncCoordinator(
            final Time time,
            final LogContext logContext,
            final GroupRebalanceConfig rebalanceConfig,
            final ConsumerNetworkClient networkClient,
            final Metrics metrics,
            final String metricGrpPrefix) {
        Objects.requireNonNull(rebalanceConfig.groupId,
                "Expected a non-null group id for coordinator construction");
        this.log = logContext.logger(this.getClass());
        this.time = time;
        this.rebalanceConfig = rebalanceConfig;
        this.networkClient = networkClient;
        this.sensors = new GroupCoordinatorMetrics(metrics, metricGrpPrefix);
        this.generation = Generation.NO_GENERATION;
    }

    /**
     * Check if the coordinator has been discovered and we maintain an active connection
     * @return true if the coordinator is unknown
     */
    public boolean coordinatorUnknown() {
        return checkAndGetCoordinator() == null;
    }

    /**
     * Get the coordinator if its connection is still active. Otherwise mark it unknown and
     * return null.
     *
     * @return the current coordinator or null if it is unknown
     */
    protected Node checkAndGetCoordinator() {
        if (coordinator != null && networkClient.isUnavailable(coordinator)) {
            markCoordinatorUnknown(true, "coordinator unavailable");
            return null;
        }
        return this.coordinator;
    }

    protected void markCoordinatorUnknown(Errors error) {
        markCoordinatorUnknown(false, "error response " + error.name());
    }

    protected void markCoordinatorUnknown(String cause) {
        markCoordinatorUnknown(false, cause);
    }

    protected void markCoordinatorUnknown(boolean isDisconnected, String cause) {
        if (this.coordinator == null) {
            // coordinator is already disconnected
            long durationOfOngoingDisconnect = time.milliseconds() - lastTimeOfConnectionMs;
            if (durationOfOngoingDisconnect > rebalanceConfig.rebalanceTimeoutMs)
                log.warn("Consumer has been disconnected from the group coordinator for {}ms", durationOfOngoingDisconnect);
            return;
        }

        log.info("Group coordinator {} is unavailable or invalid due to cause: {}. "
                        + "isDisconnected: {}. Rediscovery will be attempted.", this.coordinator,
                cause, isDisconnected);
        Node oldCoordinator = this.coordinator;

        // Mark the coordinator dead before disconnecting requests since the callbacks for any pending
        // requests may attempt to do likewise. This also prevents new requests from being sent to the
        // coordinator while the disconnect is in progress.
        this.coordinator = null;

        // Disconnect from the coordinator to ensure that there are no in-flight requests remaining.
        // Pending callbacks will be invoked with a DisconnectException on the next call to poll.
        if (!isDisconnected) {
            log.info("Requesting disconnect from last known coordinator {}", oldCoordinator);
            networkClient.disconnectAsync(oldCoordinator);
        }

        lastTimeOfConnectionMs = time.milliseconds();
    }

    /**
     * Ensure that the coordinator is ready to receive requests. This will return
     * immediately without blocking. It is intended to be called in an asynchronous
     * context when wakeups are not expected.
     *
     * @return true If coordinator discovery and initial connection succeeded, false otherwise
     */
    protected boolean ensureCoordinatorReadyAsync() {
        return ensureCoordinatorReady(time.timer(0), true);
    }

    /**
     * Ensure that the coordinator is ready to receive requests.
     *
     * @param timer Timer bounding how long this method can block
     * @return true If coordinator discovery and initial connection succeeded, false otherwise
     */
    protected boolean ensureCoordinatorReady(final Timer timer) {
        return ensureCoordinatorReady(timer, false);
    }

    private boolean ensureCoordinatorReady(final Timer timer, boolean disableWakeup) {
        if (!coordinatorUnknown())
            return true;

        do {
            if (fatalFindCoordinatorException != null) {
                final RuntimeException fatalException = fatalFindCoordinatorException;
                fatalFindCoordinatorException = null;
                throw fatalException;
            }
            final RequestFuture<Void> future = lookupCoordinator();
            networkClient.poll(future, timer, disableWakeup);

            if (!future.isDone()) {
                // ran out of time
                break;
            }

            RuntimeException fatalException = null;

            if (future.failed()) {
                if (future.isRetriable()) {
                    log.debug("Coordinator discovery failed, refreshing metadata", future.exception());
                    networkClient.awaitMetadataUpdate(timer);
                } else {
                    fatalException = future.exception();
                    log.info("FindCoordinator request hit fatal exception", fatalException);
                }
            } else if (coordinator != null && networkClient.isUnavailable(coordinator)) {
                // we found the coordinator, but the connection has failed, so mark
                // it dead and backoff before retrying discovery
                markCoordinatorUnknown("coordinator unavailable");
                timer.sleep(rebalanceConfig.retryBackoffMs);
            }

            clearFindCoordinatorFuture();
            if (fatalException != null)
                throw fatalException;
        } while (coordinatorUnknown() && timer.notExpired());

        return !coordinatorUnknown();
    }

    private void clearFindCoordinatorFuture() {
        findCoordinatorFuture = null;
    }

    /**
     * Find a broker node to send the FindCoordinator request. If no node is available, fail the
     * future with {@link NoAvailableBrokersException}.
     * @return FindCoordinator future
     */
    protected RequestFuture<Void> lookupCoordinator() {
        if (findCoordinatorFuture == null) {
            Node node = this.networkClient.leastLoadedNode();
            if (node == null) {
                log.debug("No broker available to send FindCoordinator request");
                return RequestFuture.noBrokersAvailable();
            } else {
                findCoordinatorFuture = sendFindCoordinatorRequest(node);
            }
        }
        return findCoordinatorFuture;
    }

    /**
     * Discover the current coordinator for the group. Sends a GroupMetadata request to
     * one of the brokers. The returned future should be polled to get the result of the request.
     * @return A request future which indicates the completion of the metadata request
     */
    private RequestFuture<Void> sendFindCoordinatorRequest(Node node) {
        log.debug("Sending FindCoordinator request to broker {}", node);
        FindCoordinatorRequestData data = new FindCoordinatorRequestData()
                .setKeyType(FindCoordinatorRequest.CoordinatorType.GROUP.id())
                .setKey(this.rebalanceConfig.groupId);
        FindCoordinatorRequest.Builder requestBuilder = new FindCoordinatorRequest.Builder(data);
        return networkClient.send(node, requestBuilder)
                .compose(new FindCoordinatorResponseHandler());
    }

    private class FindCoordinatorResponseHandler extends RequestFutureAdapter<ClientResponse, Void> {

        @Override
        public void onSuccess(ClientResponse resp, RequestFuture<Void> future) {
            log.debug("Received FindCoordinator response {}", resp);
            List<FindCoordinatorResponseData.Coordinator> coordinators = ((FindCoordinatorResponse) resp.responseBody()).coordinators();
            if (coordinators.size() != 1) {
                log.error("Group coordinator lookup failed: Invalid response containing more than a single coordinator");
                future.raise(new IllegalStateException("Group coordinator lookup failed: Invalid response containing more than a single coordinator"));
            }
            FindCoordinatorResponseData.Coordinator coordinatorData = coordinators.get(0);
            Errors error = Errors.forCode(coordinatorData.errorCode());
            if (error == Errors.NONE) {
                synchronized (AbstractAsyncCoordinator.this) {
                    // use MAX_VALUE - node.id as the coordinator id to allow separate connections
                    // for the coordinator in the underlying network client layer
                    int coordinatorConnectionId = Integer.MAX_VALUE - coordinatorData.nodeId();

                    AbstractAsyncCoordinator.this.coordinator = new Node(
                            coordinatorConnectionId,
                            coordinatorData.host(),
                            coordinatorData.port());
                    log.info("Discovered group coordinator {}", coordinator);
                    networkClient.tryConnect(coordinator);
                    // TODO: we need to updated heartbeat's session timeout upon finding a new
                    //  coordinator. This will be added after the heartbeat thread has been added.
                }
                future.complete(null);
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
            } else {
                log.debug("Group coordinator lookup failed: {}", coordinatorData.errorMessage());
                future.raise(error);
            }
        }

        @Override
        public void onFailure(RuntimeException e, RequestFuture<Void> future) {
            log.debug("FindCoordinator request failed due to {}", e.toString());
            if (!(e instanceof RetriableException)) {
                // Remember the exception if fatal so we can ensure it gets thrown by the main thread
                fatalFindCoordinatorException = e;
            }
            super.onFailure(e, future);
        }
    }

    public boolean hasGroup() {
        // TODO: State machine to be implemented. this.state == stable
        return true;
    }

    boolean rebalanceInProgress() {
        // TODO: State machine to be implemented. state in [rebalance states]
        return true;
    }

    private synchronized Node coordinator() {
        return this.coordinator;
    }
    protected String memberId() {
        return generation.memberId;
    }

    synchronized RequestFuture<Void> sendHeartbeatRequest() {
        log.debug("Sending Heartbeat request with generation {} and member id {} to coordinator {}",
                generation.generationId, generation.memberId, coordinator);
        HeartbeatRequest.Builder requestBuilder =
                new HeartbeatRequest.Builder(new HeartbeatRequestData()
                        .setGroupId(rebalanceConfig.groupId)
                        .setMemberId(this.generation.memberId)
                        .setGroupInstanceId(this.rebalanceConfig.groupInstanceId.orElse(null))
                        .setGenerationId(this.generation.generationId));
        return networkClient.send(coordinator, requestBuilder)
                .compose(new HeartbeatResponseHandler(generation));
    }

    private class HeartbeatResponseHandler extends CoordinatorResponseHandler<HeartbeatResponse, Void> {
        private HeartbeatResponseHandler(final Generation generation) {
            super(generation);
        }

        @Override
        public void handle(HeartbeatResponse heartbeatResponse, RequestFuture<Void> future) {
            sensors.heartbeatSensor.record(response.requestLatencyMs());
            Errors error = heartbeatResponse.error();

            if (error == Errors.NONE) {
                log.debug("Received successful Heartbeat response");
                future.complete(null);
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                    || error == Errors.NOT_COORDINATOR) {
                log.info("Attempt to heartbeat failed since coordinator {} is either not started or not valid",
                        coordinator());
                markCoordinatorUnknown(error);
                future.raise(error);
            } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                // TODO: Implement after rebalance protocol is implemented
            } else if (error == Errors.ILLEGAL_GENERATION ||
                    error == Errors.UNKNOWN_MEMBER_ID ||
                    error == Errors.FENCED_INSTANCE_ID) {
                // TODO: Implement after rebalance protocol is implemented
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
            } else {
                future.raise(new KafkaException("Unexpected error in heartbeat response: " + error.message()));
            }
        }
    }


    @Override
    public void close() {
    }

    protected Meter createMeter(
            Metrics metrics,
            String groupName,
            String baseName,
            String descriptiveName) {
        return new Meter(new WindowedCount(),
                metrics.metricName(baseName + "-rate", groupName,
                        String.format("The number of %s per second", descriptiveName)),
                metrics.metricName(baseName + "-total", groupName,
                        String.format("The total number of %s", descriptiveName)));
    }

    protected abstract class CoordinatorResponseHandler<R, T> extends RequestFutureAdapter<ClientResponse, T> {
        final Generation sentGeneration;
        ClientResponse response;

        CoordinatorResponseHandler(final Generation generation) {
            this.sentGeneration = generation;
        }

        public abstract void handle(R response, RequestFuture<T> future);

        @Override
        public void onFailure(RuntimeException e, RequestFuture<T> future) {
            // mark the coordinator as dead
            if (e instanceof DisconnectException) {
                markCoordinatorUnknown(true, e.getMessage());
            }
            future.raise(e);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onSuccess(ClientResponse clientResponse, RequestFuture<T> future) {
            try {
                this.response = clientResponse;
                R responseObj = (R) clientResponse.responseBody();
                handle(responseObj, future);
            } catch (RuntimeException e) {
                if (!future.isDone())
                    future.raise(e);
            }
        }

        boolean generationUnchanged() {
            synchronized (AbstractAsyncCoordinator.this) {
                return generation.equals(sentGeneration);
            }
        }
    }


    protected static class Generation {
        public static final Generation NO_GENERATION = new Generation(
                OffsetCommitRequest.DEFAULT_GENERATION_ID,
                JoinGroupRequest.UNKNOWN_MEMBER_ID,
                null);

        public final int generationId;
        public final String memberId;
        public final String protocolName;

        public Generation(int generationId, String memberId, String protocolName) {
            this.generationId = generationId;
            this.memberId = memberId;
            this.protocolName = protocolName;
        }

        /**
         * @return true if this generation has a valid member id, false otherwise. A member might have an id before
         * it becomes part of a group generation.
         */
        public boolean hasMemberId() {
            return !memberId.isEmpty();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Generation that = (Generation) o;
            return generationId == that.generationId &&
                    Objects.equals(memberId, that.memberId) &&
                    Objects.equals(protocolName, that.protocolName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(generationId, memberId, protocolName);
        }

        @Override
        public String toString() {
            return "Generation{" +
                    "generationId=" + generationId +
                    ", memberId='" + memberId + '\'' +
                    ", protocol='" + protocolName + '\'' +
                    '}';
        }
    }

    private class GroupCoordinatorMetrics {
        public final String metricGrpName;
        public final Sensor heartbeatSensor;
        public final Sensor joinSensor;
        public final Sensor syncSensor;
        public final Sensor successfulRebalanceSensor;
        public final Sensor failedRebalanceSensor;

        public GroupCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.heartbeatSensor = metrics.sensor("heartbeat-latency");
            this.heartbeatSensor.add(metrics.metricName("heartbeat-response-time-max",
                    this.metricGrpName,
                    "The max time taken to receive a response to a heartbeat request"), new Max());
            this.heartbeatSensor.add(createMeter(metrics, metricGrpName, "heartbeat", "heartbeats"));

            this.joinSensor = metrics.sensor("join-latency");
            this.joinSensor.add(metrics.metricName("join-time-avg",
                    this.metricGrpName,
                    "The average time taken for a group rejoin"), new Avg());
            this.joinSensor.add(metrics.metricName("join-time-max",
                    this.metricGrpName,
                    "The max time taken for a group rejoin"), new Max());
            this.joinSensor.add(createMeter(metrics, metricGrpName, "join", "group joins"));

            this.syncSensor = metrics.sensor("sync-latency");
            this.syncSensor.add(metrics.metricName("sync-time-avg",
                    this.metricGrpName,
                    "The average time taken for a group sync"), new Avg());
            this.syncSensor.add(metrics.metricName("sync-time-max",
                    this.metricGrpName,
                    "The max time taken for a group sync"), new Max());
            this.syncSensor.add(createMeter(metrics, metricGrpName, "sync", "group syncs"));

            this.successfulRebalanceSensor = metrics.sensor("rebalance-latency");
            this.successfulRebalanceSensor.add(metrics.metricName("rebalance-latency-avg",
                    this.metricGrpName,
                    "The average time taken for a group to complete a successful rebalance, which may be composed of " +
                            "several failed re-trials until it succeeded"), new Avg());
            this.successfulRebalanceSensor.add(metrics.metricName("rebalance-latency-max",
                    this.metricGrpName,
                    "The max time taken for a group to complete a successful rebalance, which may be composed of " +
                            "several failed re-trials until it succeeded"), new Max());
            this.successfulRebalanceSensor.add(metrics.metricName("rebalance-latency-total",
                            this.metricGrpName,
                            "The total number of milliseconds this consumer has spent in successful rebalances since creation"),
                    new CumulativeSum());
            this.successfulRebalanceSensor.add(
                    metrics.metricName("rebalance-total",
                            this.metricGrpName,
                            "The total number of successful rebalance events, each event is composed of " +
                                    "several failed re-trials until it succeeded"),
                    new CumulativeCount()
            );
            this.successfulRebalanceSensor.add(
                    metrics.metricName(
                            "rebalance-rate-per-hour",
                            this.metricGrpName,
                            "The number of successful rebalance events per hour, each event is composed of " +
                                    "several failed re-trials until it succeeded"),
                    new Rate(TimeUnit.HOURS, new WindowedCount())
            );

            this.failedRebalanceSensor = metrics.sensor("failed-rebalance");
            this.failedRebalanceSensor.add(
                    metrics.metricName("failed-rebalance-total",
                            this.metricGrpName,
                            "The total number of failed rebalance events"),
                    new CumulativeCount()
            );
            this.failedRebalanceSensor.add(
                    metrics.metricName(
                            "failed-rebalance-rate-per-hour",
                            this.metricGrpName,
                            "The number of failed rebalance events per hour"),
                    new Rate(TimeUnit.HOURS, new WindowedCount())
            );

            Measurable lastRebalance = (config, now) -> {
                if (lastRebalanceEndMs == -1L)
                    // if no rebalance is ever triggered, we just return -1.
                    return -1d;
                else
                    return TimeUnit.SECONDS.convert(now - lastRebalanceEndMs, TimeUnit.MILLISECONDS);
            };
            metrics.addMetric(metrics.metricName("last-rebalance-seconds-ago",
                            this.metricGrpName,
                            "The number of seconds since the last successful rebalance event"),
                    lastRebalance);
            /*
            TODO:
            Heartbeat metrics will be added in the future PR. This serves the
             purpose of the reminder
            Measurable lastHeartbeat = (config, now) -> {
                if (heartbeat.lastHeartbeatSend() == 0L)
                    // if no heartbeat is ever triggered, just return -1.
                    return -1d;
                else
                    return TimeUnit.SECONDS.convert(now - heartbeat.lastHeartbeatSend(), TimeUnit.MILLISECONDS);
            };
            metrics.addMetric(metrics.metricName("last-heartbeat-seconds-ago",
                            this.metricGrpName,
                            "The number of seconds since the last coordinator heartbeat was sent"),
                    lastHeartbeat);
             */
        }
    }
}
