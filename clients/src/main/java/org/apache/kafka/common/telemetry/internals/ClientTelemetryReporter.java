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
package org.apache.kafka.common.telemetry.internals;

import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsRequestData;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData;
import org.apache.kafka.common.message.PushTelemetryRequestData;
import org.apache.kafka.common.message.PushTelemetryResponseData;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractRequest.Builder;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionsRequest;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionsResponse;
import org.apache.kafka.common.requests.PushTelemetryRequest;
import org.apache.kafka.common.requests.PushTelemetryResponse;
import org.apache.kafka.common.telemetry.ClientTelemetryState;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

/**
 * The implementation of the {@link MetricsReporter} for client telemetry which manages the life-cycle
 * of the client telemetry collection process. The client telemetry reporter is responsible for
 * collecting the client telemetry data and sending it to the broker.
 * <p>
 *
 * The client telemetry reporter is configured with a {@link ClientTelemetrySender} which is
 * responsible for sending the client telemetry data to the broker. The client telemetry reporter
 * will attempt to fetch the telemetry subscription information from the broker and send the
 * telemetry data to the broker based on the subscription information i.e. push interval, temporality,
 * compression type, etc.
 * <p>
 *
 * The full life-cycle of the metric collection process is defined by a state machine in
 * {@link ClientTelemetryState}. Each state is associated with a different set of operations.
 * For example, the client telemetry reporter will attempt to fetch the telemetry subscription
 * from the broker when in the {@link ClientTelemetryState#SUBSCRIPTION_NEEDED} state.
 * If the push operation fails, the client telemetry reporter will attempt to re-fetch the
 * subscription information by setting the state back to {@link ClientTelemetryState#SUBSCRIPTION_NEEDED}.
 * <p>
 *
 * In an unlikely scenario, if a bad state transition is detected, an
 * {@link IllegalStateException} will be thrown.
 * <p>
 *
 * The state transition follows the following steps in order:
 * <ol>
 *     <li>{@link ClientTelemetryState#SUBSCRIPTION_NEEDED}</li>
 *     <li>{@link ClientTelemetryState#SUBSCRIPTION_IN_PROGRESS}</li>
 *     <li>{@link ClientTelemetryState#PUSH_NEEDED}</li>
 *     <li>{@link ClientTelemetryState#PUSH_IN_PROGRESS}</li>
 *     <li>{@link ClientTelemetryState#TERMINATING_PUSH_NEEDED}</li>
 *     <li>{@link ClientTelemetryState#TERMINATING_PUSH_IN_PROGRESS}</li>
 *     <li>{@link ClientTelemetryState#TERMINATED}</li>
 * </ol>
 * <p>
 *
 * For more detail in state transition, see {@link ClientTelemetryState#validateTransition}.
 */
public class ClientTelemetryReporter implements MetricsReporter {

    private static final Logger log = LoggerFactory.getLogger(ClientTelemetryReporter.class);
    /*
     Exclude client_id from the labels as the broker already knows the client_id from the request
     context. These additional labels from the request context should be added by broker prior
     exporting the metrics to the telemetry backend.
    */
    private static final Set<String> EXCLUDE_LABELS = Collections.singleton("client_id");

    public static final int DEFAULT_PUSH_INTERVAL_MS = 5 * 60 * 1000;

    private final ClientTelemetryProvider telemetryProvider;
    private final ClientTelemetrySender clientTelemetrySender;
    private final Time time;

    private Map<String, Object> rawOriginalConfig;
    private KafkaMetricsCollector kafkaMetricsCollector;

    public ClientTelemetryReporter(Time time) {
        this.time = time;
        telemetryProvider = new ClientTelemetryProvider();
        clientTelemetrySender = new DefaultClientTelemetrySender();
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized void configure(Map<String, ?> configs) {
        rawOriginalConfig = (Map<String, Object>) Objects.requireNonNull(configs);
    }

    @Override
    public synchronized void contextChange(MetricsContext metricsContext) {
        /*
         If validation succeeds: initialize the provider, start the metric collection task,
         set metrics labels for services/libraries that expose metrics.
        */
        Objects.requireNonNull(rawOriginalConfig, "configure() was not called before contextChange()");
        if (kafkaMetricsCollector != null) {
            kafkaMetricsCollector.stop();
        }

        if (!telemetryProvider.validate(metricsContext)) {
            log.warn("Validation failed for {} context {}, skip starting collectors. Metrics collection is disabled",
                    telemetryProvider.getClass(), metricsContext.contextLabels());
            return;
        }

        if (kafkaMetricsCollector == null) {
            /*
             Initialize the provider only once. contextChange(..) can be called more than once,
             but once it's been initialized and all necessary labels are present then we don't
             re-initialize.
            */
            telemetryProvider.configure(rawOriginalConfig);
        }

        telemetryProvider.contextChange(metricsContext);

        if (kafkaMetricsCollector == null) {
            initCollectors();
        }
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        /*
         metrics collector may not have been initialized (e.g. invalid context labels)
         in which case metrics collection is disabled
        */
        if (kafkaMetricsCollector != null) {
            kafkaMetricsCollector.init(metrics);
        }
    }

    /**
     * Method is invoked whenever a metric is added/registered
     */
    @Override
    public void metricChange(KafkaMetric metric) {
        /*
         metrics collector may not have been initialized (e.g. invalid context labels)
         in which case metrics collection is disabled
        */
        if (kafkaMetricsCollector != null) {
            kafkaMetricsCollector.metricChange(metric);
        }
    }

    /**
     * Method is invoked whenever a metric is removed
     */
    @Override
    public void metricRemoval(KafkaMetric metric) {
        /*
         metrics collector may not have been initialized (e.g. invalid context labels)
         in which case metrics collection is disabled
        */
        if (kafkaMetricsCollector != null) {
            kafkaMetricsCollector.metricRemoval(metric);
        }
    }

    @Override
    public void close() {
        log.debug("Stopping ClientTelemetryReporter");
        try {
            clientTelemetrySender.close();
        } catch (Exception exception) {
            log.error("Failed to close client telemetry reporter", exception);
        }
    }

    public synchronized void updateMetricsLabels(Map<String, String> labels) {
        telemetryProvider.updateLabels(labels);
    }

    public void initiateClose(long timeoutMs) {
        log.debug("Initiate close of ClientTelemetryReporter");
        try {
            clientTelemetrySender.initiateClose(timeoutMs);
        } catch (Exception exception) {
            log.error("Failed to initiate close of client telemetry reporter", exception);
        }
    }

    public ClientTelemetrySender telemetrySender() {
        return clientTelemetrySender;
    }

    private void initCollectors() {
        kafkaMetricsCollector = new KafkaMetricsCollector(
            TelemetryMetricNamingConvention.getClientTelemetryMetricNamingStrategy(
                telemetryProvider.domain()), EXCLUDE_LABELS);
    }

    private ResourceMetrics buildMetric(Metric metric) {
        return ResourceMetrics.newBuilder()
            .setResource(telemetryProvider.resource())
            .addScopeMetrics(ScopeMetrics.newBuilder()
                .addMetrics(metric)
                .build()).build();
    }

    // Visible for testing, only for unit tests
    void metricsCollector(KafkaMetricsCollector metricsCollector) {
        kafkaMetricsCollector = metricsCollector;
    }

    // Visible for testing, only for unit tests
    MetricsCollector metricsCollector() {
        return kafkaMetricsCollector;
    }

    // Visible for testing, only for unit tests
    ClientTelemetryProvider telemetryProvider() {
        return telemetryProvider;
    }

    class DefaultClientTelemetrySender implements ClientTelemetrySender {

        /*
         These are the lower and upper bounds of the jitter that we apply to the initial push
         telemetry API call. This helps to avoid a flood of requests all coming at the same time.
        */
        private static final double INITIAL_PUSH_JITTER_LOWER = 0.5;
        private static final double INITIAL_PUSH_JITTER_UPPER = 1.5;

        private final ReadWriteLock lock = new ReentrantReadWriteLock();
        private final Condition subscriptionLoaded = lock.writeLock().newCondition();
        private final Condition terminalPushInProgress = lock.writeLock().newCondition();
        /*
         Initial state should be subscription needed which should allow issuing first telemetry
         request of get telemetry subscription.
        */
        private ClientTelemetryState state = ClientTelemetryState.SUBSCRIPTION_NEEDED;

        private ClientTelemetrySubscription subscription;

        /*
         Last time a telemetry request was made. Initialized to 0 to indicate that no request has
         been made yet. Telemetry requests, get or post, should always be made after the push interval
         time has elapsed.
        */
        private long lastRequestMs;
        /*
         Interval between telemetry requests in milliseconds. Initialized to 0 to indicate that the
         interval has not yet been computed. The first get request will be immediately triggered as
         soon as the client is ready.
        */
        private int intervalMs;
        /*
         Whether the client telemetry sender is enabled or not. Initialized to true to indicate that
         the client telemetry sender is enabled. This is used to disable the client telemetry sender
         when the client receives unrecoverable error from broker.
        */
        private boolean enabled;

        private DefaultClientTelemetrySender() {
            enabled = true;
        }

        @Override
        public long timeToNextUpdate(long requestTimeoutMs) {
            final long nowMs = time.milliseconds();
            final ClientTelemetryState localState;
            final long localLastRequestMs;
            final int localIntervalMs;

            lock.readLock().lock();
            try {
                if (!enabled) {
                    return Integer.MAX_VALUE;
                }

                localState = state;
                localLastRequestMs = lastRequestMs;
                localIntervalMs = intervalMs;
            } finally {
                lock.readLock().unlock();
            }

            final long timeMs;
            final String apiName;
            final String msg;
            final boolean isTraceEnabled = log.isTraceEnabled();

            switch (localState) {
                case SUBSCRIPTION_IN_PROGRESS:
                case PUSH_IN_PROGRESS:
                    /*
                     We have a network request in progress. We record the time of the request
                     submission, so wait that amount of the time PLUS the requestTimeout that
                     is provided.
                    */
                    apiName = (localState == ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS) ? ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS.name : ApiKeys.PUSH_TELEMETRY.name;
                    timeMs = requestTimeoutMs;
                    msg = isTraceEnabled ? "" : String.format("the remaining wait time for the %s network API request, as specified by %s", apiName, CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG);
                    break;
                case TERMINATING_PUSH_IN_PROGRESS:
                    timeMs = Long.MAX_VALUE;
                    msg = isTraceEnabled ? "" : "the terminating push is in progress, disabling telemetry for further requests";
                    break;
                case TERMINATING_PUSH_NEEDED:
                    timeMs = 0;
                    msg = isTraceEnabled ? "" : String.format("the client should try to submit the final %s network API request ASAP before closing", ApiKeys.PUSH_TELEMETRY.name);
                    break;
                case SUBSCRIPTION_NEEDED:
                case PUSH_NEEDED:
                    apiName = (localState == ClientTelemetryState.SUBSCRIPTION_NEEDED) ? ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS.name : ApiKeys.PUSH_TELEMETRY.name;
                    long timeRemainingBeforeRequest = localLastRequestMs + localIntervalMs - nowMs;
                    if (timeRemainingBeforeRequest <= 0) {
                        timeMs = 0;
                        msg = isTraceEnabled ? "" : String.format("the wait time before submitting the next %s network API request has elapsed", apiName);
                    } else {
                        timeMs = timeRemainingBeforeRequest;
                        msg = isTraceEnabled ? "" : String.format("the client will wait before submitting the next %s network API request", apiName);
                    }
                    break;
                default:
                    throw new IllegalStateException("Unknown telemetry state: " + localState);
            }

            if (isTraceEnabled) {
                log.trace("For telemetry state {}, returning the value {} ms; {}", localState, timeMs, msg);
            }
            return timeMs;
        }

        @Override
        public Optional<AbstractRequest.Builder<?>> createRequest() {
            final ClientTelemetryState localState;
            final ClientTelemetrySubscription localSubscription;

            lock.readLock().lock();
            try {
                localState = state;
                localSubscription = subscription;
            } finally {
                lock.readLock().unlock();
            }

            if (localState == ClientTelemetryState.SUBSCRIPTION_NEEDED) {
                return createSubscriptionRequest(localSubscription);
            } else if (localState == ClientTelemetryState.PUSH_NEEDED || localState == ClientTelemetryState.TERMINATING_PUSH_NEEDED) {
                return createPushRequest(localSubscription);
            }

            log.warn("Cannot make telemetry request as telemetry is in state: {}", localState);
            return Optional.empty();
        }

        @Override
        public void handleResponse(GetTelemetrySubscriptionsResponse response) {
            final long nowMs = time.milliseconds();
            final GetTelemetrySubscriptionsResponseData data = response.data();

            final ClientTelemetryState oldState;
            final ClientTelemetrySubscription oldSubscription;
            lock.readLock().lock();
            try {
                oldState = state;
                oldSubscription = subscription;
            } finally {
                lock.readLock().unlock();
            }

            Optional<Integer> errorIntervalMsOpt = ClientTelemetryUtils.maybeFetchErrorIntervalMs(data.errorCode(),
                oldSubscription != null ? oldSubscription.pushIntervalMs() : -1);
            /*
             If the error code indicates that the interval ms needs to be updated as per the error
             code then update the interval ms and state so that the subscription can be retried.
            */
            if (errorIntervalMsOpt.isPresent()) {
                /*
                 Update the state from SUBSCRIPTION_INR_PROGRESS to SUBSCRIPTION_NEEDED as the error
                 response indicates that the subscription is not valid.
                */
                if (!maybeSetState(ClientTelemetryState.SUBSCRIPTION_NEEDED)) {
                    log.warn("Unable to transition state after failed get telemetry subscriptions from state {}", oldState);
                }
                updateErrorResult(errorIntervalMsOpt.get(), nowMs);
                return;
            }

            Uuid clientInstanceId = ClientTelemetryUtils.validateClientInstanceId(data.clientInstanceId());
            int intervalMs = ClientTelemetryUtils.validateIntervalMs(data.pushIntervalMs());
            Predicate<? super MetricKeyable> selector = ClientTelemetryUtils.getSelectorFromRequestedMetrics(
                data.requestedMetrics());
            List<CompressionType> acceptedCompressionTypes = ClientTelemetryUtils.getCompressionTypesFromAcceptedList(
                data.acceptedCompressionTypes());

            /*
             Check if the delta temporality has changed, if so, we need to reset the ledger tracking
             the last value sent for each metric.
            */
            if (oldSubscription != null && oldSubscription.deltaTemporality() != data.deltaTemporality()) {
                log.info("Delta temporality has changed from {} to {}, resetting metric values",
                    oldSubscription.deltaTemporality(), data.deltaTemporality());
                if (kafkaMetricsCollector != null) {
                    kafkaMetricsCollector.metricsReset();
                }
            }

            ClientTelemetrySubscription clientTelemetrySubscription = new ClientTelemetrySubscription(
                clientInstanceId,
                data.subscriptionId(),
                intervalMs,
                acceptedCompressionTypes,
                data.deltaTemporality(),
                selector);

            lock.writeLock().lock();
            try {
                /*
                 This is the case if we began termination sometime after the subscription request
                 was issued. We're just now getting our callback, but we need to ignore it.
                */
                if (isTerminatingState()) {
                    return;
                }

                ClientTelemetryState newState;
                if (selector == ClientTelemetryUtils.SELECTOR_NO_METRICS) {
                    /*
                     This is the case where no metrics are requested and/or match the filters. We need
                     to wait intervalMs then retry.
                    */
                    newState = ClientTelemetryState.SUBSCRIPTION_NEEDED;
                } else {
                    newState = ClientTelemetryState.PUSH_NEEDED;
                }

                // If we're terminating, don't update state or set the subscription.
                if (!maybeSetState(newState)) {
                    return;
                }

                updateSubscriptionResult(clientTelemetrySubscription, nowMs);
                log.info("Client telemetry registered with client instance id: {}", subscription.clientInstanceId());
            } finally {
                lock.writeLock().unlock();
            }
        }

        @Override
        public void handleResponse(PushTelemetryResponse response) {
            final long nowMs = time.milliseconds();
            final PushTelemetryResponseData data = response.data();

            lock.writeLock().lock();
            try {
                Optional<Integer> errorIntervalMsOpt = ClientTelemetryUtils.maybeFetchErrorIntervalMs(data.errorCode(),
                    subscription.pushIntervalMs());
                /*
                 If the error code indicates that the interval ms needs to be updated as per the error
                 code then update the interval ms and state so that the subscription can be re-fetched,
                 and the push retried.
                */
                if (errorIntervalMsOpt.isPresent()) {
                    /*
                     This is the case when client began termination sometime after the last push request
                     was issued. Just getting the callback, hence need to ignore it.
                    */
                    if (isTerminatingState()) {
                        return;
                    }

                    if (!maybeSetState(ClientTelemetryState.SUBSCRIPTION_NEEDED)) {
                        log.warn("Unable to transition state after failed push telemetry from state {}", state);
                    }
                    updateErrorResult(errorIntervalMsOpt.get(), nowMs);
                    return;
                }

                lastRequestMs = nowMs;
                intervalMs = subscription.pushIntervalMs();
                if (!maybeSetState(ClientTelemetryState.PUSH_NEEDED)) {
                    log.warn("Unable to transition state after successful push telemetry from state {}", state);
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        @Override
        public void handleFailedGetTelemetrySubscriptionsRequest(KafkaException maybeFatalException) {
            log.debug("The broker generated an error for the get telemetry network API request", maybeFatalException);
            handleFailedRequest(maybeFatalException != null);
        }

        @Override
        public void handleFailedPushTelemetryRequest(KafkaException maybeFatalException) {
            log.debug("The broker generated an error for the push telemetry network API request", maybeFatalException);
            handleFailedRequest(maybeFatalException != null);
        }

        @Override
        public Optional<Uuid> clientInstanceId(Duration timeout) {
            final long timeoutMs = timeout.toMillis();
            if (timeoutMs < 0) {
                throw new IllegalArgumentException("The timeout cannot be negative for fetching client instance id.");
            }

            lock.writeLock().lock();
            try {
                if (subscription == null) {
                    // If we have a non-negative timeout and no-subscription, let's wait for one to be retrieved.
                    log.debug("Waiting for telemetry subscription containing the client instance ID with timeoutMillis = {} ms.", timeoutMs);
                    try {
                        if (!subscriptionLoaded.await(timeoutMs, TimeUnit.MILLISECONDS)) {
                            log.debug("Wait for telemetry subscription elapsed; may not have actually loaded it");
                        }
                    } catch (InterruptedException e) {
                        throw new InterruptException(e);
                    }
                }

                if (subscription == null) {
                    log.debug("Client instance ID could not be retrieved with timeout {}", timeout);
                    return Optional.empty();
                }

                Uuid clientInstanceId = subscription.clientInstanceId();
                if (clientInstanceId == null) {
                    log.info("Client instance ID was null in telemetry subscription while in state {}", state);
                    return Optional.empty();
                }

                return Optional.of(clientInstanceId);
            } finally {
                lock.writeLock().unlock();
            }
        }

        @Override
        public void close() {
            log.debug("close telemetry sender for client telemetry reporter instance");

            boolean shouldClose = false;
            lock.writeLock().lock();
            try {
                if (state != ClientTelemetryState.TERMINATED) {
                    if (maybeSetState(ClientTelemetryState.TERMINATED)) {
                        shouldClose = true;
                    }
                } else {
                    log.debug("Ignoring subsequent close");
                }
            } finally {
                lock.writeLock().unlock();
            }

            if (shouldClose) {
                if (kafkaMetricsCollector != null) {
                    kafkaMetricsCollector.stop();
                }
            }
        }

        @Override
        public void initiateClose(long timeoutMs) {
            log.debug("initiate close for client telemetry, check if terminal push required. Timeout {} ms.", timeoutMs);

            lock.writeLock().lock();
            try {
                // If we never fetched a subscription, we can't really push anything.
                if (lastRequestMs == 0) {
                    log.debug("Telemetry subscription not loaded, not attempting terminating push");
                    return;
                }

                if (state == ClientTelemetryState.SUBSCRIPTION_NEEDED) {
                    log.debug("Subscription not yet loaded, ignoring terminal push");
                    return;
                }

                if (isTerminatingState() || !maybeSetState(ClientTelemetryState.TERMINATING_PUSH_NEEDED)) {
                    log.debug("Ignoring subsequent initiateClose");
                    return;
                }

                try {
                    log.info("About to wait {} ms. for terminal telemetry push to be submitted", timeoutMs);
                    if (!terminalPushInProgress.await(timeoutMs, TimeUnit.MILLISECONDS)) {
                        log.info("Wait for terminal telemetry push to be submitted has elapsed; may not have actually sent request");
                    }
                } catch (InterruptedException e) {
                    log.warn("Error during client telemetry close", e);
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        private Optional<Builder<?>> createSubscriptionRequest(ClientTelemetrySubscription localSubscription) {
            /*
             If we've previously retrieved a subscription, it will contain the client instance ID
             that the broker assigned. Otherwise, per KIP-714, we send a special "null" UUID to
             signal to the broker that we need to have a client instance ID assigned.
            */
            Uuid clientInstanceId = (localSubscription != null) ? localSubscription.clientInstanceId() : Uuid.ZERO_UUID;
            log.debug("Creating telemetry subscription request with client instance id {}", clientInstanceId);

            lock.writeLock().lock();
            try {
                if (isTerminatingState()) {
                    return Optional.empty();
                }

                if (!maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS)) {
                    return Optional.empty();
                }
            } finally {
                lock.writeLock().unlock();
            }

            AbstractRequest.Builder<?> requestBuilder = new GetTelemetrySubscriptionsRequest.Builder(
                new GetTelemetrySubscriptionsRequestData().setClientInstanceId(clientInstanceId), true);
            return Optional.of(requestBuilder);
        }

        private Optional<Builder<?>> createPushRequest(ClientTelemetrySubscription localSubscription) {
            if (localSubscription == null) {
                log.warn("Telemetry state is {} but subscription is null; not sending telemetry", state);
                if (!maybeSetState(ClientTelemetryState.SUBSCRIPTION_NEEDED)) {
                    log.warn("Unable to transition state after failed create push telemetry from state {}", state);
                }
                return Optional.empty();
            }

            log.debug("Creating telemetry push request with client instance id {}", localSubscription.clientInstanceId());
            /*
             Don't send a push request if we don't have the collector initialized. Re-attempt
             the push on the next interval.
            */
            if (kafkaMetricsCollector == null) {
                log.warn("Cannot make telemetry request as collector is not initialized");
                // Update last accessed time for push request to be retried on next interval.
                updateErrorResult(localSubscription.pushIntervalMs, time.milliseconds());
                return Optional.empty();
            }

            boolean terminating;
            lock.writeLock().lock();
            try {
                /*
                 We've already been terminated, or we've already issued our last push, so we
                 should just exit now.
                */
                if (state == ClientTelemetryState.TERMINATED || state == ClientTelemetryState.TERMINATING_PUSH_IN_PROGRESS) {
                    return Optional.empty();
                }

                /*
                 Check the *actual* state (while locked) to make sure we're still in the state
                 we expect to be in.
                */
                terminating = state == ClientTelemetryState.TERMINATING_PUSH_NEEDED;
                if (!maybeSetState(terminating ? ClientTelemetryState.TERMINATING_PUSH_IN_PROGRESS : ClientTelemetryState.PUSH_IN_PROGRESS)) {
                    return Optional.empty();
                }
            } finally {
                lock.writeLock().unlock();
            }

            return createPushRequest(localSubscription, terminating);
        }

        private Optional<Builder<?>> createPushRequest(ClientTelemetrySubscription localSubscription, boolean terminating) {
            byte[] payload;
            try (MetricsEmitter emitter = new ClientTelemetryEmitter(localSubscription.selector(), localSubscription.deltaTemporality())) {
                emitter.init();
                kafkaMetricsCollector.collect(emitter);
                payload = createPayload(emitter.emittedMetrics());
            } catch (Exception e) {
                log.warn("Error constructing client telemetry payload: ", e);
                // Update last accessed time for push request to be retried on next interval.
                updateErrorResult(localSubscription.pushIntervalMs, time.milliseconds());
                return Optional.empty();
            }

            CompressionType compressionType = ClientTelemetryUtils.preferredCompressionType(localSubscription.acceptedCompressionTypes());
            byte[] compressedPayload;
            try {
                compressedPayload = ClientTelemetryUtils.compress(payload, compressionType);
            } catch (IOException e) {
                log.info("Failed to compress telemetry payload for compression: {}, sending uncompressed data", compressionType);
                compressedPayload = payload;
                compressionType = CompressionType.NONE;
            }

            AbstractRequest.Builder<?> requestBuilder = new PushTelemetryRequest.Builder(
                new PushTelemetryRequestData()
                    .setClientInstanceId(localSubscription.clientInstanceId())
                    .setSubscriptionId(localSubscription.subscriptionId())
                    .setTerminating(terminating)
                    .setCompressionType(compressionType.id)
                    .setMetrics(compressedPayload), true);

            return Optional.of(requestBuilder);
        }

        /**
         * Updates the {@link ClientTelemetrySubscription}, {@link #intervalMs}, and
         * {@link #lastRequestMs}.
         * <p>
         * After the update, the {@link #subscriptionLoaded} condition is signaled so any threads
         * waiting on the subscription can be unblocked.
         *
         * @param subscription Updated subscription that will replace any current subscription
         * @param timeMs       Time in milliseconds representing the current time
         */
        // Visible for testing
        void updateSubscriptionResult(ClientTelemetrySubscription subscription, long timeMs) {
            lock.writeLock().lock();
            try {
                this.subscription = Objects.requireNonNull(subscription);
                /*
                 If the subscription is updated for the client, we want to attempt to spread out the push
                 requests between 50% and 150% of the push interval value from the broker. This helps us
                 to avoid the case where multiple clients are started at the same time and end up sending
                 all their data at the same time.
                */
                if (state == ClientTelemetryState.PUSH_NEEDED) {
                    intervalMs = computeStaggeredIntervalMs(subscription.pushIntervalMs(), INITIAL_PUSH_JITTER_LOWER, INITIAL_PUSH_JITTER_UPPER);
                } else {
                    intervalMs = subscription.pushIntervalMs();
                }
                lastRequestMs = timeMs;

                log.debug("Updating subscription - subscription: {}; intervalMs: {}, lastRequestMs: {}",
                    subscription, intervalMs, lastRequestMs);
                subscriptionLoaded.signalAll();
            } finally {
                lock.writeLock().unlock();
            }
        }

        /**
         * Updates the {@link #intervalMs}, {@link #lastRequestMs} and {@link #enabled}.
         * <p>
         * The contents of the method are guarded by the {@link #lock}.
         */
        private void updateErrorResult(int intervalMs, long timeMs) {
            lock.writeLock().lock();
            try {
                this.intervalMs = intervalMs;
                lastRequestMs = timeMs;
                /*
                 If the interval time is set to Integer.MAX_VALUE, then it means that the telemetry sender
                 should not send any more telemetry requests. This is used when the client received
                 unrecoverable error from broker.
                */
                if (intervalMs == Integer.MAX_VALUE) {
                    enabled = false;
                }

                log.debug("Updating intervalMs: {}, lastRequestMs: {}", intervalMs, lastRequestMs);
            } finally {
                lock.writeLock().unlock();
            }
        }

        // Visible for testing
        int computeStaggeredIntervalMs(int intervalMs, double lowerBound, double upperBound) {
            double rand = ThreadLocalRandom.current().nextDouble(lowerBound, upperBound);
            int firstPushIntervalMs = (int) Math.round(rand * intervalMs);

            log.debug("Telemetry subscription push interval value from broker was {}; to stagger requests the first push"
                + " interval is being adjusted to {}", intervalMs, firstPushIntervalMs);
            return firstPushIntervalMs;
        }

        private boolean isTerminatingState() {
            return state == ClientTelemetryState.TERMINATED || state == ClientTelemetryState.TERMINATING_PUSH_NEEDED
                || state == ClientTelemetryState.TERMINATING_PUSH_IN_PROGRESS;
        }

        // Visible for testing
        boolean maybeSetState(ClientTelemetryState newState) {
            lock.writeLock().lock();
            try {
                ClientTelemetryState oldState = state;
                state = oldState.validateTransition(newState);
                log.debug("Setting telemetry state from {} to {}", oldState, newState);

                if (newState == ClientTelemetryState.TERMINATING_PUSH_IN_PROGRESS) {
                    terminalPushInProgress.signalAll();
                }
                return true;
            } catch (IllegalStateException e) {
                log.warn("Error updating client telemetry state, disabled telemetry", e);
                enabled = false;
                return false;
            } finally {
                lock.writeLock().unlock();
            }
        }

        private void handleFailedRequest(boolean shouldWait) {
            final long nowMs = time.milliseconds();
            lock.writeLock().lock();
            try {
                if (isTerminatingState()) {
                    return;
                }
                if (state != ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS && state != ClientTelemetryState.PUSH_IN_PROGRESS) {
                    log.warn("Could not transition state after failed telemetry from state {}, disabling telemetry", state);
                    updateErrorResult(Integer.MAX_VALUE, nowMs);
                    return;
                }

                /*
                 The broker might not support telemetry. Let's sleep for a while before trying request
                 again. We may disconnect from the broker and connect to a broker that supports client
                 telemetry.
                */
                if (shouldWait) {
                    updateErrorResult(DEFAULT_PUSH_INTERVAL_MS, nowMs);
                } else {
                    log.warn("Received unrecoverable error from broker, disabling telemetry");
                    updateErrorResult(Integer.MAX_VALUE, nowMs);
                }

                if (!maybeSetState(ClientTelemetryState.SUBSCRIPTION_NEEDED)) {
                    log.warn("Could not transition state after failed telemetry from state {}", state);
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        private byte[] createPayload(List<SinglePointMetric> emittedMetrics) {
            MetricsData.Builder builder = MetricsData.newBuilder();
            emittedMetrics.forEach(metric -> {
                Metric m = metric.builder().build();
                ResourceMetrics rm = buildMetric(m);
                builder.addResourceMetrics(rm);
            });
            return builder.build().toByteArray();
        }

        // Visible for testing
        ClientTelemetrySubscription subscription() {
            lock.readLock().lock();
            try {
                return subscription;
            } finally {
                lock.readLock().unlock();
            }
        }

        // Visible for testing
        ClientTelemetryState state() {
            lock.readLock().lock();
            try {
                return state;
            } finally {
                lock.readLock().unlock();
            }
        }

        // Visible for testing
        long intervalMs() {
            lock.readLock().lock();
            try {
                return intervalMs;
            } finally {
                lock.readLock().unlock();
            }
        }

        // Visible for testing
        long lastRequestMs() {
            lock.readLock().lock();
            try {
                return lastRequestMs;
            } finally {
                lock.readLock().unlock();
            }
        }

        // Visible for testing
        void enabled(boolean enabled) {
            lock.writeLock().lock();
            try {
                this.enabled = enabled;
            } finally {
                lock.writeLock().unlock();
            }
        }

        // Visible for testing
        boolean enabled() {
            lock.readLock().lock();
            try {
                return enabled;
            } finally {
                lock.readLock().unlock();
            }
        }
    }

    /**
     * Representation of the telemetry subscription that is retrieved from the cluster at startup and
     * then periodically afterward, following the telemetry push.
     */
    static class ClientTelemetrySubscription {

        private final Uuid clientInstanceId;
        private final int subscriptionId;
        private final int pushIntervalMs;
        private final List<CompressionType> acceptedCompressionTypes;
        private final boolean deltaTemporality;
        private final Predicate<? super MetricKeyable> selector;

        ClientTelemetrySubscription(Uuid clientInstanceId, int subscriptionId, int pushIntervalMs,
                List<CompressionType> acceptedCompressionTypes, boolean deltaTemporality,
                Predicate<? super MetricKeyable> selector) {
            this.clientInstanceId = clientInstanceId;
            this.subscriptionId = subscriptionId;
            this.pushIntervalMs = pushIntervalMs;
            this.acceptedCompressionTypes = Collections.unmodifiableList(acceptedCompressionTypes);
            this.deltaTemporality = deltaTemporality;
            this.selector = selector;
        }

        public Uuid clientInstanceId() {
            return clientInstanceId;
        }

        public int subscriptionId() {
            return subscriptionId;
        }

        public int pushIntervalMs() {
            return pushIntervalMs;
        }

        public List<CompressionType> acceptedCompressionTypes() {
            return acceptedCompressionTypes;
        }

        public boolean deltaTemporality() {
            return deltaTemporality;
        }

        public Predicate<? super MetricKeyable> selector() {
            return selector;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", "ClientTelemetrySubscription{", "}")
                .add("clientInstanceId=" + clientInstanceId)
                .add("subscriptionId=" + subscriptionId)
                .add("pushIntervalMs=" + pushIntervalMs)
                .add("acceptedCompressionTypes=" + acceptedCompressionTypes)
                .add("deltaTemporality=" + deltaTemporality)
                .add("selector=" + selector)
                .toString();
        }
    }
}
