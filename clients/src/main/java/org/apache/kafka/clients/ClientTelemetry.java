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
package org.apache.kafka.clients;

import static org.apache.kafka.clients.ClientTelemetryUtils.createGetTelemetrySubscriptionRequest;
import static org.apache.kafka.clients.ClientTelemetryUtils.createPushTelemetryRequest;
import static org.apache.kafka.clients.ClientTelemetryUtils.validateAcceptedCompressionTypes;
import static org.apache.kafka.clients.ClientTelemetryUtils.validateClientInstanceId;
import static org.apache.kafka.clients.ClientTelemetryUtils.validateMetricNames;
import static org.apache.kafka.clients.ClientTelemetryUtils.validatePushIntervalMs;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.internals.ConsumerMetricsRegistry;
import org.apache.kafka.clients.producer.internals.ProducerMetricsRegistry;
import org.apache.kafka.clients.producer.internals.ProducerTopicMetricsRegistry;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.telemetry.emitter.Context;
import org.apache.kafka.common.telemetry.collector.KafkaMetricsCollector;
import org.apache.kafka.common.telemetry.collector.MetricsCollector;
import org.apache.kafka.common.telemetry.metrics.MetricKeyable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData;
import org.apache.kafka.common.message.PushTelemetryResponseData;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.telemetry.metrics.MetricKey;
import org.apache.kafka.common.telemetry.metrics.MetricNamingStrategy;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The implementation of the client telemetry manager.
 *
 * <p/>
 *
 * The full life-cycle of the metric collection process is defined by a state machine in
 * {@link ClientTelemetryState}. Each state is associated with a different set of operations.
 * For example, the client telemetry manager will attempt to fetch the telemetry subscription
 * from the broker when in the {@link ClientTelemetryState#subscription_needed} state.
 * If the push operation failed, the client telemetry manager will attempt to re-fetch the
 * subscription information by setting the state back to
 * {@link ClientTelemetryState#subscription_needed}.
 *
 * <p/>
 *
 * In an unlikely scenario, if a bad state transition is detected, an
 * {@link IllegalClientTelemetryStateException} will be thrown.
 *
 * <p/>
 *
 * The state transition follows the following steps in order:
 *
 * <ol>
 *     <li>{@link ClientTelemetryState#subscription_needed}</li>
 *     <li>{@link ClientTelemetryState#subscription_in_progress}</li>
 *     <li>{@link ClientTelemetryState#push_needed}</li>
 *     <li>{@link ClientTelemetryState#push_in_progress}</li>
 *     <li>{@link ClientTelemetryState#terminating_push_needed}</li>
 *     <li>{@link ClientTelemetryState#terminating_push_in_progress}</li>
 *     <li>{@link ClientTelemetryState#terminated}</li>
 * </ol>
 *
 * <p/>
 *
 * For more detail in state transition, see {@link ClientTelemetryState#validateTransition}.
 */
public class ClientTelemetry implements Closeable {

    static final MetricNamingStrategy<MetricName> NAMING_STRATEGY = new MetricNamingStrategy<MetricName>() {

        @Override
        public MetricKey metricKey(MetricName metricName) {
            return new MetricKey(metricName);
        }

        @Override
        public MetricKey derivedMetricKey(MetricKey key, String derivedComponent) {
            return new MetricKey(key.name() + '/' + derivedComponent, key.tags());
        }

    };

    static final Predicate<KafkaMetric> KAFKA_METRIC_PREDICATE = m -> m.metricName().name().startsWith("org.apache.kafka");

    public final static int DEFAULT_PUSH_INTERVAL_MS = 300_000;

    public final static long MAX_TERMINAL_PUSH_WAIT_MS = 100;

    private final static Logger log = LoggerFactory.getLogger(ClientTelemetry.class);

    private final static String CONTEXT = "kafka.telemetry";

    private final static String CLIENT_ID_METRIC_TAG = "client-id";

    private final Context context;

    private final Time time;

    private final Metrics metrics;

    private final List<MetricsCollector> metricsCollectors;

    private final ReadWriteLock subscriptionLock = new ReentrantReadWriteLock();

    private final Condition subscriptionLoaded = subscriptionLock.writeLock().newCondition();

    private ClientTelemetrySubscription subscription;

    private final ReadWriteLock stateLock = new ReentrantReadWriteLock();

    private final Condition terminalPushInProgress = stateLock.writeLock().newCondition();

    private ClientTelemetryState state = ClientTelemetryState.subscription_needed;

    private final ClientInstanceMetricsRegistry clientInstanceMetricsRegistry;

    private final ConsumerMetricsRegistry consumerMetricsRegistry;

    private final ProducerMetricsRegistry producerMetricsRegistry;

    private final ProducerTopicMetricsRegistry producerTopicMetricsRegistry;

    public ClientTelemetry(Time time, String clientId) {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        clientId = Objects.requireNonNull(clientId, "clientId cannot be null");

        KafkaMetricsCollector kafkaMetricsCollector = new KafkaMetricsCollector(time, NAMING_STRATEGY, KAFKA_METRIC_PREDICATE);

        this.context = new Context();
        this.metricsCollectors = new ArrayList<>();
        metricsCollectors.add(new HostProcessInfoMetricsCollector(time));
        metricsCollectors.add(kafkaMetricsCollector);

        for (MetricsCollector mc : metricsCollectors)
            mc.init();

        Map<String, String> metricsTags = Collections.singletonMap(CLIENT_ID_METRIC_TAG, clientId);
        MetricConfig metricConfig = new MetricConfig()
            .tags(metricsTags);
        MetricsContext metricsContext = new KafkaMetricsContext(CONTEXT);

        this.metrics = new Metrics(metricConfig,
            Collections.singletonList(kafkaMetricsCollector),
            time,
            metricsContext);

        this.clientInstanceMetricsRegistry = new ClientInstanceMetricsRegistry(this.metrics);
        this.consumerMetricsRegistry = new ConsumerMetricsRegistry(this.metrics);
        this.producerMetricsRegistry = new ProducerMetricsRegistry(this.metrics);
        this.producerTopicMetricsRegistry = new ProducerTopicMetricsRegistry(this.metrics);
    }

    public void initiateClose(Duration timeout) {
        log.trace("initiateClose");

        long timeoutMs = timeout.toMillis();

        if (timeoutMs < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");

        // If we never had a subscription, we can't really push anything.
        if (subscription() == null) {
            log.debug("Telemetry subscription not loaded, not attempting terminating push");
            return;
        }

        try {
            setState(ClientTelemetryState.terminating_push_needed);
        } catch (IllegalClientTelemetryStateException e) {
            log.warn("Error initiating client telemetry close", e);
        }

        try {
            stateLock.writeLock().lock();

            try {
                log.debug("About to wait {} ms. for terminal telemetry push to be submitted", timeout);

                if (!terminalPushInProgress.await(timeoutMs, TimeUnit.MILLISECONDS))
                    log.debug("Wait for terminal telemetry push to be submitted has elapsed; may not have actually sent request");
            } catch (InterruptedException e) {
                throw new InterruptException(e);
            }
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        log.trace("close");

        try {
            stateLock.writeLock().lock();
            ClientTelemetryState newState = ClientTelemetryState.terminated;

            if (state() != newState) {
                try {
                    // This *shouldn't* throw an exception, but let's wrap it anyway so that we're
                    // sure to close the metrics object.
                    setState(ClientTelemetryState.terminated);
                } finally {
                    metrics.close();

                    for (MetricsCollector mc : metricsCollectors)
                        mc.close();
                }
            } else {
                log.debug("Ignoring subsequent {} close", ClientTelemetry.class.getSimpleName());
            }
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    void setSubscription(ClientTelemetrySubscription newSubscription) {
        try {
            subscriptionLock.writeLock().lock();

            log.trace("Setting subscription from {} to {}", this.subscription, newSubscription);
            this.subscription = newSubscription;

            // In some cases we have to wait for this signal in the clientInstanceId method so that
            // we know that we have a subscription to pull from.
            subscriptionLoaded.signalAll();
        } finally {
            subscriptionLock.writeLock().unlock();
        }
    }

    ClientTelemetrySubscription subscription() {
        try {
            subscriptionLock.readLock().lock();
            return subscription;
        } finally {
            subscriptionLock.readLock().unlock();
        }
    }

    /**
     * Determines the client's unique client instance ID used for telemetry. This ID is unique to
     * the specific enclosing client instance and will not change after it is initially generated.
     * The ID is useful for correlating client operations with telemetry sent to the broker and
     * to its eventual monitoring destination(s).
     *
     * This method waits up to <code>timeout</code> for the subscription to become available in
     * order to complete the request.
     *
     * @param timeout The maximum time to wait for enclosing client instance to determine its
     *                client instance ID. The value should be non-negative. Specifying a timeout
     *                of zero means do not wait for the initial request to complete if it hasn't
     *                already.
     * @throws InterruptException If the thread is interrupted while blocked.
     * @throws KafkaException If an unexpected error occurs while trying to determine the client
     *                        instance ID, though this error does not necessarily imply the
     *                        enclosing client instance is otherwise unusable.
     * @throws IllegalArgumentException If the <code>timeout</code> is negative.
     * @return Human-readable string representation of the client instance ID
     */

    public Optional<String> clientInstanceId(Duration timeout) {
        log.trace("clientInstanceId");

        long timeoutMs = timeout.toMillis();

        if (timeoutMs < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");

        ClientTelemetryState state = state();

        try {
            subscriptionLock.writeLock().lock();

            // We can use the instance variable directly here because we're handling the locking...
            if (subscription == null) {
                // If we have a non-negative timeout and no-subscription, let's wait for one to
                // be retrieved.
                log.trace("Waiting for telemetry subscription containing the client instance ID with timeoutMillis = {} ms.", timeoutMs);

                try {
                    if (!subscriptionLoaded.await(timeoutMs, TimeUnit.MILLISECONDS))
                        log.debug("Wait for telemetry subscription elapsed; may not have actually loaded it");
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
                log.debug("Client instance ID was null in telemetry subscription while in state {}", state);
                return Optional.empty();
            }

            return Optional.of(clientInstanceId.toString());
        } finally {
            subscriptionLock.writeLock().unlock();
        }
    }

    void setState(ClientTelemetryState newState) {
        try {
            stateLock.writeLock().lock();
            log.trace("Setting telemetry state from {} to {}", this.state, newState);
            this.state = this.state.validateTransition(newState);

            if (newState == ClientTelemetryState.terminating_push_in_progress)
                terminalPushInProgress.signalAll();
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    ClientTelemetryState state() {
        try {
            stateLock.readLock().lock();
            return state;
        } finally {
            stateLock.readLock().unlock();
        }
    }

    public void subscriptionFailed(Throwable error) {
        if (error == null)
            error = new Exception();

        log.warn("Failed to retrieve telemetry subscription; using existing subscription", error);
        setState(ClientTelemetryState.subscription_needed);
    }

    public void pushFailed(Throwable error) {
        if (error == null)
            error = new Exception();

        log.warn("Failed to push telemetry", error);

        ClientTelemetryState state = state();

        if (state == ClientTelemetryState.push_in_progress)
            setState(ClientTelemetryState.subscription_needed);
        else if (state == ClientTelemetryState.terminating_push_in_progress)
            setState(ClientTelemetryState.terminated);
        else
            throw new IllegalClientTelemetryStateException(String.format("Could not transition state after failed push telemetry from state %s", state));
    }

    public void subscriptionResponseReceived(GetTelemetrySubscriptionsResponseData data) {
        if (data == null) {
            // TODO: https://confluentinc.atlassian.net/browse/CLIENTS-2284
            log.warn("null GetTelemetrySubscriptionsResponseData, re-fetching subscription from the broker");
            setState(ClientTelemetryState.subscription_needed);
            return;
        }

        if (data.errorCode() != Errors.NONE.code()) {
            handleResponseErrorCode(data.errorCode());
        } else {
            log.debug("Successfully get telemetry subscription; response: {}", data);
        }

        List<String> requestedMetrics = data.requestedMetrics();
        Predicate<? super MetricKeyable> selector = validateMetricNames(requestedMetrics);
        List<CompressionType> acceptedCompressionTypes = validateAcceptedCompressionTypes(data.acceptedCompressionTypes());
        Uuid clientInstanceId = validateClientInstanceId(data.clientInstanceId());
        final int pushIntervalMs = getPushIntervalMs(data);

        ClientTelemetrySubscription clientTelemetrySubscription = new ClientTelemetrySubscription(time.milliseconds(),
            data.throttleTimeMs(),
            clientInstanceId,
            data.subscriptionId(),
            acceptedCompressionTypes,
            pushIntervalMs,
            data.deltaTemporality(),
            selector);

        log.debug("Successfully retrieved telemetry subscription: {}", clientTelemetrySubscription);
        setSubscription(clientTelemetrySubscription);

        if (selector == ClientTelemetryUtils.SELECTOR_NO_METRICS) {
            // This is the case where no metrics are requested and/or match the filters. We need
            // to wait pushIntervalMs then retry.
            setState(ClientTelemetryState.subscription_needed);
        } else {
            setState(ClientTelemetryState.push_needed);
        }
    }

    private int getPushIntervalMs(final GetTelemetrySubscriptionsResponseData data) {
        int pushIntervalMs = validatePushIntervalMs(data.pushIntervalMs() > 0 ? data.pushIntervalMs() : DEFAULT_PUSH_INTERVAL_MS);

        if (subscription() == null) {
            // if this is the first request, subscription() returns null, and we want to
            // equally spread all the request between 0.5 pushInterval and 1.5 pushInterval.
            double rand = ThreadLocalRandom.current().nextDouble(0.5, 1.5);
            return (int) Math.round(rand * pushIntervalMs);
        }

        return pushIntervalMs;
    }

    public void pushResponseReceived(PushTelemetryResponseData data) {
        if (data == null) {
            // TODO: https://confluentinc.atlassian.net/browse/CLIENTS-2285
            log.warn("null PushTelemetryResponseData, re-fetching subscription from the broker");
            setState(ClientTelemetryState.subscription_needed);
            return;
        }

        if (data.errorCode() != Errors.NONE.code()) {
            handleResponseErrorCode(data.errorCode());
        } else {
            log.debug("Successfully pushed telemetry; response: {}", data);
        }

        ClientTelemetryState state = state();

        if (state == ClientTelemetryState.push_in_progress)
            setState(ClientTelemetryState.subscription_needed);
        else if (state == ClientTelemetryState.terminating_push_in_progress)
            setState(ClientTelemetryState.terminated);
        else
            throw new IllegalClientTelemetryStateException(String.format("Could not transition state after successful push telemetry from state %s", state));
    }

    /**
     * Examine the response data and handle different error code accordingly.
     *  - Authorization Failed: Retry 30min later
     *  - Invalid Record: Retry 5min later
     *  - UnknownSubscription or Unsupported Compression: Retry
     *  After a new subscription is created, the current state is transitioned to push_needed to wait for another push.
     *
     * @param errorCode response body error code.
     * @throws IllegalClientTelemetryStateException when subscription is null.
     */
    void handleResponseErrorCode(short errorCode) {
        if (errorCode == Errors.CLIENT_METRICS_PLUGIN_NOT_FOUND.code()) {
            log.warn("Error code: {}. Reason: Broker does not have any client metrics plugin configured.", errorCode);
            return;
        }

        ClientTelemetrySubscription currentSubscription = subscription();

        // The broker will indicate an error when there are no plugins configured, and it won't
        // return any subscription information, so we may not have it here.
        if (currentSubscription == null)
            return;

        long pushIntervalMs = -1;
        String reason = null;

        // We might want to wait and retry or retry after some failures are received
        if (isAuthorizationFailedError(errorCode)) {
            pushIntervalMs = 30 * 60 * 1000;
            reason = "Client is not permitted to send metrics";
        } else if (errorCode == Errors.INVALID_RECORD.code()) {
            pushIntervalMs = 5 * 60 * 1000;
            reason = "Broker failed to decode or validate the client’s encoded metrics";
        } else if (errorCode == Errors.UNKNOWN_CLIENT_METRICS_SUBSCRIPTION_ID.code() ||
                errorCode == Errors.UNSUPPORTED_COMPRESSION_TYPE.code()) {
            pushIntervalMs = 0;
            reason = Errors.forCode(errorCode).message();
        }

        if (pushIntervalMs >= 0) {
            log.warn("Error code: {}, reason: {}. Retry automatically in {} ms.", errorCode, reason, pushIntervalMs);

            // Create a temporary subscription based on the last one that we had so that we have
            // something to use. The temporary subscription is largely the same as the current
            // subscription except thta it has new fetch time and push interval values so that
            // we can determine the correct next push time.
            long fetchMs = time.milliseconds();
            ClientTelemetrySubscription tempSub = new ClientTelemetrySubscription(fetchMs,
                currentSubscription.throttleTimeMs(),
                currentSubscription.clientInstanceId(),
                currentSubscription.subscriptionId(),
                currentSubscription.acceptedCompressionTypes(),
                pushIntervalMs,
                currentSubscription.deltaTemporality(),
                currentSubscription.selector());
            setSubscription(tempSub);
        }
    }

    private static boolean isAuthorizationFailedError(short errorCode) {
        return errorCode == Errors.CLUSTER_AUTHORIZATION_FAILED.code() ||
                errorCode == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code() ||
                errorCode == Errors.GROUP_AUTHORIZATION_FAILED.code() ||
                errorCode == Errors.TOPIC_AUTHORIZATION_FAILED.code() ||
                errorCode == Errors.DELEGATION_TOKEN_AUTHORIZATION_FAILED.code();
    }

    public long timeToNextUpdate(long requestTimeoutMs) {
        ClientTelemetrySubscription subscription = subscription();
        ClientTelemetryState state = state();
        long t = ClientTelemetryUtils.timeToNextUpdate(state, subscription, requestTimeoutMs, time);
        log.trace("For telemetry state {}, returning {} for time to next telemetry update", state, t);
        return t;
    }

    public Optional<AbstractRequest.Builder<?>> createRequest() {
        ClientTelemetryState state = state();
        ClientTelemetrySubscription subscription = subscription();
        AbstractRequest.Builder<?> requestBuilder;
        ClientTelemetryState newState;

        if (state == ClientTelemetryState.subscription_needed) {
            requestBuilder = createGetTelemetrySubscriptionRequest(subscription);
            newState = ClientTelemetryState.subscription_in_progress;
        } else if (state == ClientTelemetryState.push_needed || state == ClientTelemetryState.terminating_push_needed) {
            if (subscription == null) {
                log.warn("Telemetry state is {} but subscription is null; not sending telemetry", state);
                return Optional.empty();
            }

            byte[] payload;

            try (ClientTelemetryEmitter emitter = new ClientTelemetryEmitter(subscription.selector(), context)) {
                emitter.init();

                for (MetricsCollector mc : metricsCollectors)
                    mc.collect(emitter);

                payload = emitter.payload();
            }

            boolean terminating = state == ClientTelemetryState.terminating_push_needed;
            requestBuilder = createPushTelemetryRequest(terminating, subscription, payload);

            if (terminating)
                newState = ClientTelemetryState.terminating_push_in_progress;
            else
                newState = ClientTelemetryState.push_in_progress;
        } else {
            log.warn("Cannot make telemetry request as telemetry is in state: {}", state);
            return Optional.empty();
        }

        log.debug("Created new {} and preparing to set state to {}", requestBuilder.getClass().getName(), newState);
        setState(newState);
        return Optional.of(requestBuilder);
    }

    public ProducerMetricsRegistry producerMetricsRegistry() {
        return producerMetricsRegistry;
    }

    public ProducerTopicMetricsRegistry producerTopicMetricsRegistry() {
        return producerTopicMetricsRegistry;
    }

    public ConsumerMetricsRegistry consumerMetricsRegistry() {
        return consumerMetricsRegistry;
    }

    public ClientInstanceMetricsRegistry clientInstanceMetricsRegistry() {
        return clientInstanceMetricsRegistry;
    }

    public Context context() {
        return context;
    }

}
