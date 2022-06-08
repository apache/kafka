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
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.protocol.ApiKeys;
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

    private final ReadWriteLock stateLock = new ReentrantReadWriteLock();

    private final Condition subscriptionLoaded = stateLock.writeLock().newCondition();

    private final Condition terminalPushInProgress = stateLock.writeLock().newCondition();

    private ClientTelemetryState state = ClientTelemetryState.subscription_needed;

    private ClientTelemetrySubscription subscription;

    private long lastFetchMs;

    private long pushIntervalMs;

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
    }

    public Metrics metrics() {
        return metrics;
    }

    public Context context() {
        return context;
    }

    public void initiateClose(Duration timeout) {
        log.trace("initiateClose");

        long timeoutMs = timeout.toMillis();

        if (timeoutMs < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");

        try {
            stateLock.writeLock().lock();

            // If we never fetched a subscription, we can't really push anything.
            if (subscription == null) {
                log.debug("Telemetry subscription not loaded, not attempting terminating push");
                return;
            }

            try {
                setState(ClientTelemetryState.terminating_push_needed);
            } catch (IllegalClientTelemetryStateException e) {
                log.warn("Error initiating client telemetry close", e);
            }


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

        boolean shouldClose = false;

        try {
            stateLock.writeLock().lock();

            if (this.state != ClientTelemetryState.terminated) {
                // This *shouldn't* throw an exception, but let's wrap it anyway so that we're
                // sure to close the metrics object.
                setState(ClientTelemetryState.terminated);
                shouldClose = true;
            } else {
                log.debug("Ignoring subsequent {} close", ClientTelemetry.class.getSimpleName());
            }
        } finally {
            stateLock.writeLock().unlock();
        }

        if (shouldClose) {
            metrics.close();

            for (MetricsCollector mc : metricsCollectors)
                mc.close();
        }
    }

    /**
     * Updates the {@link ClientTelemetrySubscription}, {@link #pushIntervalMs}, and
     * {@link #lastFetchMs}.
     *
     * <p/>
     *
     * The entire contents of the method are guarded by the {@link #stateLock}.
     *
     * <p/>
     *
     * After the update, the {@link #subscriptionLoaded} condition is signaled so any threads
     * waiting on the subscription can be unblocked.
     *
     * @param subscription Updated subscription that will replace any current subscription
     */
    void setSubscription(ClientTelemetrySubscription subscription) {
        try {
            stateLock.writeLock().lock();

            this.subscription = subscription;
            this.pushIntervalMs = this.subscription.pushIntervalMs();
            this.lastFetchMs = time.milliseconds();

            log.trace("Updating subscription - subscription: {}; pushIntervalMs: {}, lastFetchMs: {}", this.subscription, pushIntervalMs, lastFetchMs);

            // In some cases we have to wait for this signal in the clientInstanceId method so that
            // we know that we have a subscription to pull from.
            subscriptionLoaded.signalAll();
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    /**
     * Updates <em>only</em> the {@link #pushIntervalMs} and {@link #lastFetchMs}.
     *
     * <p/>
     *
     * The entire contents of the method are guarded by the {@link #stateLock}.
     *
     * <p/>
     *
     * No condition is signaled here, unlike {@link #setSubscription(ClientTelemetrySubscription)}.
     */
    private void setFetchAndPushInterval(long pushIntervalMs) {
        try {
            stateLock.writeLock().lock();

            this.pushIntervalMs = pushIntervalMs;
            this.lastFetchMs = time.milliseconds();

            log.trace("Updating pushIntervalMs: {}, lastFetchMs: {}", pushIntervalMs, lastFetchMs);
        } finally {
            stateLock.writeLock().unlock();
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

        try {
            stateLock.writeLock().lock();

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
            stateLock.writeLock().unlock();
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

    // Visible for testing
    ClientTelemetryState state() {
        try {
            stateLock.readLock().lock();
            return state;
        } finally {
            stateLock.readLock().unlock();
        }
    }

    public void handleFailedRequest(ApiKeys apiKey, Optional<KafkaException> maybeFatalException) {
        KafkaException fatalError = maybeFatalException.orElse(null);
        log.warn("The broker generated an error for the {} API request", apiKey.messageType.name, fatalError);

        try {
            stateLock.writeLock().lock();

            boolean shouldWait = false;

            if (apiKey == ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS) {
                shouldWait = fatalError != null;
                setState(ClientTelemetryState.subscription_needed);
            } else if (apiKey == ApiKeys.PUSH_TELEMETRY) {
                if (state == ClientTelemetryState.push_in_progress) {
                    shouldWait = fatalError != null;
                    setState(ClientTelemetryState.subscription_needed);
                } else if (state == ClientTelemetryState.terminating_push_in_progress) {
                    setState(ClientTelemetryState.terminated);
                } else {
                    throw new IllegalClientTelemetryStateException(String.format("Could not transition state after failed push telemetry from state %s", state));
                }
            }

            // The broker does not support telemetry. Let's sleep for a while before
            // trying things again. We may disconnect from the broker and connect to
            // a broker that supports client telemetry.
            if (shouldWait)
                setFetchAndPushInterval(DEFAULT_PUSH_INTERVAL_MS);
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    public void handleSuccessfulResponse(GetTelemetrySubscriptionsResponseData data) {
        Optional<Long> pushIntervalMsOpt = ClientTelemetryUtils.errorPushIntervalMs(data.errorCode());

        if (pushIntervalMsOpt.isPresent()) {
            setFetchAndPushInterval(pushIntervalMsOpt.get());
            return;
        }

        log.debug("Successfully retrieved telemetry subscription; response: {}", data);
        List<String> requestedMetrics = data.requestedMetrics();
        Predicate<? super MetricKeyable> selector = validateMetricNames(requestedMetrics);
        List<CompressionType> acceptedCompressionTypes = validateAcceptedCompressionTypes(data.acceptedCompressionTypes());
        Uuid clientInstanceId = validateClientInstanceId(data.clientInstanceId());
        int pushIntervalMs = getPushIntervalMs(data);

        ClientTelemetrySubscription clientTelemetrySubscription = new ClientTelemetrySubscription(data.throttleTimeMs(),
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

    public void handleSuccessfulResponse(PushTelemetryResponseData data) {
        Optional<Long> pushIntervalMsOpt = ClientTelemetryUtils.errorPushIntervalMs(data.errorCode());

        if (!pushIntervalMsOpt.isPresent())
            log.debug("Successfully pushed telemetry; response: {}", data);

        try {
            stateLock.writeLock().lock();

            if (state == ClientTelemetryState.push_in_progress)
                setState(ClientTelemetryState.subscription_needed);
            else if (state == ClientTelemetryState.terminating_push_in_progress)
                setState(ClientTelemetryState.terminated);
            else
                throw new IllegalClientTelemetryStateException(String.format("Could not transition state after successful push telemetry from state %s", state));

            pushIntervalMsOpt.ifPresent(this::setFetchAndPushInterval);
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    private int getPushIntervalMs(GetTelemetrySubscriptionsResponseData data) {
        int pushIntervalMs = validatePushIntervalMs(data.pushIntervalMs() > 0 ? data.pushIntervalMs() : DEFAULT_PUSH_INTERVAL_MS);
        boolean isFirstRequest;

        try {
            stateLock.readLock().lock();
            isFirstRequest = lastFetchMs == 0;
        } finally {
            stateLock.readLock().unlock();
        }

        if (isFirstRequest) {
            // if this is the first request, subscription() returns null, and we want to
            // equally spread all the request between 0.5 pushInterval and 1.5 pushInterval.
            double rand = ThreadLocalRandom.current().nextDouble(0.5, 1.5);
            return (int) Math.round(rand * pushIntervalMs);
        }

        return pushIntervalMs;
    }

    public long timeToNextUpdate(long requestTimeoutMs) {
        ClientTelemetryState state = state();
        long t = ClientTelemetryUtils.timeToNextUpdate(state, lastFetchMs, requestTimeoutMs, time);
        log.trace("For telemetry state {}, returning {} for time to next telemetry update", state, t);
        return t;
    }

    public Optional<AbstractRequest.Builder<?>> createRequest() {
        ClientTelemetryState state;
        ClientTelemetrySubscription subscription;

        try {
            stateLock.readLock().lock();
            state = this.state;
            subscription = this.subscription;
        } finally {
            stateLock.readLock().unlock();
        }

        AbstractRequest.Builder<?> requestBuilder;
        ClientTelemetryState newState;

        log.debug("createRequest - state: {}, subscription: {}", state, subscription);

        if (state == ClientTelemetryState.subscription_needed) {
            requestBuilder = createGetTelemetrySubscriptionRequest(subscription);
            newState = ClientTelemetryState.subscription_in_progress;
        } else if (state == ClientTelemetryState.push_needed || state == ClientTelemetryState.terminating_push_needed) {
            if (subscription == null) {
                log.warn("Telemetry state is {} but subscription is null; not sending telemetry", state);
                return Optional.empty();
            }

            byte[] payload;

            try (ClientTelemetryEmitter emitter = new ClientTelemetryEmitter(subscription.selector())) {
                emitter.init();

                for (MetricsCollector mc : metricsCollectors)
                    mc.collect(emitter);

                payload = emitter.payload(context.tags());
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

        log.debug("createRequest - created new {} and preparing to set state to {}", requestBuilder.getClass().getName(), newState);
        setState(newState);
        return Optional.of(requestBuilder);
    }

}
