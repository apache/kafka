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

import static org.apache.kafka.clients.ClientTelemetryUtils.preferredCompressionType;
import static org.apache.kafka.clients.ClientTelemetryUtils.serialize;
import static org.apache.kafka.clients.ClientTelemetryUtils.validateAcceptedCompressionTypes;
import static org.apache.kafka.clients.ClientTelemetryUtils.validateClientInstanceId;
import static org.apache.kafka.clients.ClientTelemetryUtils.validateMetricNames;
import static org.apache.kafka.clients.ClientTelemetryUtils.validatePushIntervalMs;
import static org.apache.kafka.common.Uuid.ZERO_UUID;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionRequest;
import org.apache.kafka.common.requests.PushTelemetryRequest;
import org.apache.kafka.common.telemetry.emitter.Context;
import org.apache.kafka.common.telemetry.collector.KafkaMetricsCollector;
import org.apache.kafka.common.telemetry.collector.MetricsCollector;
import org.apache.kafka.common.telemetry.metrics.Metric;
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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

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

    // These are the lower and upper bounds of the jitter that we apply to the initial push
    // telemetry API call. This helps to avoid a flood of requests all coming at the same time.
    private final static double INITIAL_PUSH_JITTER_LOWER = 0.5;

    private final static double INITIAL_PUSH_JITTER_UPPER = 1.5;

    private final static String CONTEXT = "kafka.telemetry";

    private final static String CLIENT_ID_METRIC_TAG = "client-id";

    private final Logger log;

    private final Context context;

    private final Time time;

    private final Metrics metrics;

    private final List<MetricsCollector> metricsCollectors;

    private final ReadWriteLock instanceStateLock = new ReentrantReadWriteLock();

    private final Condition subscriptionLoaded = instanceStateLock.writeLock().newCondition();

    private final Condition terminalPushInProgress = instanceStateLock.writeLock().newCondition();

    private ClientTelemetryState state = ClientTelemetryState.subscription_needed;

    private ClientTelemetrySubscription subscription;

    private long lastSubscriptionFetchMs;

    private long pushIntervalMs;

    private final Set<ClientTelemetryListener> listeners;

    public ClientTelemetry(LogContext logContext, Time time, String clientId) {
        this.log = logContext.logger(ClientTelemetry.class);
        this.time = Objects.requireNonNull(time, "time cannot be null");
        clientId = Objects.requireNonNull(clientId, "clientId cannot be null");

        KafkaMetricsCollector kafkaMetricsCollector = new KafkaMetricsCollector(time, NAMING_STRATEGY, KAFKA_METRIC_PREDICATE);

        this.context = new Context();
        this.listeners = new HashSet<>();
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

    public void addListener(ClientTelemetryListener listener) {
        try {
            instanceStateLock.writeLock().lock();
            this.listeners.add(listener);
        } finally {
            instanceStateLock.writeLock().unlock();
        }
    }

    public void removeListener(ClientTelemetryListener listener) {
        try {
            instanceStateLock.writeLock().lock();
            this.listeners.remove(listener);
        } finally {
            instanceStateLock.writeLock().unlock();
        }
    }

    private void invokeListeners(Consumer<ClientTelemetryListener> c) {
        List<ClientTelemetryListener> l;

        try {
            instanceStateLock.readLock().lock();
            l = new ArrayList<>(listeners);
        } finally {
            instanceStateLock.readLock().unlock();
        }

        if (!l.isEmpty()) {
            for (ClientTelemetryListener listener : l) {
                try {
                    c.accept(listener);
                } catch (Throwable t) {
                    log.warn(t.getMessage(), t);
                }
            }
        }
    }

    public void initiateClose(Duration timeout) {
        log.trace("initiateClose");

        long timeoutMs = timeout.toMillis();

        if (timeoutMs < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");

        try {
            instanceStateLock.writeLock().lock();

            // If we never fetched a subscription, we can't really push anything.
            if (lastSubscriptionFetchMs == 0) {
                log.debug("Telemetry subscription not loaded, not attempting terminating push");
                return;
            }

            if (isTerminatingState() || !maybeSetState(ClientTelemetryState.terminating_push_needed)) {
                log.debug("Ignoring subsequent initiateClose");
                return;
            }

            try {
                log.debug("About to wait {} ms. for terminal telemetry push to be submitted", timeout);

                if (!terminalPushInProgress.await(timeoutMs, TimeUnit.MILLISECONDS))
                    log.debug("Wait for terminal telemetry push to be submitted has elapsed; may not have actually sent request");
            } catch (InterruptedException e) {
                log.warn("Error during client telemetry close", e);
            }
        } finally {
            instanceStateLock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        log.trace("close");

        boolean shouldClose = false;

        try {
            instanceStateLock.writeLock().lock();

            if (this.state != ClientTelemetryState.terminated) {
                if (maybeSetState(ClientTelemetryState.terminated))
                    shouldClose = true;
            } else {
                log.debug("Ignoring subsequent close");
            }
        } finally {
            instanceStateLock.writeLock().unlock();
        }

        if (shouldClose) {
            metrics.close();

            for (MetricsCollector mc : metricsCollectors)
                mc.close();
        }
    }

    /**
     * Updates the {@link ClientTelemetrySubscription}, {@link #pushIntervalMs}, and
     * {@link #lastSubscriptionFetchMs}.
     *
     * <p/>
     *
     * The entire contents of the method are guarded by the {@link #instanceStateLock}.
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
            instanceStateLock.writeLock().lock();

            this.subscription = subscription;
            this.pushIntervalMs = subscription.pushIntervalMs();
            this.lastSubscriptionFetchMs = time.milliseconds();

            log.trace("Updating subscription - subscription: {}; pushIntervalMs: {}, lastFetchMs: {}", subscription, pushIntervalMs, lastSubscriptionFetchMs);

            // In some cases we have to wait for this signal in the clientInstanceId method so that
            // we know that we have a subscription to pull from.
            subscriptionLoaded.signalAll();
        } finally {
            instanceStateLock.writeLock().unlock();
        }
    }

    /**
     * Updates <em>only</em> the {@link #pushIntervalMs} and {@link #lastSubscriptionFetchMs}.
     *
     * <p/>
     *
     * The entire contents of the method are guarded by the {@link #instanceStateLock}.
     *
     * <p/>
     *
     * No condition is signaled here, unlike {@link #setSubscription(ClientTelemetrySubscription)}.
     */
    private void setFetchAndPushInterval(long pushIntervalMs) {
        try {
            instanceStateLock.writeLock().lock();

            this.pushIntervalMs = pushIntervalMs;
            this.lastSubscriptionFetchMs = time.milliseconds();

            log.trace("Updating pushIntervalMs: {}, lastFetchMs: {}", pushIntervalMs, lastSubscriptionFetchMs);
        } finally {
            instanceStateLock.writeLock().unlock();
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
            instanceStateLock.writeLock().lock();

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
            instanceStateLock.writeLock().unlock();
        }
    }

    private boolean isTerminatingState() {
        return state == ClientTelemetryState.terminated || state == ClientTelemetryState.terminating_push_needed || state == ClientTelemetryState.terminating_push_in_progress;
    }

    boolean maybeSetState(ClientTelemetryState newState) {
        try {
            instanceStateLock.writeLock().lock();

            ClientTelemetryState oldState = state;
            state = oldState.validateTransition(newState);
            log.trace("Setting telemetry state from {} to {}", oldState, newState);

            if (newState == ClientTelemetryState.terminating_push_in_progress)
                terminalPushInProgress.signalAll();

            return true;
        } catch (IllegalClientTelemetryStateException e) {
            log.warn("Error updating client telemetry state", e);
            return false;
        } finally {
            instanceStateLock.writeLock().unlock();
        }
    }

    // Visible for testing
    ClientTelemetryState state() {
        try {
            instanceStateLock.readLock().lock();
            return state;
        } finally {
            instanceStateLock.readLock().unlock();
        }
    }

    public void handleFailedRequest(ApiKeys apiKey, Optional<KafkaException> maybeFatalException) {
        KafkaException fatalError = maybeFatalException.orElse(null);
        log.warn("The broker generated an error for the {} network API request", apiKey.name, fatalError);

        try {
            instanceStateLock.writeLock().lock();

            if (isTerminatingState())
                return;

            boolean shouldWait;
            ClientTelemetryState newState;

            if (apiKey == ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS) {
                shouldWait = fatalError != null;
                newState = ClientTelemetryState.subscription_needed;
            } else if (apiKey == ApiKeys.PUSH_TELEMETRY) {
                if (state == ClientTelemetryState.push_in_progress) {
                    shouldWait = fatalError != null;
                    newState = ClientTelemetryState.subscription_needed;
                } else {
                    log.warn("Could not transition state after failed push telemetry from state {}", state);
                    return;
                }
            } else {
                log.warn("Could not transition state after failed request from state {}", state);
                return;
            }

            if (!maybeSetState(newState))
                return;

            // The broker does not support telemetry. Let's sleep for a while before
            // trying things again. We may disconnect from the broker and connect to
            // a broker that supports client telemetry.
            if (shouldWait)
                setFetchAndPushInterval(DEFAULT_PUSH_INTERVAL_MS);
        } finally {
            instanceStateLock.writeLock().unlock();
        }
    }

    public void handleResponse(GetTelemetrySubscriptionsResponseData data) {
        Optional<Long> pushIntervalMsOpt = ClientTelemetryUtils.errorPushIntervalMs(data.errorCode());

        // This is the error case...
        if (pushIntervalMsOpt.isPresent()) {
            setFetchAndPushInterval(pushIntervalMsOpt.get());
            return;
        }

        // This is the success case...
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

        try {
            instanceStateLock.writeLock().lock();

            // This is the case if we began termination sometime after the subscription request
            // was issued. We're just now getting our callback, but we need to ignore it.
            if (isTerminatingState())
                return;

            ClientTelemetryState newState;

            if (selector == ClientTelemetryUtils.SELECTOR_NO_METRICS) {
                // This is the case where no metrics are requested and/or match the filters. We need
                // to wait pushIntervalMs then retry.
                newState = ClientTelemetryState.subscription_needed;
            } else {
                newState = ClientTelemetryState.push_needed;
            }

            // If we're terminating, don't update state or set the subscription or update
            // any of the listeners.
            if (!maybeSetState(newState))
                return;

            setSubscription(clientTelemetrySubscription);
        } finally {
            instanceStateLock.writeLock().unlock();
        }

        invokeListeners(l -> l.getSubscriptionCompleted(clientTelemetrySubscription));
    }

    public void handleResponse(PushTelemetryResponseData data) {
        Optional<Long> pushIntervalMsOpt = ClientTelemetryUtils.errorPushIntervalMs(data.errorCode());

        // This is the success case...
        if (!pushIntervalMsOpt.isPresent()) {
            log.debug("Successfully pushed telemetry; response: {}", data);

            ClientTelemetrySubscription localSubscription;

            try {
                instanceStateLock.readLock().lock();
                localSubscription = subscription;
            } finally {
                instanceStateLock.readLock().unlock();
            }

            invokeListeners(l -> l.pushRequestCompleted(localSubscription.clientInstanceId()));
        }

        // This is the error case...
        try {
            instanceStateLock.writeLock().lock();

            // This is the case if we began termination sometime after the last push request
            // was issued. We're just now getting our callback, but we need to ignore it as all
            // of our state has already been updated.
            if (isTerminatingState())
                return;

            ClientTelemetryState newState;

            if (state == ClientTelemetryState.push_in_progress) {
                newState = ClientTelemetryState.subscription_needed;
            } else {
                log.warn("Could not transition state after failed push telemetry from state {}", state);
                return;
            }

            if (!maybeSetState(newState))
                return;

            pushIntervalMsOpt.ifPresent(this::setFetchAndPushInterval);
        } finally {
            instanceStateLock.writeLock().unlock();
        }
    }

    private int getPushIntervalMs(GetTelemetrySubscriptionsResponseData data) {
        final int pushIntervalMs = validatePushIntervalMs(data.pushIntervalMs() > 0 ? data.pushIntervalMs() : DEFAULT_PUSH_INTERVAL_MS);
        boolean isFirstRequest;

        try {
            instanceStateLock.readLock().lock();
            isFirstRequest = lastSubscriptionFetchMs == 0;
        } finally {
            instanceStateLock.readLock().unlock();
        }

        if (!isFirstRequest)
            return pushIntervalMs;

        // If this is the first request from this client, we want to attempt to spread out
        // the push requests between 50% and 150% of the push interval value from the broker.
        // This helps us to avoid the case where multiple clients are started at the same time
        // and end up sending all their data at the same time.
        ThreadLocalRandom r = ThreadLocalRandom.current();
        double rand = r.nextDouble(INITIAL_PUSH_JITTER_LOWER, INITIAL_PUSH_JITTER_UPPER);
        int firstPushIntervalMs = (int) Math.round(rand * pushIntervalMs);

        if (log.isDebugEnabled()) {
            String randFmt = String.format("%.0f", ((double) firstPushIntervalMs * (double) 100) / (double) pushIntervalMs);
            log.debug("Telemetry subscription push interval value from broker was {}; to stagger requests the first push interval is being adjusted to {} ({}% of {})",
                pushIntervalMs, firstPushIntervalMs, randFmt, pushIntervalMs);
        }

        return firstPushIntervalMs;
    }

    public long timeToNextUpdate(long requestTimeoutMs) {
        ClientTelemetryState localState;
        long localLastSubscriptionFetchMs;
        long localPushIntervalMs;

        try {
            instanceStateLock.readLock().lock();
            localState = state;
            localLastSubscriptionFetchMs = lastSubscriptionFetchMs;
            localPushIntervalMs = pushIntervalMs;
        } finally {
            instanceStateLock.readLock().unlock();
        }

        final long t;
        final String msg;

        switch (localState) {
            case subscription_in_progress:
            case push_in_progress:
            case terminating_push_in_progress: {
                // We have a network request in progress. We record the time of the request
                // submission, so wait that amount of the time PLUS the requestTimeout that
                // is provided.
                String apiName = localState == ClientTelemetryState.subscription_in_progress ? ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS.name : ApiKeys.PUSH_TELEMETRY.name;
                t = requestTimeoutMs;
                msg = String.format("this is the remaining wait time for the %s network API request, as specified by %s", apiName, CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG);
                break;
            }

            case terminating_push_needed: {
                t = 0;
                msg = String.format("this is to give the client a chance to submit the final %s network API request ASAP before closing", ApiKeys.PUSH_TELEMETRY.name);
                break;
            }

            case subscription_needed:
            case push_needed:
                long timeRemainingBeforeRequest = localLastSubscriptionFetchMs + localPushIntervalMs - time.milliseconds();
                String apiName = localState == ClientTelemetryState.subscription_needed ? ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS.name : ApiKeys.PUSH_TELEMETRY.name;

                if (timeRemainingBeforeRequest < 0) {
                    t = 0;
                    msg = String.format("the wait time before submitting the next %s network API request has elapsed", apiName);
                } else {
                    t = timeRemainingBeforeRequest;
                    msg = String.format("this is the time the client will wait before submitting the next %s network API request", apiName);
                }

                break;

            default:
                // Should never get to here
                t = Long.MAX_VALUE;
                msg = "this should not happen and likely signals an error";
        }

        log.trace("For telemetry state {}, returning the value {} ms; {}", localState, t, msg);

        return t;
    }

    public Optional<AbstractRequest.Builder<?>> createRequest() {
        ClientTelemetryState localState;
        ClientTelemetrySubscription localSubscription;

        try {
            instanceStateLock.readLock().lock();
            localState = state;
            localSubscription = subscription;
        } finally {
            instanceStateLock.readLock().unlock();
        }

        if (localState == ClientTelemetryState.subscription_needed) {
            Uuid clientInstanceId;

            // If we've previously retrieved a subscription, it will contain the client instance ID
            // that the broker assigned. Otherwise, per KIP-714, we send a special "null" UUID to
            // signal to the broker that we need to have a client instance ID assigned.
            if (localSubscription != null)
                clientInstanceId = localSubscription.clientInstanceId();
            else
                clientInstanceId = ZERO_UUID;

            try {
                instanceStateLock.writeLock().lock();

                if (isTerminatingState())
                    return Optional.empty();

                if (!maybeSetState(ClientTelemetryState.subscription_in_progress))
                    return Optional.empty();
            } finally {
                instanceStateLock.writeLock().unlock();
            }

            AbstractRequest.Builder<?> requestBuilder = new GetTelemetrySubscriptionRequest.Builder(clientInstanceId);
            log.debug("createRequest - created new {} network API request", requestBuilder.apiKey().name);
            invokeListeners(l -> l.getSubscriptionRequestCreated(clientInstanceId));
            return Optional.of(requestBuilder);
        } else if (localState == ClientTelemetryState.push_needed || localState == ClientTelemetryState.terminating_push_needed) {
            if (localSubscription == null) {
                log.warn("Telemetry state is {} but subscription is null; not sending telemetry", state);
                return Optional.empty();
            }

            List<Metric> emitted;
            byte[] payload;

            try (ClientTelemetryEmitter emitter = new ClientTelemetryEmitter(localSubscription.selector())) {
                emitter.init();

                for (MetricsCollector mc : metricsCollectors)
                    mc.collect(emitter);

                payload = emitter.payload(context.tags());
                emitted = emitter.emitted();
            }

            CompressionType compressionType = preferredCompressionType(localSubscription.acceptedCompressionTypes());
            ByteBuffer buf = serialize(payload, compressionType);
            Bytes metricsData =  Bytes.wrap(Utils.readBytes(buf));

            boolean terminating;

            try {
                instanceStateLock.writeLock().lock();

                // We've already been terminated, or we've already issued our last push, so we
                // should just exit now.
                if (this.state == ClientTelemetryState.terminated || this.state == ClientTelemetryState.terminating_push_in_progress)
                    return Optional.empty();

                // Check the *actual* state (while locked) to make sure we're still in the state
                // we expect to be in.
                terminating = state == ClientTelemetryState.terminating_push_needed;

                if (!maybeSetState(terminating ? ClientTelemetryState.terminating_push_in_progress : ClientTelemetryState.push_in_progress))
                    return Optional.empty();
            } finally {
                instanceStateLock.writeLock().unlock();
            }

            AbstractRequest.Builder<?> requestBuilder = new PushTelemetryRequest.Builder(localSubscription.clientInstanceId(),
                localSubscription.subscriptionId(),
                terminating,
                compressionType,
                metricsData);

            log.debug("createRequest - created new {} network API request", requestBuilder.apiKey().name);

            invokeListeners(l -> l.pushRequestCreated(localSubscription.clientInstanceId(),
                localSubscription.subscriptionId(),
                terminating,
                compressionType,
                emitted));
            return Optional.of(requestBuilder);
        } else {
            log.warn("Cannot make telemetry request as telemetry is in state: {}", localState);
            return Optional.empty();
        }
    }

}
