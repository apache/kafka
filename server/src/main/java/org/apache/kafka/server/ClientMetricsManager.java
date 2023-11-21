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
package org.apache.kafka.server;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.TelemetryTooLargeException;
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException;
import org.apache.kafka.common.errors.UnknownSubscriptionIdException;
import org.apache.kafka.common.errors.UnsupportedCompressionTypeException;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData;
import org.apache.kafka.common.message.PushTelemetryResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionsRequest;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionsResponse;
import org.apache.kafka.common.requests.PushTelemetryRequest;
import org.apache.kafka.common.requests.PushTelemetryResponse;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.utils.Crc32C;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.ClientMetricsConfigs;
import org.apache.kafka.server.metrics.ClientMetricsInstance;
import org.apache.kafka.server.metrics.ClientMetricsInstanceMetadata;
import org.apache.kafka.server.metrics.ClientMetricsReceiverPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * Handles client telemetry metrics requests/responses, subscriptions and instance information.
 */
public class ClientMetricsManager implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ClientMetricsManager.class);
    private static final List<Byte> SUPPORTED_COMPRESSION_TYPES = Collections.unmodifiableList(
        Arrays.asList(CompressionType.ZSTD.id, CompressionType.LZ4.id, CompressionType.GZIP.id, CompressionType.SNAPPY.id));
    // Max cache size (16k active client connections per broker)
    private static final int CM_CACHE_MAX_SIZE = 16384;

    private final ClientMetricsReceiverPlugin receiverPlugin;
    private final Cache<Uuid, ClientMetricsInstance> clientInstanceCache;
    private final Map<String, SubscriptionInfo> subscriptionMap;
    private final int clientTelemetryMaxBytes;
    private final Time time;

    // The latest subscription version is used to determine if subscription has changed and needs
    // to re-evaluate the client instance subscription id as per changed subscriptions.
    private final AtomicInteger subscriptionUpdateVersion;

    public ClientMetricsManager(ClientMetricsReceiverPlugin receiverPlugin, int clientTelemetryMaxBytes, Time time) {
        this.receiverPlugin = receiverPlugin;
        this.subscriptionMap = new ConcurrentHashMap<>();
        this.subscriptionUpdateVersion = new AtomicInteger(0);
        this.clientInstanceCache = new SynchronizedCache<>(new LRUCache<>(CM_CACHE_MAX_SIZE));
        this.clientTelemetryMaxBytes = clientTelemetryMaxBytes;
        this.time = time;
    }

    public void updateSubscription(String subscriptionName, Properties properties) {
        // Validate the subscription properties.
        ClientMetricsConfigs.validate(subscriptionName, properties);
        // IncrementalAlterConfigs API will send empty configs when all the configs are deleted
        // for respective subscription. In that case, we need to remove the subscription from the map.
        if (properties.isEmpty()) {
            // Remove the subscription from the map if it exists, else ignore the config update.
            if (subscriptionMap.containsKey(subscriptionName)) {
                log.info("Removing subscription [{}] from the subscription map", subscriptionName);
                subscriptionMap.remove(subscriptionName);
                subscriptionUpdateVersion.incrementAndGet();
            }
            return;
        }

        updateClientSubscription(subscriptionName, new ClientMetricsConfigs(properties));
        /*
         Increment subscription update version to indicate that there is a change in the subscription. This will
         be used to determine if the next telemetry request needs to re-evaluate the subscription id
         as per the changed subscriptions.
        */
        subscriptionUpdateVersion.incrementAndGet();
    }

    public GetTelemetrySubscriptionsResponse processGetTelemetrySubscriptionRequest(
        GetTelemetrySubscriptionsRequest request, RequestContext requestContext) {

        long now = time.milliseconds();
        Uuid clientInstanceId = Optional.ofNullable(request.data().clientInstanceId())
            .filter(id -> !id.equals(Uuid.ZERO_UUID))
            .orElse(generateNewClientId());

        /*
         Get the client instance from the cache or create a new one. If subscription has changed
         since the last request, then the client instance will be re-evaluated. Validation of the
         request will be done after the client instance is created. If client issues another get
         telemetry request prior to push interval, then the client should get a throttle error but if
         the subscription has changed since the last request then the client should get the updated
         subscription immediately.
        */
        ClientMetricsInstance clientInstance = clientInstance(clientInstanceId, requestContext);

        try {
            // Validate the get request parameters for the client instance.
            validateGetRequest(request, clientInstance, now);
        } catch (ApiException exception) {
            return request.getErrorResponse(0, exception);
        }

        clientInstance.lastKnownError(Errors.NONE);
        return createGetSubscriptionResponse(clientInstanceId, clientInstance);
    }

    public PushTelemetryResponse processPushTelemetryRequest(PushTelemetryRequest request, RequestContext requestContext) {

        Uuid clientInstanceId = request.data().clientInstanceId();
        if (clientInstanceId == null || Uuid.RESERVED.contains(clientInstanceId)) {
            String msg = String.format("Invalid request from the client [%s], invalid client instance id",
                clientInstanceId);
            return request.getErrorResponse(0, new InvalidRequestException(msg));
        }

        long now = time.milliseconds();
        ClientMetricsInstance clientInstance = clientInstance(clientInstanceId, requestContext);

        try {
            // Validate the push request parameters for the client instance.
            validatePushRequest(request, clientInstance, now);
        } catch (ApiException exception) {
            log.debug("Error validating push telemetry request from client [{}]", clientInstanceId, exception);
            clientInstance.lastKnownError(Errors.forException(exception));
            return request.getErrorResponse(0, exception);
        } finally {
            // Update the client instance with the latest push request parameters.
            clientInstance.terminating(request.data().terminating());
        }

        // Push the metrics to the external client receiver plugin.
        byte[] metrics = request.data().metrics();
        if (metrics != null && metrics.length > 0) {
            try {
                receiverPlugin.exportMetrics(requestContext, request);
            } catch (Exception exception) {
                clientInstance.lastKnownError(Errors.INVALID_RECORD);
                return request.errorResponse(0, Errors.INVALID_RECORD);
            }
        }

        clientInstance.lastKnownError(Errors.NONE);
        return new PushTelemetryResponse(new PushTelemetryResponseData());
    }

    public boolean isTelemetryReceiverConfigured() {
        return !receiverPlugin.isEmpty();
    }

    @Override
    public void close() throws IOException {
        subscriptionMap.clear();
    }

    private void updateClientSubscription(String subscriptionName, ClientMetricsConfigs configs) {
        List<String> metrics = configs.getList(ClientMetricsConfigs.SUBSCRIPTION_METRICS);
        int pushInterval = configs.getInt(ClientMetricsConfigs.PUSH_INTERVAL_MS);
        List<String> clientMatchPattern = configs.getList(ClientMetricsConfigs.CLIENT_MATCH_PATTERN);

        SubscriptionInfo newSubscription =
            new SubscriptionInfo(subscriptionName, metrics, pushInterval,
                ClientMetricsConfigs.parseMatchingPatterns(clientMatchPattern));

        subscriptionMap.put(subscriptionName, newSubscription);
    }

    private Uuid generateNewClientId() {
        Uuid id = Uuid.randomUuid();
        while (clientInstanceCache.get(id) != null) {
            id = Uuid.randomUuid();
        }
        return id;
    }

    private ClientMetricsInstance clientInstance(Uuid clientInstanceId, RequestContext requestContext) {
        ClientMetricsInstance clientInstance = clientInstanceCache.get(clientInstanceId);

        if (clientInstance == null) {
            /*
             If the client instance is not present in the cache, then create a new client instance
             and update the cache. This can also happen when the telemetry request is received by
             the separate broker instance.
             Though cache is synchronized, but it is possible that concurrent calls can create the same
             client instance. Hence, safeguard the client instance creation with a double-checked lock
             to ensure that only one instance is created.
            */
            synchronized (this) {
                clientInstance = clientInstanceCache.get(clientInstanceId);
                if (clientInstance != null) {
                    return clientInstance;
                }

                ClientMetricsInstanceMetadata instanceMetadata = new ClientMetricsInstanceMetadata(
                    clientInstanceId, requestContext);
                clientInstance = createClientInstanceAndUpdateCache(clientInstanceId, instanceMetadata);
            }
        } else if (clientInstance.subscriptionVersion() < subscriptionUpdateVersion.get()) {
            /*
             If the last subscription update version for client instance is older than the subscription
             updated version, then re-evaluate the subscription information for the client as per the
             updated subscriptions. This is to ensure that the client instance is always in sync with
             the latest subscription information.
             Though cache is synchronized, but it is possible that concurrent calls can create the same
             client instance. Hence, safeguard the client instance update with a double-checked lock
              to ensure that only one instance is created.
            */
            synchronized (this) {
                clientInstance = clientInstanceCache.get(clientInstanceId);
                if (clientInstance.subscriptionVersion() >= subscriptionUpdateVersion.get()) {
                    return clientInstance;
                }
                clientInstance = createClientInstanceAndUpdateCache(clientInstanceId, clientInstance.instanceMetadata());
            }
        }

        return clientInstance;
    }

    private ClientMetricsInstance createClientInstanceAndUpdateCache(Uuid clientInstanceId,
        ClientMetricsInstanceMetadata instanceMetadata) {

        ClientMetricsInstance clientInstance = createClientInstance(clientInstanceId, instanceMetadata);
        clientInstanceCache.put(clientInstanceId, clientInstance);
        return clientInstance;
    }

    private ClientMetricsInstance createClientInstance(Uuid clientInstanceId, ClientMetricsInstanceMetadata instanceMetadata) {

        int pushIntervalMs = ClientMetricsConfigs.DEFAULT_INTERVAL_MS;
        // Keep a set of metrics to avoid duplicates in case of overlapping subscriptions.
        Set<String> subscribedMetrics = new HashSet<>();
        boolean allMetricsSubscribed = false;

        int currentSubscriptionVersion = subscriptionUpdateVersion.get();
        for (SubscriptionInfo info : subscriptionMap.values()) {
            if (instanceMetadata.isMatch(info.matchPattern())) {
                allMetricsSubscribed = allMetricsSubscribed || info.metrics().contains(
                    ClientMetricsConfigs.ALL_SUBSCRIBED_METRICS_CONFIG);
                subscribedMetrics.addAll(info.metrics());
                pushIntervalMs = Math.min(pushIntervalMs, info.intervalMs());
            }
        }

        /*
         If client matches with any subscription that has * metrics string, then it means that client
         is subscribed to all the metrics, so just send the * string as the subscribed metrics.
        */
        if (allMetricsSubscribed) {
            // Only add an * to indicate that all metrics are subscribed.
            subscribedMetrics.clear();
            subscribedMetrics.add(ClientMetricsConfigs.ALL_SUBSCRIBED_METRICS_CONFIG);
        }

        int subscriptionId = computeSubscriptionId(subscribedMetrics, pushIntervalMs, clientInstanceId);

        return new ClientMetricsInstance(clientInstanceId, instanceMetadata, subscriptionId,
            currentSubscriptionVersion, subscribedMetrics, pushIntervalMs);
    }

    /**
     * Computes the SubscriptionId as a unique identifier for a client instance's subscription set,
     * the id is generated by calculating a CRC32C of the configured metrics subscriptions including
     * the PushIntervalMs, XORed with the ClientInstanceId.
     */
    private int computeSubscriptionId(Set<String> metrics, int pushIntervalMs, Uuid clientInstanceId) {
        byte[] metricsBytes = (metrics.toString() + pushIntervalMs).getBytes(StandardCharsets.UTF_8);
        long computedCrc = Crc32C.compute(metricsBytes, 0, metricsBytes.length);
        return (int) computedCrc ^ clientInstanceId.hashCode();
    }

    private GetTelemetrySubscriptionsResponse createGetSubscriptionResponse(Uuid clientInstanceId,
        ClientMetricsInstance clientInstance) {

        GetTelemetrySubscriptionsResponseData data = new GetTelemetrySubscriptionsResponseData()
            .setClientInstanceId(clientInstanceId)
            .setSubscriptionId(clientInstance.subscriptionId())
            .setRequestedMetrics(new ArrayList<>(clientInstance.metrics()))
            .setAcceptedCompressionTypes(SUPPORTED_COMPRESSION_TYPES)
            .setPushIntervalMs(clientInstance.pushIntervalMs())
            .setTelemetryMaxBytes(clientTelemetryMaxBytes)
            .setDeltaTemporality(true)
            .setErrorCode(Errors.NONE.code());

        return new GetTelemetrySubscriptionsResponse(data);
    }

    private void validateGetRequest(GetTelemetrySubscriptionsRequest request,
        ClientMetricsInstance clientInstance, long timestamp) {

        if (!clientInstance.maybeUpdateGetRequestTimestamp(timestamp) && (clientInstance.lastKnownError() != Errors.UNKNOWN_SUBSCRIPTION_ID
            || clientInstance.lastKnownError() != Errors.UNSUPPORTED_COMPRESSION_TYPE)) {
            String msg = String.format("Request from the client [%s] arrived before the next push interval time",
                request.data().clientInstanceId());
            throw new ThrottlingQuotaExceededException(msg);
        }
    }

    private void validatePushRequest(PushTelemetryRequest request, ClientMetricsInstance clientInstance, long timestamp) {

        if (clientInstance.terminating()) {
            String msg = String.format(
                "Client [%s] sent the previous request with state terminating to TRUE, can not accept"
                    + "any requests after that", request.data().clientInstanceId());
            throw new InvalidRequestException(msg);
        }

        if (!clientInstance.maybeUpdatePushRequestTimestamp(timestamp) && !request.data().terminating()) {
            String msg = String.format("Request from the client [%s] arrived before the next push interval time",
                request.data().clientInstanceId());
            throw new ThrottlingQuotaExceededException(msg);
        }

        if (request.data().subscriptionId() != clientInstance.subscriptionId()) {
            String msg = String.format("Unknown client subscription id for the client [%s]",
                request.data().clientInstanceId());
            throw new UnknownSubscriptionIdException(msg);
        }

        if (!isSupportedCompressionType(request.data().compressionType())) {
            String msg = String.format("Unknown compression type [%s] is received in telemetry request from [%s]",
                request.data().compressionType(), request.data().clientInstanceId());
            throw new UnsupportedCompressionTypeException(msg);
        }

        if (request.data().metrics() != null && request.data().metrics().length > clientTelemetryMaxBytes) {
            String msg = String.format("Telemetry request from [%s] is larger than the maximum allowed size [%s]",
                request.data().clientInstanceId(), clientTelemetryMaxBytes);
            throw new TelemetryTooLargeException(msg);
        }
    }

    private static boolean isSupportedCompressionType(int id) {
        try {
            CompressionType.forId(id);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    // Visible for testing
    SubscriptionInfo subscriptionInfo(String subscriptionName) {
        return subscriptionMap.get(subscriptionName);
    }

    // Visible for testing
    Collection<SubscriptionInfo> subscriptions() {
        return Collections.unmodifiableCollection(subscriptionMap.values());
    }

    // Visible for testing
    ClientMetricsInstance clientInstance(Uuid clientInstanceId) {
        return clientInstanceCache.get(clientInstanceId);
    }

    // Visible for testing
    int subscriptionUpdateVersion() {
        return subscriptionUpdateVersion.get();
    }

    public static class SubscriptionInfo {

        private final String name;
        private final Set<String> metrics;
        private final int intervalMs;
        private final Map<String, Pattern> matchPattern;

        public SubscriptionInfo(String name, List<String> metrics, int intervalMs,
            Map<String, Pattern> matchPattern) {
            this.name = name;
            this.metrics = new HashSet<>(metrics);
            this.intervalMs = intervalMs;
            this.matchPattern = matchPattern;
        }

        public String name() {
            return name;
        }

        public Set<String> metrics() {
            return metrics;
        }

        public int intervalMs() {
            return intervalMs;
        }

        public Map<String, Pattern> matchPattern() {
            return matchPattern;
        }
    }
}
