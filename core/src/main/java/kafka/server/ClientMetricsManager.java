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
package kafka.server;

import java.util.Collections;
import java.util.regex.Pattern;
import kafka.metrics.ClientMetricsConfigs;
import kafka.metrics.ClientMetricsInstance;
import kafka.metrics.ClientMetricsInstanceMetadata;
import kafka.metrics.ClientMetricsReceiverPlugin;

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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionsRequest;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionsResponse;
import org.apache.kafka.common.requests.PushTelemetryRequest;
import org.apache.kafka.common.requests.PushTelemetryResponse;
import org.apache.kafka.common.requests.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.CRC32;

/**
 * Handles client telemetry metrics requests/responses, subscriptions and instance information.
 */
public class ClientMetricsManager implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ClientMetricsManager.class);
    private static final ClientMetricsManager INSTANCE = new ClientMetricsManager();

    public static ClientMetricsManager instance() {
        return INSTANCE;
    }
    // Max cache size (16k active client connections per broker)
    private static final int CM_CACHE_MAX_SIZE = 16384;
    private final Cache<Uuid, ClientMetricsInstance> clientInstanceCache;
    private final Map<String, SubscriptionInfo> subscriptionMap;

    // The last subscription updated time is used to determine if the next telemetry request needs
    // to re-evaluate the subscription id as per changes subscriptions.
    private long lastSubscriptionUpdateEpoch;

    // Visible for testing
    ClientMetricsManager() {
        subscriptionMap = new ConcurrentHashMap<>();
        clientInstanceCache = new SynchronizedCache<>(new LRUCache<>(CM_CACHE_MAX_SIZE));
    }

    public void updateSubscription(String subscriptionName, Properties properties) {
        // IncrementalAlterConfigs API will send empty configs when all the configs are deleted
        // for respective subscription. In that case, we need to remove the subscription from the map.
        if (properties.isEmpty()) {
            // Remove the subscription from the map if it exists, else ignore the config update.
            if (subscriptionMap.containsKey(subscriptionName)) {
                log.info("Removing subscription [{}] from the subscription map", subscriptionName);
                subscriptionMap.remove(subscriptionName);
                updateLastSubscriptionUpdateEpoch();
            }
            return;
        }

        ClientMetricsConfigs configs = new ClientMetricsConfigs(properties);
        updateClientSubscription(subscriptionName, configs);
        /*
         Update last subscription updated time to current time to indicate that there is a change
         in the subscription. This will be used to determine if the next telemetry request needs
         to re-evaluate the subscription id as per changes subscriptions.
        */
        updateLastSubscriptionUpdateEpoch();
    }

    public GetTelemetrySubscriptionsResponse processGetTelemetrySubscriptionRequest(
        GetTelemetrySubscriptionsRequest request, int telemetryMaxBytes, RequestContext requestContext, int throttleMs) {

        long now = System.currentTimeMillis();
        Uuid clientInstanceId = Optional.ofNullable(request.data().clientInstanceId())
            .filter(id -> !id.equals(Uuid.ZERO_UUID))
            .orElse(generateNewClientId());

        /*
         Get the client instance from the cache or create a new one. If subscription has changed
         since the last request, then the client instance will be re-evaluated. Validation of the
         request will be done after the client instance is created. If client issued get telemetry
         request prior to push interval, then the client should get a throttle error but if the
         subscription has changed since the last request then the client should get the updated
         subscription immediately.
        */
        ClientMetricsInstance clientInstance = getClientInstance(clientInstanceId, requestContext, now);

        try {
            // Validate the get request parameters for the client instance.
            validateGetRequest(request, clientInstance);
        } catch (ApiException exception) {
            return request.getErrorResponse(throttleMs, exception);
        } finally {
            clientInstance.lastGetRequestEpoch(now);
        }

        clientInstance.lastKnownError(Errors.NONE);
        return createGetSubscriptionResponse(clientInstanceId, clientInstance, telemetryMaxBytes, throttleMs);
    }

    public PushTelemetryResponse processPushTelemetryRequest(PushTelemetryRequest request,
        int telemetryMaxBytes, RequestContext requestContext, int throttleMs) {

        Uuid clientInstanceId = request.data().clientInstanceId();
        if (clientInstanceId == null || clientInstanceId == Uuid.ZERO_UUID) {
            String msg = String.format("Invalid request from the client [%s], invalid client instance id",
                clientInstanceId);
            return request.getErrorResponse(throttleMs, new InvalidRequestException(msg));
        }

        long now = System.currentTimeMillis();
        ClientMetricsInstance clientInstance = getClientInstance(clientInstanceId, requestContext, now);

        try {
            // Validate the push request parameters for the client instance.
            validatePushRequest(request, telemetryMaxBytes, clientInstance);
        } catch (ApiException exception) {
            clientInstance.lastKnownError(Errors.forException(exception));
            return request.getErrorResponse(throttleMs, exception);
        } finally {
            // Update the client instance with the latest push request parameters.
            clientInstance.terminating(request.data().terminating());
            clientInstance.lastPushRequestEpoch(now);
        }

        // Push the metrics to the external client receiver plugin.
        byte[] metrics = request.data().metrics();
        if (metrics != null && metrics.length > 0) {
            ClientMetricsReceiverPlugin.exportMetrics(requestContext, request);
        }

        clientInstance.lastKnownError(Errors.NONE);
        return request.createResponse(throttleMs, Errors.NONE);
    }

    @Override
    public void close() throws IOException {
        // Do nothing for now.
    }

    private void updateLastSubscriptionUpdateEpoch() {
        this.lastSubscriptionUpdateEpoch = System.currentTimeMillis();
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

    private ClientMetricsInstance getClientInstance(Uuid clientInstanceId, RequestContext requestContext,
        long timestamp) {
        // Check if null can be called on the cache. if can then we can avoid the method call.
        ClientMetricsInstance clientInstance = clientInstanceCache.get(clientInstanceId);

        if (clientInstance == null) {
            // If the client instance is not present in the cache, then create a new client instance
            // and update the cache. This can also happen when the telemetry request is received by
            // the separate broker instance.
            ClientMetricsInstanceMetadata instanceMetadata = new ClientMetricsInstanceMetadata(
                clientInstanceId, requestContext);
            clientInstance = createClientInstanceAndUpdateCache(clientInstanceId, instanceMetadata, timestamp);
        } else if (clientInstance.subscriptionUpdateEpoch() < lastSubscriptionUpdateEpoch) {
            /*
             If the last subscription update time for client instance is older than the subscription
             updated time, then re-evaluate the subscription information for the client as per the
             updated subscriptions. This is to ensure that the client instance is always in sync with
             the latest subscription information.
            */
            clientInstance = createClientInstanceAndUpdateCache(clientInstanceId, clientInstance.instanceMetadata(), timestamp);
        }

        return clientInstance;
    }

    private ClientMetricsInstance createClientInstanceAndUpdateCache(Uuid clientInstanceId,
        ClientMetricsInstanceMetadata instanceMetadata, long timestamp) {

        ClientMetricsInstance clientInstance = createClientInstance(clientInstanceId, instanceMetadata,
            timestamp);
        clientInstanceCache.put(clientInstanceId, clientInstance);
        return clientInstance;
    }

    private ClientMetricsInstance createClientInstance(Uuid clientInstanceId,
        ClientMetricsInstanceMetadata instanceMetadata, long timestamp) {

        int pushIntervalMs = ClientMetricsConfigs.DEFAULT_INTERVAL_MS;
        // Keep a set of metrics to avoid duplicates in case of overlapping subscriptions.
        Set<String> subscribedMetrics = new HashSet<>();
        boolean allMetricsSubscribed = false;

        for (SubscriptionInfo info : subscriptions()) {
            if (instanceMetadata.isMatch(info.matchPattern())) {
                allMetricsSubscribed = allMetricsSubscribed || info.metrics().contains(
                    ClientMetricsConfigs.ALL_SUBSCRIBED_METRICS);
                subscribedMetrics.addAll(info.metrics());
                pushIntervalMs = Math.min(pushIntervalMs, info.intervalMs());
            }
        }

        /*
         If client matches with any subscription that has empty metrics string, then it means that client
         is subscribed to all the metrics, so just send the empty string as the subscribed metrics.
        */
        if (allMetricsSubscribed) {
            subscribedMetrics.clear();
            subscribedMetrics.add(ClientMetricsConfigs.ALL_SUBSCRIBED_METRICS);
        }

        int subscriptionId = computeSubscriptionId(subscribedMetrics, pushIntervalMs, clientInstanceId);

        return new ClientMetricsInstance(clientInstanceId, instanceMetadata, subscriptionId, timestamp,
            subscribedMetrics, pushIntervalMs);
    }

    /**
     * Computes the SubscriptionId as a unique identifier for a client instance's subscription set,
     * the id is generated by calculating a CRC32 of the configured metrics subscriptions including
     * the PushIntervalMs, XORed with the ClientInstanceId.
     */
    private int computeSubscriptionId(Set<String> metrics, int pushIntervalMs, Uuid clientInstanceId) {
        CRC32 crc = new CRC32();
        byte[] metricsBytes = (metrics.toString() + pushIntervalMs).getBytes(StandardCharsets.UTF_8);
        crc.update(ByteBuffer.wrap(metricsBytes));
        return (int) crc.getValue() ^ clientInstanceId.hashCode();
    }

    private GetTelemetrySubscriptionsResponse createGetSubscriptionResponse(Uuid clientInstanceId,
        ClientMetricsInstance clientInstance, int telemetryMaxBytes, int throttleMs) {

        GetTelemetrySubscriptionsResponseData data = new GetTelemetrySubscriptionsResponseData()
            .setClientInstanceId(clientInstanceId)
            .setSubscriptionId(clientInstance.subscriptionId())
            .setRequestedMetrics(new ArrayList<>(clientInstance.metrics()))
            .setAcceptedCompressionTypes(getSupportedCompressionTypes())
            .setPushIntervalMs(clientInstance.pushIntervalMs())
            .setTelemetryMaxBytes(telemetryMaxBytes)
            .setDeltaTemporality(true)
            .setErrorCode(Errors.NONE.code())
            .setThrottleTimeMs(throttleMs);

        return new GetTelemetrySubscriptionsResponse(data);
    }

    private void validateGetRequest(GetTelemetrySubscriptionsRequest request,
        ClientMetricsInstance clientInstance) {

        if (!clientInstance.canAcceptGetRequest() && clientInstance.lastKnownError() != Errors.UNKNOWN_SUBSCRIPTION_ID) {
            String msg = String.format("Request from the client [%s] arrived before the next push interval time",
                request.data().clientInstanceId());
            throw new ThrottlingQuotaExceededException(msg);
        }
    }

    private void validatePushRequest(PushTelemetryRequest request, int telemetryMaxBytes,
        ClientMetricsInstance clientInstance) {

        if (clientInstance.terminating()) {
            String msg = String.format(
                "Client [%s] sent the previous request with state terminating to TRUE, can not accept"
                    + "any requests after that", request.data().clientInstanceId());
            throw new InvalidRequestException(msg);
        }

        if (!clientInstance.canAcceptPushRequest() && !request.data().terminating()) {
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

        if (request.data().metrics() != null && request.data().metrics().length > telemetryMaxBytes) {
            String msg = String.format("Telemetry request from [%s] is larger than the maximum allowed size [%s]",
                request.data().clientInstanceId(), telemetryMaxBytes);
            throw new TelemetryTooLargeException(msg);
        }
    }

    private List<Byte> getSupportedCompressionTypes() {
        List<Byte> compressionTypes = new ArrayList<>();
        for (CompressionType compressionType : CompressionType.values()) {
            if (compressionType != CompressionType.NONE) {
                compressionTypes.add(compressionType.id);
            }
        }
        return compressionTypes;
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
    long lastSubscriptionUpdateEpoch() {
        return lastSubscriptionUpdateEpoch;
    }

    // Visible for testing
    ClientMetricsInstance clientInstance(Uuid clientInstanceId) {
        return clientInstanceCache.get(clientInstanceId);
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
