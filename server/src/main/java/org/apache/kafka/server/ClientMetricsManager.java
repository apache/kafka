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

import org.apache.kafka.common.MetricName;
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
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.SampledStat;
import org.apache.kafka.common.metrics.stats.WindowedCount;
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
import org.apache.kafka.server.network.ConnectionDisconnectListener;
import org.apache.kafka.server.util.timer.SystemTimer;
import org.apache.kafka.server.util.timer.SystemTimerReaper;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.server.util.timer.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Handles client telemetry metrics requests/responses, subscriptions and instance information.
 */
public class ClientMetricsManager implements AutoCloseable {
    public static final String CLIENT_METRICS_REAPER_THREAD_NAME = "client-metrics-reaper";

    private static final Logger log = LoggerFactory.getLogger(ClientMetricsManager.class);
    private static final List<Byte> SUPPORTED_COMPRESSION_TYPES = Collections.unmodifiableList(
        Arrays.asList(CompressionType.ZSTD.id, CompressionType.LZ4.id, CompressionType.GZIP.id, CompressionType.SNAPPY.id));
    // Max cache size (16k active client connections per broker)
    private static final int CACHE_MAX_SIZE = 16384;
    private static final int DEFAULT_CACHE_EXPIRY_MS = 60 * 1000;

    private final ClientMetricsReceiverPlugin receiverPlugin;
    private final Cache<Uuid, ClientMetricsInstance> clientInstanceCache;
    private final Map<String, Uuid> clientConnectionIdMap;
    private final Timer expirationTimer;
    private final Map<String, SubscriptionInfo> subscriptionMap;
    private final int clientTelemetryMaxBytes;
    private final Time time;
    private final int cacheExpiryMs;
    private final AtomicLong lastCacheErrorLogMs;
    private final Metrics metrics;
    private final ClientMetricsStats clientMetricsStats;
    private final ConnectionDisconnectListener connectionDisconnectListener;

    // The latest subscription version is used to determine if subscription has changed and needs
    // to re-evaluate the client instance subscription id as per changed subscriptions.
    private final AtomicInteger subscriptionUpdateVersion;

    public ClientMetricsManager(ClientMetricsReceiverPlugin receiverPlugin, int clientTelemetryMaxBytes, Time time, Metrics metrics) {
        this(receiverPlugin, clientTelemetryMaxBytes, time, DEFAULT_CACHE_EXPIRY_MS, metrics);
    }

    // Visible for testing
    ClientMetricsManager(ClientMetricsReceiverPlugin receiverPlugin, int clientTelemetryMaxBytes, Time time, int cacheExpiryMs, Metrics metrics) {
        this.receiverPlugin = receiverPlugin;
        this.subscriptionMap = new ConcurrentHashMap<>();
        this.subscriptionUpdateVersion = new AtomicInteger(0);
        this.clientInstanceCache = new SynchronizedCache<>(new LRUCache<>(CACHE_MAX_SIZE));
        this.clientConnectionIdMap = new ConcurrentHashMap<>();
        this.expirationTimer = new SystemTimerReaper(CLIENT_METRICS_REAPER_THREAD_NAME, new SystemTimer("client-metrics"));
        this.clientTelemetryMaxBytes = clientTelemetryMaxBytes;
        this.time = time;
        this.cacheExpiryMs = cacheExpiryMs;
        this.lastCacheErrorLogMs = new AtomicLong(0);
        this.metrics = metrics;
        this.clientMetricsStats = new ClientMetricsStats();
        this.connectionDisconnectListener = new ClientConnectionDisconnectListener();
    }

    public Set<String> listClientMetricsResources() {
        return subscriptionMap.keySet();
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
        ByteBuffer metrics = request.data().metrics();
        if (metrics != null && metrics.limit() > 0) {
            try {
                long exportTimeStartMs = time.hiResClockMs();
                receiverPlugin.exportMetrics(requestContext, request);
                clientMetricsStats.recordPluginExport(clientInstanceId, time.hiResClockMs() - exportTimeStartMs);
            } catch (Exception exception) {
                clientMetricsStats.recordPluginErrorCount(clientInstanceId);
                clientInstance.lastKnownError(Errors.INVALID_RECORD);
                log.error("Error exporting client metrics to the plugin for client instance id: {}", clientInstanceId, exception);
                return request.errorResponse(0, Errors.INVALID_RECORD);
            }
        }

        clientInstance.lastKnownError(Errors.NONE);
        return new PushTelemetryResponse(new PushTelemetryResponseData());
    }

    public boolean isTelemetryReceiverConfigured() {
        return !receiverPlugin.isEmpty();
    }

    public ConnectionDisconnectListener connectionDisconnectListener() {
        return connectionDisconnectListener;
    }

    @Override
    public void close() throws Exception {
        subscriptionMap.clear();
        expirationTimer.close();
        clientMetricsStats.unregisterMetrics();
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
                clientInstance = createClientInstanceAndUpdateCache(clientInstanceId, instanceMetadata, requestContext.connectionId());
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
                // Cancel the existing expiration timer task for the old client instance.
                clientInstance.cancelExpirationTimerTask();
                clientInstance = createClientInstanceAndUpdateCache(clientInstanceId, clientInstance.instanceMetadata(), requestContext.connectionId());
            }
        }

        // Update the expiration timer task for the client instance.
        long expirationTimeMs = Math.max(cacheExpiryMs, clientInstance.pushIntervalMs() * 3);
        TimerTask timerTask = new ExpirationTimerTask(clientInstanceId, requestContext.connectionId(), expirationTimeMs);
        clientInstance.updateExpirationTimerTask(timerTask);
        expirationTimer.add(timerTask);

        return clientInstance;
    }

    private ClientMetricsInstance createClientInstanceAndUpdateCache(Uuid clientInstanceId,
        ClientMetricsInstanceMetadata instanceMetadata, String connectionId) {

        ClientMetricsInstance clientInstance = createClientInstance(clientInstanceId, instanceMetadata);
        // Maybe add client metrics, if metrics not already added. Metrics might be already added
        // if the client instance was evicted from the cache because of size limit.
        clientMetricsStats.maybeAddClientInstanceMetrics(clientInstanceId);
        clientInstanceCache.put(clientInstanceId, clientInstance);
        clientConnectionIdMap.put(connectionId, clientInstanceId);
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
            clientMetricsStats.recordThrottleCount(clientInstance.clientInstanceId());
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
            clientMetricsStats.recordThrottleCount(clientInstance.clientInstanceId());
            String msg = String.format("Request from the client [%s] arrived before the next push interval time",
                request.data().clientInstanceId());
            throw new ThrottlingQuotaExceededException(msg);
        }

        if (request.data().subscriptionId() != clientInstance.subscriptionId()) {
            clientMetricsStats.recordUnknownSubscriptionCount();
            String msg = String.format("Unknown client subscription id for the client [%s]",
                request.data().clientInstanceId());
            throw new UnknownSubscriptionIdException(msg);
        }

        if (!isSupportedCompressionType(request.data().compressionType())) {
            String msg = String.format("Unknown compression type [%s] is received in telemetry request from [%s]",
                request.data().compressionType(), request.data().clientInstanceId());
            throw new UnsupportedCompressionTypeException(msg);
        }

        if (request.data().metrics() != null && request.data().metrics().limit() > clientTelemetryMaxBytes) {
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

    // Visible for testing
    Timer expirationTimer() {
        return expirationTimer;
    }

    // Visible for testing
    Map<String, Uuid> clientConnectionIdMap() {
        return clientConnectionIdMap;
    }

    private final class ClientConnectionDisconnectListener implements ConnectionDisconnectListener {

        @Override
        public void onDisconnect(String connectionId) {
            log.trace("Removing client connection id [{}] from the client instance cache", connectionId);

            Uuid clientInstanceId = clientConnectionIdMap.remove(connectionId);
            if (clientInstanceId == null) {
                log.trace("Client connection id [{}] is not found in the client instance cache", connectionId);
                return;
            }

            // Unregister the client instance metrics from the broker metrics.
            clientMetricsStats.unregisterClientInstanceMetrics(clientInstanceId);

            ClientMetricsInstance clientInstance = clientInstanceCache.get(clientInstanceId);
            if (clientInstance != null) {
                clientInstance.cancelExpirationTimerTask();
                clientInstanceCache.remove(clientInstanceId);
            }
        }
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

    private final class ExpirationTimerTask extends TimerTask {

        private static final long CACHE_ERROR_LOG_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes

        private final Uuid clientInstanceId;
        private final String connectionId;

        private ExpirationTimerTask(Uuid clientInstanceId, String connectionId, long delayMs) {
            super(delayMs);
            this.clientInstanceId = clientInstanceId;
            this.connectionId = connectionId;
        }

        @Override
        public void run() {
            log.trace("Expiration timer task run for client instance id: {}, after delay ms: {}", clientInstanceId, delayMs);
            clientMetricsStats.unregisterClientInstanceMetrics(clientInstanceId);
            clientConnectionIdMap.remove(connectionId);
            if (!clientInstanceCache.remove(clientInstanceId)) {
                /*
                 This can only happen if the client instance is removed from the cache by the LRU
                 eviction policy before the expiration timer task is executed. Log a warning as broker
                 cache is not able to hold all the client instances. Log only once every CACHE_ERROR_LOG_INTERVAL_MS
                 to avoid flooding the logs.
                */
                long lastErrorMs = lastCacheErrorLogMs.get();
                if (time.milliseconds() - lastErrorMs > CACHE_ERROR_LOG_INTERVAL_MS &&
                    lastCacheErrorLogMs.compareAndSet(lastErrorMs, time.milliseconds())) {
                    log.warn("Client metrics instance cache cannot find the client instance id: {}. The cache"
                            + " must be at capacity, size: {}. Connection map size: {}",
                            clientInstanceId, clientInstanceCache.size(), clientConnectionIdMap.size());
                }
            }
        }
    }

    // Visible for testing
    final class ClientMetricsStats {

        private static final String GROUP_NAME = "client-metrics";

        // Visible for testing
        static final String INSTANCE_COUNT = "instance-count";
        static final String UNKNOWN_SUBSCRIPTION_REQUEST = "unknown-subscription-request";
        static final String THROTTLE = "throttle";
        static final String PLUGIN_EXPORT = "plugin-export";
        static final String PLUGIN_ERROR = "plugin-error";
        static final String PLUGIN_EXPORT_TIME = "plugin-export-time";

        // Names of registered sensors either globally or per client instance.
        private final Set<String> sensorsName = ConcurrentHashMap.newKeySet();
        // List of metric names which are not specific to a client instance. Do not require thread
        // safe structure as it will be populated only in constructor.
        private final List<MetricName> registeredMetricNames = new ArrayList<>();

        private final Set<String> instanceSensors = Stream.of(THROTTLE, PLUGIN_EXPORT, PLUGIN_ERROR).collect(Collectors.toSet());

        ClientMetricsStats() {
            Measurable instanceCount = (config, now) -> clientInstanceCache.size();
            MetricName instanceCountMetric = metrics.metricName(INSTANCE_COUNT, GROUP_NAME,
                "The current number of client metrics instances being managed by the broker");
            metrics.addMetric(instanceCountMetric, instanceCount);
            registeredMetricNames.add(instanceCountMetric);

            Sensor unknownSubscriptionRequestCountSensor = metrics.sensor(
                ClientMetricsStats.UNKNOWN_SUBSCRIPTION_REQUEST);
            unknownSubscriptionRequestCountSensor.add(createMeter(metrics, new WindowedCount(),
                ClientMetricsStats.UNKNOWN_SUBSCRIPTION_REQUEST, Collections.emptyMap()));
            sensorsName.add(unknownSubscriptionRequestCountSensor.name());
        }

        public void maybeAddClientInstanceMetrics(Uuid clientInstanceId) {
            // If one sensor of the metrics has been registered for the client instance,
            // then all other sensors should have been registered; and vice versa.
            if (metrics.getSensor(PLUGIN_EXPORT + "-" + clientInstanceId) != null) {
                return;
            }

            Map<String, String> tags = Collections.singletonMap(ClientMetricsConfigs.CLIENT_INSTANCE_ID, clientInstanceId.toString());

            Sensor throttleCount = metrics.sensor(ClientMetricsStats.THROTTLE + "-" + clientInstanceId);
            throttleCount.add(createMeter(metrics, new WindowedCount(), ClientMetricsStats.THROTTLE, tags));
            sensorsName.add(throttleCount.name());

            Sensor pluginExport = metrics.sensor(ClientMetricsStats.PLUGIN_EXPORT + "-" + clientInstanceId);
            pluginExport.add(createMeter(metrics, new WindowedCount(), ClientMetricsStats.PLUGIN_EXPORT, tags));
            pluginExport.add(metrics.metricName(ClientMetricsStats.PLUGIN_EXPORT_TIME + "-avg",
                ClientMetricsStats.GROUP_NAME, "Average time broker spent in invoking plugin exportMetrics call", tags), new Avg());
            pluginExport.add(metrics.metricName(ClientMetricsStats.PLUGIN_EXPORT_TIME + "-max",
                ClientMetricsStats.GROUP_NAME, "Maximum time broker spent in invoking plugin exportMetrics call", tags), new Max());
            sensorsName.add(pluginExport.name());

            Sensor pluginErrorCount = metrics.sensor(ClientMetricsStats.PLUGIN_ERROR + "-" + clientInstanceId);
            pluginErrorCount.add(createMeter(metrics, new WindowedCount(), ClientMetricsStats.PLUGIN_ERROR, tags));
            sensorsName.add(pluginErrorCount.name());
        }

        public void recordUnknownSubscriptionCount() {
            record(UNKNOWN_SUBSCRIPTION_REQUEST);
        }

        public void recordThrottleCount(Uuid clientInstanceId) {
            record(THROTTLE, clientInstanceId);
        }

        public void recordPluginExport(Uuid clientInstanceId, long timeMs) {
            record(PLUGIN_EXPORT, clientInstanceId, timeMs);
        }

        public void recordPluginErrorCount(Uuid clientInstanceId) {
            record(PLUGIN_ERROR, clientInstanceId);
        }

        public void unregisterClientInstanceMetrics(Uuid clientInstanceId) {
            for (String name : instanceSensors) {
                String sensorName = name + "-" + clientInstanceId;
                metrics.removeSensor(sensorName);
                sensorsName.remove(sensorName);
            }
        }

        public void unregisterMetrics() {
            for (MetricName metricName : registeredMetricNames) {
                metrics.removeMetric(metricName);
            }
            for (String name : sensorsName) {
                metrics.removeSensor(name);
            }
            sensorsName.clear();
        }

        private Meter createMeter(Metrics metrics, SampledStat stat, String name, Map<String, String> metricTags) {
            MetricName rateMetricName = metrics.metricName(name + "-rate", ClientMetricsStats.GROUP_NAME,
                String.format("The number of %s per second", name), metricTags);
            MetricName totalMetricName = metrics.metricName(name + "-count", ClientMetricsStats.GROUP_NAME,
                String.format("The total number of %s", name), metricTags);
            return new Meter(stat, rateMetricName, totalMetricName);
        }

        private void record(String metricName) {
            record(metricName, null, 1);
        }

        private void record(String metricName, Uuid clientInstanceId) {
            record(metricName, clientInstanceId, 1);
        }

        private void record(String metricName, Uuid clientInstanceId, long value) {
            String sensorName = clientInstanceId != null ? metricName + "-" + clientInstanceId : metricName;
            Sensor sensor = metrics.getSensor(sensorName);
            if (sensor != null) {
                sensor.record(value);
            }
        }
    }
}
