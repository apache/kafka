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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.metrics.QuotaViolationException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Sanitizer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.network.Session;
import org.apache.kafka.server.config.ClientQuotaManagerConfig;
import org.apache.kafka.server.quota.ClientQuotaCallback;
import org.apache.kafka.server.quota.ClientQuotaEntity;
import org.apache.kafka.server.quota.ClientQuotaType;
import org.apache.kafka.server.quota.QuotaType;
import org.apache.kafka.server.quota.QuotaUtils;
import org.apache.kafka.server.quota.SensorAccess;
import org.apache.kafka.server.quota.ThrottleCallback;
import org.apache.kafka.server.quota.ThrottledChannel;
import org.apache.kafka.server.util.ShutdownableThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

public class ClientQuotaManager {

    private static final int NO_QUOTAS = 0;
    private static final int CLIENT_ID_QUOTA_ENABLED = 1;
    private static final int USER_QUOTA_ENABLED = 2;
    private static final int USER_CLIENT_ID_QUOTA_ENABLED = 4;
    private static final int CUSTOM_QUOTAS = 8; // No metric update optimizations are used with custom quotas

    private static final Logger LOG = LoggerFactory.getLogger(ClientQuotaManager.class);

    // Purge sensors after 1 hour of inactivity
    private static final int INACTIVE_SENSOR_EXPIRATION_TIME_SECONDS = 3600;
    private static final String DEFAULT_NAME = "<default>";

    public record UserEntity(String sanitizedUser) implements ClientQuotaEntity.ConfigEntity {

        @Override
        public ClientQuotaEntity.ConfigEntityType entityType() {
            return ClientQuotaEntity.ConfigEntityType.USER;
        }

        @Override
        public String name() {
            return Sanitizer.desanitize(sanitizedUser);
        }

        @Override
        public String toString() {
            return "user " + sanitizedUser;
        }
    }
    public record ClientIdEntity(String clientId) implements ClientQuotaEntity.ConfigEntity {

        @Override
        public ClientQuotaEntity.ConfigEntityType entityType() {
            return ClientQuotaEntity.ConfigEntityType.CLIENT_ID;
        }

        @Override
        public String name() {
            return clientId;
        }

        @Override
        public String toString() {
            return "client-id " + clientId;
        }
    }

    public static final ClientQuotaEntity.ConfigEntity DEFAULT_USER_ENTITY = new ClientQuotaEntity.ConfigEntity() {
        @Override
        public ClientQuotaEntity.ConfigEntityType entityType() {
            return ClientQuotaEntity.ConfigEntityType.DEFAULT_USER;
        }

        @Override
        public String name() {
            return DEFAULT_NAME;
        }

        @Override
        public String toString() {
            return "default user";
        }
    };
    
    public static final ClientQuotaEntity.ConfigEntity DEFAULT_USER_CLIENT_ID = new ClientQuotaEntity.ConfigEntity() {
        @Override
        public ClientQuotaEntity.ConfigEntityType entityType() {
            return ClientQuotaEntity.ConfigEntityType.DEFAULT_CLIENT_ID;
        }
        @Override
        public String name() {
            return DEFAULT_NAME;
        }  
        @Override
        public String toString() {
            return "default client-id";
        }
    };

    public static final KafkaQuotaEntity DEFAULT_CLIENT_ID_QUOTA_ENTITY =
            new KafkaQuotaEntity(null, DEFAULT_USER_CLIENT_ID);
    public static final KafkaQuotaEntity DEFAULT_USER_QUOTA_ENTITY =
            new KafkaQuotaEntity(DEFAULT_USER_ENTITY, null);
    public static final KafkaQuotaEntity DEFAULT_USER_CLIENT_ID_QUOTA_ENTITY =
            new KafkaQuotaEntity(DEFAULT_USER_ENTITY, DEFAULT_USER_CLIENT_ID);

    public record KafkaQuotaEntity(ClientQuotaEntity.ConfigEntity userEntity,
                                          ClientQuotaEntity.ConfigEntity clientIdEntity) implements ClientQuotaEntity {

        @Override
        public List<ConfigEntity> configEntities() {
            List<ClientQuotaEntity.ConfigEntity> entities = new ArrayList<>();
            if (userEntity != null) {
                entities.add(userEntity);
            }
            if (clientIdEntity != null) {
                entities.add(clientIdEntity);
            }
            return entities;
        }

        public String sanitizedUser() {
            if (userEntity instanceof UserEntity userRecord) {
                return userRecord.sanitizedUser();
            } else if (userEntity == DEFAULT_USER_ENTITY) {
                return DEFAULT_NAME;
            }
            return "";
        }

        public String clientId() {
            return clientIdEntity != null ? clientIdEntity.name() : "";
        }

        @Override
        public String toString() {
            String user = userEntity != null ? userEntity.toString() : "";
            String clientId = clientIdEntity != null ? clientIdEntity.toString() : "";
            return (user + " " + clientId).trim();
        }
    }

    public static class DefaultTags {
        public static final String USER = "user";
        public static final String CLIENT_ID = "client-id";
    }

    private final ClientQuotaManagerConfig config;
    protected final Metrics metrics;
    private final QuotaType quotaType;
    protected final Time time;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final SensorAccess sensorAccessor;
    private final ClientQuotaCallback quotaCallback;
    private final ClientQuotaType clientQuotaType;

    private volatile int quotaTypesEnabled;

    private final Sensor delayQueueSensor;
    private final DelayQueue<ThrottledChannel> delayQueue = new DelayQueue<>();
    private final ThrottledChannelReaper throttledChannelReaper;

    public void processThrottledChannelReaperDoWork() {
        throttledChannelReaper.doWork();
    }

    /**
     * Helper class that records per-client metrics. It is also responsible for maintaining Quota usage statistics
     * for all clients.
     * <p/>
     * Quotas can be set at <user, client-id>, user or client-id levels. For a given client connection,
     * the most specific quota matching the connection will be applied. For example, if both a <user, client-id>
     * and a user quota match a connection, the <user, client-id> quota will be used. Otherwise, user quota takes
     * precedence over client-id quota. The order of precedence is:
     * <ul>
     *   <li>/config/users/<user>/clients/<client-id>
     *   <li>/config/users/<user>/clients/<default>
     *   <li>/config/users/<user>
     *   <li>/config/users/<default>/clients/<client-id>
     *   <li>/config/users/<default>/clients/<default>
     *   <li>/config/users/<default>
     *   <li>/config/clients/<client-id>
     *   <li>/config/clients/<default>
     * </ul>
     * Quota limits including defaults may be updated dynamically. The implementation is optimized for the case
     * where a single level of quotas is configured.
     * @param config the ClientQuotaManagerConfig containing quota configurations
     * @param metrics the Metrics instance for recording quota-related metrics
     * @param quotaType the quota type managed by this quota manager
     * @param time the Time object used for time-based operations
     * @param threadNamePrefix the thread name prefix used for internal threads
     * @param clientQuotaCallbackPlugin optional Plugin containing a ClientQuotaCallback for custom quota logic
     */
    public ClientQuotaManager(ClientQuotaManagerConfig config,
                              Metrics metrics,
                              QuotaType quotaType,
                              Time time,
                              String threadNamePrefix,
                              Optional<Plugin<ClientQuotaCallback>> clientQuotaCallbackPlugin) {
        this.config = config;
        this.metrics = metrics;
        this.quotaType = quotaType;
        this.time = time;
        this.sensorAccessor = new SensorAccess(lock, metrics);
        this.clientQuotaType = QuotaType.toClientQuotaType(quotaType);
        this.quotaTypesEnabled = clientQuotaCallbackPlugin.isPresent() ?
                CUSTOM_QUOTAS : NO_QUOTAS;
        this.delayQueueSensor = metrics.sensor(quotaType + "-delayQueue");
        this.delayQueueSensor.add(metrics.metricName("queue-size", quotaType.toString(),
                "Tracks the size of the delay queue"), new CumulativeSum());
        this.throttledChannelReaper = new ThrottledChannelReaper(delayQueue, threadNamePrefix);
        this.quotaCallback = clientQuotaCallbackPlugin
                .map(Plugin::get)
                .orElse(new DefaultQuotaCallback());

        start(); // Extract thread start to separate method to avoid SC_START_IN_CTOR warning
    }

    public ClientQuotaManager(ClientQuotaManagerConfig config,
                              Metrics metrics,
                              QuotaType quotaType,
                              Time time,
                              String threadNamePrefix) {
        this(config, metrics, quotaType, time, threadNamePrefix, Optional.empty());
    }

    protected Metrics metrics() {
        return metrics;
    }
    protected Time time() {
        return time;
    }

    private void start() {
        throttledChannelReaper.start();
    }

    /**
     * Reaper thread that triggers channel unmute callbacks on all throttled channels
     */
    public class ThrottledChannelReaper extends ShutdownableThread {
        private final DelayQueue<ThrottledChannel> delayQueue;

        public ThrottledChannelReaper(DelayQueue<ThrottledChannel> delayQueue, String prefix) {
            super(prefix + "ThrottledChannelReaper-" + quotaType, false);
            this.delayQueue = delayQueue;
        }

        @Override
        public void doWork() {
            ThrottledChannel throttledChannel;
            try {
                throttledChannel = delayQueue.poll(1, TimeUnit.SECONDS);
                if (throttledChannel != null) {
                    // Decrement the size of the delay queue
                    delayQueueSensor.record(-1);
                    // Notify the socket server that throttling is done for this channel
                    throttledChannel.notifyThrottlingDone();
                }
            } catch (InterruptedException e) {
                // Ignore and continue
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Returns true if any quotas are enabled for this quota manager. This is used
     * to determine if quota-related metrics should be created.
     * Note: If any quotas (static defaults, dynamic defaults or quota overrides) have
     * been configured for this broker at any time for this quota type, quotasEnabled will
     * return true until the next broker restarts, even if all quotas are subsequently deleted.
     */
    public boolean quotasEnabled() {
        return quotaTypesEnabled != NO_QUOTAS;
    }

    /**
     * See recordAndGetThrottleTimeMs.
     */
    public int maybeRecordAndGetThrottleTimeMs(Session session, String clientId, double value, long timeMs) {
        // Record metrics only if quotas are enabled.
        if (quotasEnabled()) {
            return recordAndGetThrottleTimeMs(session, clientId, value, timeMs);
        } else {
            return 0;
        }
    }

    /**
     * Records that a user/clientId accumulated or would like to accumulate the provided amount at the
     * specified time, returns throttle time in milliseconds.
     *
     * @param session The session from which the user is extracted
     * @param clientId The client id
     * @param value The value to accumulate
     * @param timeMs The time at which to accumulate the value
     * @return The throttle time in milliseconds defines as the time to wait until the average
     *         rate gets back to the defined quota
     */
    public int recordAndGetThrottleTimeMs(Session session, String clientId, double value, long timeMs) {
        var clientSensors = getOrCreateQuotaSensors(session, clientId);
        try {
            clientSensors.quotaSensor().record(value, timeMs, true);
            return 0;
        } catch (QuotaViolationException e) {
            var throttleTimeMs = (int) throttleTime(e, timeMs);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Quota violated for sensor ({}). Delay time: ({})",
                        clientSensors.quotaSensor().name(), throttleTimeMs);
            }
            return throttleTimeMs;
        }
    }

    /**
     * Records that a user/clientId changed some metric being throttled without checking for
     * quota violation. The aggregate value will subsequently be used for throttling when the
     * next request is processed.
     */
    public void recordNoThrottle(Session session, String clientId, double value) {
        var clientSensors = getOrCreateQuotaSensors(session, clientId);
        clientSensors.quotaSensor().record(value, time.milliseconds(), false);
    }

    /**
     * "Unrecord" the given value that has already been recorded for the given user/client by recording a negative value
     * of the same quantity.
     * For a throttled fetch, the broker should return an empty response and thus should not record the value. Ideally,
     * we would like to compute the throttle time before actually recording the value, but the current Sensor code
     * couples value recording and quota checking very tightly. As a workaround, we will unrecord the value for the fetch
     * in case of throttling. Rate keeps the sum of values that fall in each time window, so this should bring the
     * overall sum back to the previous value.
     */
    public void unrecordQuotaSensor(Session session, String clientId, double value, long timeMs) {
        var clientSensors = getOrCreateQuotaSensors(session, clientId);
        clientSensors.quotaSensor().record(value * -1, timeMs, false);
    }

    /**
     * Returns the maximum value that could be recorded without guaranteed throttling.
     * Recording any larger value will always be throttled, even if no other values were recorded in the quota window.
     * This is used for deciding the maximum bytes that can be fetched at once
     */
    public double maxValueInQuotaWindow(Session session, String clientId) {
        if (!quotasEnabled()) return Double.MAX_VALUE;
        var clientSensors = getOrCreateQuotaSensors(session, clientId);
        var limit = quotaCallback.quotaLimit(clientQuotaType, clientSensors.metricTags());
        if (limit != null) return limit * (config.numQuotaSamples - 1) * config.quotaWindowSizeSeconds;
        return Double.MAX_VALUE;
    }

    /**
     * Throttle a client by muting the associated channel for the given throttle time.
     * @param clientId request client id
     * @param session request session
     * @param throttleTimeMs Duration in milliseconds for which the channel is to be muted.
     * @param throttleCallback Callback for channel throttling
     */
    public void throttle(
            String clientId,
            Session session,
            ThrottleCallback throttleCallback,
            int throttleTimeMs
    ) {
        if (throttleTimeMs > 0) {
            var clientSensors = getOrCreateQuotaSensors(session, clientId);
            clientSensors.throttleTimeSensor().record(throttleTimeMs);
            var throttledChannel = new ThrottledChannel(time, throttleTimeMs, throttleCallback);
            delayQueue.add(throttledChannel);
            delayQueueSensor.record();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Channel throttled for sensor ({}). Delay time: ({})",
                        clientSensors.quotaSensor().name(), throttleTimeMs);
            }
        }
    }

    /**
     * Returns the quota for the client with the specified (non-encoded) user principal and client-id.
     * Note: this method is expensive, it is meant to be used by tests only
     */
    public Quota quota(String user, String clientId) {
        var userPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, user);
        return quota(userPrincipal, clientId);
    }

    /**
     * Returns the quota for the client with the specified user principal and client-id.
     * Note: this method is expensive, it is meant to be used by tests only
     */
    public Quota quota(KafkaPrincipal userPrincipal, String clientId) {
        var metricTags = quotaCallback.quotaMetricTags(clientQuotaType, userPrincipal, clientId);
        return Quota.upperBound(quotaLimit(metricTags));
    }

    private double quotaLimit(Map<String, String> metricTags) {
        var limit = quotaCallback.quotaLimit(clientQuotaType, metricTags);
        return limit != null ? limit : Long.MAX_VALUE;
    }

    /**
     * This calculates the amount of time needed to bring the metric within quota
     * assuming that no new metrics are recorded.
     */
    protected long throttleTime(QuotaViolationException e, long timeMs) {
        return QuotaUtils.throttleTime(e, timeMs);
    }


    /**
     * This function either returns the sensors for a given client id or creates them if they don't exist
     */
    public ClientSensors getOrCreateQuotaSensors(Session session, String clientId) {
        var metricTags = quotaCallback instanceof DefaultQuotaCallback defaultCallback
                        ? defaultCallback.quotaMetricTags(session.sanitizedUser, clientId)
                        : quotaCallback.quotaMetricTags(clientQuotaType, session.principal, clientId);
        var sensors = new ClientSensors(
                metricTags,
                sensorAccessor.getOrCreate(
                        getQuotaSensorName(metricTags),
                        INACTIVE_SENSOR_EXPIRATION_TIME_SECONDS,
                        sensor -> registerQuotaMetrics(metricTags, sensor)  // quotaLimit() called here only for new sensors
                ),
                sensorAccessor.getOrCreate(
                        getThrottleTimeSensorName(metricTags),
                        INACTIVE_SENSOR_EXPIRATION_TIME_SECONDS,
                        sensor -> sensor.add(throttleMetricName(metricTags), new Avg())
                )
        );

        if (quotaCallback.quotaResetRequired(clientQuotaType)) {
            updateQuotaMetricConfigs();
        }

        return sensors;
    }

    protected void registerQuotaMetrics(Map<String, String> metricTags, Sensor sensor) {
        sensor.add(
                clientQuotaMetricName(metricTags),
                new Rate(),
                getQuotaMetricConfig(metricTags)
        );
    }

    private String metricTagsToSensorSuffix(Map<String, String> metricTags) {
        return String.join(":", metricTags.values());
    }

    private String getThrottleTimeSensorName(Map<String, String> metricTags) {
        return quotaType.toString() + "ThrottleTime-" + metricTagsToSensorSuffix(metricTags);
    }

    private String getQuotaSensorName(Map<String, String> metricTags) {
        return quotaType.toString() + "-" + metricTagsToSensorSuffix(metricTags);
    }

    protected MetricConfig getQuotaMetricConfig(Map<String, String> metricTags) {
        return getQuotaMetricConfig(quotaLimit(metricTags));
    }

    private MetricConfig getQuotaMetricConfig(double quotaLimit) {
        return new MetricConfig()
                .timeWindow(config.quotaWindowSizeSeconds, TimeUnit.SECONDS)
                .samples(config.numQuotaSamples)
                .quota(new Quota(quotaLimit, true));
    }

    protected Sensor getOrCreateSensor(String sensorName, long expirationTimeSeconds, Consumer<Sensor> registerMetrics) {
        return sensorAccessor.getOrCreate(
                sensorName,
                expirationTimeSeconds,
                registerMetrics);
    }

    /**
     * Overrides quotas for <user>, <client-id> or <user, client-id> or the dynamic defaults
     * for any of these levels.
     *
     * @param userEntity   user to override if quota applies to <user> or <user, client-id>
     * @param clientEntity sanitized client entity to override if quota applies to <client-id> or <user, client-id>
     * @param quota        custom quota to apply or None if quota override is being removed
     */
    public void updateQuota(
            Optional<ClientQuotaEntity.ConfigEntity> userEntity,
            Optional<ClientQuotaEntity.ConfigEntity> clientEntity,
            Optional<Quota> quota
    ) {
        /*
         * Acquire the write lock to apply changes in the quota objects.
         * This method changes the quota in the overriddenQuota map and applies the update on the actual KafkaMetric object (if it exists).
         * If the KafkaMetric hasn't been created, the most recent value will be used from the overriddenQuota map.
         * The write lock prevents quota update and creation at the same time. It also guards against concurrent quota change
         * notifications
         */
        lock.writeLock().lock();
        try {
            var quotaEntity = new KafkaQuotaEntity(userEntity.orElse(null), clientEntity.orElse(null));

            // Update quota types enabled flags atomically
            var currentQuotaTypes = quotaTypesEnabled;

            if (userEntity.isPresent()) {
                if (clientEntity.isPresent()) {
                    currentQuotaTypes |= USER_CLIENT_ID_QUOTA_ENABLED;
                } else {
                    currentQuotaTypes |= USER_QUOTA_ENABLED;
                }
            } else if (clientEntity.isPresent()) {
                currentQuotaTypes |= CLIENT_ID_QUOTA_ENABLED;
            }

            quotaTypesEnabled = currentQuotaTypes;

            // Apply quota changes
            if (quota.isPresent()) {
                quotaCallback.updateQuota(clientQuotaType, quotaEntity, quota.get().bound());
            } else {
                quotaCallback.removeQuota(clientQuotaType, quotaEntity);
            }

            // Determine which entities need metric config updates
            Optional<KafkaQuotaEntity> updatedEntity;
            if (userEntity.filter(entity -> entity == DEFAULT_USER_ENTITY).isPresent() ||
                    clientEntity.filter(entity -> entity == DEFAULT_USER_CLIENT_ID).isPresent()) {
                // More than one entity may need updating, so updateQuotaMetricConfigs will go through all metrics
                updatedEntity = Optional.empty();
            } else {
                updatedEntity = Optional.of(quotaEntity);
            }

            updateQuotaMetricConfigs(updatedEntity);
        } finally {
            lock.writeLock().unlock();
        }
    }


    /**
     * Updates metrics configs. This is invoked when quota configs are updated when partition leaders change,
     * and custom callbacks that implement partition-based quotas have updated quotas.
     * Param updatedQuotaEntity If set to one entity and quotas have only been enabled at one
     *    level, then an optimized update is performed with a single metric update. If None is provided,
     *    or if custom callbacks are used or if multi-level quotas have been enabled, all metric configs
     *    are checked and updated if required.
     */
    public void updateQuotaMetricConfigs() {
        updateQuotaMetricConfigs(Optional.empty());
    }

    public void updateQuotaMetricConfigs(Optional<KafkaQuotaEntity> updatedQuotaEntity) {
        var allMetrics = metrics.metrics();

        // If using custom quota callbacks or if multiple-levels of quotas are defined or
        // if this is a default quota update, traverse metrics to find all affected values.
        // Otherwise, update just the single matching one.
        var singleUpdate = switch (quotaTypesEnabled) {
            case NO_QUOTAS,
                 CLIENT_ID_QUOTA_ENABLED,
                 USER_QUOTA_ENABLED,
                 USER_CLIENT_ID_QUOTA_ENABLED -> updatedQuotaEntity.isPresent();
            default -> false;
        };

        if (singleUpdate) {
            var quotaEntity = updatedQuotaEntity.orElseThrow(
                    () -> new IllegalStateException("Quota entity not specified"));
            var user = quotaEntity.sanitizedUser();
            var clientId = quotaEntity.clientId();
            var metricTags = Map.of(DefaultTags.USER, user, DefaultTags.CLIENT_ID, clientId);

            var quotaMetricName = clientQuotaMetricName(metricTags);
            // Change the underlying metric config if the sensor has been created
            var metric = allMetrics.get(quotaMetricName);
            if (metric != null) {
                var newQuota = quotaLimit(metricTags);
                LOG.info("Sensor for {} already exists. Changing quota to {} in MetricConfig",
                        quotaEntity, newQuota);
                metric.config(getQuotaMetricConfig(newQuota));
            }
        } else {
            var quotaMetricName = clientQuotaMetricName(Collections.emptyMap());
            allMetrics.forEach((metricName, metric) -> {
                if (metricName.name().equals(quotaMetricName.name()) &&
                        metricName.group().equals(quotaMetricName.group())) {
                    var metricTags = metricName.tags();
                    var newQuota = quotaLimit(metricTags);
                    if (Double.compare(newQuota, metric.config().quota().bound()) != 0) {
                        LOG.info("Sensor for quota-id {} already exists. Setting quota to {} in MetricConfig",
                                metricTags, newQuota);
                        metric.config(getQuotaMetricConfig(newQuota));
                    }
                }
            });
        }
    }

    /**
     * Returns the MetricName of the metric used for the quota. The name is used to create the
     * metric but also to find the metric when the quota is changed.
     */
    protected MetricName clientQuotaMetricName(Map<String, String> quotaMetricTags) {
        return metrics.metricName("byte-rate", quotaType.toString(),
                "Tracking byte-rate per user/client-id",
                quotaMetricTags);
    }

    private MetricName throttleMetricName(Map<String, String> quotaMetricTags) {
        return metrics.metricName("throttle-time",
                quotaType.toString(),
                "Tracking average throttle-time per user/client-id",
                quotaMetricTags);
    }

    public void initiateShutdown() {
        throttledChannelReaper.initiateShutdown();
        // improve shutdown time by waking up any ShutdownThread(s) blocked on poll by sending a no-op
        delayQueue.add(new ThrottledChannel(time, 0, new ThrottleCallback() {
            @Override
            public void startThrottling() {}

            @Override
            public void endThrottling() {}
        }));
    }

    public void shutdown() {
        initiateShutdown();
        try {
            throttledChannelReaper.awaitShutdown();
        } catch (InterruptedException e) {
            LOG.warn("Shutdown was interrupted", e);
            Thread.currentThread().interrupt(); // Restore interrupt status
        }
    }

    private class DefaultQuotaCallback implements ClientQuotaCallback {
        private final ConcurrentHashMap<ClientQuotaEntity, Quota> overriddenQuotas = new ConcurrentHashMap<>();

        @Override
        public void configure(Map<String, ?> configs) {
        }

        @Override
        public Map<String, String> quotaMetricTags(ClientQuotaType quotaType, KafkaPrincipal principal, String clientId) {
            return quotaMetricTags(Sanitizer.sanitize(principal.getName()), clientId);
        }

        @Override
        public Double quotaLimit(ClientQuotaType quotaType, Map<String, String> metricTags) {
            String sanitizedUser = metricTags.get(DefaultTags.USER);
            String clientId = metricTags.get(DefaultTags.CLIENT_ID);

            if (sanitizedUser == null || clientId == null) {
                return null;
            }

            var userEntity = new UserEntity(sanitizedUser);
            var clientIdEntity = new ClientIdEntity(clientId);

            Quota quota = findQuota(sanitizedUser, clientId, userEntity, clientIdEntity);
            return quota != null ? quota.bound() : null;
        }

        private Quota findQuota(String sanitizedUser, String clientId, UserEntity userEntity, ClientIdEntity clientIdEntity) {
            if (!sanitizedUser.isEmpty() && !clientId.isEmpty()) {
                return findUserClientQuota(userEntity, clientIdEntity);
            }

            if (!sanitizedUser.isEmpty()) {
                return findUserQuota(userEntity);
            }

            if (!clientId.isEmpty()) {
                return findClientQuota(clientIdEntity);
            }

            return null;
        }

        private Quota findUserClientQuota(UserEntity userEntity, ClientIdEntity clientIdEntity) {
            // /config/users/<user>/clients/<client-id>
            var quota = overriddenQuotas.get(new KafkaQuotaEntity(userEntity, clientIdEntity));
            if (quota != null) return quota;

            // /config/users/<user>/clients/<default>
            quota = overriddenQuotas.get(new KafkaQuotaEntity(userEntity, DEFAULT_USER_CLIENT_ID));
            if (quota != null) return quota;

            // /config/users/<default>/clients/<client-id>
            quota = overriddenQuotas.get(new KafkaQuotaEntity(DEFAULT_USER_ENTITY, clientIdEntity));
            if (quota != null) return quota;

            // /config/users/<default>/clients/<default>
            return overriddenQuotas.get(DEFAULT_USER_CLIENT_ID_QUOTA_ENTITY);
        }

        private Quota findUserQuota(UserEntity userEntity) {
            // /config/users/<user>
            var quota = overriddenQuotas.get(new KafkaQuotaEntity(userEntity, null));
            if (quota != null) return quota;

            // /config/users/<default>
            return overriddenQuotas.get(DEFAULT_USER_QUOTA_ENTITY);
        }

        private Quota findClientQuota(ClientIdEntity clientIdEntity) {
            // /config/clients/<client-id>
            var quota = overriddenQuotas.get(new KafkaQuotaEntity(null, clientIdEntity));
            if (quota != null) return quota;

            // /config/clients/<default>
            return overriddenQuotas.get(DEFAULT_CLIENT_ID_QUOTA_ENTITY);
        }

        @Override
        public boolean updateClusterMetadata(Cluster cluster) {
            // The default quota callback does not use any cluster metadata
            return false;
        }

        @Override
        public void updateQuota(ClientQuotaType quotaType, ClientQuotaEntity entity, double newValue) {
            KafkaQuotaEntity quotaEntity = (KafkaQuotaEntity) entity;
            LOG.info("Changing {} quota for {} to {}", quotaType, quotaEntity, newValue);
            overriddenQuotas.put(quotaEntity, new Quota(newValue, true));
        }

        @Override
        public void removeQuota(ClientQuotaType quotaType, ClientQuotaEntity entity) {
            KafkaQuotaEntity quotaEntity = (KafkaQuotaEntity) entity;
            LOG.info("Removing {} quota for {}", quotaType, quotaEntity);
            overriddenQuotas.remove(quotaEntity);
        }

        @Override
        public boolean quotaResetRequired(ClientQuotaType quotaType) {
            return false;
        }

        public Map<String, String> quotaMetricTags(String sanitizedUser, String clientId) {
            String userTag;
            String clientIdTag;

            // Access the outer class's quotaTypesEnabled field
            switch (quotaTypesEnabled) {
                case NO_QUOTAS:
                case CLIENT_ID_QUOTA_ENABLED:
                    userTag = "";
                    clientIdTag = clientId;
                    break;
                case USER_QUOTA_ENABLED:
                    userTag = sanitizedUser;
                    clientIdTag = "";
                    break;
                case USER_CLIENT_ID_QUOTA_ENABLED:
                    userTag = sanitizedUser;
                    clientIdTag = clientId;
                    break;
                default:
                    // Complex lookup logic for optimizing quota lookups when multiple types of quotas are defined
                    var userEntity = new UserEntity(sanitizedUser);
                    var clientIdEntity = new ClientIdEntity(clientId);

                    // Start with full tags (sanitizedUser, clientId)
                    // 1) /config/users/<user>/clients/<client-id>
                    userTag = sanitizedUser;
                    clientIdTag = clientId;

                    // 2) /config/users/<user>/clients/<default>
                    if (overriddenQuotas.containsKey(new KafkaQuotaEntity(userEntity, clientIdEntity))) break;
                    userTag = sanitizedUser;
                    clientIdTag = clientId;

                    // 3) /config/users/<user>
                    if (overriddenQuotas.containsKey(new KafkaQuotaEntity(userEntity, DEFAULT_USER_CLIENT_ID))) break;
                    userTag = sanitizedUser;
                    clientIdTag = "";

                    // 4) /config/users/<default>/clients/<client-id>
                    if (overriddenQuotas.containsKey(new KafkaQuotaEntity(userEntity, null))) break;
                    userTag = sanitizedUser;
                    clientIdTag = clientId;

                    // 5) /config/users/<default>/clients/<default>
                    if (overriddenQuotas.containsKey(new KafkaQuotaEntity(DEFAULT_USER_ENTITY, clientIdEntity))) break;
                    userTag = sanitizedUser;
                    clientIdTag = clientId;

                    // 6) /config/users/<default>
                    if (overriddenQuotas.containsKey(DEFAULT_USER_CLIENT_ID_QUOTA_ENTITY)) break;
                    userTag = sanitizedUser;
                    clientIdTag = "";

                    // 7) /config/clients/<client-id>
                    // 8) /config/clients/<default>
                    if (overriddenQuotas.containsKey(DEFAULT_USER_QUOTA_ENTITY)) break;
                    userTag = "";
                    clientIdTag = clientId;
                    break;
            }

            var result = new LinkedHashMap<String, String>();
            result.put(DefaultTags.USER, userTag);
            result.put(DefaultTags.CLIENT_ID, clientIdTag);
            return result;
        }

        @Override
        public void close() {}
    }
}