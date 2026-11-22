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
package org.apache.kafka.server.log.remote.storage;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.replica.ClientMetadata;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.KafkaThread;
import org.apache.kafka.common.utils.internals.Sanitizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Opt-in metrics showing which principal reads from remote (tiered) storage, and optionally which topic
 * and client id. See KIP-1404.
 * <p>
 * Metrics are registered in the {@value #GROUP} group with the tags {@code user}, optionally {@code topic}
 * and optionally {@code client-id}. A key is created on its first remote read and removed, together with
 * its metrics, after it has been idle for the configured TTL.
 */
public class RemoteFetchPrincipalMetrics implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteFetchPrincipalMetrics.class);

    public static final String GROUP = "remote-fetch-principal-metrics";
    public static final String USER_TAG = "user";
    public static final String TOPIC_TAG = "topic";
    public static final String CLIENT_ID_TAG = "client-id";

    public static final String BYTE_RATE = "remote-fetch-byte-rate";
    public static final String BYTE_TOTAL = "remote-fetch-byte-total";
    public static final String REQUEST_RATE = "remote-fetch-request-rate";
    public static final String REQUEST_TOTAL = "remote-fetch-request-total";

    private static final long MAX_SWEEP_INTERVAL_MS = 30_000L;

    private final Metrics metrics;
    private final boolean topicEnabled;
    private final boolean clientIdEnabled;
    private final long ttlMs;
    private final int maxKeys;
    private final Time time;
    private final long sweepIntervalMs;
    private final ScheduledExecutorService reaper;
    // guarded by itself
    private final Map<Map<String, String>, KeySensors> keys = new HashMap<>();
    private long lastWarnMs = Long.MIN_VALUE;

    /**
     * Creates the metrics and starts a background task that removes idle keys, so that removal does not
     * depend on new remote reads arriving.
     */
    public RemoteFetchPrincipalMetrics(Metrics metrics, boolean topicEnabled, boolean clientIdEnabled,
                                       long ttlMs, int maxKeys, Time time) {
        this(metrics, topicEnabled, clientIdEnabled, ttlMs, maxKeys, time, true);
    }

    // visible for testing
    RemoteFetchPrincipalMetrics(Metrics metrics, boolean topicEnabled, boolean clientIdEnabled,
                                long ttlMs, int maxKeys, Time time, boolean startReaper) {
        this.metrics = metrics;
        this.topicEnabled = topicEnabled;
        this.clientIdEnabled = clientIdEnabled;
        this.ttlMs = ttlMs;
        this.maxKeys = maxKeys;
        this.time = time;
        this.sweepIntervalMs = Math.min(ttlMs, MAX_SWEEP_INTERVAL_MS);
        if (startReaper) {
            reaper = Executors.newSingleThreadScheduledExecutor(r -> KafkaThread.daemon("remote-fetch-principal-metrics-reaper", r));
            reaper.scheduleWithFixedDelay(() -> expireIdleKeys(time.milliseconds()), sweepIntervalMs, sweepIntervalMs, TimeUnit.MILLISECONDS);
        } else {
            reaper = null;
        }
    }

    @Override
    public void close() {
        if (reaper != null) {
            reaper.shutdownNow();
        }
        clear();
    }

    public void recordRequest(String topic, Optional<ClientMetadata> clientMetadata, long nowMs) {
        KeySensors sensors = sensors(topic, clientMetadata, nowMs);
        if (sensors != null) {
            sensors.requests.record(1.0, nowMs);
        }
    }

    public void recordBytes(String topic, Optional<ClientMetadata> clientMetadata, long bytes, long nowMs) {
        KeySensors sensors = sensors(topic, clientMetadata, nowMs);
        if (sensors != null) {
            sensors.bytes.record(bytes, nowMs);
        }
    }

    /** Removes all metrics, e.g. when the feature is disabled. */
    public void clear() {
        synchronized (keys) {
            keys.values().forEach(KeySensors::remove);
            keys.clear();
        }
    }

    // visible for testing
    int trackedKeys() {
        synchronized (keys) {
            return keys.size();
        }
    }

    /**
     * Builds the tag set identifying the requester, or returns null if any enabled tag has no usable value,
     * in which case the fetch is not attributed (instead of collapsing unrelated fetches onto one key).
     */
    private Map<String, String> key(String topic, Optional<ClientMetadata> clientMetadata) {
        if (clientMetadata.isEmpty()) {
            return null;
        }
        ClientMetadata metadata = clientMetadata.get();
        Map<String, String> key = new LinkedHashMap<>();
        if (!put(key, USER_TAG, metadata.principal() == null ? null : metadata.principal().getName())) {
            return null;
        }
        if (topicEnabled && !put(key, TOPIC_TAG, topic)) {
            return null;
        }
        if (clientIdEnabled && !put(key, CLIENT_ID_TAG, metadata.clientId())) {
            return null;
        }
        return key;
    }

    private static boolean put(Map<String, String> key, String tag, String value) {
        if (value == null || value.isBlank()) {
            return false;
        }
        key.put(tag, Sanitizer.sanitize(value));
        return true;
    }

    private KeySensors sensors(String topic, Optional<ClientMetadata> clientMetadata, long nowMs) {
        Map<String, String> key = key(topic, clientMetadata);
        if (key == null) {
            return null;
        }
        synchronized (keys) {
            KeySensors sensors = keys.get(key);
            if (sensors == null) {
                // an idle key may not have been swept yet, so make room by removing the expired ones first
                if (keys.size() >= maxKeys) {
                    expireIdleKeys(nowMs);
                }
                if (keys.size() >= maxKeys) {
                    warnAtCapacity(nowMs);
                    return null;
                }
                sensors = new KeySensors(key);
                keys.put(key, sensors);
            }
            sensors.lastUsedMs = nowMs;
            return sensors;
        }
    }

    // caller holds the lock on keys
    private void warnAtCapacity(long nowMs) {
        if (lastWarnMs == Long.MIN_VALUE || nowMs - lastWarnMs >= ttlMs) {
            lastWarnMs = nowMs;
            LOGGER.warn("Remote fetch principal metrics are tracked for {} keys, the maximum set by {}. Reads from new keys are not recorded " +
                "until idle keys expire.", maxKeys, RemoteLogManagerConfig.REMOTE_FETCH_PRINCIPAL_METRICS_MAX_KEYS_PROP);
        }
    }

    // visible for testing
    void expireIdleKeys(long nowMs) {
        synchronized (keys) {
            Iterator<KeySensors> it = keys.values().iterator();
            while (it.hasNext()) {
                KeySensors sensors = it.next();
                if (nowMs - sensors.lastUsedMs >= ttlMs) {
                    sensors.remove();
                    it.remove();
                }
            }
        }
    }

    private class KeySensors {
        private final Sensor bytes;
        private final Sensor requests;
        private long lastUsedMs;

        KeySensors(Map<String, String> tagValues) {
            String suffix = tagValues.toString();
            bytes = sensor(BYTE_RATE + ":" + suffix, BYTE_RATE, BYTE_TOTAL, "Bytes read from remote storage", tagValues);
            requests = sensor(REQUEST_RATE + ":" + suffix, REQUEST_RATE, REQUEST_TOTAL, "Remote storage read requests", tagValues);
        }

        private Sensor sensor(String sensorName, String rateName, String totalName, String description, Map<String, String> tagValues) {
            Sensor sensor = metrics.sensor(sensorName);
            MetricName rate = metrics.metricName(rateName, GROUP, description + " per second for this principal.", tagValues);
            MetricName total = metrics.metricName(totalName, GROUP, description + " in total for this principal.", tagValues);
            sensor.add(rate, new Rate());
            sensor.add(total, new CumulativeSum());
            return sensor;
        }

        void remove() {
            metrics.removeSensor(bytes.name());
            metrics.removeSensor(requests.name());
        }
    }
}
