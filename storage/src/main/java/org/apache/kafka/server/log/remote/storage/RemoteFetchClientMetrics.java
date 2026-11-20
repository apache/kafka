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

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.server.quota.SensorAccess;

import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RemoteFetchClientMetrics {
    public static final String GROUP = "RemoteFetchMetrics";
    public static final String CLIENT_ID_TAG = "client-id";

    public static final String BYTES_RATE = "RemoteFetchBytesPerSec";
    public static final String REQUESTS_RATE = "RemoteFetchRequestsPerSec";

    private final Metrics metrics;
    private final SensorAccess sensorAccess;
    private final long expirationTimeSeconds;

    public RemoteFetchClientMetrics(Metrics metrics, long expirationTimeSeconds) {
        this.metrics = metrics;
        this.sensorAccess = new SensorAccess(new ReentrantReadWriteLock(), metrics);
        this.expirationTimeSeconds = expirationTimeSeconds;
    }

    public void recordBytes(String clientId, long bytes, long timeMs) {
        bytesSensor(clientId).record(bytes, timeMs);
    }

    public void recordRequest(String clientId, long timeMs) {
        requestSensor(clientId).record(1L, timeMs);
    }

    private Sensor bytesSensor(String clientId) {
        return sensorAccess.getOrCreate(BYTES_RATE + ":" + clientId, expirationTimeSeconds, sensor ->
            sensor.add(metrics.metricName(BYTES_RATE, GROUP, "Bytes read from remote storage per second for this client-id.", tags(clientId)), new Rate()));
    }

    private Sensor requestSensor(String clientId) {
        return sensorAccess.getOrCreate(REQUESTS_RATE + ":" + clientId, expirationTimeSeconds, sensor ->
            sensor.add(metrics.metricName(REQUESTS_RATE, GROUP, "Remote storage fetch requests per second for this client-id.", tags(clientId)), new Rate()));
    }

    private Map<String, String> tags(String clientId) {
        return Map.of(CLIENT_ID_TAG, clientId);
    }
}
