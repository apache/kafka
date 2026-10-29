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
package org.apache.kafka.network;

import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Class for connection quota configuration. Connection quotas can be configured at the
 * broker, listener or IP level.
 */
public class ConnectionQuotaEntity {

    public static final String CONNECTION_RATE_SENSOR_NAME = "Connection-Accept-Rate";
    public static final String CONNECTION_RATE_METRIC_NAME = "connection-accept-rate";
    public static final String LISTENER_THROTTLE_PREFIX = "";
    public static final String IP_METRIC_TAG = "ip";
    public static final String IP_THROTTLE_PREFIX = "ip-";

    public static ConnectionQuotaEntity listenerQuotaEntity(String listenerName) {
        return new ConnectionQuotaEntity(CONNECTION_RATE_SENSOR_NAME + "-" + listenerName,
                CONNECTION_RATE_METRIC_NAME,
                Long.MAX_VALUE,
                Map.of("listener", listenerName));
    }

    public static ConnectionQuotaEntity brokerQuotaEntity() {
        return new ConnectionQuotaEntity(CONNECTION_RATE_SENSOR_NAME,
                "broker-" + ConnectionQuotaEntity.CONNECTION_RATE_METRIC_NAME,
                Long.MAX_VALUE,
                Map.of());
    }

    public static ConnectionQuotaEntity ipQuotaEntity(InetAddress ip) {
        return new ConnectionQuotaEntity(CONNECTION_RATE_SENSOR_NAME + "-" + ip.getHostAddress(),
                CONNECTION_RATE_METRIC_NAME,
                TimeUnit.HOURS.toSeconds(1),
                Map.of(IP_METRIC_TAG, ip.getHostAddress()));
    }

    private final String sensorName;
    private final String metricName;
    private final long sensorExpiration;
    private final Map<String, String> metricTags;

    private ConnectionQuotaEntity(String sensorName, String metricName, long sensorExpiration, Map<String, String> metricTags) {
        this.sensorName = sensorName;
        this.metricName = metricName;
        this.sensorExpiration = sensorExpiration;
        this.metricTags = metricTags;
    }

    /**
     * The name of the sensor for this quota entity
     */
    public String sensorName() {
        return sensorName;
    }

    /**
     * The name of the metric for this quota entity
     */
    public String metricName() {
        return metricName;
    }

    /**
     * The duration in second to keep the sensor even if no new values are recorded
     */
    public long sensorExpiration() {
        return sensorExpiration;
    }

    /**
     * Tags associated with this quota entity
     */
    public Map<String, String> metricTags() {
        return metricTags;
    }
}
