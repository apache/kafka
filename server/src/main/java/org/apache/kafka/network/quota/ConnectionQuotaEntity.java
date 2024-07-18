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
package org.apache.kafka.network.quota;

import java.util.Map;

/**
 * Interface for connection quota configuration. Connection quotas can be configured at the
 * broker, listener or IP level.
 */
public interface ConnectionQuotaEntity {

    String CONNECTION_RATE_SENSOR_NAME = "Connection-Accept-Rate";
    String CONNECTION_RATE_METRIC_NAME = "connection-accept-rate";

    /**
     * The name of the sensor for this quota entity
     */
    String sensorName();

    /**
     * The name of the metric for this quota entity
     */
    String metricName();

    /**
     * The duration in second to keep the sensor even if no new values are recorded
     */
    long sensorExpiration();

    /**
     * Tags associated with this quota entity
     */
    Map<String, String> metricTags();
}
