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
package org.apache.kafka.network.metrics;

import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.protocol.ApiKeys;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public class RequestChannelMetrics {

    private final Map<String, RequestMetrics> metricsMap;

    public RequestChannelMetrics(Set<ApiKeys> enabledApis) {
        metricsMap = new HashMap<>();
        for (ApiKeys apiKey : enabledApis) {
            metricsMap.put(apiKey.name, new RequestMetrics(apiKey.name));
        }
        for (String name : Arrays.asList(RequestMetrics.CONSUMER_FETCH_METRIC_NAME, RequestMetrics.FOLLOW_FETCH_METRIC_NAME, RequestMetrics.VERIFY_PARTITIONS_IN_TXN_METRIC_NAME)) {
            metricsMap.put(name, new RequestMetrics(name));
        }
    }

    public RequestChannelMetrics(ApiMessageType.ListenerType scope) {
        this(ApiKeys.apisForListener(scope));
    }

    public RequestMetrics apply(String metricName) {
        RequestMetrics requestMetrics = metricsMap.get(metricName);
        if (requestMetrics == null) {
            throw new NoSuchElementException("No RequestMetrics for " + metricName);
        }
        return requestMetrics;
    }

    public void close() {
        for (RequestMetrics requestMetrics : metricsMap.values()) {
            requestMetrics.removeMetrics();
        }
    }
}
