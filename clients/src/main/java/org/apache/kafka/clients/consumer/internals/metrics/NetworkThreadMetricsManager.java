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
package org.apache.kafka.clients.consumer.internals.metrics;

import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

import java.util.HashMap;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRICS_SUFFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;

public class NetworkThreadMetricsManager {
    MetricName timeBetweenNetworkThreadPollMax;
    MetricName timeBetweenNetworkThreadPollAvg;
    private long lastNetworkThreadPoll = -1;
    private long timeBetweenPollsMax = -1;
    private int totalPolls = 0;
    private long totalTimeBetweenPolls = 0;
    private long timeBetweenPollsAvg = -1;

    MetricName applicationEventQueueSize;
    MetricName applicationEventQueueTimeAvg;
    MetricName applicationEventQueueTimeMax;
    private HashMap<Uuid, Long> applicationEventsMap;

    MetricName applicationEventQueueProcessingTimeAvg;
    MetricName applicationEventQueueProcessingTimeMax;

    MetricName unsentRequestsQueueSize;
    MetricName unsentRequestsQueueTimeMax;
    MetricName unsentRequestsQueueTimeAvg;
    private Sensor networkThreadPollSensor;
    private Sensor applicationEventQueueSensor;
    private Sensor unsentRequestsQueueSensor;

    public NetworkThreadMetricsManager(Metrics metrics) {
        this(metrics, CONSUMER_METRIC_GROUP_PREFIX);
    }

    public NetworkThreadMetricsManager(Metrics metrics, String metricGroupPrefix) {
        String metricGroupName = metricGroupPrefix + CONSUMER_METRICS_SUFFIX;
        applicationEventsMap = new HashMap<>();
    }

    public void recordNetworkThreadPoll(long nowMs) {
        long timeBetweenPolls = nowMs - lastNetworkThreadPoll;
        lastNetworkThreadPoll = nowMs;
        totalPolls++;
        totalTimeBetweenPolls += timeBetweenPolls;
        timeBetweenPollsAvg = totalTimeBetweenPolls / totalPolls;

        if (timeBetweenPolls > timeBetweenPollsMax)
            timeBetweenPollsMax = timeBetweenPolls;
    }

    public void recordApplicationEventQueueChange(long nowMs, ApplicationEvent event) {
        Uuid id = event.id();
        if (!applicationEventsMap.containsKey(event.id())) {
            applicationEventsMap.put(event.id())
        }
    }
}
