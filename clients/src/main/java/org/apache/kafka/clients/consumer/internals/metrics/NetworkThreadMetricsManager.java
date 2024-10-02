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

import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Value;

import java.util.HashMap;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRICS_SUFFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;

public class NetworkThreadMetricsManager {
    // MetricNames visible for testing
    final MetricName pollTimeMax;
    final MetricName pollTimeAvg;
    final MetricName applicationEventQueueSize;
    final MetricName appEventTimeAvg;
    final MetricName appEventTimeMax;
    final MetricName appEventProcessingTimeAvg;
    final MetricName appEventProcessingTimeMax;
    final MetricName unsentRequestsQueueSize;
    final MetricName unsentRequestTimeAvg;
    final MetricName unsentRequestTimeMax;
    private final Sensor consumerNetworkThreadPollSensor;
    private final Sensor applicationEventQueueSizeSensor;
    private final Sensor applicationEventQueueChangeSensor;
    private final Sensor unsentRequestsQueueSensor;
    private final Sensor unsentRequestsQueueSizeSensor;
    private final Sensor applicationEventProcessingSensor;
    private long lastPollTime = -1;
    private final HashMap<Uuid, Long> applicationEventQueueMap = new HashMap<>();
    private final HashMap<NetworkClientDelegate.UnsentRequest, Long> unsentRequestsQueueMap = new HashMap<>();

    public NetworkThreadMetricsManager(Metrics metrics) {
        final String metricGroupName = CONSUMER_METRIC_GROUP_PREFIX + CONSUMER_METRICS_SUFFIX;

        consumerNetworkThreadPollSensor = metrics.sensor("consumer-network-thread-poll");
        pollTimeMax = metrics.metricName("time-between-network-thread-polls-max",
                metricGroupName,
                "The maximum time in milliseconds between each poll in the network thread");
        pollTimeAvg = metrics.metricName("time-between-network-thread-polls-avg",
                metricGroupName,
                "The average time in milliseconds between each poll in the network thread");
        consumerNetworkThreadPollSensor.add(pollTimeMax, new Max());
        consumerNetworkThreadPollSensor.add(pollTimeAvg, new Avg());

        applicationEventQueueSizeSensor = metrics.sensor("application-event-queue-size");
        applicationEventQueueSize = metrics.metricName("application-event-queue-size",
                metricGroupName,
                "The current size of the consumer network application event queue");
        applicationEventQueueSizeSensor.add(applicationEventQueueSize, new Value());

        applicationEventQueueChangeSensor = metrics.sensor("application-event-queue-change");
        appEventTimeAvg = metrics.metricName("application-event-queue-time-avg",
                metricGroupName,
                "The average time in milliseconds that application events are taking to be dequeued");
        appEventTimeMax = metrics.metricName("application-event-queue-time-max",
                metricGroupName,
                "The maximum time in milliseconds that application events are taking to be dequeued");
        applicationEventQueueChangeSensor.add(appEventTimeAvg, new Avg());
        applicationEventQueueChangeSensor.add(appEventTimeMax, new Max());

        applicationEventProcessingSensor = metrics.sensor("application-event-queue-processing");
        appEventProcessingTimeAvg = metrics.metricName("application-event-queue-processing-time-avg",
                metricGroupName,
                "The average time in milliseconds that all available application events are taking to be processed");
        appEventProcessingTimeMax = metrics.metricName("application-event-queue-processing-time-max",
                metricGroupName,
                "The maximum time in milliseconds that all available application events are taking to be processed");
        applicationEventProcessingSensor.add(appEventProcessingTimeAvg, new Avg());
        applicationEventProcessingSensor.add(appEventProcessingTimeMax, new Max());

        unsentRequestsQueueSizeSensor = metrics.sensor("unsent-requests-queue-size");
        unsentRequestsQueueSize = metrics.metricName("unsent-requests-queue-size",
                metricGroupName,
                "The current size of the unsent requests queue");
        unsentRequestsQueueSizeSensor.add(unsentRequestsQueueSize, new Value());

        unsentRequestsQueueSensor = metrics.sensor("unsent-requests-queue-change");
        unsentRequestTimeAvg = metrics.metricName("unsent-requests-queue-time-avg",
                metricGroupName,
                "The average time in milliseconds that unsent requests are taking to be sent");
        unsentRequestTimeMax = metrics.metricName("unsent-requests-queue-time-max",
                metricGroupName,
                "The maximum time in milliseconds that unsent requests are taking to be sent");
        unsentRequestsQueueSensor.add(unsentRequestTimeAvg, new Avg());
        unsentRequestsQueueSensor.add(unsentRequestTimeMax, new Max());
    }

    public void updatePollTime(long nowMs) {
        if (lastPollTime != -1) {
            long timeSinceLastPoll = nowMs - lastPollTime;
            consumerNetworkThreadPollSensor.record(timeSinceLastPoll);
        }
        lastPollTime = nowMs;
    }

    public void recordApplicationEventQueueSize(int queueSize) {
        applicationEventQueueSizeSensor.record(queueSize);
    }

    public void recordApplicationEventQueueChange(ApplicationEvent event, long nowMs, boolean isNewEvent) {
        Uuid id = event.id();

        if (isNewEvent) {
            applicationEventQueueMap.put(id, nowMs);
        } else {
            long timeSinceEventAdded = nowMs - applicationEventQueueMap.get(id);
            applicationEventQueueChangeSensor.record(timeSinceEventAdded);
            applicationEventQueueMap.remove(id);
        }
    }

    public void recordApplicationEventProcessingTime(long timeMs) {
        applicationEventProcessingSensor.record(timeMs);
    }

    public void recordUnsentRequestQueueSize(int queueSize) {
        unsentRequestsQueueSizeSensor.record(queueSize);
    }

    public void recordUnsentRequestsQueueChange(NetworkClientDelegate.UnsentRequest request, long nowMs, boolean isNewRequest) {
        if (isNewRequest) {
            unsentRequestsQueueMap.put(request, nowMs);
        } else {
            long timeSinceRequestAdded = nowMs - unsentRequestsQueueMap.get(request);
            unsentRequestsQueueSensor.record(timeSinceRequestAdded);
            unsentRequestsQueueMap.remove(request);
        }
    }
}
