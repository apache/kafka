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

import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Value;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRICS_SUFFIX;

public class KafkaConsumerMetrics implements AutoCloseable {
    private final GroupProtocol groupProtocol;
    private final Metrics metrics;
    private final MetricName lastPollMetricName;
    private final Sensor timeBetweenPollSensor;
    private final Sensor pollIdleSensor;
    private final Sensor committedSensor;
    private final Sensor commitSyncSensor;
    private long lastPollMs;
    private long pollStartMs;
    private long timeSinceLastPollMs;

    // CONSUMER group protocol metrics
    public static final String TIME_BETWEEN_NETWORK_THREAD_POLL_SENSOR_NAME = "time-between-network-thread-poll";
    public static final String APPLICATION_EVENT_QUEUE_SIZE_SENSOR_NAME = "application-event-queue-size";
    public static final String APPLICATION_EVENT_QUEUE_TIME_SENSOR_NAME = "application-event-queue-time";
    public static final String APPLICATION_EVENT_QUEUE_PROCESSING_TIME_SENSOR_NAME = "application-event-queue-processing-time";
    public static final String BACKGROUND_EVENT_QUEUE_SIZE_SENSOR_NAME = "background-event-queue-size";
    public static final String BACKGROUND_EVENT_QUEUE_TIME_SENSOR_NAME = "background-event-queue-time";
    public static final String BACKGROUND_EVENT_QUEUE_PROCESSING_TIME_SENSOR_NAME = "background-event-queue-processing-time";
    public static final String UNSENT_REQUESTS_QUEUE_SIZE_SENSOR_NAME = "unsent-requests-queue-size";
    public static final String UNSENT_REQUESTS_QUEUE_TIME_SENSOR_NAME = "unsent-requests-queue-time";
    private Sensor timeBetweenNetworkThreadPollSensor;
    private Sensor applicationEventQueueSizeSensor;
    private Sensor applicationEventQueueTimeSensor;
    private Sensor applicationEventQueueProcessingTimeSensor;
    private Sensor backgroundEventQueueSizeSensor;
    private Sensor backgroundEventQueueTimeSensor;
    private Sensor backgroundEventQueueProcessingTimeSensor;
    private Sensor unsentRequestsQueueSizeSensor;
    private Sensor unsentRequestsQueueTimeSensor;

    public KafkaConsumerMetrics(Metrics metrics, String metricGrpPrefix, GroupProtocol groupProtocol) {
        this.metrics = metrics;
        this.groupProtocol = groupProtocol;
        final String metricGroupName = metricGrpPrefix + CONSUMER_METRICS_SUFFIX;
        Measurable lastPoll = (mConfig, now) -> {
            if (lastPollMs == 0L)
                // if no poll is ever triggered, just return -1.
                return -1d;
            else
                return TimeUnit.SECONDS.convert(now - lastPollMs, TimeUnit.MILLISECONDS);
        };
        this.lastPollMetricName = metrics.metricName("last-poll-seconds-ago",
                metricGroupName, "The number of seconds since the last poll() invocation.");
        metrics.addMetric(lastPollMetricName, lastPoll);

        this.timeBetweenPollSensor = metrics.sensor("time-between-poll");
        this.timeBetweenPollSensor.add(metrics.metricName("time-between-poll-avg",
                        metricGroupName,
                        "The average delay between invocations of poll() in milliseconds."),
                new Avg());
        this.timeBetweenPollSensor.add(metrics.metricName("time-between-poll-max",
                        metricGroupName,
                        "The max delay between invocations of poll() in milliseconds."),
                new Max());

        this.pollIdleSensor = metrics.sensor("poll-idle-ratio-avg");
        this.pollIdleSensor.add(metrics.metricName("poll-idle-ratio-avg",
                        metricGroupName,
                        "The average fraction of time the consumer's poll() is idle as opposed to waiting for the user code to process records."),
                new Avg());

        this.commitSyncSensor = metrics.sensor("commit-sync-time-ns-total");
        this.commitSyncSensor.add(
                metrics.metricName(
                        "commit-sync-time-ns-total",
                        metricGroupName,
                        "The total time the consumer has spent in commitSync in nanoseconds"
                ),
                new CumulativeSum()
        );

        this.committedSensor = metrics.sensor("committed-time-ns-total");
        this.committedSensor.add(
                metrics.metricName(
                        "committed-time-ns-total",
                        metricGroupName,
                        "The total time the consumer has spent in committed in nanoseconds"
                ),
                new CumulativeSum()
        );

        if (groupProtocol == GroupProtocol.CONSUMER) {
            this.timeBetweenNetworkThreadPollSensor = metrics.sensor(TIME_BETWEEN_NETWORK_THREAD_POLL_SENSOR_NAME);
            this.timeBetweenNetworkThreadPollSensor.add(metrics.metricName("time-between-network-thread-poll-avg",
                    metricGroupName,
                    "The average time taken, in milliseconds, between each poll in the network thread."),
                    new Avg());
            this.timeBetweenNetworkThreadPollSensor.add(metrics.metricName("time-between-network-thread-poll-max",
                    metricGroupName,
                    "The maximum time taken, in milliseconds, between each poll in the network thread."),
                    new Max());

            this.applicationEventQueueSizeSensor = metrics.sensor(APPLICATION_EVENT_QUEUE_SIZE_SENSOR_NAME);
            this.applicationEventQueueSizeSensor.add(metrics.metricName("application-event-queue-size",
                    metricGroupName,
                    "The current number of events in the consumer network application event queue."),
                    new Value());

            this.applicationEventQueueTimeSensor = metrics.sensor(APPLICATION_EVENT_QUEUE_TIME_SENSOR_NAME);
            this.applicationEventQueueTimeSensor.add(metrics.metricName("application-event-queue-time-avg",
                    metricGroupName,
                    "The average time, in milliseconds, that application events are taking to be dequeued."),
                    new Avg());
            this.applicationEventQueueTimeSensor.add(metrics.metricName("application-event-queue-time-max",
                    metricGroupName,
                    "The maximum time, in milliseconds, that an application event took to be dequeued."),
                    new Max());

            this.applicationEventQueueProcessingTimeSensor = metrics.sensor(APPLICATION_EVENT_QUEUE_PROCESSING_TIME_SENSOR_NAME);
            this.applicationEventQueueProcessingTimeSensor.add(metrics.metricName("application-event-queue-processing-time-avg",
                    metricGroupName,
                    "The average time, in milliseconds, that the consumer network takes to process all available application events."),
                    new Avg());
            this.applicationEventQueueProcessingTimeSensor.add(metrics.metricName("application-event-queue-processing-time-max",
                    metricGroupName,
                    "The maximum time, in milliseconds, that the consumer network took to process all available application events."),
                    new Max());

            this.unsentRequestsQueueSizeSensor = metrics.sensor(UNSENT_REQUESTS_QUEUE_SIZE_SENSOR_NAME);
            this.unsentRequestsQueueSizeSensor.add(metrics.metricName("unsent-requests-queue-size",
                    metricGroupName,
                    "The current number of unsent requests in the consumer network."),
                    new Value());

            this.unsentRequestsQueueTimeSensor = metrics.sensor(UNSENT_REQUESTS_QUEUE_TIME_SENSOR_NAME);
            this.unsentRequestsQueueTimeSensor.add(metrics.metricName("unsent-requests-queue-time-avg",
                    metricGroupName,
                    "The average time, in milliseconds, that requests are taking to be sent in the consumer network."),
                    new Avg());
            this.unsentRequestsQueueTimeSensor.add(metrics.metricName("unsent-requests-queue-time-max",
                    metricGroupName,
                    "The maximum time, in milliseconds, that a request remained unsent in the consumer network."),
                    new Max());

            this.backgroundEventQueueSizeSensor = metrics.sensor(BACKGROUND_EVENT_QUEUE_SIZE_SENSOR_NAME);
            this.backgroundEventQueueSizeSensor.add(metrics.metricName("background-event-queue-size",
                    metricGroupName,
                    "The current number of events in the consumer background event queue."),
                    new Value());

            this.backgroundEventQueueTimeSensor = metrics.sensor(BACKGROUND_EVENT_QUEUE_TIME_SENSOR_NAME);
            this.backgroundEventQueueTimeSensor.add(metrics.metricName("background-event-queue-time-avg",
                    metricGroupName,
                    "The average time, in milliseconds, that background events are taking to be dequeued."),
                    new Avg());
            this.backgroundEventQueueTimeSensor.add(metrics.metricName("background-event-queue-time-max",
                    metricGroupName,
                    "The maximum time, in milliseconds, that background events are taking to be dequeued."),
                    new Max());

            this.backgroundEventQueueProcessingTimeSensor = metrics.sensor(BACKGROUND_EVENT_QUEUE_PROCESSING_TIME_SENSOR_NAME);
            this.backgroundEventQueueProcessingTimeSensor.add(metrics.metricName("background-event-queue-processing-time-avg",
                    metricGroupName,
                    "The average time, in milliseconds, that the consumer took to process all available background events."),
                    new Avg());
            this.backgroundEventQueueProcessingTimeSensor.add(metrics.metricName("background-event-queue-processing-time-max",
                    metricGroupName,
                    "The maximum time, in milliseconds, that the consumer took to process all available background events."),
                    new Max());
        }
    }

    public void recordPollStart(long pollStartMs) {
        this.pollStartMs = pollStartMs;
        this.timeSinceLastPollMs = lastPollMs != 0L ? pollStartMs - lastPollMs : 0;
        this.timeBetweenPollSensor.record(timeSinceLastPollMs);
        this.lastPollMs = pollStartMs;
    }

    public void recordPollEnd(long pollEndMs) {
        long pollTimeMs = pollEndMs - pollStartMs;
        double pollIdleRatio = pollTimeMs * 1.0 / (pollTimeMs + timeSinceLastPollMs);
        this.pollIdleSensor.record(pollIdleRatio);
    }

    public void recordCommitSync(long duration) {
        this.commitSyncSensor.record(duration);
    }

    public void recordCommitted(long duration) {
        this.committedSensor.record(duration);
    }

    public void recordTimeBetweenNetworkThreadPoll(long timeBetweenNetworkThreadPoll) {
        this.timeBetweenNetworkThreadPollSensor.record(timeBetweenNetworkThreadPoll);
    }

    public void recordApplicationEventQueueSize(int size) {
        this.applicationEventQueueSizeSensor.record(size);
    }

    public void recordApplicationEventQueueTime(long time) {
        this.applicationEventQueueTimeSensor.record(time);
    }

    public void recordApplicationEventQueueProcessingTime(long processingTime) {
        this.applicationEventQueueProcessingTimeSensor.record(processingTime);
    }

    public void recordUnsentRequestsQueueSize(int size) {
        this.unsentRequestsQueueSizeSensor.record(size);
    }

    public void recordUnsentRequestsQueueTime(long time) {
        this.unsentRequestsQueueTimeSensor.record(time);
    }

    public void recordBackgroundEventQueueSize(int size) {
        this.backgroundEventQueueSizeSensor.record(size);
    }

    public void recordBackgroundEventQueueTime(long time) {
        this.backgroundEventQueueTimeSensor.record(time);
    }

    public void recordBackgroundEventQueueProcessingTime(long processingTime) {
        this.backgroundEventQueueProcessingTimeSensor.record(processingTime);
    }

    @Override
    public void close() {
        metrics.removeMetric(lastPollMetricName);
        metrics.removeSensor(timeBetweenPollSensor.name());
        metrics.removeSensor(pollIdleSensor.name());
        metrics.removeSensor(commitSyncSensor.name());
        metrics.removeSensor(committedSensor.name());

        if (groupProtocol == GroupProtocol.CONSUMER) {
            Arrays.asList(
                    timeBetweenNetworkThreadPollSensor.name(),
                    applicationEventQueueSizeSensor.name(),
                    applicationEventQueueTimeSensor.name(),
                    applicationEventQueueProcessingTimeSensor.name(),
                    backgroundEventQueueSizeSensor.name(),
                    backgroundEventQueueTimeSensor.name(),
                    backgroundEventQueueProcessingTimeSensor.name(),
                    unsentRequestsQueueSizeSensor.name(),
                    unsentRequestsQueueTimeSensor.name()
            ).forEach(metrics::removeSensor);
        }
    }
}
