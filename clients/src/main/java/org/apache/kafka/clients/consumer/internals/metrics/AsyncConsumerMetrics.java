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

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Value;

import java.util.Arrays;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;

public class AsyncConsumerMetrics extends KafkaConsumerMetrics implements AutoCloseable {
    private final Metrics metrics;

    public static final String TIME_BETWEEN_NETWORK_THREAD_POLL_SENSOR_NAME = "time-between-network-thread-poll";
    public static final String APPLICATION_EVENT_QUEUE_SIZE_SENSOR_NAME = "application-event-queue-size";
    public static final String APPLICATION_EVENT_QUEUE_TIME_SENSOR_NAME = "application-event-queue-time";
    public static final String APPLICATION_EVENT_QUEUE_PROCESSING_TIME_SENSOR_NAME = "application-event-queue-processing-time";
    public static final String APPLICATION_EVENT_EXPIRED_SIZE_SENSOR_NAME = "application-events-expired-count";
    public static final String BACKGROUND_EVENT_QUEUE_SIZE_SENSOR_NAME = "background-event-queue-size";
    public static final String BACKGROUND_EVENT_QUEUE_TIME_SENSOR_NAME = "background-event-queue-time";
    public static final String BACKGROUND_EVENT_QUEUE_PROCESSING_TIME_SENSOR_NAME = "background-event-queue-processing-time";
    public static final String UNSENT_REQUESTS_QUEUE_SIZE_SENSOR_NAME = "unsent-requests-queue-size";
    public static final String UNSENT_REQUESTS_QUEUE_TIME_SENSOR_NAME = "unsent-requests-queue-time";
    private final Sensor timeBetweenNetworkThreadPollSensor;
    private final Sensor applicationEventQueueSizeSensor;
    private final Sensor applicationEventQueueTimeSensor;
    private final Sensor applicationEventQueueProcessingTimeSensor;
    private final Sensor applicationEventExpiredSizeSensor;
    private final Sensor backgroundEventQueueSizeSensor;
    private final Sensor backgroundEventQueueTimeSensor;
    private final Sensor backgroundEventQueueProcessingTimeSensor;
    private final Sensor unsentRequestsQueueSizeSensor;
    private final Sensor unsentRequestsQueueTimeSensor;

    public AsyncConsumerMetrics(Metrics metrics) {
        super(metrics, CONSUMER_METRIC_GROUP_PREFIX);

        this.metrics = metrics;
        this.timeBetweenNetworkThreadPollSensor = metrics.sensor(TIME_BETWEEN_NETWORK_THREAD_POLL_SENSOR_NAME);
        this.timeBetweenNetworkThreadPollSensor.add(
            metrics.metricName(
                "time-between-network-thread-poll-avg",
                CONSUMER_METRIC_GROUP,
                "The average time taken, in milliseconds, between each poll in the network thread."
            ),
            new Avg()
        );
        this.timeBetweenNetworkThreadPollSensor.add(
            metrics.metricName(
                "time-between-network-thread-poll-max",
                CONSUMER_METRIC_GROUP,
                "The maximum time taken, in milliseconds, between each poll in the network thread."
            ),
            new Max()
        );

        this.applicationEventQueueSizeSensor = metrics.sensor(APPLICATION_EVENT_QUEUE_SIZE_SENSOR_NAME);
        this.applicationEventQueueSizeSensor.add(
            metrics.metricName(
                APPLICATION_EVENT_QUEUE_SIZE_SENSOR_NAME,
                CONSUMER_METRIC_GROUP,
                "The current number of events in the queue to send from the application thread to the background thread."
            ),
            new Value()
        );

        this.applicationEventQueueTimeSensor = metrics.sensor(APPLICATION_EVENT_QUEUE_TIME_SENSOR_NAME);
        this.applicationEventQueueTimeSensor.add(
            metrics.metricName(
                "application-event-queue-time-avg",
                CONSUMER_METRIC_GROUP,
                "The average time, in milliseconds, that application events are taking to be dequeued."
            ),
            new Avg()
        );
        this.applicationEventQueueTimeSensor.add(
            metrics.metricName(
                "application-event-queue-time-max",
                CONSUMER_METRIC_GROUP,
                "The maximum time, in milliseconds, that an application event took to be dequeued."
            ),
            new Max()
        );

        this.applicationEventQueueProcessingTimeSensor = metrics.sensor(APPLICATION_EVENT_QUEUE_PROCESSING_TIME_SENSOR_NAME);
        this.applicationEventQueueProcessingTimeSensor.add(
            metrics.metricName(
                "application-event-queue-processing-time-avg",
                CONSUMER_METRIC_GROUP,
                "The average time, in milliseconds, that the background thread takes to process all available application events."
            ),
            new Avg()
        );
        this.applicationEventQueueProcessingTimeSensor.add(
            metrics.metricName("application-event-queue-processing-time-max",
                CONSUMER_METRIC_GROUP,
                "The maximum time, in milliseconds, that the background thread took to process all available application events."
            ),
            new Max()
        );

        this.applicationEventExpiredSizeSensor = metrics.sensor(APPLICATION_EVENT_EXPIRED_SIZE_SENSOR_NAME);
        this.applicationEventExpiredSizeSensor.add(
            metrics.metricName(
                APPLICATION_EVENT_EXPIRED_SIZE_SENSOR_NAME,
                CONSUMER_METRIC_GROUP,
                "The current number of expired application events."
            ),
            new Value()
        );

        this.unsentRequestsQueueSizeSensor = metrics.sensor(UNSENT_REQUESTS_QUEUE_SIZE_SENSOR_NAME);
        this.unsentRequestsQueueSizeSensor.add(
            metrics.metricName(
                UNSENT_REQUESTS_QUEUE_SIZE_SENSOR_NAME,
                CONSUMER_METRIC_GROUP,
                "The current number of unsent requests in the background thread."
            ),
            new Value()
        );

        this.unsentRequestsQueueTimeSensor = metrics.sensor(UNSENT_REQUESTS_QUEUE_TIME_SENSOR_NAME);
        this.unsentRequestsQueueTimeSensor.add(
            metrics.metricName(
                "unsent-requests-queue-time-avg",
                CONSUMER_METRIC_GROUP,
                "The average time, in milliseconds, that requests are taking to be sent in the background thread."
            ),
            new Avg()
        );
        this.unsentRequestsQueueTimeSensor.add(
            metrics.metricName(
                "unsent-requests-queue-time-max",
                CONSUMER_METRIC_GROUP,
                "The maximum time, in milliseconds, that a request remained unsent in the background thread."
            ),
            new Max()
        );

        this.backgroundEventQueueSizeSensor = metrics.sensor(BACKGROUND_EVENT_QUEUE_SIZE_SENSOR_NAME);
        this.backgroundEventQueueSizeSensor.add(
            metrics.metricName(
                BACKGROUND_EVENT_QUEUE_SIZE_SENSOR_NAME,
                CONSUMER_METRIC_GROUP,
                "The current number of events in the queue to send from the background thread to the application thread."
            ),
            new Value()
        );

        this.backgroundEventQueueTimeSensor = metrics.sensor(BACKGROUND_EVENT_QUEUE_TIME_SENSOR_NAME);
        this.backgroundEventQueueTimeSensor.add(
            metrics.metricName(
                "background-event-queue-time-avg",
                CONSUMER_METRIC_GROUP,
                "The average time, in milliseconds, that background events are taking to be dequeued."
            ),
            new Avg()
        );
        this.backgroundEventQueueTimeSensor.add(
            metrics.metricName(
                "background-event-queue-time-max",
                CONSUMER_METRIC_GROUP,
                "The maximum time, in milliseconds, that background events are taking to be dequeued."
            ),
            new Max()
        );

        this.backgroundEventQueueProcessingTimeSensor = metrics.sensor(BACKGROUND_EVENT_QUEUE_PROCESSING_TIME_SENSOR_NAME);
        this.backgroundEventQueueProcessingTimeSensor.add(
            metrics.metricName(
                "background-event-queue-processing-time-avg",
                CONSUMER_METRIC_GROUP,
                "The average time, in milliseconds, that the consumer took to process all available background events."
            ),
            new Avg()
        );
        this.backgroundEventQueueProcessingTimeSensor.add(
            metrics.metricName(
                "background-event-queue-processing-time-max",
                CONSUMER_METRIC_GROUP,
                "The maximum time, in milliseconds, that the consumer took to process all available background events."
            ),
            new Max()
        );
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

    public void recordApplicationEventExpiredSize(long size) {
        this.applicationEventExpiredSizeSensor.record(size);
    }

    public void recordUnsentRequestsQueueSize(int size, long timeMs) {
        this.unsentRequestsQueueSizeSensor.record(size, timeMs);
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
        Arrays.asList(
            timeBetweenNetworkThreadPollSensor.name(),
            applicationEventQueueSizeSensor.name(),
            applicationEventQueueTimeSensor.name(),
            applicationEventQueueProcessingTimeSensor.name(),
            applicationEventExpiredSizeSensor.name(),
            backgroundEventQueueSizeSensor.name(),
            backgroundEventQueueTimeSensor.name(),
            backgroundEventQueueProcessingTimeSensor.name(),
            unsentRequestsQueueSizeSensor.name(),
            unsentRequestsQueueTimeSensor.name()
        ).forEach(metrics::removeSensor);
        super.close();
    }
}
