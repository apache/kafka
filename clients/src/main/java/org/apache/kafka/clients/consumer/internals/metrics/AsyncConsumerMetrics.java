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

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;

public class AsyncConsumerMetrics extends KafkaConsumerMetrics implements AutoCloseable {
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

        this.timeBetweenNetworkThreadPollSensor = newSensorBuilder(TIME_BETWEEN_NETWORK_THREAD_POLL_SENSOR_NAME)
            .withAvg(
                TIME_BETWEEN_NETWORK_THREAD_POLL_SENSOR_NAME + "-avg",
                "The average time taken, in milliseconds, between each poll in the network thread."
            )
            .withMax(
                TIME_BETWEEN_NETWORK_THREAD_POLL_SENSOR_NAME + "-max",
                "The maximum time taken, in milliseconds, between each poll in the network thread."
            )
            .sensor();

        this.applicationEventQueueSizeSensor = newSensorBuilder(APPLICATION_EVENT_QUEUE_SIZE_SENSOR_NAME)
            .withValue(
                APPLICATION_EVENT_QUEUE_SIZE_SENSOR_NAME,
                "The current number of events in the queue to send from the application thread to the background thread."
            )
            .sensor();

        this.applicationEventQueueTimeSensor = newSensorBuilder(APPLICATION_EVENT_QUEUE_TIME_SENSOR_NAME)
            .withAvg(
                APPLICATION_EVENT_QUEUE_TIME_SENSOR_NAME + "-avg",
                "The average time, in milliseconds, that application events are taking to be dequeued."
            )
            .withMax(
                APPLICATION_EVENT_QUEUE_TIME_SENSOR_NAME + "-max",
                "The maximum time, in milliseconds, that an application event took to be dequeued."
            )
            .sensor();

        this.applicationEventQueueProcessingTimeSensor = newSensorBuilder(APPLICATION_EVENT_QUEUE_PROCESSING_TIME_SENSOR_NAME)
            .withAvg(
                APPLICATION_EVENT_QUEUE_PROCESSING_TIME_SENSOR_NAME + "-avg",
                "The average time, in milliseconds, that the background thread takes to process all available application events."
            )
            .withMax(
                APPLICATION_EVENT_QUEUE_PROCESSING_TIME_SENSOR_NAME + "-max",
                "The maximum time, in milliseconds, that the background thread took to process all available application events."
            )
            .sensor();

        this.applicationEventExpiredSizeSensor = newSensorBuilder(APPLICATION_EVENT_EXPIRED_SIZE_SENSOR_NAME)
            .withValue(
                APPLICATION_EVENT_EXPIRED_SIZE_SENSOR_NAME,
                "The current number of expired application events."
            )
            .sensor();

        this.unsentRequestsQueueSizeSensor = newSensorBuilder(UNSENT_REQUESTS_QUEUE_SIZE_SENSOR_NAME)
            .withValue(
                UNSENT_REQUESTS_QUEUE_SIZE_SENSOR_NAME,
                "The current number of unsent requests in the background thread."
            )
            .sensor();

        this.unsentRequestsQueueTimeSensor = newSensorBuilder(UNSENT_REQUESTS_QUEUE_TIME_SENSOR_NAME)
            .withAvg(
                UNSENT_REQUESTS_QUEUE_TIME_SENSOR_NAME + "-avg",
                "The average time, in milliseconds, that requests are taking to be sent in the background thread."
            )
            .withMax(
                UNSENT_REQUESTS_QUEUE_TIME_SENSOR_NAME + "-max",
                "The maximum time, in milliseconds, that a request remained unsent in the background thread."
            )
            .sensor();

        this.backgroundEventQueueSizeSensor = newSensorBuilder(BACKGROUND_EVENT_QUEUE_SIZE_SENSOR_NAME)
            .withValue(
                BACKGROUND_EVENT_QUEUE_SIZE_SENSOR_NAME,
                "The current number of events in the queue to send from the background thread to the application thread."
            )
            .sensor();

        this.backgroundEventQueueTimeSensor = newSensorBuilder(BACKGROUND_EVENT_QUEUE_TIME_SENSOR_NAME)
            .withAvg(
                BACKGROUND_EVENT_QUEUE_TIME_SENSOR_NAME + "-avg",
                "The average time, in milliseconds, that background events are taking to be dequeued."
            )
            .withMax(
                BACKGROUND_EVENT_QUEUE_TIME_SENSOR_NAME + "-max",
                "The maximum time, in milliseconds, that background events are taking to be dequeued."
            )
            .sensor();

        this.backgroundEventQueueProcessingTimeSensor = newSensorBuilder(BACKGROUND_EVENT_QUEUE_PROCESSING_TIME_SENSOR_NAME)
            .withAvg(
                BACKGROUND_EVENT_QUEUE_PROCESSING_TIME_SENSOR_NAME + "-avg",
                "The average time, in milliseconds, that the consumer took to process all available background events."
            )
            .withMax(
                BACKGROUND_EVENT_QUEUE_PROCESSING_TIME_SENSOR_NAME + "-max",
                "The maximum time, in milliseconds, that the consumer took to process all available background events."
            )
            .sensor();
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
}
