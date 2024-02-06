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

import com.yammer.metrics.core.MetricName;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * This class contains the metrics related to tiered storage feature, which is to have a centralized
 * place to store them, so that we can verify all of them easily.
 *
 * @see kafka.api.MetricsTest
 */
public class RemoteStorageMetrics {
    private static final String REMOTE_LOG_READER_METRICS_NAME_PREFIX = "RemoteLogReader";
    private static final String REMOTE_LOG_MANAGER_TASKS_AVG_IDLE_PERCENT = "RemoteLogManagerTasksAvgIdlePercent";
    private static final String TASK_QUEUE_SIZE = "TaskQueueSize";
    private static final String AVG_IDLE_PERCENT = "AvgIdlePercent";
    private static final String REMOTE_COPY_BYTES_PER_SEC = "RemoteCopyBytesPerSec";
    private static final String REMOTE_FETCH_BYTES_PER_SEC = "RemoteFetchBytesPerSec";
    private static final String REMOTE_FETCH_REQUESTS_PER_SEC = "RemoteFetchRequestsPerSec";
    private static final String REMOTE_COPY_REQUESTS_PER_SEC = "RemoteCopyRequestsPerSec";
    private static final String REMOTE_DELETE_REQUESTS_PER_SEC = "RemoteDeleteRequestsPerSec";
    private static final String BUILD_REMOTE_LOG_AUX_STATE_REQUESTS_PER_SEC = "BuildRemoteLogAuxStateRequestsPerSec";
    private static final String FAILED_REMOTE_FETCH_PER_SEC = "RemoteFetchErrorsPerSec";
    private static final String FAILED_REMOTE_COPY_PER_SEC = "RemoteCopyErrorsPerSec";
    private static final String REMOTE_LOG_METADATA_COUNT = "RemoteLogMetadataCount";
    private static final String REMOTE_LOG_SIZE_BYTES = "RemoteLogSizeBytes";
    private static final String REMOTE_LOG_SIZE_COMPUTATION_TIME = "RemoteLogSizeComputationTime";
    private static final String FAILED_REMOTE_DELETE_PER_SEC = "RemoteDeleteErrorsPerSec";
    private static final String FAILED_BUILD_REMOTE_LOG_AUX_STATE_PER_SEC = "BuildRemoteLogAuxStateErrorsPerSec";
    private static final String REMOTE_COPY_LAG_BYTES = "RemoteCopyLagBytes";
    private static final String REMOTE_COPY_LAG_SEGMENTS = "RemoteCopyLagSegments";
    private static final String REMOTE_DELETE_LAG_BYTES = "RemoteDeleteLagBytes";
    private static final String REMOTE_DELETE_LAG_SEGMENTS = "RemoteDeleteLagSegments";
    private static final String REMOTE_LOG_READER_TASK_QUEUE_SIZE = REMOTE_LOG_READER_METRICS_NAME_PREFIX + TASK_QUEUE_SIZE;
    private static final String REMOTE_LOG_READER_AVG_IDLE_PERCENT = REMOTE_LOG_READER_METRICS_NAME_PREFIX + AVG_IDLE_PERCENT;
    public static final Set<String> REMOTE_STORAGE_THREAD_POOL_METRICS = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REMOTE_LOG_READER_TASK_QUEUE_SIZE, REMOTE_LOG_READER_AVG_IDLE_PERCENT)));

    public final static MetricName REMOTE_COPY_BYTES_PER_SEC_METRIC = getMetricName(
            "kafka.server", "BrokerTopicMetrics", REMOTE_COPY_BYTES_PER_SEC);
    public final static MetricName REMOTE_FETCH_BYTES_PER_SEC_METRIC = getMetricName(
            "kafka.server", "BrokerTopicMetrics", REMOTE_FETCH_BYTES_PER_SEC);
    public final static MetricName REMOTE_FETCH_REQUESTS_PER_SEC_METRIC = getMetricName(
            "kafka.server", "BrokerTopicMetrics", REMOTE_FETCH_REQUESTS_PER_SEC);
    public final static MetricName REMOTE_COPY_REQUESTS_PER_SEC_METRIC = getMetricName(
            "kafka.server", "BrokerTopicMetrics", REMOTE_COPY_REQUESTS_PER_SEC);
    public final static MetricName REMOTE_DELETE_REQUESTS_PER_SEC_METRIC = getMetricName(
            "kafka.server", "BrokerTopicMetrics", REMOTE_DELETE_REQUESTS_PER_SEC);
    public final static MetricName BUILD_REMOTE_LOG_AUX_STATE_REQUESTS_PER_SEC_METRIC = getMetricName(
            "kafka.server", "BrokerTopicMetrics", BUILD_REMOTE_LOG_AUX_STATE_REQUESTS_PER_SEC);
    public final static MetricName FAILED_REMOTE_FETCH_PER_SEC_METRIC = getMetricName(
            "kafka.server", "BrokerTopicMetrics", FAILED_REMOTE_FETCH_PER_SEC);
    public final static MetricName FAILED_REMOTE_COPY_PER_SEC_METRIC = getMetricName(
            "kafka.server", "BrokerTopicMetrics", FAILED_REMOTE_COPY_PER_SEC);
    public final static MetricName REMOTE_LOG_METADATA_COUNT_METRIC = getMetricName(
            "kafka.server", "BrokerTopicMetrics", REMOTE_LOG_METADATA_COUNT);
    public final static MetricName REMOTE_LOG_SIZE_BYTES_METRIC = getMetricName(
            "kafka.server", "BrokerTopicMetrics", REMOTE_LOG_SIZE_BYTES);
    public final static MetricName REMOTE_LOG_SIZE_COMPUTATION_TIME_METRIC = getMetricName(
            "kafka.server", "BrokerTopicMetrics", REMOTE_LOG_SIZE_COMPUTATION_TIME);
    public final static MetricName FAILED_REMOTE_DELETE_PER_SEC_METRIC = getMetricName(
            "kafka.server", "BrokerTopicMetrics", FAILED_REMOTE_DELETE_PER_SEC);
    public final static MetricName FAILED_BUILD_REMOTE_LOG_AUX_STATE_PER_SEC_METRIC = getMetricName(
            "kafka.server", "BrokerTopicMetrics", FAILED_BUILD_REMOTE_LOG_AUX_STATE_PER_SEC);
    public final static MetricName REMOTE_COPY_LAG_BYTES_METRIC = getMetricName(
            "kafka.server", "BrokerTopicMetrics", REMOTE_COPY_LAG_BYTES);
    public final static MetricName REMOTE_COPY_LAG_SEGMENTS_METRIC = getMetricName(
            "kafka.server", "BrokerTopicMetrics", REMOTE_COPY_LAG_SEGMENTS);
    public final static MetricName REMOTE_DELETE_LAG_BYTES_METRIC = getMetricName(
            "kafka.server", "BrokerTopicMetrics", REMOTE_DELETE_LAG_BYTES);
    public final static MetricName REMOTE_DELETE_LAG_SEGMENTS_METRIC = getMetricName(
            "kafka.server", "BrokerTopicMetrics", REMOTE_DELETE_LAG_SEGMENTS);
    public final static MetricName REMOTE_LOG_MANAGER_TASKS_AVG_IDLE_PERCENT_METRIC = getMetricName(
            "kafka.log.remote", "RemoteLogManager", REMOTE_LOG_MANAGER_TASKS_AVG_IDLE_PERCENT);
    public final static MetricName REMOTE_LOG_READER_TASK_QUEUE_SIZE_METRIC = getMetricName(
            "org.apache.kafka.storage.internals.log", "RemoteStorageThreadPool", REMOTE_LOG_READER_TASK_QUEUE_SIZE);
    public final static MetricName REMOTE_LOG_READER_AVG_IDLE_PERCENT_METRIC = getMetricName(
            "org.apache.kafka.storage.internals.log", "RemoteStorageThreadPool", REMOTE_LOG_READER_AVG_IDLE_PERCENT);

    public static Set<MetricName> allMetrics() {
        Set<MetricName> metrics = new HashSet<>();

        metrics.add(REMOTE_COPY_BYTES_PER_SEC_METRIC);
        metrics.add(REMOTE_FETCH_BYTES_PER_SEC_METRIC);
        metrics.add(REMOTE_FETCH_REQUESTS_PER_SEC_METRIC);
        metrics.add(REMOTE_COPY_REQUESTS_PER_SEC_METRIC);
        metrics.add(REMOTE_DELETE_REQUESTS_PER_SEC_METRIC);
        metrics.add(BUILD_REMOTE_LOG_AUX_STATE_REQUESTS_PER_SEC_METRIC);
        metrics.add(FAILED_REMOTE_FETCH_PER_SEC_METRIC);
        metrics.add(FAILED_REMOTE_COPY_PER_SEC_METRIC);
        metrics.add(FAILED_REMOTE_DELETE_PER_SEC_METRIC);
        metrics.add(FAILED_BUILD_REMOTE_LOG_AUX_STATE_PER_SEC_METRIC);
        metrics.add(REMOTE_COPY_LAG_BYTES_METRIC);
        metrics.add(REMOTE_COPY_LAG_SEGMENTS_METRIC);
        metrics.add(REMOTE_DELETE_LAG_BYTES_METRIC);
        metrics.add(REMOTE_DELETE_LAG_SEGMENTS_METRIC);
        metrics.add(REMOTE_LOG_MANAGER_TASKS_AVG_IDLE_PERCENT_METRIC);
        metrics.add(REMOTE_LOG_READER_AVG_IDLE_PERCENT_METRIC);
        metrics.add(REMOTE_LOG_READER_TASK_QUEUE_SIZE_METRIC);
        metrics.add(REMOTE_LOG_METADATA_COUNT_METRIC);
        metrics.add(REMOTE_LOG_SIZE_COMPUTATION_TIME_METRIC);
        metrics.add(REMOTE_LOG_SIZE_BYTES_METRIC);

        return metrics;
    }

    public static Set<MetricName> brokerTopicStatsMetrics() {
        Set<MetricName> metrics = new HashSet<>();

        metrics.add(REMOTE_COPY_BYTES_PER_SEC_METRIC);
        metrics.add(REMOTE_FETCH_BYTES_PER_SEC_METRIC);
        metrics.add(REMOTE_FETCH_REQUESTS_PER_SEC_METRIC);
        metrics.add(REMOTE_COPY_REQUESTS_PER_SEC_METRIC);
        metrics.add(REMOTE_DELETE_REQUESTS_PER_SEC_METRIC);
        metrics.add(BUILD_REMOTE_LOG_AUX_STATE_REQUESTS_PER_SEC_METRIC);
        metrics.add(FAILED_REMOTE_FETCH_PER_SEC_METRIC);
        metrics.add(FAILED_REMOTE_COPY_PER_SEC_METRIC);
        metrics.add(REMOTE_LOG_METADATA_COUNT_METRIC);
        metrics.add(REMOTE_LOG_SIZE_COMPUTATION_TIME_METRIC);
        metrics.add(REMOTE_LOG_SIZE_BYTES_METRIC);
        metrics.add(FAILED_REMOTE_DELETE_PER_SEC_METRIC);
        metrics.add(FAILED_BUILD_REMOTE_LOG_AUX_STATE_PER_SEC_METRIC);
        metrics.add(REMOTE_COPY_LAG_BYTES_METRIC);
        metrics.add(REMOTE_COPY_LAG_SEGMENTS_METRIC);
        metrics.add(REMOTE_DELETE_LAG_BYTES_METRIC);
        metrics.add(REMOTE_DELETE_LAG_SEGMENTS_METRIC);

        return metrics;
    }

    private static MetricName getMetricName(String group, String type, String name) {
        return KafkaYammerMetrics.getMetricName(group, type, name);
    }
}
