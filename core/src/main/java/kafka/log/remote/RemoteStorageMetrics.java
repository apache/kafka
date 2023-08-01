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
package kafka.log.remote;

import com.yammer.metrics.core.MetricName;
import kafka.server.BrokerTopicStats;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import java.util.HashSet;
import java.util.Set;

import static org.apache.kafka.storage.internals.log.RemoteStorageThreadPool.AVG_IDLE_PERCENT;
import static org.apache.kafka.storage.internals.log.RemoteStorageThreadPool.TASK_QUEUE_SIZE;

/**
 * This class contains the metrics related to tiered storage feature, which is to have a centralized
 * place to store them, so that we can verify all of them easily.
 *
 * @see kafka.api.MetricsTest
 */
public class RemoteStorageMetrics {
    final static MetricName REMOTE_BYTES_OUT_PER_SEC = getMetricName(
        "kafka.server", "BrokerTopicMetrics", BrokerTopicStats.RemoteBytesOutPerSec());
    final static MetricName REMOTE_BYTES_IN_PER_SEC = getMetricName(
        "kafka.server", "BrokerTopicMetrics", BrokerTopicStats.RemoteBytesInPerSec());
    final static MetricName REMOTE_READ_REQUESTS_PER_SEC = getMetricName(
        "kafka.server", "BrokerTopicMetrics", BrokerTopicStats.RemoteReadRequestsPerSec());
    final static MetricName REMOTE_WRITE_REQUESTS_PER_SEC = getMetricName(
        "kafka.server", "BrokerTopicMetrics", BrokerTopicStats.RemoteWriteRequestsPerSec());
    final static MetricName FAILED_REMOTE_READ_REQUESTS_PER_SEC = getMetricName(
        "kafka.server", "BrokerTopicMetrics", BrokerTopicStats.FailedRemoteReadRequestsPerSec());
    final static MetricName FAILED_REMOTE_WRITE_REQUESTS_PER_SEC = getMetricName(
        "kafka.server", "BrokerTopicMetrics", BrokerTopicStats.FailedRemoteWriteRequestsPerSec());
    final static MetricName REMOTE_LOG_MANAGER_TASKS_AVG_IDLE_PERCENT = getMetricName(
        "kafka.log.remote", "RemoteLogManager", RemoteLogManager.REMOTE_LOG_MANAGER_TASKS_AVG_IDLE_PERCENT);
    final static MetricName REMOTE_LOG_READER_TASK_QUEUE_SIZE = getMetricName(
        "org.apache.kafka.storage.internals.log", "RemoteStorageThreadPool", RemoteLogManager.REMOTE_LOG_READER_METRICS_NAME_PREFIX + TASK_QUEUE_SIZE);
    final static MetricName REMOTE_LOG_READER_AVG_IDLE_PERCENT = getMetricName(
        "org.apache.kafka.storage.internals.log", "RemoteStorageThreadPool", RemoteLogManager.REMOTE_LOG_READER_METRICS_NAME_PREFIX + AVG_IDLE_PERCENT);

    public static Set<MetricName> allMetrics() {
        Set<MetricName> metrics = new HashSet<>();
        metrics.add(REMOTE_BYTES_OUT_PER_SEC);
        metrics.add(REMOTE_BYTES_IN_PER_SEC);
        metrics.add(REMOTE_READ_REQUESTS_PER_SEC);
        metrics.add(REMOTE_WRITE_REQUESTS_PER_SEC);
        metrics.add(FAILED_REMOTE_READ_REQUESTS_PER_SEC);
        metrics.add(FAILED_REMOTE_WRITE_REQUESTS_PER_SEC);
        metrics.add(REMOTE_LOG_MANAGER_TASKS_AVG_IDLE_PERCENT);
        metrics.add(REMOTE_LOG_READER_AVG_IDLE_PERCENT);
        metrics.add(REMOTE_LOG_READER_TASK_QUEUE_SIZE);

        return metrics;
    }
    private static MetricName getMetricName(String group, String type, String name) {
        return KafkaYammerMetrics.getMetricName(group, type, name);
    }
}
