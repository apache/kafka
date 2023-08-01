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
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import java.util.HashSet;
import java.util.Set;

public class RemoteStorageMetrics {
    final static MetricName REMOTE_BYTES_OUT_PER_SEC = getMetricName(
            "BrokerTopicMetrics", "RemoteBytesOutPerSec");
    final static MetricName REMOTE_BYTES_IN_PER_SEC = getMetricName(
            "BrokerTopicMetrics", "RemoteBytesInPerSec");
    final static MetricName REMOTE_READ_REQUESTS_PER_SEC = getMetricName(
            "BrokerTopicMetrics", "RemoteReadRequestsPerSec");
    final static MetricName REMOTE_WRITE_REQUESTS_PER_SEC = getMetricName(
            "BrokerTopicMetrics", "RemoteWriteRequestsPerSec");
    final static MetricName FAILED_REMOTE_READ_REQUESTS_PER_SEC = getMetricName(
            "BrokerTopicMetrics", "RemoteReadErrorsPerSec");
    final static MetricName FAILED_REMOTE_WRITE_REQUESTS_PER_SEC = getMetricName(
            "BrokerTopicMetrics", "RemoteWriteErrorsPerSec");

    public static Set<MetricName> allMetrics() {
        Set<MetricName> metrics = new HashSet<>();
        metrics.add(REMOTE_BYTES_OUT_PER_SEC);
        metrics.add(REMOTE_BYTES_IN_PER_SEC);
        metrics.add(REMOTE_READ_REQUESTS_PER_SEC);
        metrics.add(REMOTE_WRITE_REQUESTS_PER_SEC);
        metrics.add(FAILED_REMOTE_READ_REQUESTS_PER_SEC);
        metrics.add(FAILED_REMOTE_WRITE_REQUESTS_PER_SEC);

        return metrics;
    }
    private static MetricName getMetricName(String type, String name) {
        return KafkaYammerMetrics.getMetricName("kafka.server", type, name);
    }
}
