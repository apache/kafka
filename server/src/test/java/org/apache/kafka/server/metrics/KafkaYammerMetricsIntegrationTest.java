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
package org.apache.kafka.server.metrics;

import com.yammer.metrics.core.MetricName;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.log.remote.storage.RemoteStorageMetrics;

import java.util.HashMap;

import static org.apache.kafka.server.config.ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG;
import static org.apache.kafka.server.config.ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaYammerMetricsIntegrationTest {

    @SuppressWarnings("deprecation")
    @ClusterTest(
        types = {Type.KRAFT, Type.CO_KRAFT},
        brokers = 3,
        serverProperties = {
            @ClusterConfigProperty(key = StandardAuthorizer.SUPER_USERS_CONFIG, value = "User:ANONYMOUS"),
            @ClusterConfigProperty(key = AUTHORIZER_CLASS_NAME_CONFIG, value = "org.apache.kafka.metadata.authorizer.StandardAuthorizer"),
            @ClusterConfigProperty(key = REPLICA_SELECTOR_CLASS_CONFIG, value = "org.apache.kafka.common.replica.RackAwareReplicaSelector"),
            @ClusterConfigProperty(key = REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, value = "true"),
            @ClusterConfigProperty(key = REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, value = "org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager"),
            @ClusterConfigProperty(key = REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, value = "org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager")
        }
    )
    public void testRemoteStorageMetricsKafkaYammerMetricsNaming(ClusterInstance cluster) {
        var allMetrics = KafkaYammerMetrics.defaultRegistry().allMetrics();
        var metricNameCounts = new HashMap<MetricName, Integer>();

        allMetrics.keySet().forEach(metricName ->
                metricNameCounts.merge(metricName, 1, Integer::sum)
        );
        
        assertEquals(1, metricNameCounts.get(RemoteStorageMetrics.REMOTE_LOG_READER_TASK_QUEUE_SIZE_METRIC));
        assertEquals(1, metricNameCounts.get(RemoteStorageMetrics.REMOTE_LOG_READER_AVG_IDLE_PERCENT_METRIC));
        assertEquals(1, metricNameCounts.get(RemoteStorageMetrics.BRIDGE_REMOTE_LOG_READER_TASK_QUEUE_SIZE_METRIC));
        assertEquals(1, metricNameCounts.get(RemoteStorageMetrics.BRIDGE_REMOTE_LOG_READER_AVG_IDLE_PERCENT_METRIC));
    }
}
