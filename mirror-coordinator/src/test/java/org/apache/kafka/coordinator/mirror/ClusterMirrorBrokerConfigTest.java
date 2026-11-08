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
package org.apache.kafka.coordinator.mirror;

import org.apache.kafka.common.config.ConfigDef;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClusterMirrorBrokerConfigTest {
    @Test
    public void testConfigDefIsNotNull() {
        ConfigDef configDef = ClusterMirrorBrokerConfig.CONFIG_DEF;
        assertNotNull(configDef);
    }

    @Test
    public void testConfigDefContainsCoordinatorConfigs() {
        ConfigDef configDef = ClusterMirrorBrokerConfig.CONFIG_DEF;
        assertTrue(configDef.names().contains(ClusterMirrorBrokerConfig.MIRROR_COORDINATOR_THREADS_CONFIG));
        assertTrue(configDef.names().contains(ClusterMirrorBrokerConfig.MIRROR_COORDINATOR_WRITE_TIMEOUT_MS_CONFIG));
        assertTrue(configDef.names().contains(ClusterMirrorBrokerConfig.MIRROR_COORDINATOR_APPEND_LINGER_MS_CONFIG));
        assertTrue(configDef.names().contains(ClusterMirrorBrokerConfig.MIRROR_COORDINATOR_LOAD_BUFFER_SIZE_CONFIG));
    }

    @Test
    public void testConfigDefContainsStateTopicConfigs() {
        ConfigDef configDef = ClusterMirrorBrokerConfig.CONFIG_DEF;
        assertTrue(configDef.names().contains(ClusterMirrorBrokerConfig.MIRROR_STATE_TOPIC_NUM_PARTITIONS_CONFIG));
        assertTrue(configDef.names().contains(ClusterMirrorBrokerConfig.MIRROR_STATE_TOPIC_REPLICATION_FACTOR_CONFIG));
        assertTrue(configDef.names().contains(ClusterMirrorBrokerConfig.MIRROR_STATE_TOPIC_MIN_ISR_CONFIG));
        assertTrue(configDef.names().contains(ClusterMirrorBrokerConfig.MIRROR_STATE_TOPIC_SEGMENT_BYTES_CONFIG));
    }

    @Test
    public void testConfigDefContainsReplicationConfigs() {
        ConfigDef configDef = ClusterMirrorBrokerConfig.CONFIG_DEF;
        assertTrue(configDef.names().contains(ClusterMirrorBrokerConfig.MIRROR_NUM_REPLICA_FETCHERS_CONFIG));
        assertTrue(configDef.names().contains(ClusterMirrorBrokerConfig.MIRROR_METADATA_REFRESH_INTERVAL_MS_CONFIG));
    }

    @Test
    public void testConfigDefContainsRetryConfigs() {
        ConfigDef configDef = ClusterMirrorBrokerConfig.CONFIG_DEF;
        assertTrue(configDef.names().contains(ClusterMirrorBrokerConfig.MIRROR_FAILED_RETRY_INITIAL_BACKOFF_MS_CONFIG));
        assertTrue(configDef.names().contains(ClusterMirrorBrokerConfig.MIRROR_FAILED_RETRY_MAX_BACKOFF_MS_CONFIG));
        assertTrue(configDef.names().contains(ClusterMirrorBrokerConfig.MIRROR_FAILED_RETRY_MAX_ATTEMPTS_CONFIG));
    }
}
