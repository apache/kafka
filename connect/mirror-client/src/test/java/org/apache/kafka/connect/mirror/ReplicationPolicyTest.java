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

package org.apache.kafka.connect.mirror;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class ReplicationPolicyTest {
    private static final DefaultReplicationPolicy DEFAULT_REPLICATION_POLICY = new DefaultReplicationPolicy();

    @BeforeEach
    public void setUp() {
        DEFAULT_REPLICATION_POLICY.configure(Collections.emptyMap());
    }

    @Test
    public void testInternalTopic() {
        // starts with '__'
        assertTrue(DEFAULT_REPLICATION_POLICY.isInternalTopic("__consumer_offsets"));
        // starts with '.'
        assertTrue(DEFAULT_REPLICATION_POLICY.isInternalTopic(".hiddentopic"));

        // ends with '.internal': default DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG in standalone mode.
        assertTrue(DEFAULT_REPLICATION_POLICY.isInternalTopic("mm2-offsets.CLUSTER.internal"));
        // ends with '-internal'
        assertTrue(DEFAULT_REPLICATION_POLICY.isInternalTopic("mm2-offsets-CLUSTER-internal"));
        // non-internal topic.
        assertFalse(DEFAULT_REPLICATION_POLICY.isInternalTopic("mm2-offsets_CLUSTER_internal"));
    }


    @Test
    public void offsetSyncsTopic_shouldBeEffectedByInternalTopicSeparatorEnabled() {
        Map<String, Object> config =  new HashMap<>();
        config.put(MirrorClientConfig.REPLICATION_POLICY_SEPARATOR, "__");

        config.put(MirrorClientConfig.INTERNAL_TOPIC_SEPARATOR_ENABLED, false);
        DEFAULT_REPLICATION_POLICY.configure(config);
        assertEquals("mm2-offset-syncs.CLUSTER.internal", DEFAULT_REPLICATION_POLICY.offsetSyncsTopic("CLUSTER"));

        config.put(MirrorClientConfig.INTERNAL_TOPIC_SEPARATOR_ENABLED, true);
        DEFAULT_REPLICATION_POLICY.configure(config);
        assertEquals("mm2-offset-syncs__CLUSTER__internal", DEFAULT_REPLICATION_POLICY.offsetSyncsTopic("CLUSTER"));
    }

    @Test
    public void checkpointsTopic_shouldBeEffectedByInternalTopicSeparatorEnabled() {
        Map<String, Object> config =  new HashMap<>();
        config.put(MirrorClientConfig.REPLICATION_POLICY_SEPARATOR, "__");

        config.put(MirrorClientConfig.INTERNAL_TOPIC_SEPARATOR_ENABLED, false);
        DEFAULT_REPLICATION_POLICY.configure(config);
        assertEquals("CLUSTER.checkpoints.internal", DEFAULT_REPLICATION_POLICY.checkpointsTopic("CLUSTER"));

        config.put(MirrorClientConfig.INTERNAL_TOPIC_SEPARATOR_ENABLED, true);
        DEFAULT_REPLICATION_POLICY.configure(config);
        assertEquals("CLUSTER__checkpoints__internal", DEFAULT_REPLICATION_POLICY.checkpointsTopic("CLUSTER"));
    }

    @Test
    public void heartbeatsTopic_shouldNotBeEffectedByInternalTopicSeparatorConfig() {
        Map<String, Object> config =  new HashMap<>();
        config.put(MirrorClientConfig.REPLICATION_POLICY_SEPARATOR, "__");

        config.put(MirrorClientConfig.INTERNAL_TOPIC_SEPARATOR_ENABLED, true);
        assertEquals("heartbeats", DEFAULT_REPLICATION_POLICY.heartbeatsTopic());

        config.put(MirrorClientConfig.INTERNAL_TOPIC_SEPARATOR_ENABLED, false);
        DEFAULT_REPLICATION_POLICY.configure(config);
        assertEquals("heartbeats", DEFAULT_REPLICATION_POLICY.heartbeatsTopic());
    }
}
