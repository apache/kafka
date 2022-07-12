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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class ReplicationPolicyTest {
    private static final DefaultReplicationPolicy DEFAULT_REPLICATION_POLICY = new DefaultReplicationPolicy();

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
}
