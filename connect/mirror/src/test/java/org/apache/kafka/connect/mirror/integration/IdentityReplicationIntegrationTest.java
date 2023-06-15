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
package org.apache.kafka.connect.mirror.integration;

import org.apache.kafka.connect.mirror.IdentityReplicationPolicy;

import java.util.HashMap;

import org.junit.jupiter.api.Tag;

import org.junit.jupiter.api.BeforeEach;

/**
 * Tests MM2 replication and failover logic for {@link IdentityReplicationPolicy}.
 *
 * <p>MM2 is configured with active/passive replication between two Kafka clusters with {@link IdentityReplicationPolicy}.
 * Tests validate that records sent to the primary cluster arrive at the backup cluster. Then, a consumer group is
 * migrated from the primary cluster to the backup cluster. Tests validate that consumer offsets
 * are translated and replicated from the primary cluster to the backup cluster during this failover.
 */
@Tag("integration")
public class IdentityReplicationIntegrationTest extends MirrorConnectorsIntegrationBaseTest {
    @BeforeEach
    public void startClusters() throws Exception {
        replicateBackupToPrimary = false;
        super.startClusters(new HashMap<String, String>() {{
                put("replication.policy.class", IdentityReplicationPolicy.class.getName());
                put("topics", "test-topic-.*");
            }});
    }

    /*
     * Returns expected topic name on target cluster.
     */
    @Override
    String remoteTopicName(String topic, String clusterAlias) {
        return topic;
    }
}
