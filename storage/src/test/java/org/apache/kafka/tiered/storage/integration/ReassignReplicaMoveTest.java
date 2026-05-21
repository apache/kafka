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
package org.apache.kafka.tiered.storage.integration;

import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterTemplate;

import java.util.List;

public final class ReassignReplicaMoveTest extends BaseReassignReplicaTest {

    private static List<ClusterConfig> clusterConfig() {
        return BaseReassignReplicaTest.clusterConfig(ReassignReplicaMoveTest.class.getSimpleName());
    }

    @ClusterTemplate("clusterConfig")
    public void testReassignReplicaMoveWithClassicGroupProtocol(ClusterInstance clusterInstance) throws Exception {
        executeReassignReplicaTest(clusterInstance, GroupProtocol.CLASSIC);
    }

    @ClusterTemplate("clusterConfig")
    public void testReassignReplicaMoveWithConsumerGroupProtocol(ClusterInstance clusterInstance) throws Exception {
        executeReassignReplicaTest(clusterInstance, GroupProtocol.CONSUMER);
    }

    /**
     * Move the replica of the topic from broker0 to broker1
     * @return the replica-ids of the topic
     */
    @Override
    protected List<Integer> replicaIds() {
        return List.of(broker1);
    }
}
