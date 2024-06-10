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
package org.apache.kafka.common.replica;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.test.TestUtils.assertOptional;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ReplicaSelectorTest {

    @Test
    public void testSameRackSelector() {
        TopicPartition tp = new TopicPartition("test", 0);

        List<ReplicaView> replicaViewSet = replicaInfoSet();
        ReplicaView leader = replicaViewSet.get(0);
        PartitionView partitionView = partitionInfo(new HashSet<>(replicaViewSet), leader);

        ReplicaSelector selector = new RackAwareReplicaSelector();
        Optional<ReplicaView> selected = selector.select(tp, metadata("rack-b"), partitionView);
        assertOptional(selected, replicaInfo -> {
            assertEquals(replicaInfo.endpoint().rack(), "rack-b", "Expect replica to be in rack-b");
            assertEquals(replicaInfo.endpoint().id(), 3, "Expected replica 3 since it is more caught-up");
        });

        selected = selector.select(tp, metadata("not-a-rack"), partitionView);
        assertOptional(selected, replicaInfo -> {
            assertEquals(replicaInfo, leader, "Expect leader when we can't find any nodes in given rack");
        });

        selected = selector.select(tp, metadata("rack-a"), partitionView);
        assertOptional(selected, replicaInfo -> {
            assertEquals(replicaInfo.endpoint().rack(), "rack-a", "Expect replica to be in rack-a");
            assertEquals(replicaInfo, leader, "Expect the leader since it's in rack-a");
        });


    }

    static List<ReplicaView> replicaInfoSet() {
        return Stream.of(
                replicaInfo(new Node(0, "host0", 1234, "rack-a"), 4, 0),
                replicaInfo(new Node(1, "host1", 1234, "rack-a"), 2, 5),
                replicaInfo(new Node(2, "host2", 1234, "rack-b"), 3, 3),
                replicaInfo(new Node(3, "host3", 1234, "rack-b"), 4, 2)

        ).collect(Collectors.toList());
    }

    static ReplicaView replicaInfo(Node node, long logOffset, long timeSinceLastCaughtUpMs) {
        return new ReplicaView.DefaultReplicaView(node, logOffset, timeSinceLastCaughtUpMs);
    }

    static PartitionView partitionInfo(Set<ReplicaView> replicaViewSet, ReplicaView leader) {
        return new PartitionView.DefaultPartitionView(replicaViewSet, leader);
    }

    static ClientMetadata metadata(String rack) {
        return new ClientMetadata.DefaultClientMetadata(rack, "test-client",
                InetAddress.getLoopbackAddress(), KafkaPrincipal.ANONYMOUS, "TEST");
    }
}
