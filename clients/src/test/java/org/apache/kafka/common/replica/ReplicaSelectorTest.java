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
import org.junit.Test;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.test.TestUtils.assertOptional;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReplicaSelectorTest {
    @Test
    public void testLeaderSelector() {
        TopicPartition tp = new TopicPartition("test", 0);

        Set<ReplicaView> replicaViewSet = replicaInfoSet();
        PartitionView partitionView = partitionInfo(replicaViewSet);

        ReplicaSelector selector = new LeaderReplicaSelector();
        Optional<ReplicaView> selected;

        ClientMetadata metadata = metadata("doesnt-matter");
        selected = selector.select(tp, metadata, partitionView);
        assertOptional(selected, replicaInfo -> {
            assertTrue(replicaInfo.isLeader());
            assertEquals(replicaInfo.endpoint().id(), 0);
        });

        selected = selector.select(tp, metadata, partitionInfo(Collections.emptySet()));
        assertFalse(selected.isPresent());
    }

    @Test
    public void testSameRackSelector() {
        TopicPartition tp = new TopicPartition("test", 0);

        Set<ReplicaView> replicaViewSet = replicaInfoSet();
        PartitionView partitionView = partitionInfo(replicaViewSet);

        ReplicaSelector selector = new RackAwareReplicaSelector();
        Optional<ReplicaView> selected = selector.select(tp, metadata("rack-b"), partitionView);
        assertOptional(selected, replicaInfo -> {
            assertEquals("Expect replica to be in rack-b", replicaInfo.endpoint().rack(), "rack-b");
            assertEquals("Expected replica 3 since it is more caught-up", replicaInfo.endpoint().id(), 3);
        });

        selected = selector.select(tp, metadata("not-a-rack"), partitionView);
        assertOptional(selected, replicaInfo -> {
            assertTrue("Expect leader when we can't find any nodes in given rack", replicaInfo.isLeader());
        });

        selected = selector.select(tp, metadata("rack-a"), partitionView);
        assertOptional(selected, replicaInfo -> {
            assertEquals("Expect replica to be in rack-a", replicaInfo.endpoint().rack(), "rack-a");
            assertTrue("Expect the leader since it's in rack-a", replicaInfo.isLeader());
        });


    }

    static Set<ReplicaView> replicaInfoSet() {
        return Stream.of(
                replicaInfo(new Node(0, "host0", 1234, "rack-a"), true, 4, 10),
                replicaInfo(new Node(1, "host1", 1234, "rack-a"), false, 2, 5),
                replicaInfo(new Node(2, "host2", 1234, "rack-b"), false, 3, 7),
                replicaInfo(new Node(3, "host3", 1234, "rack-b"), false, 4, 8)

        ).collect(Collectors.toSet());
    }

    static ReplicaView replicaInfo(Node node, boolean isLeader, long logOffset, long lastCaughtUpTimeMs) {
        return new ReplicaView() {

            @Override
            public boolean isLeader() {
                return isLeader;
            }

            @Override
            public Node endpoint() {
                return node;
            }

            @Override
            public long logEndOffset() {
                return logOffset;
            }

            @Override
            public Optional<Long> lastCaughtUpTimeMs() {
                return Optional.of(lastCaughtUpTimeMs);
            }
        };
    }

    static PartitionView partitionInfo(Set<ReplicaView> replicaViewSet) {
        return () -> replicaViewSet;
    }

    static ClientMetadata metadata(String rack) {
        return new ClientMetadata() {
            @Override
            public String rackId() {
                return rack;
            }

            @Override
            public String clientId() {
                return "test-client";
            }

            @Override
            public InetAddress clientAddress() {
                return InetAddress.getLoopbackAddress();
            }

            @Override
            public KafkaPrincipal principal() {
                return KafkaPrincipal.ANONYMOUS;
            }

            @Override
            public String listenerName() {
                return "test";
            }
        };
    }
}
