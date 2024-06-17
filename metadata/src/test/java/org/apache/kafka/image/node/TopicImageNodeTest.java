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

package org.apache.kafka.image.node;

import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.node.printer.NodeStringifier;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;


@Timeout(value = 40)
public class TopicImageNodeTest {
    private final static TopicImageNode NODE = new TopicImageNode(newTopicImage("topic-image-node-test-topic", Uuid.ZERO_UUID, new PartitionRegistration.Builder().setReplicas(new int[] {2, 3, 4}).
            setDirectories(DirectoryId.migratingArray(3)).
            setIsr(new int[] {2, 3}).setLeader(2).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(1).setPartitionEpoch(345).build()));

    private static TopicImage newTopicImage(String name, Uuid id, PartitionRegistration... partitions) {
        Map<Integer, PartitionRegistration> partitionMap = new HashMap<>();
        int i = 0;
        for (PartitionRegistration partition : partitions) {
            partitionMap.put(i++, partition);
        }
        return new TopicImage(name, id, partitionMap);
    }
    @Test
    public void testChildNames() {
        assertEquals(Arrays.asList("name", "id", "0"), NODE.childNames());
    }

    @Test
    public void testNameChild() {
        MetadataNode child = NODE.child("name");
        assertNotNull(child);
        assertEquals(MetadataLeafNode.class, child.getClass());
    }

    @Test
    public void testIdChild() {
        MetadataNode child = NODE.child("id");
        assertNotNull(child);
        assertEquals(MetadataLeafNode.class, child.getClass());
    }

    @Test
    public void testUnknownChild() {
        assertNull(NODE.child("unknown"));
    }

    @Test
    public void testChildPartitionId() {
        MetadataNode child = NODE.child("0");
        assertNotNull(child);
        NodeStringifier stringifier = new NodeStringifier();
        child.print(stringifier);
        assertEquals("PartitionRegistration(replicas=[2, 3, 4], " +
                "directories=[AAAAAAAAAAAAAAAAAAAAAA, AAAAAAAAAAAAAAAAAAAAAA, AAAAAAAAAAAAAAAAAAAAAA], " +
                "isr=[2, 3], removingReplicas=[], addingReplicas=[], elr=[], lastKnownElr=[], leader=2, " +
                "leaderRecoveryState=RECOVERED, leaderEpoch=1, partitionEpoch=345)", stringifier.toString());
    }

    @Test
    public void testChildPartitionIdNull() {
        MetadataNode child1 = NODE.child("1");
        MetadataNode child2 = NODE.child("a");
        assertNull(child1);
        assertNull(child2);
    }
}
