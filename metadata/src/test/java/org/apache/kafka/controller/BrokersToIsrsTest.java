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

package org.apache.kafka.controller;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.controller.BrokersToIsrs.PartitionsOnReplicaIterator;
import org.apache.kafka.controller.BrokersToIsrs.TopicIdPartition;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(40)
public class BrokersToIsrsTest {
    private static final Uuid[] UUIDS = new Uuid[] {
        Uuid.fromString("z5XgH_fQSAK3-RYoF2ymgw"),
        Uuid.fromString("U52uRe20RsGI0RvpcTx33Q")
    };

    private static Set<TopicIdPartition> toSet(TopicIdPartition... partitions) {
        HashSet<TopicIdPartition> set = new HashSet<>();
        for (TopicIdPartition partition : partitions) {
            set.add(partition);
        }
        return set;
    }

    private static Set<TopicIdPartition> toSet(PartitionsOnReplicaIterator iterator) {
        HashSet<TopicIdPartition> set = new HashSet<>();
        while (iterator.hasNext()) {
            set.add(iterator.next());
        }
        return set;
    }

    @Test
    public void testIterator() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        BrokersToIsrs brokersToIsrs = new BrokersToIsrs(snapshotRegistry);
        assertEquals(toSet(), toSet(brokersToIsrs.iterator(1, false)));
        brokersToIsrs.update(UUIDS[0], 0, null, new int[] {1, 2, 3}, -1, 1);
        brokersToIsrs.update(UUIDS[1], 1, null, new int[] {2, 3, 4}, -1, 4);
        assertEquals(toSet(new TopicIdPartition(UUIDS[0], 0)),
            toSet(brokersToIsrs.iterator(1, false)));
        assertEquals(toSet(new TopicIdPartition(UUIDS[0], 0),
                           new TopicIdPartition(UUIDS[1], 1)),
            toSet(brokersToIsrs.iterator(2, false)));
        assertEquals(toSet(new TopicIdPartition(UUIDS[1], 1)),
            toSet(brokersToIsrs.iterator(4, false)));
        assertEquals(toSet(), toSet(brokersToIsrs.iterator(5, false)));
        brokersToIsrs.update(UUIDS[1], 2, null, new int[] {3, 2, 1}, -1, 3);
        assertEquals(toSet(new TopicIdPartition(UUIDS[0], 0),
                new TopicIdPartition(UUIDS[1], 1),
                new TopicIdPartition(UUIDS[1], 2)),
            toSet(brokersToIsrs.iterator(2, false)));
    }

    @Test
    public void testLeadersOnlyIterator() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        BrokersToIsrs brokersToIsrs = new BrokersToIsrs(snapshotRegistry);
        brokersToIsrs.update(UUIDS[0], 0, null, new int[]{1, 2, 3}, -1, 1);
        brokersToIsrs.update(UUIDS[1], 1, null, new int[]{2, 3, 4}, -1, 4);
        assertEquals(toSet(new TopicIdPartition(UUIDS[0], 0)),
            toSet(brokersToIsrs.iterator(1, true)));
        assertEquals(toSet(), toSet(brokersToIsrs.iterator(2, true)));
        assertEquals(toSet(new TopicIdPartition(UUIDS[1], 1)),
            toSet(brokersToIsrs.iterator(4, true)));
        brokersToIsrs.update(UUIDS[0], 0, new int[]{1, 2, 3}, new int[]{1, 2, 3}, 1, 2);
        assertEquals(toSet(), toSet(brokersToIsrs.iterator(1, true)));
        assertEquals(toSet(new TopicIdPartition(UUIDS[0], 0)),
            toSet(brokersToIsrs.iterator(2, true)));
    }

    @Test
    public void testNoLeader() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        BrokersToIsrs brokersToIsrs = new BrokersToIsrs(snapshotRegistry);
        brokersToIsrs.update(UUIDS[0], 2, null, new int[]{1, 2, 3}, -1, 3);
        assertEquals(toSet(new TopicIdPartition(UUIDS[0], 2)),
            toSet(brokersToIsrs.iterator(3, true)));
        assertEquals(toSet(), toSet(brokersToIsrs.iterator(2, true)));
        assertEquals(toSet(), toSet(brokersToIsrs.noLeaderIterator()));
        brokersToIsrs.update(UUIDS[0], 2, new int[]{1, 2, 3}, new int[]{1, 2, 3}, 3, -1);
        assertEquals(toSet(new TopicIdPartition(UUIDS[0], 2)),
            toSet(brokersToIsrs.noLeaderIterator()));
    }
}
