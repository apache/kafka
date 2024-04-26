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
import org.apache.kafka.server.common.TopicIdPartition;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BrokerToElrsTest {
    private static final Uuid[] UUIDS = new Uuid[] {
            Uuid.fromString("z5XgH_fQSAK3-RYoF2ymgw"),
            Uuid.fromString("U52uRe20RsGI0RvpcTx33Q")
    };

    private static Set<TopicIdPartition> toSet(TopicIdPartition... partitions) {
        return new HashSet<>(Arrays.asList(partitions));
    }

    private static Set<TopicIdPartition> toSet(BrokersToIsrs.PartitionsOnReplicaIterator iterator) {
        HashSet<TopicIdPartition> set = new HashSet<>();
        while (iterator.hasNext()) {
            set.add(iterator.next());
        }
        return set;
    }

    @Test
    public void testIterator() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        BrokersToElrs brokersToElrs = new BrokersToElrs(snapshotRegistry);
        assertEquals(toSet(), toSet(brokersToElrs.partitionsWithBrokerInElr(1)));
        brokersToElrs.update(UUIDS[0], 0, null, new int[] {1, 2, 3});
        brokersToElrs.update(UUIDS[1], 1, null, new int[] {2, 3, 4});
        assertEquals(toSet(new TopicIdPartition(UUIDS[0], 0)),
            toSet(brokersToElrs.partitionsWithBrokerInElr(1)));
        assertEquals(toSet(new TopicIdPartition(UUIDS[0], 0),
                new TopicIdPartition(UUIDS[1], 1)),
            toSet(brokersToElrs.partitionsWithBrokerInElr(2)));
        assertEquals(toSet(new TopicIdPartition(UUIDS[1], 1)),
            toSet(brokersToElrs.partitionsWithBrokerInElr(4)));
        assertEquals(toSet(), toSet(brokersToElrs.partitionsWithBrokerInElr(5)));
        brokersToElrs.update(UUIDS[1], 2, null, new int[] {3, 2, 1});
        assertEquals(toSet(new TopicIdPartition(UUIDS[0], 0),
                new TopicIdPartition(UUIDS[1], 1),
                new TopicIdPartition(UUIDS[1], 2)),
            toSet(brokersToElrs.partitionsWithBrokerInElr(2)));
    }
}
