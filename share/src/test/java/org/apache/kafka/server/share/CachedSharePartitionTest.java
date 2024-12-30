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
package org.apache.kafka.server.share;

import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class CachedSharePartitionTest {

    @Test
    public void testCachedSharePartitionEqualsAndHashCode() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic";
        int partition = 0;

        CachedSharePartition cachedSharePartitionWithIdAndName = new
                CachedSharePartition(topicName, topicId, partition, false);
        CachedSharePartition cachedSharePartitionWithIdAndNoName = new
                CachedSharePartition(null, topicId, partition, false);
        CachedSharePartition cachedSharePartitionWithDifferentIdAndName = new
                CachedSharePartition(topicName, Uuid.randomUuid(), partition, false);
        CachedSharePartition cachedSharePartitionWithZeroIdAndName = new
                CachedSharePartition(topicName, Uuid.ZERO_UUID, partition, false);

        // CachedSharePartitions with valid topic IDs will compare topic ID and partition but not topic name.
        assertEquals(cachedSharePartitionWithIdAndName, cachedSharePartitionWithIdAndNoName);
        assertEquals(cachedSharePartitionWithIdAndName.hashCode(), cachedSharePartitionWithIdAndNoName.hashCode());

        assertNotEquals(cachedSharePartitionWithIdAndName, cachedSharePartitionWithDifferentIdAndName);
        assertNotEquals(cachedSharePartitionWithIdAndName.hashCode(), cachedSharePartitionWithDifferentIdAndName.hashCode());

        assertNotEquals(cachedSharePartitionWithIdAndName, cachedSharePartitionWithZeroIdAndName);
        assertNotEquals(cachedSharePartitionWithIdAndName.hashCode(), cachedSharePartitionWithZeroIdAndName.hashCode());

        // CachedSharePartitions with null name and valid IDs will act just like ones with valid names

        assertNotEquals(cachedSharePartitionWithIdAndNoName, cachedSharePartitionWithDifferentIdAndName);
        assertNotEquals(cachedSharePartitionWithIdAndNoName.hashCode(), cachedSharePartitionWithDifferentIdAndName.hashCode());

        assertNotEquals(cachedSharePartitionWithIdAndNoName, cachedSharePartitionWithZeroIdAndName);
        assertNotEquals(cachedSharePartitionWithIdAndNoName.hashCode(), cachedSharePartitionWithZeroIdAndName.hashCode());

        assertEquals(cachedSharePartitionWithZeroIdAndName.hashCode(), cachedSharePartitionWithZeroIdAndName.hashCode());
    }
}
