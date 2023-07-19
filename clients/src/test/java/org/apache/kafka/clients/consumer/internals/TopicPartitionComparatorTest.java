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
package org.apache.kafka.clients.consumer.internals;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kafka.clients.consumer.internals.Utils.TopicPartitionComparator;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

public class TopicPartitionComparatorTest {
    private final static TopicPartition TP1 = new TopicPartition("t1", 0);
    private final static TopicPartition TP1_COPY = new TopicPartition("t1", 0);
    private final static TopicPartition TP2 = new TopicPartition("t1", 1);
    private final static TopicPartition TP3 = new TopicPartition("t11", 1);

    private final TopicPartitionComparator comparator = new TopicPartitionComparator();

    @Test
    public void shouldBeEqual() {
        assertEquals(0, comparator.compare(TP1, TP1_COPY));
    }

    @Test
    public void shouldBeSmallerSameTopic() {
        assertTrue(comparator.compare(TP1, TP2) < 0);
    }

    @Test
    public void shouldBeLargerSameTopic() {
        assertTrue(comparator.compare(TP2, TP1) > 0);
    }

    @Test
    public void shouldBeSmallerSamePartitionNumber() {
        assertTrue(comparator.compare(TP1, TP3) < 0);
    }

    @Test
    public void shouldBeLargerSamePartitionNumber() {
        assertTrue(comparator.compare(TP3, TP1) > 0);
    }

}
