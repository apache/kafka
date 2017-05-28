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

package org.apache.kafka.clients.consumer;

import static org.hamcrest.core.Is.is;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.kafka.clients.consumer.LagBasedAssignor.TopicPartitionLag;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;


public class LagBasedAssignorTest {

    @Test
    public void testComputePartitionLag() {

        final long lag = LagBasedAssignor.computePartitionLag(
            new OffsetAndMetadata(5555),
            1111,
            9999,
            "none"
        );

        Assert.assertThat(lag, is(4444L));

    }

    /**
     * In the case where lookup of the partition begin/end offsets fails, the lag should be 0
     */
    @Test
    public void testComputePartitionLagNoEndOffset() {

        final long lag = LagBasedAssignor.computePartitionLag(
            new OffsetAndMetadata(5555),
            0,
            0,
            "none"
        );

        Assert.assertThat(lag, is(0L));

    }

    @Test
    public void testComputePartitionLagNoCommittedOffsetResetModeLatest() {

        final long lag = LagBasedAssignor.computePartitionLag(
            null,
            1111,
            9999,
            "latest"
        );

        Assert.assertThat(lag, is(0L));

    }

    @Test
    public void testComputePartitionLagNoCommittedOffsetResetModeEarliest() {

        final long beginOffset = 1111;
        final long endOffset = 9999;
        final long lag = LagBasedAssignor.computePartitionLag(
            null,
            beginOffset,
            endOffset,
            "earliest"
        );

        Assert.assertThat(lag, is(endOffset - beginOffset));

    }

    @Test
    public void testAssign() {

        final Map<String, List<TopicPartitionLag>> partitionLagPerTopic = new HashMap<>();
        partitionLagPerTopic.put(
            "topic1",
            Arrays.asList(
                new TopicPartitionLag("topic1", 0, 100000),
                new TopicPartitionLag("topic1", 1, 100000),
                new TopicPartitionLag("topic1", 2, 500),
                new TopicPartitionLag("topic1", 3, 1)
            )
        );
        partitionLagPerTopic.put(
            "topic2",
            Arrays.asList(
                new TopicPartitionLag("topic2", 0, 900000),
                new TopicPartitionLag("topic2", 1, 100000)
            )
        );

        final Map<String, List<String>> subscriptions = new HashMap<>();
        subscriptions.put(
            "consumer-1",
            Arrays.asList(
                "topic1",
                "topic2"
            )
        );
        subscriptions.put(
            "consumer-2",
            Arrays.asList(
                "topic1"
            )
        );

        final Map<String, List<TopicPartition>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(
            "consumer-1",
            Arrays.asList(
                new TopicPartition("topic1", 0),
                new TopicPartition("topic1", 2),
                new TopicPartition("topic2", 0),
                new TopicPartition("topic2", 1)
            )
        );
        expectedAssignment.put(
            "consumer-2",
            Arrays.asList(
                new TopicPartition("topic1", 1),
                new TopicPartition("topic1", 3)
            )
        );

        final Map<String, List<TopicPartition>> actualAssignment =
            LagBasedAssignor.assign(partitionLagPerTopic, subscriptions);

        Assert.assertThat(actualAssignment.size(), is (2));
        Assert.assertThat(new HashSet<>(actualAssignment.get("consumer-1")), is(new HashSet<>(expectedAssignment.get("consumer-1"))));
        Assert.assertThat(new HashSet<>(actualAssignment.get("consumer-2")), is(new HashSet<>(expectedAssignment.get("consumer-2"))));

    }

    @Test
    public void testAssignWithZeroLags() {

        final Map<String, List<TopicPartitionLag>> partitionLagPerTopic = new HashMap<>();
        partitionLagPerTopic.put(
            "topic1",
            Arrays.asList(
                new TopicPartitionLag("topic1", 0, 0),
                new TopicPartitionLag("topic1", 1, 0),
                new TopicPartitionLag("topic1", 2, 0),
                new TopicPartitionLag("topic1", 3, 0),
                new TopicPartitionLag("topic1", 4, 0),
                new TopicPartitionLag("topic1", 5, 0),
                new TopicPartitionLag("topic1", 6, 0)
            )
        );

        final Map<String, List<String>> subscriptions = new HashMap<>();
        subscriptions.put(
            "consumer-1",
            Collections.singletonList("topic1")
        );
        subscriptions.put(
            "consumer-2",
            Collections.singletonList("topic1")
        );

        final Map<String, List<TopicPartition>> actualAssignment =
            LagBasedAssignor.assign(partitionLagPerTopic, subscriptions);

        final Comparator<List> listSizeComparator = new Comparator<List>() {
            @Override
            public int compare(List o1, List o2) {
                return Integer.compare(o1.size(), o2.size());
            }
        };

        final int maxAssignedPartitions = Collections.max(
            actualAssignment.values(),
            listSizeComparator
        ).size();

        final int minAssignedPartitions = Collections.min(
            actualAssignment.values(),
            listSizeComparator
        ).size();

        Assert.assertTrue("Partitions should be distributed evenly amongst consumers",
                          maxAssignedPartitions <= minAssignedPartitions + 1);

    }

    @Test
    public void testAssignWithHeavilySkewedLags() {

        // For this test, the number of partitions must NOT be divisible by the number of consumers
        final Map<String, List<TopicPartitionLag>> partitionLagPerTopic = new HashMap<>();
        partitionLagPerTopic.put(
            "topic1",
            Arrays.asList(
                new TopicPartitionLag("topic1", 0, 360),
                new TopicPartitionLag("topic1", 1, 359),
                new TopicPartitionLag("topic1", 2, 230),
                new TopicPartitionLag("topic1", 3, 118),
                new TopicPartitionLag("topic1", 4, 444),
                new TopicPartitionLag("topic1", 5, 122),
                new TopicPartitionLag("topic1", 6, 65),
                new TopicPartitionLag("topic1", 7, 111),
                new TopicPartitionLag("topic1", 8, 455000),
                new TopicPartitionLag("topic1", 9, 424000)
            )
        );

        final Map<String, List<String>> subscriptions = new HashMap<>();
        subscriptions.put(
            "consumer-1",
            Collections.singletonList("topic1")
        );
        subscriptions.put(
            "consumer-2",
            Collections.singletonList("topic1")
        );
        subscriptions.put(
            "consumer-3",
            Collections.singletonList("topic1")
        );

        final Map<String, List<TopicPartition>> actualAssignment =
            LagBasedAssignor.assign(partitionLagPerTopic, subscriptions);

        final Comparator<Entry<String, List<TopicPartition>>> mapValueSizeComparator =
            new Comparator<Entry<String, List<TopicPartition>>>() {
                @Override
                public int compare(
                    Entry<String, List<TopicPartition>> e1, Entry<String, List<TopicPartition>> e2
                ) {
                    return Integer.compare(e1.getValue().size(), e2.getValue().size());
                }
            };

        final Map.Entry<String, List<TopicPartition>> consumerWithMaxPartitions = Collections.max(
            actualAssignment.entrySet(),
            mapValueSizeComparator
        );
        final Map.Entry<String, List<TopicPartition>> consumerWithMinPartitions = Collections.min(
            actualAssignment.entrySet(),
            mapValueSizeComparator
        );

        final int maxAssignedPartitions = consumerWithMaxPartitions.getValue().size();
        final int minAssignedPartitions = consumerWithMinPartitions.getValue().size();

        Assert.assertTrue("Partitions should be distributed evenly amongst consumers",
                          maxAssignedPartitions <= minAssignedPartitions + 1
        );

    }

}
