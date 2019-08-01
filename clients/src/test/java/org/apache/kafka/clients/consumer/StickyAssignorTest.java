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

import static org.apache.kafka.clients.consumer.StickyAssignor.serializeTopicPartitionAssignment;
import static org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor.DEFAULT_GENERATION;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor;
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor.MemberData;
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignorTest;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

public class StickyAssignorTest extends AbstractStickyAssignorTest {

    private StickyAssignor stickyAssignor;

    @Override
    public AbstractStickyAssignor createAssignor() {
        stickyAssignor = new StickyAssignor();
        return stickyAssignor;
    }

    @Override
    public Subscription buildSubscription(List<String> topics, List<TopicPartition> partitions) {
        return new Subscription(topics,
            serializeTopicPartitionAssignment(new MemberData(partitions, Optional.of(DEFAULT_GENERATION))));
    }

    @Override
    public Subscription buildSubscriptionWithGeneration(List<String> topics, List<TopicPartition> partitions, int generation) {
        return new Subscription(topics,
            serializeTopicPartitionAssignment(new MemberData(partitions, Optional.of(generation))));
    }

    private static ByteBuffer serializeTopicPartitionAssignmentToOldSchema(List<TopicPartition> partitions) {
        Struct struct = new Struct(StickyAssignor.STICKY_ASSIGNOR_USER_DATA_V0);
        List<Struct> topicAssignments = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> topicEntry : CollectionUtils.groupPartitionsByTopic(partitions).entrySet()) {
            Struct topicAssignment = new Struct(StickyAssignor.TOPIC_ASSIGNMENT);
            topicAssignment.set(StickyAssignor.TOPIC_KEY_NAME, topicEntry.getKey());
            topicAssignment.set(StickyAssignor.PARTITIONS_KEY_NAME, topicEntry.getValue().toArray());
            topicAssignments.add(topicAssignment);
        }
        struct.set(StickyAssignor.TOPIC_PARTITIONS_KEY_NAME, topicAssignments.toArray());
        ByteBuffer buffer = ByteBuffer.allocate(StickyAssignor.STICKY_ASSIGNOR_USER_DATA_V0.sizeOf(struct));
        StickyAssignor.STICKY_ASSIGNOR_USER_DATA_V0.write(buffer, struct);
        buffer.flip();
        return buffer;
    }
}