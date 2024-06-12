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
package org.apache.kafka.tiered.storage.actions;

import org.apache.kafka.tiered.storage.TieredStorageTestAction;
import org.apache.kafka.tiered.storage.TieredStorageTestContext;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.test.TestUtils;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.describeTopic;

public final class ShrinkReplicaAction implements TieredStorageTestAction {

    private final TopicPartition topicPartition;
    private final List<Integer> replicaIds;

    public ShrinkReplicaAction(TopicPartition topicPartition,
                               List<Integer> replicaIds) {
        this.topicPartition = topicPartition;
        this.replicaIds = replicaIds;
    }

    @Override
    public void doExecute(TieredStorageTestContext context) throws InterruptedException, ExecutionException {
        String topic = topicPartition.topic();
        int partition = topicPartition.partition();
        TopicPartitionInfo partitionInfo = describeTopic(context, topic).partitions().get(partition);
        List<Integer> currentReplicaIds = new ArrayList<>();
        List<Integer> targetReplicaIds = new ArrayList<>();
        if (partitionInfo != null) {
            partitionInfo.replicas().forEach(node -> currentReplicaIds.add(node.id()));
            partitionInfo.replicas().stream().filter(node -> replicaIds.contains(node.id()))
                    .forEach(node -> targetReplicaIds.add(node.id()));
        }

        Map<TopicPartition, Optional<NewPartitionReassignment>> proposed =
                Collections.singletonMap(topicPartition, Optional.of(new NewPartitionReassignment(targetReplicaIds)));
        context.admin().alterPartitionReassignments(proposed).all().get();

        TestUtils.waitForCondition(() -> {
            Set<Integer> actualReplicaIds = new HashSet<>();
            TopicPartitionInfo topicPartitionInfo = describeTopic(context, topic).partitions().get(partition);
            if (topicPartitionInfo != null) {
                topicPartitionInfo.replicas().forEach(node -> actualReplicaIds.add(node.id()));
            }
            return targetReplicaIds.size() == actualReplicaIds.size()
                    && targetReplicaIds.containsAll(actualReplicaIds)
                    && actualReplicaIds.containsAll(targetReplicaIds);
        }, "Unable to shrink the replicas of " + topicPartition + ", replica-ids: " + replicaIds
                + ", actual-replica-ids: " + currentReplicaIds);
    }

    @Override
    public void describe(PrintStream output) {
        output.printf("shrink-replica topic-partition: %s replica-ids: %s%n", topicPartition, replicaIds);
    }
}
