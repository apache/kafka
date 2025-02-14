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

import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tiered.storage.TieredStorageTestAction;
import org.apache.kafka.tiered.storage.TieredStorageTestContext;

import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.describeTopic;

public final class ReassignReplicaAction implements TieredStorageTestAction {

    private final TopicPartition topicPartition;
    private final List<Integer> replicaIds;

    public ReassignReplicaAction(TopicPartition topicPartition,
                                 List<Integer> replicaIds) {
        this.topicPartition = topicPartition;
        this.replicaIds = replicaIds;
    }

    @Override
    public void doExecute(TieredStorageTestContext context) throws InterruptedException, ExecutionException {
        String topic = topicPartition.topic();
        int partition = topicPartition.partition();
        Map<TopicPartition, Optional<NewPartitionReassignment>> proposed =
                Collections.singletonMap(topicPartition, Optional.of(new NewPartitionReassignment(replicaIds)));
        context.admin().alterPartitionReassignments(proposed);

        TestUtils.waitForCondition(() -> {
            try {
                TopicDescription description = describeTopic(context, topic);
                List<Integer> actualReplicaIds = description.partitions().get(partition).replicas()
                        .stream()
                        .map(Node::id)
                        .toList();
                return replicaIds.equals(actualReplicaIds);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                    return false;
                }
                throw new RuntimeException(e);
            }
        }, "Unable to reassign the replicas of " + topicPartition + ", replica-ids: " + replicaIds);
    }

    @Override
    public void describe(PrintStream output) {
        output.printf("reassign-replica topic-partition: %s replica-ids: %s%n", topicPartition, replicaIds);
    }
}
