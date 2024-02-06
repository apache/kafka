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
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.test.TestUtils;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.describeTopic;

public final class ExpectLeaderAction implements TieredStorageTestAction {

    private final TopicPartition topicPartition;
    private final int replicaId;
    private final Boolean electLeader;

    public ExpectLeaderAction(TopicPartition topicPartition,
                              int replicaId,
                              Boolean electLeader) {
        this.topicPartition = topicPartition;
        this.replicaId = replicaId;
        this.electLeader = electLeader;
    }

    @Override
    public void doExecute(TieredStorageTestContext context) throws InterruptedException, ExecutionException {
        String topic = topicPartition.topic();
        int partition = topicPartition.partition();
        TestUtils.waitForCondition(() -> {
            TopicDescription description = describeTopic(context, topic);
            List<Node> isr = description.partitions().get(partition).isr();
            return isr != null && isr.stream().anyMatch(node -> node.id() == replicaId);
        }, "Broker " + replicaId + " is out of sync for " + partition + ". Cannot be elected as leader.");

        reassignPartition(context);
        if (electLeader) {
            context.admin().electLeaders(ElectionType.PREFERRED, Collections.singleton(topicPartition));
        }
        AtomicInteger actualLeader = new AtomicInteger(-1);
        TestUtils.waitForCondition(() -> {
            try {
                TopicDescription description = describeTopic(context, topic);
                actualLeader.set(
                        Optional.of(description.partitions().get(partition).leader()).map(Node::id).orElse(-1));
                return replicaId == actualLeader.get();
            } catch (ExecutionException ex) {
                if (ex.getCause() instanceof UnknownTopicOrPartitionException) {
                    return false;
                }
                throw new RuntimeException(ex);
            }
        }, "Leader of " + topicPartition + " was not " + replicaId + ". Actual leader: " + actualLeader);
    }

    @Override
    public void describe(PrintStream output) {
        output.printf("expect-leader: topic-partition: %s, replicaId: %d, electLeader: %s%n",
                topicPartition, replicaId, electLeader);
    }

    private void reassignPartition(TieredStorageTestContext context) throws ExecutionException, InterruptedException {
        String topic = topicPartition.topic();
        TopicPartitionInfo partitionInfo = describeTopic(context, topic)
                .partitions()
                .get(topicPartition.partition());

        List<Integer> targetReplicas = new ArrayList<>();
        targetReplicas.add(replicaId);
        partitionInfo.replicas().forEach(replica -> {
            if (replica.id() != replicaId) {
                targetReplicas.add(replica.id());
            }
        });

        Map<TopicPartition, Optional<NewPartitionReassignment>> proposed =
                Collections.singletonMap(topicPartition, Optional.of(new NewPartitionReassignment(targetReplicas)));
        context.admin().alterPartitionReassignments(proposed);
    }
}
