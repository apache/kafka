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

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tiered.storage.TieredStorageTestAction;
import org.apache.kafka.tiered.storage.TieredStorageTestContext;

import java.io.PrintStream;

import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.describeTopic;

public final class ExpectBrokerInISRAction implements TieredStorageTestAction {

    private final TopicPartition topicPartition;
    private final Integer replicaId;

    public ExpectBrokerInISRAction(TopicPartition topicPartition,
                                   Integer replicaId) {
        this.topicPartition = topicPartition;
        this.replicaId = replicaId;
    }

    @Override
    public void doExecute(TieredStorageTestContext context) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            TopicDescription description = describeTopic(context, topicPartition.topic());
            TopicPartitionInfo partitionInfo = description.partitions()
                    .get(topicPartition.partition());
            if (partitionInfo != null) {
                return partitionInfo.isr().stream().anyMatch(node -> node.id() == replicaId);
            }
            return false;
        }, "Expected broker " + replicaId + " to be in ISR for " + topicPartition);
    }

    @Override
    public void describe(PrintStream output) {
        output.printf("expect-broker-in-isr topic-partition: %s broker-id: %d%n", topicPartition, replicaId);
    }
}
