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
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.metadata.storage.RemoteLogMetadataTopicPartitioner;
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.describeTopics;

public final class ExpectUserTopicMappedToMetadataPartitionsAction implements TieredStorageTestAction {

    private final String topic;
    private final List<Integer> metadataPartitions;

    public ExpectUserTopicMappedToMetadataPartitionsAction(String topic,
                                                           List<Integer> metadataPartitions) {
        this.topic = topic;
        this.metadataPartitions = metadataPartitions;
    }

    @Override
    public void doExecute(TieredStorageTestContext context) throws InterruptedException, ExecutionException {
        String metadataTopic = TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_NAME;
        Map<String, TopicDescription> descriptions = describeTopics(context, Arrays.asList(topic, metadataTopic));
        int metadataTopicPartitionCount = descriptions.get(metadataTopic).partitions().size();
        RemoteLogMetadataTopicPartitioner partitioner =
                new RemoteLogMetadataTopicPartitioner(metadataTopicPartitionCount);

        Uuid topicId = descriptions.get(topic).topicId();
        Set<Integer> actualMetadataPartitions = descriptions.get(topic).partitions()
                .stream()
                .map(info -> new TopicIdPartition(topicId, new TopicPartition(topic, info.partition())))
                .map(partitioner::metadataPartition)
                .collect(Collectors.toSet());
        assertTrue(actualMetadataPartitions.containsAll(metadataPartitions),
                () -> "metadata-partition distribution expected: " + metadataPartitions + ", actual: "
                        + actualMetadataPartitions);
    }

    @Override
    public void describe(PrintStream output) {
        output.printf("expect-user-topic-mapped-to-metadata-partitions topic: %s metadata-partitions: %s%n",
                topic, metadataPartitions);
    }
}
