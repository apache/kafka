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
package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;

class ConsumerManagerTest {

    @Test
    void testWaitTillConsumptionCatchesUpOnMultipleAssignments() {
        int numMetadataTopicPartitions = 2;
        RemoteLogMetadataTopicPartitioner partitioner = new RemoteLogMetadataTopicPartitioner(numMetadataTopicPartitions);
        // sample-0 partition maps to meta-partition 0
        // sample-3 partition maps to meta-partition 1
        Uuid topicId = Uuid.fromString("Egeme0kaTIeVHU-SQedS-g");
        String topic = "sample";
        TopicIdPartition idPartition0 = new TopicIdPartition(topicId, new TopicPartition(topic, 0));
        TopicIdPartition idPartition3 = new TopicIdPartition(topicId, new TopicPartition(topic, 3));

        TopicBasedRemoteLogMetadataManagerConfig rlmmConfig = new TopicBasedRemoteLogMetadataManagerConfig(getRLMMConfigProps(numMetadataTopicPartitions));
        RemotePartitionMetadataEventHandler handler = mock(RemotePartitionMetadataEventHandler.class);
        Time time = new MockTime();
        ConsumerManager manager = new ConsumerManager(rlmmConfig, handler, partitioner, time);

        // first assignment is directly processed by the primary consumer
        manager.addAssignmentsForPartitions(Collections.singleton(idPartition0));
        TopicPartition remoteLogPartition = new TopicPartition(rlmmConfig.remoteLogMetadataTopicName(), 0);
        manager.waitTillConsumptionCatchesUp(
                new RecordMetadata(remoteLogPartition, -1, 0, time.milliseconds(), 100, 100));

        // subsequent assignments are handled by the secondary consumer, then handed it over to primary consumer
        manager.addAssignmentsForPartitions(Collections.singleton(idPartition3));
        remoteLogPartition = new TopicPartition(rlmmConfig.remoteLogMetadataTopicName(), 1);
        manager.waitTillConsumptionCatchesUp(
                new RecordMetadata(remoteLogPartition, -1, 0, time.milliseconds(), 100, 100));
    }

    private Map<String, Object> getRLMMConfigProps(int numMetadataTopicPartitions) {
        final Map<String, Object> props = new HashMap<>();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_SECONDARY_CONSUMER_SUBSCRIPTION_INTERVAL_MS_PROP, "10");
        props.put(TopicBasedRemoteLogMetadataManagerConfig.LOG_DIR, TestUtils.tempDirectory().getName());
        props.put(TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP, numMetadataTopicPartitions);
        return props;
    }
}