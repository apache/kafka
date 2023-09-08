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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.AlterPartitionRequestData;
import org.apache.kafka.common.message.AlterPartitionRequestData.BrokerState;
import org.apache.kafka.common.message.AlterPartitionRequestData.PartitionData;
import org.apache.kafka.common.message.AlterPartitionRequestData.TopicData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AlterPartitionRequestTest {
    String topic = "test-topic";
    Uuid topicId = Uuid.randomUuid();

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.ALTER_PARTITION)
    public void testBuildAlterPartitionRequest(short version) {
        AlterPartitionRequestData request = new AlterPartitionRequestData()
            .setBrokerId(1)
            .setBrokerEpoch(1);

        TopicData topicData = new TopicData()
            .setTopicId(topicId)
            .setTopicName(topic);

        List<BrokerState> newIsrWithBrokerEpoch = new LinkedList<>();
        newIsrWithBrokerEpoch.add(new BrokerState().setBrokerId(1).setBrokerEpoch(1001));
        newIsrWithBrokerEpoch.add(new BrokerState().setBrokerId(2).setBrokerEpoch(1002));
        newIsrWithBrokerEpoch.add(new BrokerState().setBrokerId(3).setBrokerEpoch(1003));

        topicData.partitions().add(new PartitionData()
            .setPartitionIndex(0)
            .setLeaderEpoch(1)
            .setPartitionEpoch(10)
            .setNewIsrWithEpochs(newIsrWithBrokerEpoch));

        request.topics().add(topicData);

        AlterPartitionRequest.Builder builder = new AlterPartitionRequest.Builder(request, version > 1);
        AlterPartitionRequest alterPartitionRequest = builder.build(version);
        assertEquals(1, alterPartitionRequest.data().topics().size());
        assertEquals(1, alterPartitionRequest.data().topics().get(0).partitions().size());
        PartitionData partitionData = alterPartitionRequest.data().topics().get(0).partitions().get(0);
        if (version < 3) {
            assertEquals(Arrays.asList(1, 2, 3), partitionData.newIsr());
            assertTrue(partitionData.newIsrWithEpochs().isEmpty());
        } else {
            assertEquals(newIsrWithBrokerEpoch, partitionData.newIsrWithEpochs());
            assertTrue(partitionData.newIsr().isEmpty());
        }

        // Build the request again to make sure build() is idempotent.
        alterPartitionRequest = builder.build(version);
        assertEquals(1, alterPartitionRequest.data().topics().size());
        assertEquals(1, alterPartitionRequest.data().topics().get(0).partitions().size());
        alterPartitionRequest.data().topics().get(0).partitions().get(0);
        if (version < 3) {
            assertEquals(Arrays.asList(1, 2, 3), partitionData.newIsr());
            assertTrue(partitionData.newIsrWithEpochs().isEmpty());
        } else {
            assertEquals(newIsrWithBrokerEpoch, partitionData.newIsrWithEpochs());
            assertTrue(partitionData.newIsr().isEmpty());
        }
    }
}
