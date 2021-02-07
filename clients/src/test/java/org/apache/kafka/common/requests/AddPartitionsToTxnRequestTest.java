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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AddPartitionsToTxnRequestTest {

    private static String transactionalId = "transactionalId";
    private static int producerId = 10;
    private static short producerEpoch = 1;
    private static int throttleTimeMs = 10;

    @Test
    public void testConstructor() {
        List<TopicPartition> partitions = new ArrayList<>();
        partitions.add(new TopicPartition("topic", 0));
        partitions.add(new TopicPartition("topic", 1));

        AddPartitionsToTxnRequest.Builder builder = new AddPartitionsToTxnRequest.Builder(transactionalId, producerId, producerEpoch, partitions);

        for (short version = 0; version <= ApiKeys.ADD_PARTITIONS_TO_TXN.latestVersion(); version++) {
            AddPartitionsToTxnRequest request = builder.build(version);

            assertEquals(transactionalId, request.data().transactionalId());
            assertEquals(producerId, request.data().producerId());
            assertEquals(producerEpoch, request.data().producerEpoch());
            assertEquals(partitions, request.partitions());

            AddPartitionsToTxnResponse response = request.getErrorResponse(throttleTimeMs, Errors.UNKNOWN_TOPIC_OR_PARTITION.exception());

            assertEquals(Collections.singletonMap(Errors.UNKNOWN_TOPIC_OR_PARTITION, 2), response.errorCounts());
            assertEquals(throttleTimeMs, response.throttleTimeMs());
        }
    }
}
