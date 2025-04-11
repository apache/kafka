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
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.ApiKeys.PRODUCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProduceResponseTest {

    @SuppressWarnings("deprecation")
    @Test
    public void produceResponseVersionTest() {
        Map<TopicPartition, ProduceResponse.PartitionResponse> responseData = new HashMap<>();
        responseData.put(new TopicPartition("test", 0), new ProduceResponse.PartitionResponse(Errors.NONE, 10000, RecordBatch.NO_TIMESTAMP, 100));
        ProduceResponse v0Response = new ProduceResponse(responseData);
        ProduceResponse v1Response = new ProduceResponse(responseData, 10);
        ProduceResponse v2Response = new ProduceResponse(responseData, 10);
        assertEquals(0, v0Response.throttleTimeMs(), "Throttle time must be zero");
        assertEquals(10, v1Response.throttleTimeMs(), "Throttle time must be 10");
        assertEquals(10, v2Response.throttleTimeMs(), "Throttle time must be 10");

        List<ProduceResponse> arrResponse = Arrays.asList(v0Response, v1Response, v2Response);
        for (ProduceResponse produceResponse : arrResponse) {
            assertEquals(1, produceResponse.data().responses().size());
            ProduceResponseData.TopicProduceResponse topicProduceResponse = produceResponse.data().responses().iterator().next();
            assertEquals(1, topicProduceResponse.partitionResponses().size());  
            ProduceResponseData.PartitionProduceResponse partitionProduceResponse = topicProduceResponse.partitionResponses().iterator().next();
            assertEquals(100, partitionProduceResponse.logStartOffset());
            assertEquals(10000, partitionProduceResponse.baseOffset());
            assertEquals(RecordBatch.NO_TIMESTAMP, partitionProduceResponse.logAppendTimeMs());
            assertEquals(Errors.NONE, Errors.forCode(partitionProduceResponse.errorCode()));
            assertNull(partitionProduceResponse.errorMessage());
            assertTrue(partitionProduceResponse.recordErrors().isEmpty());
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void produceResponseRecordErrorsTest() {
        Map<TopicPartition, ProduceResponse.PartitionResponse> responseData = new HashMap<>();
        TopicPartition tp = new TopicPartition("test", 0);
        ProduceResponse.PartitionResponse partResponse = new ProduceResponse.PartitionResponse(Errors.NONE,
                10000, RecordBatch.NO_TIMESTAMP, 100,
                Collections.singletonList(new ProduceResponse.RecordError(3, "Record error")),
                "Produce failed");
        responseData.put(tp, partResponse);

        for (short version : PRODUCE.allVersions()) {
            ProduceResponse response = new ProduceResponse(responseData);

            ProduceResponse produceResponse = ProduceResponse.parse(response.serialize(version), version);
            ProduceResponseData.TopicProduceResponse topicProduceResponse = produceResponse.data().responses().iterator().next();
            ProduceResponseData.PartitionProduceResponse deserialized = topicProduceResponse.partitionResponses().iterator().next();
            if (version >= 8) {
                assertEquals(1, deserialized.recordErrors().size());
                assertEquals(3, deserialized.recordErrors().get(0).batchIndex());
                assertEquals("Record error", deserialized.recordErrors().get(0).batchIndexErrorMessage());
                assertEquals("Produce failed", deserialized.errorMessage());
            } else {
                assertEquals(0, deserialized.recordErrors().size());
                assertNull(deserialized.errorMessage());
            }
        }
    }
}
