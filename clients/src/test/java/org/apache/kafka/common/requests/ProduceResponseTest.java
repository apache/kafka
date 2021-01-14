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
import org.apache.kafka.common.record.RecordBatch;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.protocol.ApiKeys.PRODUCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProduceResponseTest {

    @SuppressWarnings("deprecation")
    @Test
    public void produceResponseV5Test() {
        Map<TopicPartition, ProduceResponse.PartitionResponse> responseData = new HashMap<>();
        TopicPartition tp0 = new TopicPartition("test", 0);
        responseData.put(tp0, new ProduceResponse.PartitionResponse(Errors.NONE,
                10000, RecordBatch.NO_TIMESTAMP, 100));

        ProduceResponse v5Response = new ProduceResponse(responseData, 10);
        short version = 5;

        ByteBuffer buffer = RequestTestUtils.serializeResponseWithHeader(v5Response, version, 0);

        ResponseHeader.parse(buffer, ApiKeys.PRODUCE.responseHeaderVersion(version)); // throw away.
        ProduceResponse v5FromBytes = (ProduceResponse) AbstractResponse.parseResponse(ApiKeys.PRODUCE,
                buffer, version);

        assertEquals(1, v5FromBytes.responses().size());
        assertTrue(v5FromBytes.responses().containsKey(tp0));
        ProduceResponse.PartitionResponse partitionResponse = v5FromBytes.responses().get(tp0);
        assertEquals(100, partitionResponse.logStartOffset);
        assertEquals(10000, partitionResponse.baseOffset);
        assertEquals(10, v5FromBytes.throttleTimeMs());
        assertEquals(responseData, v5Response.responses());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void produceResponseVersionTest() {
        Map<TopicPartition, ProduceResponse.PartitionResponse> responseData = new HashMap<>();
        responseData.put(new TopicPartition("test", 0), new ProduceResponse.PartitionResponse(Errors.NONE,
                10000, RecordBatch.NO_TIMESTAMP, 100));
        ProduceResponse v0Response = new ProduceResponse(responseData);
        ProduceResponse v1Response = new ProduceResponse(responseData, 10);
        ProduceResponse v2Response = new ProduceResponse(responseData, 10);
        assertEquals(0, v0Response.throttleTimeMs(), "Throttle time must be zero");
        assertEquals(10, v1Response.throttleTimeMs(), "Throttle time must be 10");
        assertEquals(10, v2Response.throttleTimeMs(), "Throttle time must be 10");
        assertEquals(responseData, v0Response.responses(), "Response data does not match");
        assertEquals(responseData, v1Response.responses(), "Response data does not match");
        assertEquals(responseData, v2Response.responses(), "Response data does not match");
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

        for (short ver = 0; ver <= PRODUCE.latestVersion(); ver++) {
            ProduceResponse response = new ProduceResponse(responseData);
            ProduceResponse.PartitionResponse deserialized = ProduceResponse.parse(response.serialize(ver), ver).responses().get(tp);
            if (ver >= 8) {
                assertEquals(1, deserialized.recordErrors.size());
                assertEquals(3, deserialized.recordErrors.get(0).batchIndex);
                assertEquals("Record error", deserialized.recordErrors.get(0).message);
                assertEquals("Produce failed", deserialized.errorMessage);
            } else {
                assertEquals(0, deserialized.recordErrors.size());
                assertNull(deserialized.errorMessage);
            }
        }
    }
}
