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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.RecordBatch;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.protocol.ApiKeys.PRODUCE;
import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.INT64;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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

        ByteBuffer buffer = v5Response.serialize(ApiKeys.PRODUCE, version, 0);
        buffer.rewind();

        ResponseHeader.parse(buffer, ApiKeys.PRODUCE.responseHeaderVersion(version)); // throw away.

        Struct deserializedStruct = ApiKeys.PRODUCE.parseResponse(version, buffer);

        ProduceResponse v5FromBytes = (ProduceResponse) AbstractResponse.parseResponse(ApiKeys.PRODUCE,
                deserializedStruct, version);

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
        assertEquals("Throttle time must be zero", 0, v0Response.throttleTimeMs());
        assertEquals("Throttle time must be 10", 10, v1Response.throttleTimeMs());
        assertEquals("Throttle time must be 10", 10, v2Response.throttleTimeMs());
        assertEquals("Should use schema version 0", ApiKeys.PRODUCE.responseSchema((short) 0),
                v0Response.toStruct((short) 0).schema());
        assertEquals("Should use schema version 1", ApiKeys.PRODUCE.responseSchema((short) 1),
                v1Response.toStruct((short) 1).schema());
        assertEquals("Should use schema version 2", ApiKeys.PRODUCE.responseSchema((short) 2),
                v2Response.toStruct((short) 2).schema());
        assertEquals("Response data does not match", responseData, v0Response.responses());
        assertEquals("Response data does not match", responseData, v1Response.responses());
        assertEquals("Response data does not match", responseData, v2Response.responses());
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
            Struct struct = response.toStruct(ver);
            assertEquals("Should use schema version " + ver, ApiKeys.PRODUCE.responseSchema(ver), struct.schema());
            ProduceResponse.PartitionResponse deserialized = new ProduceResponse(new ProduceResponseData(struct, ver)).responses().get(tp);
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

    /**
     * the schema in this test is from previous code and the automatic protocol should be compatible to previous schema.
     */
    @Test
    public void testCompatibility() {
        String responseKeyName = "responses";
        String partitionResponsesKeyName = "partition_responses";
        long invalidOffset = -1L;
        String baseOffsetKeyName = "base_offset";
        String logAppendTimeKeyName = "log_append_time";
        String logStartOffsetKeyName = "log_start_offset";
        String recordErrorsKeyName = "record_errors";
        String batchIndexKeyName = "batch_index";
        String batchIndexErrorMessageKeyName = "batch_index_error_message";
        String errorMessageKeyName = "error_message";

        Field.Int64 logStartOffsetField = new Field.Int64(logStartOffsetKeyName,
                "The start offset of the log at the time this produce response was created", invalidOffset);
        Field.NullableStr batchIndexErrorMessageField = new Field.NullableStr(batchIndexErrorMessageKeyName,
                "The error message of the record that caused the batch to be dropped");
        Field.NullableStr errorMessageField = new Field.NullableStr(errorMessageKeyName,
                "The global error message summarizing the common root cause of the records that caused the batch to be dropped");

        Schema produceResponseV0 = new Schema(
                new Field(responseKeyName, new ArrayOf(new Schema(TOPIC_NAME,
                        new Field(partitionResponsesKeyName, new ArrayOf(new Schema(PARTITION_ID, ERROR_CODE,
                                new Field(baseOffsetKeyName, INT64))))))));

        Schema produceResponseV1 = new Schema(
                new Field(responseKeyName, new ArrayOf(new Schema(TOPIC_NAME,
                        new Field(partitionResponsesKeyName, new ArrayOf(new Schema(PARTITION_ID, ERROR_CODE,
                                new Field(baseOffsetKeyName, INT64))))))),
                THROTTLE_TIME_MS);

        Schema produceResponseV2 = new Schema(
                new Field(responseKeyName, new ArrayOf(new Schema(TOPIC_NAME,
                        new Field(partitionResponsesKeyName, new ArrayOf(new Schema(PARTITION_ID, ERROR_CODE,
                                new Field(baseOffsetKeyName, INT64),
                                new Field(logAppendTimeKeyName, INT64))))))),
                THROTTLE_TIME_MS);
        Schema produceResponseV3 = produceResponseV2;
        Schema produceResponseV4 = produceResponseV3;
        Schema produceResponseV5 = new Schema(
                new Field(responseKeyName, new ArrayOf(new Schema(TOPIC_NAME,
                        new Field(partitionResponsesKeyName, new ArrayOf(new Schema(PARTITION_ID, ERROR_CODE,
                                new Field(baseOffsetKeyName, INT64),
                                new Field(logAppendTimeKeyName, INT64),
                                logStartOffsetField)))))),
                THROTTLE_TIME_MS);
        Schema produceResponseV6 = produceResponseV5;
        Schema produceResponseV7 = produceResponseV6;
        Schema produceResponseV8 = new Schema(
                new Field(responseKeyName, new ArrayOf(new Schema(TOPIC_NAME,
                        new Field(partitionResponsesKeyName, new ArrayOf(new Schema(PARTITION_ID, ERROR_CODE,
                                new Field(baseOffsetKeyName, INT64),
                                new Field(logAppendTimeKeyName, INT64),
                                logStartOffsetField,
                                new Field(recordErrorsKeyName, new ArrayOf(new Schema(
                                        new Field.Int32(batchIndexKeyName, "The batch index of the record " +
                                                "that caused the batch to be dropped"),
                                        batchIndexErrorMessageField
                                ))),
                                errorMessageField)))))),
                THROTTLE_TIME_MS);

        Schema[] schemaVersions = new Schema[]{
            produceResponseV0, produceResponseV1, produceResponseV2,
            produceResponseV3, produceResponseV4, produceResponseV5,
            produceResponseV6, produceResponseV7, produceResponseV8};

        int schemaVersion = 0;
        for (Schema previousSchema : schemaVersions) {
            SchemaTestUtils.assertEquals(previousSchema, ProduceResponseData.SCHEMAS[schemaVersion++]);
        }
    }
}
