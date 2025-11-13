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
import org.apache.kafka.common.message.WriteTxnMarkersRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class WriteTxnMarkersRequestTest {

    private static final long PRODUCER_ID = 10L;
    private static final short PRODUCER_EPOCH = 2;
    private static final int COORDINATOR_EPOCH = 1;
    private static final TransactionResult RESULT = TransactionResult.COMMIT;
    private static final TopicPartition TOPIC_PARTITION = new TopicPartition("topic", 73);

    protected static int throttleTimeMs = 10;

    private static List<WriteTxnMarkersRequest.TxnMarkerEntry> markers;

    @BeforeEach
    public void setUp() {
        markers = Collections.singletonList(
             new WriteTxnMarkersRequest.TxnMarkerEntry(
                 PRODUCER_ID, PRODUCER_EPOCH, COORDINATOR_EPOCH,
                 RESULT, Collections.singletonList(TOPIC_PARTITION), (short) 0)
        );
    }

    @Test
    public void testConstructor() {
        // We always set the transaction version in the request data using the arguments provided to the builder.
        // If the version doesn't support it, it will be omitted during serialization.

        // Test constructor with transactionVersion = 2
        List<WriteTxnMarkersRequest.TxnMarkerEntry> markersWithVersion = Collections.singletonList(
            new WriteTxnMarkersRequest.TxnMarkerEntry(
                PRODUCER_ID, PRODUCER_EPOCH, COORDINATOR_EPOCH,
                RESULT, Collections.singletonList(TOPIC_PARTITION), (short) 2)
        );

        // Build with request version 1.
        WriteTxnMarkersRequest.Builder builder = new WriteTxnMarkersRequest.Builder(markersWithVersion);
        WriteTxnMarkersRequest request = builder.build((short) 1);
        assertEquals(1, request.data().markers().size());
        WriteTxnMarkersRequestData.WritableTxnMarker dataMarker = request.data().markers().get(0);
        assertEquals(PRODUCER_ID, dataMarker.producerId());
        assertEquals(PRODUCER_EPOCH, dataMarker.producerEpoch());
        assertEquals(COORDINATOR_EPOCH, dataMarker.coordinatorEpoch());
        assertEquals(RESULT.id, dataMarker.transactionResult());
        assertEquals(1, dataMarker.topics().size());
        assertEquals(TOPIC_PARTITION.topic(), dataMarker.topics().get(0).name());
        assertEquals(Collections.singletonList(TOPIC_PARTITION.partition()), dataMarker.topics().get(0).partitionIndexes());
        // Verify TransactionVersion is set to 2 in the data irrespective of the request version
        assertEquals((byte) 2, dataMarker.transactionVersion());

        // Build with request version 2
        WriteTxnMarkersRequest.Builder builderWithVersions = new WriteTxnMarkersRequest.Builder(markersWithVersion);
        WriteTxnMarkersRequest requestWithVersion = builderWithVersions.build((short) 2);
        assertEquals(1, requestWithVersion.data().markers().size());
        WriteTxnMarkersRequestData.WritableTxnMarker dataMarkerWithVersion = requestWithVersion.data().markers().get(0);
        assertEquals(PRODUCER_ID, dataMarkerWithVersion.producerId());
        assertEquals(PRODUCER_EPOCH, dataMarkerWithVersion.producerEpoch());
        assertEquals(COORDINATOR_EPOCH, dataMarkerWithVersion.coordinatorEpoch());
        assertEquals(RESULT.id, dataMarkerWithVersion.transactionResult());
        assertEquals(1, dataMarkerWithVersion.topics().size());
        assertEquals(TOPIC_PARTITION.topic(), dataMarkerWithVersion.topics().get(0).name());
        assertEquals(Collections.singletonList(TOPIC_PARTITION.partition()), dataMarkerWithVersion.topics().get(0).partitionIndexes());
        // Verify TransactionVersion is set to 2 in the data
        assertEquals((byte) 2, dataMarkerWithVersion.transactionVersion());
    }

    @Test
    public void testGetErrorResponse() {
        WriteTxnMarkersRequest.Builder builder = new WriteTxnMarkersRequest.Builder(markers);
        for (short version : ApiKeys.WRITE_TXN_MARKERS.allVersions()) {
            WriteTxnMarkersRequest request = builder.build(version);
            WriteTxnMarkersResponse errorResponse =
                request.getErrorResponse(throttleTimeMs, Errors.UNKNOWN_PRODUCER_ID.exception());

            assertEquals(Collections.singletonMap(
                TOPIC_PARTITION, Errors.UNKNOWN_PRODUCER_ID), errorResponse.errorsByProducerId().get(PRODUCER_ID));
            assertEquals(Collections.singletonMap(Errors.UNKNOWN_PRODUCER_ID, 1), errorResponse.errorCounts());
            // Write txn marker has no throttle time defined in response.
            assertEquals(0, errorResponse.throttleTimeMs());
        }
    }

    @Test
    public void testTransactionVersion() {
        // Test that TransactionVersion is set correctly and serialization handles it properly.
        List<WriteTxnMarkersRequest.TxnMarkerEntry> markersWithVersion = Collections.singletonList(
            new WriteTxnMarkersRequest.TxnMarkerEntry(
                PRODUCER_ID, PRODUCER_EPOCH, COORDINATOR_EPOCH,
                RESULT, Collections.singletonList(TOPIC_PARTITION), (short) 2)
        );
        WriteTxnMarkersRequest.Builder builder = new WriteTxnMarkersRequest.Builder(markersWithVersion);
        
        // Test request version 2 - TransactionVersion should be included.
        WriteTxnMarkersRequest requestV2 = builder.build((short) 2);
        assertNotNull(requestV2);
        assertEquals(1, requestV2.markers().size());

        // Verify TransactionVersion is set to 2 in the request data.
        assertEquals((byte) 2, requestV2.data().markers().get(0).transactionVersion());
        // Verify the request can be serialized for version 2 (TransactionVersion field included).
        // This should not throw an exception.
        ByteBufferAccessor serializedV2 = requestV2.serialize();
        assertNotNull(serializedV2, "Serialization should succeed without error for version 2");
        // Test deserialization for version 2 - verify TransactionVersion field was included during serialization.
        // Use the already serialized request and parse it back to verify the field is present.
        serializedV2.buffer().rewind();
        RequestAndSize requestAndSizeV2 = AbstractRequest.parseRequest(
            ApiKeys.WRITE_TXN_MARKERS, (short) 2, serializedV2);
        WriteTxnMarkersRequest parsedRequestV2 = (WriteTxnMarkersRequest) requestAndSizeV2.request;
        assertNotNull(parsedRequestV2);
        assertEquals(1, parsedRequestV2.markers().size());
        // After deserialization, TransactionVersion should be 2 because it was included during serialization.
        assertEquals((short) 2, parsedRequestV2.markers().get(0).transactionVersion());
        // Verify the data also shows 2 (since it was read from serialized bytes with the field).
        assertEquals((byte) 2, parsedRequestV2.data().markers().get(0).transactionVersion());

        // Test request version 1 - TransactionVersion should be omitted (ignorable field).
        WriteTxnMarkersRequest requestV1 = builder.build((short) 1);
        assertNotNull(requestV1);
        assertEquals(1, requestV1.markers().size());

        // Verify TransactionVersion is still set to 2 in the request data (even for version 1).
        // This is what the coordinator has when building the request - data() is used before serialization.
        // The field value is preserved in the data, but will be omitted during serialization.
        assertEquals((byte) 2, requestV1.data().markers().get(0).transactionVersion());
        // Verify the request can be serialized for version 1 (TransactionVersion field omitted).
        // This should not throw an exception even though TransactionVersion is set to 2
        // because the field is marked as ignorable.
        ByteBufferAccessor serializedV1 = requestV1.serialize();
        assertNotNull(serializedV1, "Serialization should succeed without error for version 1 even with TransactionVersion set");
        // Test deserialization for version 1 - verify TransactionVersion field was omitted during serialization.
        // Use the already serialized request and parse it back to verify the field is not present.
        serializedV1.buffer().rewind();
        RequestAndSize requestAndSizeV1 = AbstractRequest.parseRequest(
            ApiKeys.WRITE_TXN_MARKERS, (short) 1, serializedV1);
        WriteTxnMarkersRequest parsedRequestV1 = (WriteTxnMarkersRequest) requestAndSizeV1.request;
        assertNotNull(parsedRequestV1);
        assertEquals(1, parsedRequestV1.markers().size());
        // After deserialization, TransactionVersion should be 0 because it was omitted during serialization.
        // The field is not present in the serialized bytes for version 1, so it defaults to 0.
        assertEquals((short) 0, parsedRequestV1.markers().get(0).transactionVersion());
        // Verify the data also shows 0 (since it was read from serialized bytes without the field).
        assertEquals((byte) 0, parsedRequestV1.data().markers().get(0).transactionVersion());
    }

    @Test
    public void testRequestWithMultipleMarkersDifferentTransactionVersions() {
        // Test building a request with two markers - one with tv1 and one with tv2
        // and verify that the right transaction versions are updated in the request data
        TopicPartition topicPartition1 = new TopicPartition("topic1", 0);
        TopicPartition topicPartition2 = new TopicPartition("topic2", 1);
        long producerId1 = 100L;
        long producerId2 = 200L;
        
        List<WriteTxnMarkersRequest.TxnMarkerEntry> markersWithDifferentVersions = List.of(
            new WriteTxnMarkersRequest.TxnMarkerEntry(
                producerId1, PRODUCER_EPOCH, COORDINATOR_EPOCH,
                RESULT, Collections.singletonList(topicPartition1), (short) 1), // tv1
            new WriteTxnMarkersRequest.TxnMarkerEntry(
                producerId2, PRODUCER_EPOCH, COORDINATOR_EPOCH,
                RESULT, Collections.singletonList(topicPartition2), (short) 2)  // tv2
        );
        
        WriteTxnMarkersRequest.Builder builder = new WriteTxnMarkersRequest.Builder(markersWithDifferentVersions);
        WriteTxnMarkersRequest request = builder.build((short) 2);
        
        assertNotNull(request);
        assertEquals(2, request.data().markers().size());
        
        // Verify first marker has tv1 (transactionVersion = 1) in the request data
        WriteTxnMarkersRequestData.WritableTxnMarker dataMarker1 = request.data().markers().get(0);
        assertEquals(producerId1, dataMarker1.producerId());
        assertEquals((byte) 1, dataMarker1.transactionVersion());
        
        // Verify second marker has tv2 (transactionVersion = 2) in the request data
        WriteTxnMarkersRequestData.WritableTxnMarker dataMarker2 = request.data().markers().get(1);
        assertEquals(producerId2, dataMarker2.producerId());
        assertEquals((byte) 2, dataMarker2.transactionVersion());
        
        // Verify markers() method also returns correct transaction versions
        List<WriteTxnMarkersRequest.TxnMarkerEntry> markers = request.markers();
        assertEquals(2, markers.size());
        assertEquals((short) 1, markers.get(0).transactionVersion());
        assertEquals(producerId1, markers.get(0).producerId());
        assertEquals((short) 2, markers.get(1).transactionVersion());
        assertEquals(producerId2, markers.get(1).producerId());
    }
}
