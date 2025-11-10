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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
                 RESULT, Collections.singletonList(TOPIC_PARTITION))
        );
    }

    @Test
    public void testConstructors() {
        // Test single-arg constructor (default - transactionVersions = null)
        WriteTxnMarkersRequest.Builder builder = new WriteTxnMarkersRequest.Builder(markers);
        for (short version : ApiKeys.WRITE_TXN_MARKERS.allVersions()) {
            WriteTxnMarkersRequest request = builder.build(version);
            assertEquals(1, request.markers().size());
            WriteTxnMarkersRequest.TxnMarkerEntry marker = request.markers().get(0);
            assertEquals(PRODUCER_ID, marker.producerId());
            assertEquals(PRODUCER_EPOCH, marker.producerEpoch());
            assertEquals(COORDINATOR_EPOCH, marker.coordinatorEpoch());
            assertEquals(RESULT, marker.transactionResult());
            assertEquals(Collections.singletonList(TOPIC_PARTITION), marker.partitions());
            // Verify TransactionVersion defaults to 0 when not provided
            assertEquals((byte) 0, request.data().markers().get(0).transactionVersion());
        }
        
        // Test two-arg constructor (with a transactionVersions list)
        List<Byte> transactionVersions = new ArrayList<>();
        transactionVersions.add((byte) 2);
        WriteTxnMarkersRequest.Builder builderWithVersions = new WriteTxnMarkersRequest.Builder(markers, transactionVersions);
        for (short version : ApiKeys.WRITE_TXN_MARKERS.allVersions()) {
            WriteTxnMarkersRequest request = builderWithVersions.build(version);
            assertEquals(1, request.markers().size());
            WriteTxnMarkersRequest.TxnMarkerEntry marker = request.markers().get(0);
            assertEquals(PRODUCER_ID, marker.producerId());
            assertEquals(PRODUCER_EPOCH, marker.producerEpoch());
            assertEquals(COORDINATOR_EPOCH, marker.coordinatorEpoch());
            assertEquals(RESULT, marker.transactionResult());
            assertEquals(Collections.singletonList(TOPIC_PARTITION), marker.partitions());
            // Verify TransactionVersion is set to the provided value (2)
            assertEquals((byte) 2, request.data().markers().get(0).transactionVersion());
        }
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
        List<Byte> transactionVersions = new ArrayList<>();
        transactionVersions.add((byte) 2);
        WriteTxnMarkersRequest.Builder builder = new WriteTxnMarkersRequest.Builder(markers, transactionVersions);
        
        // Test version 2 - TransactionVersion should be included.
        WriteTxnMarkersRequest requestV2 = builder.build((short) 2);
        assertNotNull(requestV2);
        assertEquals(1, requestV2.markers().size());
        // Verify TransactionVersion is set to 2 in the request data.
        assertEquals((byte) 2, requestV2.data().markers().get(0).transactionVersion());
        
        // Verify the request can be serialized for version 2 (TransactionVersion field included).
        // This should not throw an exception.
        requestV2.serialize();
        int sizeV2 = requestV2.sizeInBytes();
        
        // Test version 1 - TransactionVersion should be omitted (ignorable field).
        WriteTxnMarkersRequest requestV1 = builder.build((short) 1);
        assertNotNull(requestV1);
        assertEquals(1, requestV1.markers().size());
        // Verify TransactionVersion is still set to 2 in the request data (even for version 1).
        // The field value is preserved, but will be omitted during serialization.
        assertEquals((byte) 2, requestV1.data().markers().get(0).transactionVersion());
        
        // Verify the request can be serialized for version 1 (TransactionVersion field omitted).
        // This should not throw an exception even though TransactionVersion is set to 2
        // because the field is marked as ignorable.
        requestV1.serialize();
        int sizeV1 = requestV1.sizeInBytes();
        
        // Verify that version 2 is larger than version 1 because it includes TransactionVersion field.
        // TransactionVersion is int8 (1 byte), so version 2 should be at least 1 byte larger.
        // This check ensures that the serialization logic correctly includes/excludes the field.
        assertTrue(sizeV2 > sizeV1, 
            String.format("Version 2 (%d bytes) should be larger than version 1 (%d bytes) " +
                "because it includes the TransactionVersion field", sizeV2, sizeV1
            )
        );
    }
}
