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
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class WriteTxnMarkersRequestTest {

    private static long producerId = 10L;
    private static short producerEpoch = 2;
    private static int coordinatorEpoch = 1;
    private static TransactionResult result = TransactionResult.COMMIT;
    private static TopicPartition topicPartition = new TopicPartition("topic", 73);

    protected static int throttleTimeMs = 10;

    private static List<WriteTxnMarkersRequest.TxnMarkerEntry> markers;

    @Before
    public void setUp() {
        markers = Collections.singletonList(
             new WriteTxnMarkersRequest.TxnMarkerEntry(
                 producerId, producerEpoch, coordinatorEpoch,
                 result, Collections.singletonList(topicPartition))
        );
    }

    @Test
    public void testConstructor() {
        WriteTxnMarkersRequest.Builder builder = new WriteTxnMarkersRequest.Builder(ApiKeys.WRITE_TXN_MARKERS.latestVersion(), markers);
        for (short version = 0; version <= ApiKeys.WRITE_TXN_MARKERS.latestVersion(); version++) {
            WriteTxnMarkersRequest request = builder.build(version);
            assertEquals(1, request.markers().size());
            WriteTxnMarkersRequest.TxnMarkerEntry marker = request.markers().get(0);
            assertEquals(producerId, marker.producerId());
            assertEquals(producerEpoch, marker.producerEpoch());
            assertEquals(coordinatorEpoch, marker.coordinatorEpoch());
            assertEquals(result, marker.transactionResult());
            assertEquals(Collections.singletonList(topicPartition), marker.partitions());
        }
    }

    @Test
    public void testGetErrorResponse() {
        WriteTxnMarkersRequest.Builder builder = new WriteTxnMarkersRequest.Builder(ApiKeys.WRITE_TXN_MARKERS.latestVersion(), markers);
        for (short version = 0; version <= ApiKeys.WRITE_TXN_MARKERS.latestVersion(); version++) {
            WriteTxnMarkersRequest request = builder.build(version);
            WriteTxnMarkersResponse errorResponse =
                request.getErrorResponse(throttleTimeMs, Errors.UNKNOWN_PRODUCER_ID.exception());

            assertEquals(Collections.singletonMap(
                topicPartition, Errors.UNKNOWN_PRODUCER_ID), errorResponse.errorsByProducerId().get(producerId));
            assertEquals(Collections.singletonMap(Errors.UNKNOWN_PRODUCER_ID, 1), errorResponse.errorCounts());
            // Write txn marker has no throttle time defined in response.
            assertEquals(0, errorResponse.throttleTimeMs());
        }
    }
}
