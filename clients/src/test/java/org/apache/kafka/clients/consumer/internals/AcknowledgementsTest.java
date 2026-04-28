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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.common.KafkaException;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AcknowledgementsTest {

    private final Acknowledgements acks = Acknowledgements.empty();
    private final int maxRecordsWithSameAcknowledgeType = Acknowledgements.MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE;

    @Test
    public void testEmptyBatch() {
        List<AcknowledgementBatch> ackList = acks.getAcknowledgementBatches();
        assertTrue(ackList.isEmpty());

        List<AcknowledgementBatch> ackList2 = acks.getAcknowledgementBatches();
        assertTrue(ackList2.isEmpty());
    }

    @Test
    public void testSingleStateSingleRecord() {
        acks.add(0L, AcknowledgeType.ACCEPT);

        List<AcknowledgementBatch> ackList = acks.getAcknowledgementBatches();
        assertEquals(1, ackList.size());
        assertEquals(0L, ackList.get(0).firstOffset());
        assertEquals(0L, ackList.get(0).lastOffset());
        assertEquals(1, ackList.get(0).acknowledgeTypes().size());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeTypes().get(0));

        List<AcknowledgementBatch> ackList2 = acks.getAcknowledgementBatches();
        assertEquals(1, ackList2.size());
        assertEquals(0L, ackList2.get(0).firstOffset());
        assertEquals(0L, ackList2.get(0).lastOffset());
        assertEquals(1, ackList2.get(0).acknowledgeTypes().size());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList2.get(0).acknowledgeTypes().get(0));
    }

    @Test
    public void testSingleStateMultiRecord() {
        acks.add(0L, AcknowledgeType.ACCEPT);
        acks.add(1L, AcknowledgeType.ACCEPT);
        acks.add(2L, AcknowledgeType.ACCEPT);
        acks.add(3L, AcknowledgeType.ACCEPT);
        acks.add(4L, AcknowledgeType.ACCEPT);

        List<AcknowledgementBatch> ackList = acks.getAcknowledgementBatches();
        assertEquals(1, ackList.size());
        assertEquals(0L, ackList.get(0).firstOffset());
        assertEquals(4L, ackList.get(0).lastOffset());
        assertNotEquals(0, ackList.get(0).acknowledgeTypes().size());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeTypes().get(0));

        List<AcknowledgementBatch> ackList2 = acks.getAcknowledgementBatches();
        assertEquals(1, ackList2.size());
        assertEquals(0L, ackList2.get(0).firstOffset());
        assertEquals(4L, ackList2.get(0).lastOffset());
        assertNotEquals(0, ackList2.get(0).acknowledgeTypes().size());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList2.get(0).acknowledgeTypes().get(0));
    }

    @Test
    public void testSingleAcknowledgeTypeExceedingLimit() {
        int i = 0;
        for (; i < maxRecordsWithSameAcknowledgeType; i++) {
            acks.add(i, AcknowledgeType.ACCEPT);
        }
        acks.add(i++, AcknowledgeType.ACCEPT);
        acks.add(i++, AcknowledgeType.ACCEPT);
        for (int j = 0; j <= maxRecordsWithSameAcknowledgeType; j++) {
            acks.add(i + j, AcknowledgeType.REJECT);
        }


        List<AcknowledgementBatch> ackList = acks.getAcknowledgementBatches();
        assertEquals(2, ackList.size());
        assertEquals(0L, ackList.get(0).firstOffset());
        assertEquals(maxRecordsWithSameAcknowledgeType + 1, ackList.get(0).lastOffset());
        assertEquals(1, ackList.get(0).acknowledgeTypes().size());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeTypes().get(0));
        assertEquals(maxRecordsWithSameAcknowledgeType + 2, ackList.get(1).firstOffset());
        assertEquals(i + maxRecordsWithSameAcknowledgeType, ackList.get(1).lastOffset());
        assertEquals(1, ackList.get(1).acknowledgeTypes().size());
        assertEquals(AcknowledgeType.REJECT.id, ackList.get(1).acknowledgeTypes().get(0));

        List<AcknowledgementBatch> ackList2 = acks.getAcknowledgementBatches();
        assertEquals(2, ackList2.size());
        assertEquals(0L, ackList2.get(0).firstOffset());
        assertEquals(maxRecordsWithSameAcknowledgeType + 1, ackList2.get(0).lastOffset());
        assertEquals(1, ackList2.get(0).acknowledgeTypes().size());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList2.get(0).acknowledgeTypes().get(0));
        assertEquals(maxRecordsWithSameAcknowledgeType + 2, ackList2.get(1).firstOffset());
        assertEquals(i + maxRecordsWithSameAcknowledgeType, ackList2.get(1).lastOffset());
        assertEquals(1, ackList2.get(1).acknowledgeTypes().size());
        assertEquals(AcknowledgeType.REJECT.id, ackList2.get(1).acknowledgeTypes().get(0));
    }

    @Test
    public void testSingleAcknowledgeTypeWithGap() {
        for (int i = 0; i < maxRecordsWithSameAcknowledgeType; i++) {
            acks.add(i, null);
        }
        acks.add(maxRecordsWithSameAcknowledgeType, null);
        acks.add(maxRecordsWithSameAcknowledgeType + 1, null);

        List<AcknowledgementBatch> ackList = acks.getAcknowledgementBatches();
        assertEquals(1, ackList.size());
        assertEquals(0L, ackList.get(0).firstOffset());
        assertEquals(maxRecordsWithSameAcknowledgeType + 1, ackList.get(0).lastOffset());
        assertEquals(1, ackList.get(0).acknowledgeTypes().size());
        assertEquals(Acknowledgements.ACKNOWLEDGE_TYPE_GAP, ackList.get(0).acknowledgeTypes().get(0));

        List<AcknowledgementBatch> ackList2 = acks.getAcknowledgementBatches();
        assertEquals(1, ackList2.size());
        assertEquals(0L, ackList2.get(0).firstOffset());
        assertEquals(maxRecordsWithSameAcknowledgeType + 1, ackList2.get(0).lastOffset());
        assertEquals(1, ackList2.get(0).acknowledgeTypes().size());
        assertEquals(Acknowledgements.ACKNOWLEDGE_TYPE_GAP, ackList2.get(0).acknowledgeTypes().get(0));
    }

    @Test
    public void testOptimiseBatches() {
        int offset = 0;
        for (; offset < maxRecordsWithSameAcknowledgeType; offset++) {
            acks.add(offset, AcknowledgeType.ACCEPT);
        }
        acks.add(offset++, AcknowledgeType.REJECT);
        acks.add(offset++, AcknowledgeType.ACCEPT);
        acks.add(offset++, AcknowledgeType.RELEASE);
        acks.add(offset++, null);

        // Adding more than the max records
        for (int j = 0; j <= maxRecordsWithSameAcknowledgeType; j++) {
            acks.add(offset + j, AcknowledgeType.ACCEPT);
        }
        offset += maxRecordsWithSameAcknowledgeType + 1;

        // Adding 2 more records of different type
        acks.add(offset++, AcknowledgeType.REJECT);
        acks.add(offset, AcknowledgeType.RELEASE);

        List<AcknowledgementBatch> ackList = acks.getAcknowledgementBatches();
        assertEquals(3, ackList.size());
        assertEquals(0, ackList.get(0).firstOffset());
        assertEquals(maxRecordsWithSameAcknowledgeType + 3, ackList.get(0).lastOffset());
        assertEquals(maxRecordsWithSameAcknowledgeType + 4, ackList.get(1).firstOffset());
        assertEquals(2 * maxRecordsWithSameAcknowledgeType + 4, ackList.get(1).lastOffset());
        assertEquals(1, ackList.get(1).acknowledgeTypes().size());
        assertEquals(offset - 1, ackList.get(2).firstOffset());
        assertEquals(offset, ackList.get(2).lastOffset());
        assertEquals(2, ackList.get(2).acknowledgeTypes().size());

        List<AcknowledgementBatch> ackList2 = acks.getAcknowledgementBatches();
        assertEquals(3, ackList2.size());
        assertEquals(0, ackList2.get(0).firstOffset());
        assertEquals(maxRecordsWithSameAcknowledgeType + 3, ackList2.get(0).lastOffset());
        assertEquals(maxRecordsWithSameAcknowledgeType + 4, ackList2.get(1).firstOffset());
        assertEquals(2 * maxRecordsWithSameAcknowledgeType + 4, ackList2.get(1).lastOffset());
        assertEquals(1, ackList2.get(1).acknowledgeTypes().size());
        assertEquals(offset - 1, ackList2.get(2).firstOffset());
        assertEquals(offset, ackList2.get(2).lastOffset());
        assertEquals(2, ackList2.get(2).acknowledgeTypes().size());
    }

    @Test
    public void testSingleAcknowledgeTypeWithinLimit() {
        acks.add(0L, AcknowledgeType.ACCEPT);
        acks.add(1L, AcknowledgeType.ACCEPT);
        acks.add(2L, AcknowledgeType.ACCEPT);

        List<AcknowledgementBatch> ackList = acks.getAcknowledgementBatches();
        assertEquals(1, ackList.size());
        assertEquals(1, ackList.get(0).acknowledgeTypes().size());
    }

    @Test
    public void testMultiStateMultiRecord() {
        acks.add(0L, AcknowledgeType.ACCEPT);
        acks.add(1L, AcknowledgeType.ACCEPT);
        acks.add(2L, AcknowledgeType.ACCEPT);
        acks.add(3L, AcknowledgeType.RELEASE);
        acks.add(4L, AcknowledgeType.RELEASE);

        List<AcknowledgementBatch> ackList = acks.getAcknowledgementBatches();
        assertEquals(1, ackList.size());
        assertEquals(0L, ackList.get(0).firstOffset());
        assertEquals(4L, ackList.get(0).lastOffset());
        assertEquals(5, ackList.get(0).acknowledgeTypes().size());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeTypes().get(0));
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeTypes().get(1));
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeTypes().get(2));
        assertEquals(AcknowledgeType.RELEASE.id, ackList.get(0).acknowledgeTypes().get(3));
        assertEquals(AcknowledgeType.RELEASE.id, ackList.get(0).acknowledgeTypes().get(4));

        List<AcknowledgementBatch> ackList2 = acks.getAcknowledgementBatches();
        assertEquals(1, ackList2.size());
        assertEquals(0L, ackList2.get(0).firstOffset());
        assertEquals(4L, ackList2.get(0).lastOffset());
        assertEquals(5, ackList2.get(0).acknowledgeTypes().size());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList2.get(0).acknowledgeTypes().get(0));
        assertEquals(AcknowledgeType.ACCEPT.id, ackList2.get(0).acknowledgeTypes().get(1));
        assertEquals(AcknowledgeType.ACCEPT.id, ackList2.get(0).acknowledgeTypes().get(2));
        assertEquals(AcknowledgeType.RELEASE.id, ackList2.get(0).acknowledgeTypes().get(3));
        assertEquals(AcknowledgeType.RELEASE.id, ackList2.get(0).acknowledgeTypes().get(4));
    }

    @Test
    public void testMultiStateSingleMultiRecord() {
        acks.add(0L, AcknowledgeType.ACCEPT);
        acks.add(1L, AcknowledgeType.RELEASE);
        acks.add(2L, AcknowledgeType.RELEASE);
        acks.add(3L, AcknowledgeType.RELEASE);
        acks.add(4L, AcknowledgeType.RELEASE);

        List<AcknowledgementBatch> ackList = acks.getAcknowledgementBatches();
        assertEquals(1, ackList.size());
        assertEquals(0L, ackList.get(0).firstOffset());
        assertEquals(4L, ackList.get(0).lastOffset());
        assertEquals(5, ackList.get(0).acknowledgeTypes().size());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeTypes().get(0));
        assertEquals(AcknowledgeType.RELEASE.id, ackList.get(0).acknowledgeTypes().get(1));
        assertEquals(AcknowledgeType.RELEASE.id, ackList.get(0).acknowledgeTypes().get(2));
        assertEquals(AcknowledgeType.RELEASE.id, ackList.get(0).acknowledgeTypes().get(3));
        assertEquals(AcknowledgeType.RELEASE.id, ackList.get(0).acknowledgeTypes().get(4));

        List<AcknowledgementBatch> ackList2 = acks.getAcknowledgementBatches();
        assertEquals(1, ackList2.size());
        assertEquals(0L, ackList2.get(0).firstOffset());
        assertEquals(4L, ackList2.get(0).lastOffset());
        assertEquals(5, ackList2.get(0).acknowledgeTypes().size());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList2.get(0).acknowledgeTypes().get(0));
        assertEquals(AcknowledgeType.RELEASE.id, ackList2.get(0).acknowledgeTypes().get(1));
        assertEquals(AcknowledgeType.RELEASE.id, ackList2.get(0).acknowledgeTypes().get(2));
        assertEquals(AcknowledgeType.RELEASE.id, ackList2.get(0).acknowledgeTypes().get(3));
        assertEquals(AcknowledgeType.RELEASE.id, ackList2.get(0).acknowledgeTypes().get(4));
    }

    @Test
    public void testMultiStateMultiSingleRecord() {
        acks.add(0L, AcknowledgeType.ACCEPT);
        acks.add(1L, AcknowledgeType.ACCEPT);
        acks.add(2L, AcknowledgeType.ACCEPT);
        acks.add(3L, AcknowledgeType.ACCEPT);
        acks.add(4L, AcknowledgeType.RELEASE);

        List<AcknowledgementBatch> ackList = acks.getAcknowledgementBatches();
        assertEquals(1, ackList.size());
        assertEquals(0L, ackList.get(0).firstOffset());
        assertEquals(4L, ackList.get(0).lastOffset());
        assertEquals(5, ackList.get(0).acknowledgeTypes().size());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeTypes().get(0));
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeTypes().get(1));
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeTypes().get(2));
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeTypes().get(3));
        assertEquals(AcknowledgeType.RELEASE.id, ackList.get(0).acknowledgeTypes().get(4));

        List<AcknowledgementBatch> ackList2 = acks.getAcknowledgementBatches();
        assertEquals(1, ackList2.size());
        assertEquals(0L, ackList2.get(0).firstOffset());
        assertEquals(4L, ackList2.get(0).lastOffset());
        assertEquals(5, ackList2.get(0).acknowledgeTypes().size());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList2.get(0).acknowledgeTypes().get(0));
        assertEquals(AcknowledgeType.ACCEPT.id, ackList2.get(0).acknowledgeTypes().get(1));
        assertEquals(AcknowledgeType.ACCEPT.id, ackList2.get(0).acknowledgeTypes().get(2));
        assertEquals(AcknowledgeType.ACCEPT.id, ackList2.get(0).acknowledgeTypes().get(3));
        assertEquals(AcknowledgeType.RELEASE.id, ackList2.get(0).acknowledgeTypes().get(4));
    }

    @Test
    public void testSingleGap() {
        acks.addGap(0L);

        List<AcknowledgementBatch> ackList = acks.getAcknowledgementBatches();
        assertEquals(1, ackList.size());
        assertEquals(0L, ackList.get(0).firstOffset());
        assertEquals(0L, ackList.get(0).lastOffset());
        assertEquals(1, ackList.get(0).acknowledgeTypes().size());
        assertEquals(Acknowledgements.ACKNOWLEDGE_TYPE_GAP, ackList.get(0).acknowledgeTypes().get(0));

        List<AcknowledgementBatch> ackList2 = acks.getAcknowledgementBatches();
        assertEquals(1, ackList2.size());
        assertEquals(0L, ackList2.get(0).firstOffset());
        assertEquals(0L, ackList2.get(0).lastOffset());
        assertEquals(1, ackList2.get(0).acknowledgeTypes().size());
        assertEquals(Acknowledgements.ACKNOWLEDGE_TYPE_GAP, ackList2.get(0).acknowledgeTypes().get(0));
    }

    @Test
    public void testMultiGap() {
        acks.addGap(0L);
        acks.addGap(1L);

        List<AcknowledgementBatch> ackList = acks.getAcknowledgementBatches();
        assertEquals(1, ackList.size());
        assertEquals(0L, ackList.get(0).firstOffset());
        assertEquals(1L, ackList.get(0).lastOffset());
        assertEquals(1, ackList.get(0).acknowledgeTypes().size());
        assertEquals(Acknowledgements.ACKNOWLEDGE_TYPE_GAP, ackList.get(0).acknowledgeTypes().get(0));

        List<AcknowledgementBatch> ackList2 = acks.getAcknowledgementBatches();
        assertEquals(1, ackList2.size());
        assertEquals(0L, ackList2.get(0).firstOffset());
        assertEquals(1L, ackList2.get(0).lastOffset());
        assertEquals(1, ackList2.get(0).acknowledgeTypes().size());
        assertEquals(Acknowledgements.ACKNOWLEDGE_TYPE_GAP, ackList2.get(0).acknowledgeTypes().get(0));
    }

    @Test
    public void testSingleGapSingleState() {
        acks.addGap(0L);
        acks.add(1L, AcknowledgeType.ACCEPT);

        List<AcknowledgementBatch> ackList = acks.getAcknowledgementBatches();
        assertEquals(1, ackList.size());
        assertEquals(0L, ackList.get(0).firstOffset());
        assertEquals(1L, ackList.get(0).lastOffset());
        assertEquals(2, ackList.get(0).acknowledgeTypes().size());
        assertEquals(Acknowledgements.ACKNOWLEDGE_TYPE_GAP, ackList.get(0).acknowledgeTypes().get(0));
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeTypes().get(1));

        List<AcknowledgementBatch> ackList2 = acks.getAcknowledgementBatches();
        assertEquals(1, ackList2.size());
        assertEquals(0L, ackList2.get(0).firstOffset());
        assertEquals(1L, ackList2.get(0).lastOffset());
        assertEquals(2, ackList2.get(0).acknowledgeTypes().size());
        assertEquals(Acknowledgements.ACKNOWLEDGE_TYPE_GAP, ackList2.get(0).acknowledgeTypes().get(0));
        assertEquals(AcknowledgeType.ACCEPT.id, ackList2.get(0).acknowledgeTypes().get(1));
    }

    @Test
    public void testSingleStateSingleGap() {
        acks.add(0L, AcknowledgeType.ACCEPT);
        acks.addGap(1L);

        List<AcknowledgementBatch> ackList = acks.getAcknowledgementBatches();
        assertEquals(1, ackList.size());
        assertEquals(0L, ackList.get(0).firstOffset());
        assertEquals(1L, ackList.get(0).lastOffset());
        assertEquals(2, ackList.get(0).acknowledgeTypes().size());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeTypes().get(0));
        assertEquals(Acknowledgements.ACKNOWLEDGE_TYPE_GAP, ackList.get(0).acknowledgeTypes().get(1));

        List<AcknowledgementBatch> ackList2 = acks.getAcknowledgementBatches();
        assertEquals(1, ackList2.size());
        assertEquals(0L, ackList2.get(0).firstOffset());
        assertEquals(1L, ackList2.get(0).lastOffset());
        assertEquals(2, ackList2.get(0).acknowledgeTypes().size());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList2.get(0).acknowledgeTypes().get(0));
        assertEquals(Acknowledgements.ACKNOWLEDGE_TYPE_GAP, ackList2.get(0).acknowledgeTypes().get(1));
    }

    @Test
    public void testMultiStateMultiGap() {
        acks.add(0L, AcknowledgeType.RELEASE);
        acks.addGap(1L);
        acks.addGap(2L);
        acks.add(3L, AcknowledgeType.ACCEPT);
        acks.add(4L, AcknowledgeType.ACCEPT);

        List<AcknowledgementBatch> ackList = acks.getAcknowledgementBatches();
        assertEquals(1, ackList.size());
        assertEquals(0L, ackList.get(0).firstOffset());
        assertEquals(4L, ackList.get(0).lastOffset());
        assertEquals(5, ackList.get(0).acknowledgeTypes().size());
        assertEquals(AcknowledgeType.RELEASE.id, ackList.get(0).acknowledgeTypes().get(0));
        assertEquals(Acknowledgements.ACKNOWLEDGE_TYPE_GAP, ackList.get(0).acknowledgeTypes().get(1));
        assertEquals(Acknowledgements.ACKNOWLEDGE_TYPE_GAP, ackList.get(0).acknowledgeTypes().get(2));
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeTypes().get(3));
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeTypes().get(4));

        List<AcknowledgementBatch> ackList2 = acks.getAcknowledgementBatches();
        assertEquals(1, ackList2.size());
        assertEquals(0L, ackList2.get(0).firstOffset());
        assertEquals(4L, ackList2.get(0).lastOffset());
        assertEquals(5, ackList2.get(0).acknowledgeTypes().size());
        assertEquals(AcknowledgeType.RELEASE.id, ackList2.get(0).acknowledgeTypes().get(0));
        assertEquals(Acknowledgements.ACKNOWLEDGE_TYPE_GAP, ackList2.get(0).acknowledgeTypes().get(1));
        assertEquals(Acknowledgements.ACKNOWLEDGE_TYPE_GAP, ackList2.get(0).acknowledgeTypes().get(2));
        assertEquals(AcknowledgeType.ACCEPT.id, ackList2.get(0).acknowledgeTypes().get(3));
        assertEquals(AcknowledgeType.ACCEPT.id, ackList2.get(0).acknowledgeTypes().get(4));
    }

    @Test
    public void testMultiStateMultiGaps() {
        acks.add(0L, AcknowledgeType.ACCEPT);
        acks.add(1L, AcknowledgeType.RELEASE);
        acks.addGap(2L);
        acks.add(3L, AcknowledgeType.RELEASE);
        acks.addGap(4L);

        List<AcknowledgementBatch> ackList = acks.getAcknowledgementBatches();
        assertEquals(1, ackList.size());
        assertEquals(0L, ackList.get(0).firstOffset());
        assertEquals(4L, ackList.get(0).lastOffset());
        assertEquals(5, ackList.get(0).acknowledgeTypes().size());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeTypes().get(0));
        assertEquals(AcknowledgeType.RELEASE.id, ackList.get(0).acknowledgeTypes().get(1));
        assertEquals(Acknowledgements.ACKNOWLEDGE_TYPE_GAP, ackList.get(0).acknowledgeTypes().get(2));
        assertEquals(AcknowledgeType.RELEASE.id, ackList.get(0).acknowledgeTypes().get(3));
        assertEquals(Acknowledgements.ACKNOWLEDGE_TYPE_GAP, ackList.get(0).acknowledgeTypes().get(4));

        List<AcknowledgementBatch> ackList2 = acks.getAcknowledgementBatches();
        assertEquals(1, ackList2.size());
        assertEquals(0L, ackList2.get(0).firstOffset());
        assertEquals(4L, ackList2.get(0).lastOffset());
        assertEquals(5, ackList2.get(0).acknowledgeTypes().size());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList2.get(0).acknowledgeTypes().get(0));
        assertEquals(AcknowledgeType.RELEASE.id, ackList2.get(0).acknowledgeTypes().get(1));
        assertEquals(Acknowledgements.ACKNOWLEDGE_TYPE_GAP, ackList2.get(0).acknowledgeTypes().get(2));
        assertEquals(AcknowledgeType.RELEASE.id, ackList2.get(0).acknowledgeTypes().get(3));
        assertEquals(Acknowledgements.ACKNOWLEDGE_TYPE_GAP, ackList2.get(0).acknowledgeTypes().get(4));
    }

    @Test
    public void testNoncontiguousBatches() {
        acks.add(0L, AcknowledgeType.ACCEPT);
        acks.add(1L, AcknowledgeType.RELEASE);
        acks.add(3L, AcknowledgeType.REJECT);
        acks.add(4L, AcknowledgeType.REJECT);
        acks.add(6L, AcknowledgeType.REJECT);

        List<AcknowledgementBatch> ackList = acks.getAcknowledgementBatches();
        assertEquals(3, ackList.size());
        assertEquals(0L, ackList.get(0).firstOffset());
        assertEquals(1L, ackList.get(0).lastOffset());
        assertEquals(2, ackList.get(0).acknowledgeTypes().size());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeTypes().get(0));
        assertEquals(AcknowledgeType.RELEASE.id, ackList.get(0).acknowledgeTypes().get(1));
        assertEquals(3L, ackList.get(1).firstOffset());
        assertEquals(4L, ackList.get(1).lastOffset());
        assertEquals(1, ackList.get(1).acknowledgeTypes().size());
        assertEquals(AcknowledgeType.REJECT.id, ackList.get(1).acknowledgeTypes().get(0));
        assertEquals(6L, ackList.get(2).firstOffset());
        assertEquals(6L, ackList.get(2).lastOffset());
        assertEquals(1, ackList.get(2).acknowledgeTypes().size());
        assertEquals(AcknowledgeType.REJECT.id, ackList.get(2).acknowledgeTypes().get(0));

        List<AcknowledgementBatch> ackList2 = acks.getAcknowledgementBatches();
        assertEquals(3, ackList2.size());
        assertEquals(0L, ackList2.get(0).firstOffset());
        assertEquals(1L, ackList2.get(0).lastOffset());
        assertEquals(2, ackList2.get(0).acknowledgeTypes().size());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList2.get(0).acknowledgeTypes().get(0));
        assertEquals(AcknowledgeType.RELEASE.id, ackList2.get(0).acknowledgeTypes().get(1));
        assertEquals(3L, ackList2.get(1).firstOffset());
        assertEquals(4L, ackList2.get(1).lastOffset());
        assertEquals(1, ackList2.get(1).acknowledgeTypes().size());
        assertEquals(AcknowledgeType.REJECT.id, ackList2.get(1).acknowledgeTypes().get(0));
        assertEquals(6L, ackList2.get(2).firstOffset());
        assertEquals(6L, ackList2.get(2).lastOffset());
        assertEquals(1, ackList2.get(2).acknowledgeTypes().size());
        assertEquals(AcknowledgeType.REJECT.id, ackList2.get(2).acknowledgeTypes().get(0));
    }

    @Test
    public void testNoncontiguousGaps() {
        acks.addGap(2L);
        acks.addGap(4L);

        List<AcknowledgementBatch> ackList = acks.getAcknowledgementBatches();
        assertEquals(2, ackList.size());
        assertEquals(2L, ackList.get(0).firstOffset());
        assertEquals(2L, ackList.get(0).lastOffset());
        assertEquals(1, ackList.get(0).acknowledgeTypes().size());
        assertEquals(Acknowledgements.ACKNOWLEDGE_TYPE_GAP, ackList.get(0).acknowledgeTypes().get(0));
        assertEquals(4L, ackList.get(1).firstOffset());
        assertEquals(4L, ackList.get(1).lastOffset());
        assertEquals(1, ackList.get(1).acknowledgeTypes().size());
        assertEquals(Acknowledgements.ACKNOWLEDGE_TYPE_GAP, ackList.get(1).acknowledgeTypes().get(0));

        List<AcknowledgementBatch> ackList2 = acks.getAcknowledgementBatches();
        assertEquals(2, ackList2.size());
        assertEquals(2L, ackList2.get(0).firstOffset());
        assertEquals(2L, ackList2.get(0).lastOffset());
        assertEquals(1, ackList2.get(0).acknowledgeTypes().size());
        assertEquals(Acknowledgements.ACKNOWLEDGE_TYPE_GAP, ackList2.get(0).acknowledgeTypes().get(0));
        assertEquals(4L, ackList2.get(1).firstOffset());
        assertEquals(4L, ackList2.get(1).lastOffset());
        assertEquals(1, ackList2.get(1).acknowledgeTypes().size());
        assertEquals(Acknowledgements.ACKNOWLEDGE_TYPE_GAP, ackList2.get(1).acknowledgeTypes().get(0));
    }

    @Test
    public void testCompleteSuccess() {
        acks.add(0, AcknowledgeType.RENEW);
        assertFalse(acks.isCompleted());

        acks.complete(null);
        assertTrue(acks.isCompleted());
        assertNull(acks.getAcknowledgeException());
    }

    @Test
    public void testCompleteException() {
        acks.add(0, AcknowledgeType.RENEW);
        assertFalse(acks.isCompleted());

        acks.complete(new KafkaException());
        assertTrue(acks.isCompleted());
        assertNotNull(acks.getAcknowledgeException());
    }
}
