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
import org.apache.kafka.common.message.ShareFetchRequestData;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AcknowledgementsTest {

    private final Acknowledgements acks = Acknowledgements.empty();

    @Test
    public void testEmptyBatch() {
        List<ShareFetchRequestData.AcknowledgementBatch> ackList = acks.getAcknowledgmentBatches();
        assertTrue(ackList.isEmpty());
    }

    @Test
    public void testSingleBatchSingleRecord() {
        acks.add(0L, AcknowledgeType.ACCEPT);

        List<ShareFetchRequestData.AcknowledgementBatch> ackList = acks.getAcknowledgmentBatches();
        assertEquals(1, ackList.size());
        assertEquals(0L, ackList.get(0).baseOffset());
        assertEquals(0L, ackList.get(0).lastOffset());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeType());
        assertTrue(ackList.get(0).gapOffsets().isEmpty());
    }

    @Test
    public void testSingleBatchMultiRecord() {
        acks.add(0L, AcknowledgeType.ACCEPT);
        acks.add(1L, AcknowledgeType.ACCEPT);
        acks.add(2L, AcknowledgeType.ACCEPT);
        acks.add(3L, AcknowledgeType.ACCEPT);
        acks.add(4L, AcknowledgeType.ACCEPT);

        List<ShareFetchRequestData.AcknowledgementBatch> ackList = acks.getAcknowledgmentBatches();
        assertEquals(1, ackList.size());
        assertEquals(0L, ackList.get(0).baseOffset());
        assertEquals(4L, ackList.get(0).lastOffset());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeType());
        assertTrue(ackList.get(0).gapOffsets().isEmpty());
    }

    @Test
    public void testMultiBatchMultiRecord() {
        acks.add(0L, AcknowledgeType.ACCEPT);
        acks.add(1L, AcknowledgeType.ACCEPT);
        acks.add(2L, AcknowledgeType.ACCEPT);
        acks.add(3L, AcknowledgeType.RELEASE);
        acks.add(4L, AcknowledgeType.RELEASE);

        List<ShareFetchRequestData.AcknowledgementBatch> ackList = acks.getAcknowledgmentBatches();
        assertEquals(2, ackList.size());
        assertEquals(0L, ackList.get(0).baseOffset());
        assertEquals(2L, ackList.get(0).lastOffset());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeType());
        assertTrue(ackList.get(0).gapOffsets().isEmpty());
        assertEquals(3L, ackList.get(1).baseOffset());
        assertEquals(4L, ackList.get(1).lastOffset());
        assertEquals(AcknowledgeType.RELEASE.id, ackList.get(1).acknowledgeType());
        assertTrue(ackList.get(1).gapOffsets().isEmpty());
    }

    @Test
    public void testMultiBatchSingleMultiRecord() {
        acks.add(0L, AcknowledgeType.ACCEPT);
        acks.add(1L, AcknowledgeType.RELEASE);
        acks.add(2L, AcknowledgeType.RELEASE);
        acks.add(3L, AcknowledgeType.RELEASE);
        acks.add(4L, AcknowledgeType.RELEASE);

        List<ShareFetchRequestData.AcknowledgementBatch> ackList = acks.getAcknowledgmentBatches();
        assertEquals(2, ackList.size());
        assertEquals(0L, ackList.get(0).baseOffset());
        assertEquals(0L, ackList.get(0).lastOffset());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeType());
        assertTrue(ackList.get(0).gapOffsets().isEmpty());
        assertEquals(1L, ackList.get(1).baseOffset());
        assertEquals(4L, ackList.get(1).lastOffset());
        assertEquals(AcknowledgeType.RELEASE.id, ackList.get(1).acknowledgeType());
        assertTrue(ackList.get(1).gapOffsets().isEmpty());
    }

    @Test
    public void testMultiBatchMultiSingleRecord() {
        acks.add(0L, AcknowledgeType.ACCEPT);
        acks.add(1L, AcknowledgeType.ACCEPT);
        acks.add(2L, AcknowledgeType.ACCEPT);
        acks.add(3L, AcknowledgeType.ACCEPT);
        acks.add(4L, AcknowledgeType.RELEASE);

        List<ShareFetchRequestData.AcknowledgementBatch> ackList = acks.getAcknowledgmentBatches();
        assertEquals(2, ackList.size());
        assertEquals(0L, ackList.get(0).baseOffset());
        assertEquals(3L, ackList.get(0).lastOffset());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeType());
        assertTrue(ackList.get(0).gapOffsets().isEmpty());
        assertEquals(4L, ackList.get(1).baseOffset());
        assertEquals(4L, ackList.get(1).lastOffset());
        assertEquals(AcknowledgeType.RELEASE.id, ackList.get(1).acknowledgeType());
        assertTrue(ackList.get(1).gapOffsets().isEmpty());
    }

    @Test
    public void testSingleBatchSingleGap() {
        acks.addGap(0L);

        List<ShareFetchRequestData.AcknowledgementBatch> ackList = acks.getAcknowledgmentBatches();
        assertEquals(1, ackList.size());
        assertEquals(0L, ackList.get(0).baseOffset());
        assertEquals(0L, ackList.get(0).lastOffset());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeType());
        assertEquals(1, ackList.get(0).gapOffsets().size());
    }

    @Test
    public void testSingleBatchMultiGap() {
        acks.addGap(0L);
        acks.addGap(1L);

        List<ShareFetchRequestData.AcknowledgementBatch> ackList = acks.getAcknowledgmentBatches();
        assertEquals(1, ackList.size());
        assertEquals(0L, ackList.get(0).baseOffset());
        assertEquals(1L, ackList.get(0).lastOffset());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeType());
        assertEquals(2, ackList.get(0).gapOffsets().size());
    }

    @Test
    public void testSingleBatchSingleGapSingleRecord() {
        acks.addGap(0L);
        acks.add(1L, AcknowledgeType.ACCEPT);

        List<ShareFetchRequestData.AcknowledgementBatch> ackList = acks.getAcknowledgmentBatches();
        assertEquals(1, ackList.size());
        assertEquals(0L, ackList.get(0).baseOffset());
        assertEquals(1L, ackList.get(0).lastOffset());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeType());
        assertEquals(1, ackList.get(0).gapOffsets().size());
    }

    @Test
    public void testSingleBatchSingleRecordSingleGap() {
        acks.add(0L, AcknowledgeType.ACCEPT);
        acks.addGap(1L);

        List<ShareFetchRequestData.AcknowledgementBatch> ackList = acks.getAcknowledgmentBatches();
        assertEquals(1, ackList.size());
        assertEquals(0L, ackList.get(0).baseOffset());
        assertEquals(1L, ackList.get(0).lastOffset());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeType());
        assertEquals(1, ackList.get(0).gapOffsets().size());
    }

    @Test
    public void testMultiBatchSingleMultiGapRecords() {
        acks.add(0L, AcknowledgeType.RELEASE);
        acks.addGap(1L);
        acks.addGap(2L);
        acks.add(3L, AcknowledgeType.ACCEPT);
        acks.add(4L, AcknowledgeType.ACCEPT);

        List<ShareFetchRequestData.AcknowledgementBatch> ackList = acks.getAcknowledgmentBatches();
        assertEquals(2, ackList.size());
        assertEquals(0L, ackList.get(0).baseOffset());
        assertEquals(2L, ackList.get(0).lastOffset());
        assertEquals(AcknowledgeType.RELEASE.id, ackList.get(0).acknowledgeType());
        assertEquals(2, ackList.get(0).gapOffsets().size());
        assertEquals(3L, ackList.get(1).baseOffset());
        assertEquals(4L, ackList.get(1).lastOffset());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(1).acknowledgeType());
        assertTrue(ackList.get(1).gapOffsets().isEmpty());
    }

    @Test
    public void testMultiBatchSingleMultiRecordGaps() {
        acks.add(0L, AcknowledgeType.ACCEPT);
        acks.add(1L, AcknowledgeType.RELEASE);
        acks.addGap(2L);
        acks.add(3L, AcknowledgeType.RELEASE);
        acks.addGap(4L);

        List<ShareFetchRequestData.AcknowledgementBatch> ackList = acks.getAcknowledgmentBatches();
        assertEquals(2, ackList.size());
        assertEquals(0L, ackList.get(0).baseOffset());
        assertEquals(0L, ackList.get(0).lastOffset());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeType());
        assertTrue(ackList.get(0).gapOffsets().isEmpty());
        assertEquals(1L, ackList.get(1).baseOffset());
        assertEquals(4L, ackList.get(1).lastOffset());
        assertEquals(AcknowledgeType.RELEASE.id, ackList.get(1).acknowledgeType());
        assertEquals(2, ackList.get(1).gapOffsets().size());
    }

    @Test
    public void testNoncontiguousBatches() {
        acks.add(0L, AcknowledgeType.ACCEPT);
        acks.add(1L, AcknowledgeType.RELEASE);
        acks.add(3L, AcknowledgeType.REJECT);
        acks.add(4L, AcknowledgeType.REJECT);
        acks.add(6L, AcknowledgeType.REJECT);

        List<ShareFetchRequestData.AcknowledgementBatch> ackList = acks.getAcknowledgmentBatches();
        assertEquals(4, ackList.size());
        assertEquals(0L, ackList.get(0).baseOffset());
        assertEquals(0L, ackList.get(0).lastOffset());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeType());
        assertTrue(ackList.get(0).gapOffsets().isEmpty());
        assertEquals(1L, ackList.get(1).baseOffset());
        assertEquals(1L, ackList.get(1).lastOffset());
        assertEquals(AcknowledgeType.RELEASE.id, ackList.get(1).acknowledgeType());
        assertTrue(ackList.get(1).gapOffsets().isEmpty());
        assertEquals(3L, ackList.get(2).baseOffset());
        assertEquals(4L, ackList.get(2).lastOffset());
        assertEquals(AcknowledgeType.REJECT.id, ackList.get(2).acknowledgeType());
        assertTrue(ackList.get(2).gapOffsets().isEmpty());
        assertEquals(6L, ackList.get(3).baseOffset());
        assertEquals(6L, ackList.get(3).lastOffset());
        assertEquals(AcknowledgeType.REJECT.id, ackList.get(3).acknowledgeType());
        assertTrue(ackList.get(3).gapOffsets().isEmpty());
    }


    @Test
    public void testNoncontiguousGaps() {
        acks.addGap(2L);
        acks.addGap(4L);

        List<ShareFetchRequestData.AcknowledgementBatch> ackList = acks.getAcknowledgmentBatches();
        assertEquals(2, ackList.size());
        assertEquals(2L, ackList.get(0).baseOffset());
        assertEquals(2L, ackList.get(0).lastOffset());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(0).acknowledgeType());
        assertEquals(1, ackList.get(0).gapOffsets().size());
        assertEquals(4L, ackList.get(1).baseOffset());
        assertEquals(4L, ackList.get(1).lastOffset());
        assertEquals(AcknowledgeType.ACCEPT.id, ackList.get(1).acknowledgeType());
        assertEquals(1, ackList.get(1).gapOffsets().size());
    }
}