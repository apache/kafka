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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ShareAcknowledgementsTest {

    @Test
    public void testShareAcknowledgementBatchValidation() {
        assertThrows(IllegalArgumentException.class,
            () -> new ShareAcknowledgementBatch(2L, 1L, List.of(AcknowledgeType.ACCEPT.id)));
        assertThrows(IllegalArgumentException.class,
            () -> new ShareAcknowledgementBatch(1L, 2L, List.of()));
        assertThrows(IllegalArgumentException.class,
            () -> new ShareAcknowledgementBatch(1L, 3L, List.of(AcknowledgeType.ACCEPT.id, AcknowledgeType.REJECT.id)));
    }

    @Test
    public void testShareAcknowledgementsMakesDefensiveCopies() {
        TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic", 0));
        List<ShareAcknowledgementBatch> batches = new ArrayList<>();
        batches.add(new ShareAcknowledgementBatch(1L, 2L, List.of(AcknowledgeType.ACCEPT.id)));
        Map<TopicIdPartition, List<ShareAcknowledgementBatch>> acknowledgements = new LinkedHashMap<>();
        acknowledgements.put(topicIdPartition, batches);

        ShareAcknowledgements shareAcknowledgements = new ShareAcknowledgements(acknowledgements);
        batches.add(new ShareAcknowledgementBatch(3L, 3L, List.of(AcknowledgeType.REJECT.id)));
        acknowledgements.clear();

        assertEquals(
            List.of(new ShareAcknowledgementBatch(1L, 2L, List.of(AcknowledgeType.ACCEPT.id))),
            shareAcknowledgements.acknowledgements().get(topicIdPartition));
        assertThrows(UnsupportedOperationException.class,
            () -> shareAcknowledgements.acknowledgements().put(topicIdPartition, List.of()));
    }
}
