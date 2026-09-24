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
package org.apache.kafka.tools;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LogCompactionTesterTest {

    private static final String TOPIC = "log-cleaner-test";
    private static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    @Test
    public void testConsumeMessagesDoesNotStopAtEmptyPollBeforeEndOffset() throws Exception {
        MockConsumer<String, String> consumer = createConsumer(15L);
        consumer.schedulePollTask(() -> {
            consumer.rebalance(List.of(TP));
            addRecords(consumer, 0, 5);
        });
        // Offsets 5 to 9 were removed by compaction. The position moves past them, but poll() returns no records.
        consumer.schedulePollTask(() -> consumer.seek(TP, 10));
        consumer.schedulePollTask(() -> addRecords(consumer, 10, 15));

        Path consumedFile = LogCompactionTester.consumeMessages(consumer, Set.of(TOPIC));
        try {
            assertEquals(10, Files.readAllLines(consumedFile).size());
            assertEquals(15L, consumer.position(TP));
        } finally {
            Files.deleteIfExists(consumedFile);
        }
    }

    @Test
    public void testConsumeMessagesFailsWhenNoProgress() {
        MockConsumer<String, String> consumer = createConsumer(20L);
        consumer.schedulePollTask(() -> {
            consumer.rebalance(List.of(TP));
            addRecords(consumer, 0, 15);
        });
        // The next poll returns no records and moves no position: offsets 15..19 never arrive.

        RuntimeException e = assertThrows(RuntimeException.class,
            () -> LogCompactionTester.consumeMessages(consumer, Set.of(TOPIC)));
        assertTrue(e.getMessage().startsWith("No progress"), e.getMessage());
    }

    private static MockConsumer<String, String> createConsumer(long endOffset) {
        MockConsumer<String, String> consumer = new MockConsumer<>("earliest");
        consumer.updateBeginningOffsets(Map.of(TP, 0L));
        consumer.updateEndOffsets(Map.of(TP, endOffset));
        return consumer;
    }

    private static void addRecords(MockConsumer<String, String> consumer, long from, long to) {
        LongStream.range(from, to).forEach(offset ->
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset, String.valueOf(offset % 3), String.valueOf(offset))));
    }
}
