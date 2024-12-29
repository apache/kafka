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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class MockShareConsumerTest {
    private final MockShareConsumer<String, String> consumer = new MockShareConsumer<>();

    @Test
    public void testSimpleMock() {
        consumer.subscribe(Collections.singleton("test"));
        assertEquals(0, consumer.poll(Duration.ZERO).count());
        ConsumerRecord<String, String> rec1 = new ConsumerRecord<>("test", 0, 0, 0L, TimestampType.CREATE_TIME,
                0, 0, "key1", "value1", new RecordHeaders(), Optional.empty());
        ConsumerRecord<String, String> rec2 = new ConsumerRecord<>("test", 0, 1, 0L, TimestampType.CREATE_TIME,
                0, 0, "key2", "value2", new RecordHeaders(), Optional.empty());
        consumer.addRecord(rec1);
        consumer.addRecord(rec2);
        ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(1));
        Iterator<ConsumerRecord<String, String>> iter = recs.iterator();
        assertEquals(rec1, iter.next());
        assertEquals(rec2, iter.next());
        assertFalse(iter.hasNext());
        assertEquals(0, recs.nextOffsets().size());
    }
}