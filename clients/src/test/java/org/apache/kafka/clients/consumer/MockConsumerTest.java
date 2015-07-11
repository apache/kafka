/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.junit.Assert.*;

import java.util.Iterator;

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

public class MockConsumerTest {
    
    private MockConsumer<String, String> consumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);

    @Test
    public void testSimpleMock() {
        consumer.subscribe("topic");
        assertEquals(0, consumer.poll(1000).count());
        ConsumerRecord<String, String> rec1 = new ConsumerRecord<String, String>("test", 0, 0, "key1", "value1");
        ConsumerRecord<String, String> rec2 = new ConsumerRecord<String, String>("test", 0, 1, "key2", "value2");
        consumer.addRecord(rec1);
        consumer.addRecord(rec2);
        ConsumerRecords<String, String> recs = consumer.poll(1);
        Iterator<ConsumerRecord<String, String>> iter = recs.iterator();
        assertEquals(rec1, iter.next());
        assertEquals(rec2, iter.next());
        assertFalse(iter.hasNext());
        assertEquals(1L, consumer.position(new TopicPartition("test", 0)));
        consumer.commit(CommitType.SYNC);
        assertEquals(1L, consumer.committed(new TopicPartition("test", 0)));
    }

}
