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
package org.apache.kafka.clients.producer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class ProducerRecordTest {

    @Test
    public void testEqualsAndHashCode() {
        ProducerRecord<String, Integer> producerRecord = new ProducerRecord<>("test", 1 , "key", 1);
        assertEquals(producerRecord, producerRecord);
        assertEquals(producerRecord.hashCode(), producerRecord.hashCode());

        ProducerRecord<String, Integer> equalRecord = new ProducerRecord<>("test", 1 , "key", 1);
        assertEquals(producerRecord, equalRecord);
        assertEquals(producerRecord.hashCode(), equalRecord.hashCode());

        ProducerRecord<String, Integer> topicMisMatch = new ProducerRecord<>("test-1", 1 , "key", 1);
        assertFalse(producerRecord.equals(topicMisMatch));

        ProducerRecord<String, Integer> partitionMismatch = new ProducerRecord<>("test", 2 , "key", 1);
        assertFalse(producerRecord.equals(partitionMismatch));

        ProducerRecord<String, Integer> keyMisMatch = new ProducerRecord<>("test", 1 , "key-1", 1);
        assertFalse(producerRecord.equals(keyMisMatch));

        ProducerRecord<String, Integer> valueMisMatch = new ProducerRecord<>("test", 1 , "key", 2);
        assertFalse(producerRecord.equals(valueMisMatch));

        ProducerRecord<String, Integer> nullFieldsRecord = new ProducerRecord<>("topic", null, null, null, null);
        assertEquals(nullFieldsRecord, nullFieldsRecord);
        assertEquals(nullFieldsRecord.hashCode(), nullFieldsRecord.hashCode());
    }

    @Test
    public void testInvalidRecords() {
        try {
            new ProducerRecord<>(null, 0, "key", 1);
            fail("Expected IllegalArgumentException to be raised because topic is null");
        } catch (IllegalArgumentException e) {
            //expected
        }

        try {
            new ProducerRecord<>("test", 0, -1L, "key", 1);
            fail("Expected IllegalArgumentException to be raised because of negative timestamp");
        } catch (IllegalArgumentException e) {
            //expected
        }

        try {
            new ProducerRecord<>("test", -1, "key", 1);
            fail("Expected IllegalArgumentException to be raised because of negative partition");
        } catch (IllegalArgumentException e) {
            //expected
        }
    }

}
