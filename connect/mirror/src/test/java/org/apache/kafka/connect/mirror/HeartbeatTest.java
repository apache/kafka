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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HeartbeatTest {

    @Test
    public void testSerde() {
        Heartbeat heartbeat = new Heartbeat("source-1", "target-2", 1234567890L);
        byte[] key = heartbeat.recordKey();
        byte[] value = heartbeat.recordValue();
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("any-topic", 6, 7, key, value);
        Heartbeat deserialized = Heartbeat.deserializeRecord(record);
        assertEquals(heartbeat.sourceClusterAlias(), deserialized.sourceClusterAlias());
        assertEquals(heartbeat.targetClusterAlias(), deserialized.targetClusterAlias());
        assertEquals(heartbeat.timestamp(), deserialized.timestamp());
    }
}
