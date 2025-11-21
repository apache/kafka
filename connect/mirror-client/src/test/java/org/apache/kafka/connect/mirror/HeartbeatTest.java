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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HeartbeatTest {

    @Test
    public void testConstructorAndGetters() {
        Heartbeat hb = new Heartbeat("sourceA", "targetB", 123456L);
        assertEquals("sourceA", hb.sourceClusterAlias());
        assertEquals("targetB", hb.targetClusterAlias());
        assertEquals(123456L, hb.timestamp());
    }

    @Test
    public void testConnectPartition() {
        Heartbeat hb = new Heartbeat("src", "dst", 999L);
        Map<String, ?> partition = hb.connectPartition();
        assertEquals("src", partition.get(Heartbeat.SOURCE_CLUSTER_ALIAS_KEY));
        assertEquals("dst", partition.get(Heartbeat.TARGET_CLUSTER_ALIAS_KEY));
    }

    @Test
    public void testToString() {
        Heartbeat hb = new Heartbeat("x", "y", 42);
        String str = hb.toString();
        assertTrue(str.contains("x"));
        assertTrue(str.contains("y"));
        assertTrue(str.contains("42"));
    }

    @Test
    public void testSerializationRoundtrip() {
        Heartbeat hb = new Heartbeat("src1", "dst2", 7891011L);
        byte[] key = hb.recordKey();
        byte[] value = hb.recordValue();
        ConsumerRecord<byte[], byte[]> rec = new ConsumerRecord<>("heartbeat", 0, 0L, key, value);

        Heartbeat restored = Heartbeat.deserializeRecord(rec);
        assertEquals(hb.sourceClusterAlias(), restored.sourceClusterAlias());
        assertEquals(hb.targetClusterAlias(), restored.targetClusterAlias());
        assertEquals(hb.timestamp(), restored.timestamp());
    }
}
