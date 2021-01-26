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
import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OffsetSyncStoreTest {

    static TopicPartition tp = new TopicPartition("topic1", 2);

    static class FakeOffsetSyncStore extends OffsetSyncStore {

        FakeOffsetSyncStore() {
            super(null, null);
        }

        void sync(TopicPartition topicPartition, long upstreamOffset, long downstreamOffset) {
            OffsetSync offsetSync = new OffsetSync(topicPartition, upstreamOffset, downstreamOffset);
            byte[] key = offsetSync.recordKey();
            byte[] value = offsetSync.recordValue();
            ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test.offsets.internal", 0, 3, key, value);
            handleRecord(record);
        }
    }

    @Test
    public void testOffsetTranslation() {
        FakeOffsetSyncStore store = new FakeOffsetSyncStore();

        store.sync(tp, 100, 200);
        assertEquals(store.translateDownstream(tp, 150), 250);

        // Translate exact offsets
        store.sync(tp, 150, 251);
        assertEquals(store.translateDownstream(tp, 150), 251);

        // Use old offset (5) prior to any sync -> can't translate
        assertEquals(-1, store.translateDownstream(tp, 5));

        // Downstream offsets reset
        store.sync(tp, 200, 10);
        assertEquals(store.translateDownstream(tp, 200), 10);

        // Upstream offsets reset
        store.sync(tp, 20, 20);
        assertEquals(store.translateDownstream(tp, 20), 20);
    }
}
