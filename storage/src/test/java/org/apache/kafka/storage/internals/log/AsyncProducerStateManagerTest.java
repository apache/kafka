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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.server.log.remote.metadata.storage.generated.ProducerSnapshot;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class AsyncProducerStateManagerTest {

    @Test
    public void testProducerEntrySnapshotCodec() throws Exception {
        TopicPartition tp = new TopicPartition("test", 0);
        long lastMapOffset0 = 100;

        ProducerSnapshot.ProducerEntry entry0 = new ProducerSnapshot.ProducerEntry()
                .setProducerId(1)
                .setEpoch((short) 0)
                .setLastSequence(0)
                .setTimestamp(100)
                .setLastOffset(10)
                .setOffsetDelta(0)
                .setCurrentTxnFirstOffset(-1);
        ProducerSnapshot.ProducerEntry entry1 = new ProducerSnapshot.ProducerEntry()
                .setProducerId(2)
                .setEpoch((short) 0)
                .setLastSequence(1)
                .setTimestamp(200)
                .setLastOffset(20)
                .setOffsetDelta(10)
                .setCurrentTxnFirstOffset(1);

        AsyncProducerStateManager.ProducerStateSnapshot snapshot = new AsyncProducerStateManager.ProducerStateSnapshot(tp, lastMapOffset0, List.of(entry0, entry1));
        byte[] bytes = snapshot.toByteArray();

        AsyncProducerStateManager.ProducerStateSnapshot snapshot1 = AsyncProducerStateManager.ProducerStateSnapshot.fromByteArray(bytes);

        Assertions.assertEquals(snapshot1.topicPartition(), tp);
        Assertions.assertEquals(lastMapOffset0, snapshot1.lastMapOffset());

        ProducerSnapshot.ProducerEntry decodedEntry0 = snapshot1.producerEntries().stream().filter(entry -> entry.producerId() == 1).findFirst().get();
        Assertions.assertEquals(entry0, decodedEntry0);
        ProducerSnapshot.ProducerEntry decodedEntry1 = snapshot1.producerEntries().stream().filter(entry -> entry.producerId() == 2).findFirst().get();
        Assertions.assertEquals(entry1, decodedEntry1);
    }
}
