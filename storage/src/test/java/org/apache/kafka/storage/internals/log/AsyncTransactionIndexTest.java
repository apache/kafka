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

import com.google.common.collect.ImmutableList;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AsyncTransactionIndexTest {

    @Test
    public void testTransactionIndexSnapshotCodec() {

        AsyncTransactionIndex.TransactionIndexSnapshot snapshot = new AsyncTransactionIndex.TransactionIndexSnapshot(
                new TopicPartition("test", 0), 100, 1000,
                ImmutableList.of(
                        new AbortedTxn(1, 10, 20, 30),
                        new AbortedTxn(2, 30, 40, 50)));

        byte[] bytes = snapshot.toByteArray();

        AsyncTransactionIndex.TransactionIndexSnapshot decoded = AsyncTransactionIndex.TransactionIndexSnapshot.fromByteArray(bytes);

        TopicPartition decodedTopicPartition = decoded.topicPartition();
        Assertions.assertEquals(new TopicPartition("test", 0), decodedTopicPartition);
        Assertions.assertEquals(100, decoded.lastOffset());
        Assertions.assertEquals(1000, decoded.mapEndOffset());
        Assertions.assertEquals(2, decoded.abortedTxns().size());

        AbortedTxn abortedTxn0 = decoded.abortedTxns().stream().filter(txn -> txn.producerId() == 1).findFirst().get();
        Assertions.assertEquals(1, abortedTxn0.producerId());
        Assertions.assertEquals(10, abortedTxn0.firstOffset());
        Assertions.assertEquals(20, abortedTxn0.lastOffset());
        Assertions.assertEquals(30, abortedTxn0.lastStableOffset());

        AbortedTxn abortedTxn1 = decoded.abortedTxns().stream().filter(txn -> txn.producerId() == 2).findFirst().get();
        Assertions.assertEquals(2, abortedTxn1.producerId());
        Assertions.assertEquals(30, abortedTxn1.firstOffset());
        Assertions.assertEquals(40, abortedTxn1.lastOffset());
        Assertions.assertEquals(50, abortedTxn1.lastStableOffset());
    }
}
