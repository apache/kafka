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
package org.apache.kafka.clients.producer.internals;


import org.apache.kafka.clients.producer.TransactionState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TransactionStateTest {

    private TopicPartition topicPartition;

    @Before
    public void setUp() {
        topicPartition = new TopicPartition("topic-0", 0);
    }

    @Test(expected = IllegalStateException.class)
    public void testInvalidSequenceIncrement() {
        TransactionState transactionState = new TransactionState(new MockTime());
        transactionState.incrementSequenceNumber(topicPartition, 3333);
    }

    @Test
    public void testDefaultSequenceNumber() {
        TransactionState transactionState = new TransactionState(new MockTime());
        assertEquals((int) transactionState.sequenceNumber(topicPartition), 0);
        transactionState.incrementSequenceNumber(topicPartition, 3);
        assertEquals((int) transactionState.sequenceNumber(topicPartition), 3);
    }


    @Test
    public void testProducerIdReset() {
        TransactionState transactionState = new TransactionState(new MockTime());
        assertEquals((int) transactionState.sequenceNumber(topicPartition), 0);
        transactionState.incrementSequenceNumber(topicPartition, 3);
        assertEquals((int) transactionState.sequenceNumber(topicPartition), 3);
        transactionState.resetProducerId();
        assertEquals((int) transactionState.sequenceNumber(topicPartition), 0);
    }
}
