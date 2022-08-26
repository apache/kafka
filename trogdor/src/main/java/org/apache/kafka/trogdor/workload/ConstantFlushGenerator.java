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

package org.apache.kafka.trogdor.workload;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.errors.InterruptException;

/**
 * This generator will flush the producer after a specific number of messages.  This is useful to simulate a specific
 * number of messages in a batch regardless of the message size, since batch flushing is not exposed in the
 * KafkaProducer client code.
 *
 * WARNING: This does not directly control when KafkaProducer will batch, this only makes best effort.  This also
 * cannot tell when a KafkaProducer batch is closed.  If the KafkaProducer sends a batch before this executes, this
 * will continue to execute on its own cadence.  To alleviate this, make sure to set `linger.ms` to allow for at least
 * `messagesPerFlush` messages to be generated, and make sure to set `batch.size` to allow for all these messages.
 *
 * Here is an example spec:
 *
 * {
 *    "type": "constant",
 *    "messagesPerFlush": 16
 * }
 *
 * This example will flush the producer every 16 messages.
 */

public class ConstantFlushGenerator implements FlushGenerator {
    private final int messagesPerFlush;
    private int messageTracker = 0;

    @JsonCreator
    public ConstantFlushGenerator(@JsonProperty("messagesPerFlush") int messagesPerFlush) {
        this.messagesPerFlush = messagesPerFlush;
    }

    @JsonProperty
    public int messagesPerFlush() {
        return messagesPerFlush;
    }

    @Override
    public synchronized <K, V> void increment(KafkaProducer<K, V> producer) {
        // Increment the message tracker.
        messageTracker += 1;

        // Flush when we reach the desired number of messages.
        if (messageTracker >= messagesPerFlush) {
            messageTracker = 0;
            try {
                producer.flush();
            } catch (InterruptException e) {
                // Ignore flush interruption exceptions.
            }
        }
    }
}
