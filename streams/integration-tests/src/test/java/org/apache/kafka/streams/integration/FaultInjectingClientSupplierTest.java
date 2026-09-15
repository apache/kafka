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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.integration.utils.ClientFault;
import org.apache.kafka.streams.integration.utils.FaultInjectingClientSupplier;
import org.apache.kafka.streams.integration.utils.FaultInjectingClientSupplier.ProducerCall;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Fast, deterministic mechanics test for {@link FaultInjectingClientSupplier}: proves the DSL triggers
 * ({@code once}/{@code onCall}/{@code times}), that only the targeted call throws, that other calls delegate,
 * that consumers/admin pass through untouched, and that {@link ClientFault#remove()} disarms. No broker.
 */
public class FaultInjectingClientSupplierTest {

    private FaultInjectingClientSupplier newSupplier(final Producer<byte[], byte[]> producer) {
        final KafkaClientSupplier base = new KafkaClientSupplier() {
            @Override
            public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
                return producer;
            }

            @Override
            public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
                return new MockConsumer<>("earliest");
            }

            @Override
            public Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> config) {
                return new MockConsumer<>("earliest");
            }

            @Override
            public Consumer<byte[], byte[]> getGlobalConsumer(final Map<String, Object> config) {
                return new MockConsumer<>("earliest");
            }
        };
        return FaultInjectingClientSupplier.wrapping(base);
    }

    @Test
    public void onceShouldThrowOnFirstCallThenDelegate() {
        final MockProducer<byte[], byte[]> mock = new MockProducer<>();
        final FaultInjectingClientSupplier supplier = newSupplier(mock);
        final ClientFault fault = supplier.failOn(ProducerCall.COMMIT_TRANSACTION,
            () -> new TimeoutException("injected")).once();

        final Producer<byte[], byte[]> producer = supplier.getProducer(Map.of());
        producer.initTransactions();
        producer.beginTransaction();

        // First commit throws the injected exception...
        assertThrows(TimeoutException.class, producer::commitTransaction);
        // ...and the real producer never saw it (transaction still open).
        assertEquals(0, mock.commitCount());

        // Retrying the commit passes through to the delegate (fault was one-shot).
        assertDoesNotThrow(producer::commitTransaction);
        assertEquals(1, mock.commitCount());

        assertEquals(1, fault.timesTriggered());
        assertEquals(2, fault.timesMatched());
    }

    @Test
    public void onCallShouldThrowOnlyOnTheNthInvocation() {
        final MockProducer<byte[], byte[]> mock = new MockProducer<>();
        final FaultInjectingClientSupplier supplier = newSupplier(mock);
        supplier.failOn(ProducerCall.COMMIT_TRANSACTION, () -> new TimeoutException("boom")).onCall(2);

        final Producer<byte[], byte[]> producer = supplier.getProducer(Map.of());
        producer.initTransactions();

        producer.beginTransaction();
        assertDoesNotThrow(producer::commitTransaction);       // invocation #1 ok (closes txn)
        producer.beginTransaction();
        assertThrows(TimeoutException.class, producer::commitTransaction); // #2 throws (txn stays open)
        assertDoesNotThrow(producer::commitTransaction);       // #3 retry ok (closes txn)

        assertEquals(2, mock.commitCount());
    }

    @Test
    public void faultShouldTargetOnlyItsOwnCall() {
        final MockProducer<byte[], byte[]> mock = new MockProducer<>();
        final FaultInjectingClientSupplier supplier = newSupplier(mock);
        supplier.failOn(ProducerCall.COMMIT_TRANSACTION, () -> new TimeoutException("only-commit")).everyTime();

        final Producer<byte[], byte[]> producer = supplier.getProducer(Map.of());
        // initTransactions / beginTransaction / abort are unaffected.
        assertDoesNotThrow(() -> producer.initTransactions());
        assertDoesNotThrow(producer::beginTransaction);
        assertThrows(TimeoutException.class, producer::commitTransaction);
        assertDoesNotThrow(producer::abortTransaction);
    }

    @Test
    public void removeShouldDisarmTheFault() {
        final MockProducer<byte[], byte[]> mock = new MockProducer<>();
        final FaultInjectingClientSupplier supplier = newSupplier(mock);
        final ClientFault fault = supplier.failOn(ProducerCall.COMMIT_TRANSACTION,
            () -> new TimeoutException("boom")).everyTime();

        final Producer<byte[], byte[]> producer = supplier.getProducer(Map.of());
        producer.initTransactions();
        producer.beginTransaction();
        assertThrows(TimeoutException.class, producer::commitTransaction);

        fault.remove();
        // Retry on the still-open transaction now succeeds.
        assertDoesNotThrow(producer::commitTransaction);
        assertEquals(1, mock.commitCount());
    }

    @Test
    public void consumersShouldPassThroughUntouched() {
        final FaultInjectingClientSupplier supplier = newSupplier(new MockProducer<>());
        // No exception configured for any consumer; they must be the delegate's instances.
        assertSame(MockConsumer.class, supplier.getConsumer(Map.of()).getClass());
        assertSame(MockConsumer.class, supplier.getRestoreConsumer(Map.of()).getClass());
        assertSame(MockConsumer.class, supplier.getGlobalConsumer(Map.of()).getClass());
    }
}
