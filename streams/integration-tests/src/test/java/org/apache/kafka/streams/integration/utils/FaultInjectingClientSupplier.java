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
package org.apache.kafka.streams.integration.utils;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.PreparedTxnState;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.streams.KafkaClientSupplier;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * A {@link KafkaClientSupplier} decorator that injects <em>client-side exceptions</em> into the transactional
 * producer that Kafka Streams uses, without any broker round-trip. It is the leaner, Streams-specific
 * complement to {@link KafkaProtocolFaultProxy}: the proxy rewrites broker responses on the wire (useful for
 * any client and any language), while this makes the producer throw a Java exception directly — modelling
 * client-library failures such as a fenced producer or a {@code commitTransaction} timeout that never surface
 * as a broker error code.
 *
 * <p>Consumers and admin clients are passed through untouched; only the producer is decorated, because the
 * KIP-892 commit path (send-offsets → commit/abort) lives entirely on the producer.
 *
 * <p>Usage mirrors the proxy DSL:
 * <pre>{@code
 * FaultInjectingClientSupplier supplier = FaultInjectingClientSupplier.wrapping(new DefaultKafkaClientSupplier());
 * ClientFault fault = supplier.failOn(ProducerCall.COMMIT_TRANSACTION,
 *                                     () -> new TimeoutException("injected")).onCall(2);
 * props.put(StreamsConfig..., supplier); // pass to KafkaStreams(topology, props, supplier)
 * // ... run, then:
 * assertEquals(1, fault.timesTriggered());
 * }</pre>
 */
public final class FaultInjectingClientSupplier implements KafkaClientSupplier {

    /** The transactional producer entry points that can be made to throw. */
    public enum ProducerCall {
        INIT_TRANSACTIONS,
        BEGIN_TRANSACTION,
        SEND_OFFSETS_TO_TRANSACTION,
        COMMIT_TRANSACTION,
        ABORT_TRANSACTION,
        SEND,
        FLUSH
    }

    private final KafkaClientSupplier delegate;
    private final CopyOnWriteArrayList<ClientFault> faults = new CopyOnWriteArrayList<>();

    private FaultInjectingClientSupplier(final KafkaClientSupplier delegate) {
        this.delegate = delegate;
    }

    /** Wrap an existing supplier (e.g. {@code new DefaultKafkaClientSupplier()}). */
    public static FaultInjectingClientSupplier wrapping(final KafkaClientSupplier delegate) {
        return new FaultInjectingClientSupplier(delegate);
    }

    /** Begin defining a fault on {@code call}; the returned builder's terminal registers it and returns a handle. */
    public ClientFault.Builder failOn(final ProducerCall call, final Supplier<? extends RuntimeException> exception) {
        return new ClientFault.Builder(this, call, exception);
    }

    /** Remove all registered faults. */
    public void clearFaults() {
        faults.clear();
    }

    void addFault(final ClientFault fault) {
        faults.add(fault);
    }

    void removeFault(final ClientFault fault) {
        faults.remove(fault);
    }

    /** Consult the registered faults for {@code call}; returns the exception to throw, or {@code null}. */
    private RuntimeException maybeFail(final ProducerCall call) {
        for (final ClientFault fault : faults) {
            if (fault.call() == call) {
                final RuntimeException e = fault.maybeFail();
                if (e != null) {
                    return e;
                }
            }
        }
        return null;
    }

    @Override
    public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
        return new FaultInjectingProducer(delegate.getProducer(config));
    }

    @Override
    public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
        return delegate.getConsumer(config);
    }

    @Override
    public Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> config) {
        return delegate.getRestoreConsumer(config);
    }

    @Override
    public Consumer<byte[], byte[]> getGlobalConsumer(final Map<String, Object> config) {
        return delegate.getGlobalConsumer(config);
    }

    @Override
    public Admin getAdmin(final Map<String, Object> config) {
        return delegate.getAdmin(config);
    }

    /**
     * Delegating producer that checks for a registered fault before each intercepted transactional call and,
     * if one fires, throws instead of delegating. All other methods pass straight through.
     */
    private final class FaultInjectingProducer implements Producer<byte[], byte[]> {
        private final Producer<byte[], byte[]> delegate;

        FaultInjectingProducer(final Producer<byte[], byte[]> delegate) {
            this.delegate = delegate;
        }

        private void checkFault(final ProducerCall call) {
            final RuntimeException e = maybeFail(call);
            if (e != null) {
                throw e;
            }
        }

        @Override
        public void initTransactions() {
            checkFault(ProducerCall.INIT_TRANSACTIONS);
            delegate.initTransactions();
        }

        @Override
        public void initTransactions(final boolean keepPreparedTxn) {
            checkFault(ProducerCall.INIT_TRANSACTIONS);
            delegate.initTransactions(keepPreparedTxn);
        }

        @Override
        public void beginTransaction() throws ProducerFencedException {
            checkFault(ProducerCall.BEGIN_TRANSACTION);
            delegate.beginTransaction();
        }

        @Override
        public void sendOffsetsToTransaction(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                             final ConsumerGroupMetadata groupMetadata) throws ProducerFencedException {
            checkFault(ProducerCall.SEND_OFFSETS_TO_TRANSACTION);
            delegate.sendOffsetsToTransaction(offsets, groupMetadata);
        }

        @Override
        public void commitTransaction() throws ProducerFencedException {
            checkFault(ProducerCall.COMMIT_TRANSACTION);
            delegate.commitTransaction();
        }

        @Override
        public void abortTransaction() throws ProducerFencedException {
            checkFault(ProducerCall.ABORT_TRANSACTION);
            delegate.abortTransaction();
        }

        @Override
        public PreparedTxnState prepareTransaction() throws ProducerFencedException {
            return delegate.prepareTransaction();
        }

        @Override
        public void completeTransaction(final PreparedTxnState preparedTxnState) throws ProducerFencedException {
            delegate.completeTransaction(preparedTxnState);
        }

        @Override
        public Future<RecordMetadata> send(final ProducerRecord<byte[], byte[]> record) {
            checkFault(ProducerCall.SEND);
            return delegate.send(record);
        }

        @Override
        public Future<RecordMetadata> send(final ProducerRecord<byte[], byte[]> record, final Callback callback) {
            checkFault(ProducerCall.SEND);
            return delegate.send(record, callback);
        }

        @Override
        public void flush() {
            checkFault(ProducerCall.FLUSH);
            delegate.flush();
        }

        // ---- pass-through (no fault injection) ----

        @Override
        public List<PartitionInfo> partitionsFor(final String topic) {
            return delegate.partitionsFor(topic);
        }

        @Override
        public Map<MetricName, ? extends Metric> metrics() {
            return delegate.metrics();
        }

        @Override
        public Uuid clientInstanceId(final Duration timeout) {
            return delegate.clientInstanceId(timeout);
        }

        @Override
        public void registerMetricForSubscription(final KafkaMetric metric) {
            delegate.registerMetricForSubscription(metric);
        }

        @Override
        public void unregisterMetricFromSubscription(final KafkaMetric metric) {
            delegate.unregisterMetricFromSubscription(metric);
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public void close(final Duration timeout) {
            delegate.close(timeout);
        }
    }
}
