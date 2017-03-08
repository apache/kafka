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
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * The interface for the {@link KafkaProducer}.
 *s
 * @see KafkaProducer
 * @see MockProducer
 */
public interface Producer<K, V> extends Closeable {

    /**
     * Aborts the ongoing transaction.
     *
     * @throws ProducerFencedException if another producer is with the same
     * {@link ProducerConfig#TRANSACTIONAL_ID_CONFIG transactional.id} is active.
     */
    void abortTransaction() throws ProducerFencedException;

    /**
     * Should be called before the start of each new transaction.
     *
     * @throws ProducerFencedException if another producer is with the same
     * {@link ProducerConfig#TRANSACTIONAL_ID_CONFIG transactional.id} is active.
     */
    void beginTransaction() throws ProducerFencedException;

    /**
     * Close this producer.
     */
    @Override
    void close();

    /**
     * Tries to close the producer cleanly within the specified timeout. If the close does not complete within the
     * timeout, fail any pending send requests and force close the producer.
     */
    void close(final long timeout, final TimeUnit unit);

    /**
     * Commits the ongoing transaction.
     *
     * @throws ProducerFencedException if another producer is with the same
     * {@link ProducerConfig#TRANSACTIONAL_ID_CONFIG transactional.id} is active.
     */
    void commitTransaction() throws ProducerFencedException;

    /**
     * Flush any accumulated records from the producer. Blocks until all sends are complete.
     */
    void flush();

    /**
     * Needs to be called before any of the other transaction methods are used.
     * Assumes that the {@link ProducerConfig#TRANSACTIONAL_ID_DOC transactional.id} is specified in the
     * {@link ProducerConfig producer configuration}.
     * <p>
     * This method does the following:
     * <ol>
     *   <li>Ensures any transactions initiated by previous instances of the producer are completed.
     *       If the previous instance had failed with a transaction in progress, it will be aborted.
     *       If the last transaction had begun completion, but not yet finished, this method awaits its completion.</li>
     *   <li>Gets the internal producer id and epoch, used in all future transactional messages issued by the producer.</li>
     * </ol>
     *
     * @throws IllegalStateException if the TransactionalId for the producer is not set in the configuration
     */
    void initTransactions() throws IllegalStateException;

    /**
     * Return a map of metrics maintained by the producer.
     */
    Map<MetricName, ? extends Metric> metrics();

    /**
     * Get a list of partitions for the given topic for custom partition assignment. The partition metadata will change
     * over time so this list should not be cached.
     */
    List<PartitionInfo> partitionsFor(final String topic);

    /**
     * Send the given record asynchronously and return a future which will eventually contain the response information.
     *
     * @param record The record to send
     * @return A future which will eventually contain the response information.
     */
    Future<RecordMetadata> send(final ProducerRecord<K, V> record);

    /**
     * Send a record and invoke the given callback when the record has been acknowledged by the server.
     */
    Future<RecordMetadata> send(final ProducerRecord<K, V> record, final Callback callback);

    /**
     * Sends a list of consumed offsets and marks those offsets as part of the current transaction.
     * These offsets will be considered consumed only if the transaction is committed successfully.
     * <p>
     * This method should be used when you need to batch consumed and produced messages together, typically in a
     * consume-transform-produce pattern.
     * If this method is used, it's not required to {@link Consumer#commitSync() commit offsets with the consumer}.
     *
     * @throws ProducerFencedException if another producer is with the same
     * {@link ProducerConfig#TRANSACTIONAL_ID_CONFIG transactional.id} is active.
     */
    void sendOffsetsToTransaction(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                  final String consumerGroupId) throws ProducerFencedException;

}
