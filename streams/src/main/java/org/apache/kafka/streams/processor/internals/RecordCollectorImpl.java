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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.OffsetMetadataTooLarge;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.errors.ProductionExceptionHandler.ProductionExceptionHandlerResponse;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecordCollectorImpl implements RecordCollector {
    private final static String SEND_EXCEPTION_MESSAGE = "Error encountered sending record to topic %s for task %s due to:%n%s";

    private final Logger log;
    private final TaskId taskId;
    private final boolean eosEnabled;
    private final String applicationId;
    private final Sensor droppedRecordsSensor;
    private final Map<TopicPartition, Long> offsets;
    private final Consumer<byte[], byte[]> consumer;
    private final ProductionExceptionHandler productionExceptionHandler;

    // used when eosEnabled is true only
    private boolean transactionInFlight = false;
    private boolean transactionInitialized = false;
    private Producer<byte[], byte[]> producer;
    private volatile KafkaException sendException;

    /**
     * @throws StreamsException fatal error that should cause the thread to die (from producer.initTxn)
     */
    public RecordCollectorImpl(final TaskId taskId,
                               final StreamsConfig config,
                               final LogContext logContext,
                               final StreamsMetricsImpl streamsMetrics,
                               final Consumer<byte[], byte[]> consumer,
                               final StreamThread.ProducerSupplier producerSupplier) {
        this.taskId = taskId;
        this.consumer = consumer;
        this.offsets = new HashMap<>();
        this.log = logContext.logger(getClass());

        this.applicationId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);
        this.productionExceptionHandler = config.defaultProductionExceptionHandler();
        this.eosEnabled = StreamsConfig.EXACTLY_ONCE.equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG));

        final String threadId = Thread.currentThread().getName();
        this.droppedRecordsSensor = TaskMetrics.droppedRecordsSensorOrSkippedRecordsSensor(threadId, taskId.toString(), streamsMetrics);

        producer = producerSupplier.get(taskId);
    }

    @Override
    public void initialize() {
        maybeInitTxns();
    }

    private void maybeInitTxns() {
        if (eosEnabled && !transactionInitialized) {
            // initialize transactions if eos is turned on, which will block if the previous transaction has not
            // completed yet; do not start the first transaction until the topology has been initialized later
            try {
                producer.initTransactions();

                transactionInitialized = true;
            } catch (final TimeoutException exception) {
                log.warn("Timeout exception caught when initializing transactions for task {}. " +
                    "\nThe broker is either slow or in bad state (like not having enough replicas) in responding to the request, " +
                    "or the connection to broker was interrupted sending the request or receiving the response. " +
                    "Would retry initializing the task in the next loop." +
                    "\nConsider overwriting producer config {} to a larger value to avoid timeout errors",
                    ProducerConfig.MAX_BLOCK_MS_CONFIG, taskId);

                throw exception;
            } catch (final KafkaException exception) {
                throw new StreamsException("Error encountered while initializing transactions for task " + taskId, exception);
            }
        }
    }

    private void maybeBeginTxn() {
        if (eosEnabled && !transactionInFlight) {
            try {
                producer.beginTransaction();
            } catch (final ProducerFencedException error) {
                throw new TaskMigratedException(taskId, "Producer get fenced trying to begin a new transaction", error);
            } catch (final KafkaException error) {
                throw new StreamsException("Producer encounter unexpected error trying to begin a new transaction", error);
            }
            transactionInFlight = true;
        }
    }

    private void maybeAbortTxn() {
        if (eosEnabled && transactionInFlight) {
            try {
                producer.abortTransaction();
            } catch (final ProducerFencedException ignore) {
                /* TODO
                 * this should actually never happen atm as we guard the call to #abortTransaction
                 * -> the reason for the guard is a "bug" in the Producer -- it throws IllegalStateException
                 * instead of ProducerFencedException atm. We can remove the isZombie flag after KAFKA-5604 got
                 * fixed and fall-back to this catch-and-swallow code
                 */

                // can be ignored: transaction got already aborted by brokers/transactional-coordinator if this happens
            } catch (final KafkaException error) {
                throw new StreamsException("Producer encounter unexpected error trying to abort the transaction", error);
            }
            transactionInFlight = false;
        }
    }

    public void commit(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (eosEnabled) {
            maybeBeginTxn();

            try {
                producer.sendOffsetsToTransaction(offsets, applicationId);
                producer.commitTransaction();
                transactionInFlight = false;
            } catch (final ProducerFencedException error) {
                throw new TaskMigratedException(taskId, "Producer get fenced trying to commit a transaction", error);
            } catch (final TimeoutException error) {
                // TODO KIP-447: we can consider treating it as non-fatal and retry on the thread level
                throw new StreamsException("Timed out while committing transaction via producer for task " + taskId, error);
            } catch (final KafkaException error) {
                throw new StreamsException("Error encountered sending offsets and committing transaction " +
                    "via producer for task " + taskId, error);
            }
        } else {
            try {
                consumer.commitSync(offsets);
            } catch (final CommitFailedException error) {
                throw new TaskMigratedException(taskId, "Consumer committing offsets failed, " +
                    "indicating the corresponding thread is no longer part of the group.", error);
            } catch (final TimeoutException error) {
                // TODO KIP-447: we can consider treating it as non-fatal and retry on the thread level
                throw new StreamsException("Timed out while committing offsets via consumer for task " + taskId, error);
            } catch (final KafkaException error) {
                throw new StreamsException("Error encountered committing offsets via consumer for task " + taskId, error);
            }
        }

    }

    private boolean productionExceptionIsFatal(final Exception exception) {
        final boolean securityException = exception instanceof AuthenticationException ||
            exception instanceof AuthorizationException ||
            exception instanceof SecurityDisabledException;

        final boolean communicationException = exception instanceof InvalidTopicException ||
            exception instanceof UnknownServerException ||
            exception instanceof SerializationException ||
            exception instanceof OffsetMetadataTooLarge ||
            exception instanceof IllegalStateException;

        return securityException || communicationException;
    }

    private void recordSendError(final String topic, final Exception exception, final ProducerRecord<byte[], byte[]> serializedRecord) {
        String errorMessage = String.format(SEND_EXCEPTION_MESSAGE, topic, taskId, exception.toString());

        if (productionExceptionIsFatal(exception)) {
            errorMessage += "\nWritten offsets would not be recorded and no more records would be sent since this is a fatal error.";
            sendException = new StreamsException(errorMessage, exception);
        } else if (exception instanceof ProducerFencedException || exception instanceof OutOfOrderSequenceException) {
            errorMessage += "\nWritten offsets would not be recorded and no more records would be sent since the producer is fenced, " +
                "indicating the task may be migrated out.";
            sendException = new TaskMigratedException(taskId, errorMessage, exception);
        } else {
            if (exception instanceof RetriableException) {
                errorMessage += "\nThe broker is either slow or in bad state (like not having enough replicas) in responding the request, " +
                    "or the connection to broker was interrupted sending the request or receiving the response. " +
                    "\nConsider overwriting `max.block.ms` and /or " +
                    "`delivery.timeout.ms` to a larger value to wait longer for such scenarios and avoid timeout errors";
            }

            if (productionExceptionHandler.handle(serializedRecord, exception) == ProductionExceptionHandlerResponse.FAIL) {
                errorMessage += "\nException handler choose to FAIL the processing, no more records would be sent.";
                sendException = new StreamsException(errorMessage, exception);
            } else {
                errorMessage += "\nException handler choose to CONTINUE processing in spite of this error but written offsets would not be recorded.";
                droppedRecordsSensor.record();
            }
        }

        log.error(errorMessage);
    }

    /**
     * @throws StreamsException fatal error that should cause the thread to die
     * @throws TaskMigratedException recoverable error that would cause the task to be removed
     */
    @Override
    public <K, V> void send(final String topic,
                            final K key,
                            final V value,
                            final Headers headers,
                            final Long timestamp,
                            final Serializer<K> keySerializer,
                            final Serializer<V> valueSerializer,
                            final StreamPartitioner<? super K, ? super V> partitioner) {
        final Integer partition;

        if (partitioner != null) {
            final List<PartitionInfo> partitions;
            try {
                partitions = producer.partitionsFor(topic);
            } catch (final KafkaException e) {
                // here we cannot drop the message on the floor even if it is a transient timeout exception,
                // so we treat everything the same as a fatal exception
                throw new StreamsException("Could not determine the number of partitions for topic '" + topic +
                    "' for task " + taskId + " due to " + e.toString());
            }
            if (partitions.size() > 0) {
                partition = partitioner.partition(topic, key, value, partitions.size());
            } else {
                throw new StreamsException("Could not get partition information for topic '" + topic + "'  for task " + taskId +
                    ". This can happen if the topic does not exist.");
            }
        } else {
            partition = null;
        }

        send(topic, key, value, headers, partition, timestamp, keySerializer, valueSerializer);
    }

    @Override
    public <K, V> void send(final String topic,
                            final K key,
                            final V value,
                            final Headers headers,
                            final Integer partition,
                            final Long timestamp,
                            final Serializer<K> keySerializer,
                            final Serializer<V> valueSerializer) {
        checkForException();

        maybeBeginTxn();

        try {
            final byte[] keyBytes = keySerializer.serialize(topic, headers, key);
            final byte[] valBytes = valueSerializer.serialize(topic, headers, value);

            final ProducerRecord<byte[], byte[]> serializedRecord = new ProducerRecord<>(topic, partition, timestamp, keyBytes, valBytes, headers);

            producer.send(serializedRecord, (metadata, exception) -> {
                // if there's already an exception record, skip logging offsets or new exceptions
                if (sendException != null) {
                    return;
                }

                if (exception == null) {
                    final TopicPartition tp = new TopicPartition(metadata.topic(), metadata.partition());
                    offsets.put(tp, metadata.offset());
                } else {
                    recordSendError(topic, exception, serializedRecord);

                    // KAFKA-7510 only put message key and value in TRACE level log so we don't leak data by default
                    log.trace("Failed record: (key {} value {} timestamp {}) topic=[{}] partition=[{}]", key, value, timestamp, topic, partition);
                }
            });
        } catch (final RuntimeException uncaughtException) {
            if (isRecoverable(uncaughtException)) {
                // producer.send() call may throw a KafkaException which wraps a FencedException,
                // in this case we should throw its wrapped inner cause so that it can be captured and re-wrapped as TaskMigrationException
                throw new TaskMigratedException(taskId, "Producer cannot send records anymore since it got fenced", uncaughtException.getCause());
            } else {
                final String errorMessage = String.format(SEND_EXCEPTION_MESSAGE, topic, taskId, uncaughtException.toString());
                throw new StreamsException(errorMessage, uncaughtException);
            }
        }
    }

    private static boolean isRecoverable(final RuntimeException uncaughtException) {
        return uncaughtException instanceof KafkaException && (
            uncaughtException.getCause() instanceof ProducerFencedException ||
                uncaughtException.getCause() instanceof UnknownProducerIdException);
    }

    private void checkForException() {
        if (sendException != null) {
            throw sendException;
        }
    }

    /**
     * @throws StreamsException fatal error that should cause the thread to die
     * @throws TaskMigratedException recoverable error that would cause the task to be removed
     */
    @Override
    public void flush() {
        log.debug("Flushing record collector");

        producer.flush();

        checkForException();
    }

    /**
     * @throws StreamsException fatal error that should cause the thread to die
     * @throws TaskMigratedException recoverable error that would cause the task to be removed
     */
    @Override
    public void close() {
        log.debug("Closing record collector");
        maybeAbortTxn();

        if (eosEnabled) {
            try {
                producer.close();
            } catch (final KafkaException e) {
                throw new StreamsException("Caught a recoverable exception while closing", e);
            }
        }

        checkForException();
    }

    @Override
    public Map<TopicPartition, Long> offsets() {
        return Collections.unmodifiableMap(new HashMap<>(offsets));
    }

    // for testing only
    Producer<byte[], byte[]> producer() {
        return producer;
    }

}
