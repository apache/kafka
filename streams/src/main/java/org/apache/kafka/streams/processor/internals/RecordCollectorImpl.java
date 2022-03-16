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

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.OffsetMetadataTooLarge;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.errors.ProductionExceptionHandler.ProductionExceptionHandlerResponse;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskCorruptedException;
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
import java.util.concurrent.atomic.AtomicReference;

public class RecordCollectorImpl implements RecordCollector {
    private final static String SEND_EXCEPTION_MESSAGE = "Error encountered sending record to topic %s for task %s due to:%n%s";

    private final Logger log;
    private final TaskId taskId;
    private final StreamsProducer streamsProducer;
    private final ProductionExceptionHandler productionExceptionHandler;
    private final Sensor droppedRecordsSensor;
    private final boolean eosEnabled;
    private final Map<TopicPartition, Long> offsets;

    private final AtomicReference<KafkaException> sendException = new AtomicReference<>(null);

    /**
     * @throws StreamsException fatal error that should cause the thread to die (from producer.initTxn)
     */
    public RecordCollectorImpl(final LogContext logContext,
                               final TaskId taskId,
                               final StreamsProducer streamsProducer,
                               final ProductionExceptionHandler productionExceptionHandler,
                               final StreamsMetricsImpl streamsMetrics) {
        this.log = logContext.logger(getClass());
        this.taskId = taskId;
        this.streamsProducer = streamsProducer;
        this.productionExceptionHandler = productionExceptionHandler;
        this.eosEnabled = streamsProducer.eosEnabled();

        final String threadId = Thread.currentThread().getName();
        this.droppedRecordsSensor = TaskMetrics.droppedRecordsSensor(threadId, taskId.toString(), streamsMetrics);

        this.offsets = new HashMap<>();
    }

    @Override
    public void initialize() {
        if (eosEnabled) {
            streamsProducer.initTransaction();
        }
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
                partitions = streamsProducer.partitionsFor(topic);
            } catch (final TimeoutException timeoutException) {
                log.warn("Could not get partitions for topic {}, will retry", topic);

                // re-throw to trigger `task.timeout.ms`
                throw timeoutException;
            } catch (final KafkaException fatal) {
                // here we cannot drop the message on the floor even if it is a transient timeout exception,
                // so we treat everything the same as a fatal exception
                throw new StreamsException("Could not determine the number of partitions for topic '" + topic +
                    "' for task " + taskId + " due to " + fatal.toString(),
                    fatal
                );
            }
            if (partitions.size() > 0) {
                partition = partitioner.partition(topic, key, value, partitions.size());
            } else {
                throw new StreamsException("Could not get partition information for topic " + topic + " for task " + taskId +
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

        final byte[] keyBytes;
        final byte[] valBytes;
        try {
            keyBytes = keySerializer.serialize(topic, headers, key);
            valBytes = valueSerializer.serialize(topic, headers, value);
        } catch (final ClassCastException exception) {
            final String keyClass = key == null ? "unknown because key is null" : key.getClass().getName();
            final String valueClass = value == null ? "unknown because value is null" : value.getClass().getName();
            throw new StreamsException(
                String.format(
                    "ClassCastException while producing data to topic %s. " +
                        "A serializer (key: %s / value: %s) is not compatible to the actual key or value type " +
                        "(key type: %s / value type: %s). " +
                        "Change the default Serdes in StreamConfig or provide correct Serdes via method parameters " +
                        "(for example if using the DSL, `#to(String topic, Produced<K, V> produced)` with " +
                        "`Produced.keySerde(WindowedSerdes.timeWindowedSerdeFrom(String.class))`).",
                    topic,
                    keySerializer.getClass().getName(),
                    valueSerializer.getClass().getName(),
                    keyClass,
                    valueClass),
                exception);
        } catch (final RuntimeException exception) {
            final String errorMessage = String.format(SEND_EXCEPTION_MESSAGE, topic, taskId, exception.toString());
            throw new StreamsException(errorMessage, exception);
        }

        final ProducerRecord<byte[], byte[]> serializedRecord = new ProducerRecord<>(topic, partition, timestamp, keyBytes, valBytes, headers);

        streamsProducer.send(serializedRecord, (metadata, exception) -> {
            // if there's already an exception record, skip logging offsets or new exceptions
            if (sendException.get() != null) {
                return;
            }

            if (exception == null) {
                final TopicPartition tp = new TopicPartition(metadata.topic(), metadata.partition());
                if (metadata.offset() >= 0L) {
                    offsets.put(tp, metadata.offset());
                } else {
                    log.warn("Received offset={} in produce response for {}", metadata.offset(), tp);
                }
            } else {
                recordSendError(topic, exception, serializedRecord);

                // KAFKA-7510 only put message key and value in TRACE level log so we don't leak data by default
                log.trace("Failed record: (key {} value {} timestamp {}) topic=[{}] partition=[{}]", key, value, timestamp, topic, partition);
            }
        });
    }

    private void recordSendError(final String topic, final Exception exception, final ProducerRecord<byte[], byte[]> serializedRecord) {
        String errorMessage = String.format(SEND_EXCEPTION_MESSAGE, topic, taskId, exception.toString());

        if (isFatalException(exception)) {
            errorMessage += "\nWritten offsets would not be recorded and no more records would be sent since this is a fatal error.";
            sendException.set(new StreamsException(errorMessage, exception));
        } else if (exception instanceof ProducerFencedException ||
                exception instanceof InvalidProducerEpochException ||
                exception instanceof OutOfOrderSequenceException) {
            errorMessage += "\nWritten offsets would not be recorded and no more records would be sent since the producer is fenced, " +
                "indicating the task may be migrated out";
            sendException.set(new TaskMigratedException(errorMessage, exception));
        } else {
            if (exception instanceof RetriableException) {
                errorMessage += "\nThe broker is either slow or in bad state (like not having enough replicas) in responding the request, " +
                    "or the connection to broker was interrupted sending the request or receiving the response. " +
                    "\nConsider overwriting `max.block.ms` and /or " +
                    "`delivery.timeout.ms` to a larger value to wait longer for such scenarios and avoid timeout errors";
                sendException.set(new TaskCorruptedException(Collections.singleton(taskId)));
            } else {
                if (productionExceptionHandler.handle(serializedRecord, exception) == ProductionExceptionHandlerResponse.FAIL) {
                    errorMessage += "\nException handler choose to FAIL the processing, no more records would be sent.";
                    sendException.set(new StreamsException(errorMessage, exception));
                } else {
                    errorMessage += "\nException handler choose to CONTINUE processing in spite of this error but written offsets would not be recorded.";
                    droppedRecordsSensor.record();
                }
            }
        }

        log.error(errorMessage, exception);
    }

    private boolean isFatalException(final Exception exception) {
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

    /**
     * @throws StreamsException fatal error that should cause the thread to die
     * @throws TaskMigratedException recoverable error that would cause the task to be removed
     */
    @Override
    public void flush() {
        log.debug("Flushing record collector");
        streamsProducer.flush();
        checkForException();
    }

    /**
     * @throws StreamsException fatal error that should cause the thread to die
     * @throws TaskMigratedException recoverable error that would cause the task to be removed
     */
    @Override
    public void closeClean() {
        log.info("Closing record collector clean");

        // No need to abort transaction during a clean close: either we have successfully committed the ongoing
        // transaction during handleRevocation and thus there is no transaction in flight, or else none of the revoked
        // tasks had any data in the current transaction and therefore there is no need to commit or abort it.

        checkForException();
    }

    /**
     * @throws StreamsException fatal error that should cause the thread to die
     * @throws TaskMigratedException recoverable error that would cause the task to be removed
     */
    @Override
    public void closeDirty() {
        log.info("Closing record collector dirty");

        if (eosEnabled) {
            // We may be closing dirty because the commit failed, so we must abort the transaction to be safe
            streamsProducer.abortTransaction();
        }

        checkForException();
    }

    @Override
    public Map<TopicPartition, Long> offsets() {
        return Collections.unmodifiableMap(new HashMap<>(offsets));
    }

    private void checkForException() {
        final KafkaException exception = sendException.get();

        if (exception != null) {
            sendException.set(null);
            throw exception;
        }
    }

    // for testing only
    Producer<byte[], byte[]> producer() {
        return streamsProducer.kafkaProducer();
    }
}
