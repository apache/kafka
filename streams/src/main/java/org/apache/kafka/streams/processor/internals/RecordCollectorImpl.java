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
import org.apache.kafka.common.errors.InvalidPidMappingException;
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
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.errors.ProductionExceptionHandler.ProductionExceptionHandlerResponse;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.errors.internals.DefaultErrorHandlerContext;
import org.apache.kafka.streams.errors.internals.FailedProcessingException;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.processor.internals.metrics.TopicMetrics;

import org.slf4j.Logger;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.streams.processor.internals.ClientUtils.producerRecordSizeInBytes;

public class RecordCollectorImpl implements RecordCollector {
    private static final String SEND_EXCEPTION_MESSAGE = "Error encountered sending record to topic %s for task %s due to:%n%s";

    private final Logger log;
    private final TaskId taskId;
    private final StreamsProducer streamsProducer;
    private final ProductionExceptionHandler productionExceptionHandler;
    private final Map<TopicPartition, Long> offsets;

    private final StreamsMetricsImpl streamsMetrics;
    private final Sensor droppedRecordsSensor;
    private final Map<String, Sensor> producedSensorByTopic = new HashMap<>();

    // we get `sendException` from "singleton" `StreamsProducer` to share it across all instances of `RecordCollectorImpl`
    private final AtomicReference<KafkaException> sendException;

    /**
     * @throws StreamsException fatal error that should cause the thread to die (from producer.initTxn)
     */
    public RecordCollectorImpl(final LogContext logContext,
                               final TaskId taskId,
                               final StreamsProducer streamsProducer,
                               final ProductionExceptionHandler productionExceptionHandler,
                               final StreamsMetricsImpl streamsMetrics,
                               final ProcessorTopology topology) {
        this.log = logContext.logger(getClass());
        this.taskId = taskId;
        this.streamsProducer = streamsProducer;
        this.sendException = streamsProducer.sendException();
        this.productionExceptionHandler = productionExceptionHandler;
        this.streamsMetrics = streamsMetrics;

        final String threadId = Thread.currentThread().getName();
        this.droppedRecordsSensor = TaskMetrics.droppedRecordsSensor(threadId, taskId.toString(), streamsMetrics);
        for (final String topic : topology.sinkTopics()) {
            final String processorNodeId = topology.sink(topic).name();
            producedSensorByTopic.put(
                topic,
                TopicMetrics.producedSensor(
                    threadId,
                    taskId.toString(),
                    processorNodeId,
                    topic,
                    streamsMetrics
                ));
        }

        this.offsets = new HashMap<>();
    }

    @Override
    public void initialize() {
        if (streamsProducer.eosEnabled()) {
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
                            final String processorNodeId,
                            final InternalProcessorContext<Void, Void> context,
                            final StreamPartitioner<? super K, ? super V> partitioner) {

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
                    "' for task " + taskId + " due to " + fatal,
                    fatal
                );
            }
            if (!partitions.isEmpty()) {
                final Optional<Set<Integer>> maybeMulticastPartitions = partitioner.partitions(topic, key, value, partitions.size());
                if (!maybeMulticastPartitions.isPresent()) {
                    // A null//empty partition indicates we should use the default partitioner
                    send(topic, key, value, headers, null, timestamp, keySerializer, valueSerializer, processorNodeId, context);
                } else {
                    final Set<Integer> multicastPartitions = maybeMulticastPartitions.get();
                    if (multicastPartitions.isEmpty()) {
                        // If a record is not to be sent to any partition, mark it as a dropped record.
                        log.warn("Skipping record as partitioner returned empty partitions. "
                                + "topic=[{}]", topic);
                        droppedRecordsSensor.record();
                    } else {
                        for (final int multicastPartition: multicastPartitions) {
                            send(topic, key, value, headers, multicastPartition, timestamp, keySerializer, valueSerializer, processorNodeId, context);
                        }
                    }
                }
            } else {
                throw new StreamsException("Could not get partition information for topic " + topic + " for task " + taskId +
                    ". This can happen if the topic does not exist.");
            }
        } else {
            send(topic, key, value, headers, null, timestamp, keySerializer, valueSerializer, processorNodeId, context);
        }

    }

    @Override
    public <K, V> void send(final String topic,
                            final K key,
                            final V value,
                            final Headers headers,
                            final Integer partition,
                            final Long timestamp,
                            final Serializer<K> keySerializer,
                            final Serializer<V> valueSerializer,
                            final String processorNodeId,
                            final InternalProcessorContext<Void, Void> context) {
        checkForException();

        final byte[] keyBytes;
        final byte[] valBytes;
        try {
            keyBytes = keySerializer.serialize(topic, headers, key);
        } catch (final ClassCastException exception) {
            throw createStreamsExceptionForClassCastException(
                ProductionExceptionHandler.SerializationExceptionOrigin.KEY,
                topic,
                key,
                keySerializer,
                exception);
        } catch (final Exception serializationException) {
            // while Java distinguishes checked vs unchecked exceptions, other languages
            // like Scala or Kotlin do not, and thus we need to catch `Exception`
            // (instead of `RuntimeException`) to work well with those languages
            handleException(
                ProductionExceptionHandler.SerializationExceptionOrigin.KEY,
                topic,
                key,
                value,
                headers,
                partition,
                timestamp,
                processorNodeId,
                context,
                serializationException
            );
            return;
        }

        try {
            valBytes = valueSerializer.serialize(topic, headers, value);
        } catch (final ClassCastException exception) {
            throw createStreamsExceptionForClassCastException(
                ProductionExceptionHandler.SerializationExceptionOrigin.VALUE,
                topic,
                value,
                valueSerializer,
                exception);
        } catch (final Exception serializationException) {
            // while Java distinguishes checked vs unchecked exceptions, other languages
            // like Scala or Kotlin do not, and thus we need to catch `Exception`
            // (instead of `RuntimeException`) to work well with those languages
            handleException(
                ProductionExceptionHandler.SerializationExceptionOrigin.VALUE,
                topic,
                key,
                value,
                headers,
                partition,
                timestamp,
                processorNodeId,
                context,
                serializationException);
            return;
        }

        final ProducerRecord<byte[], byte[]> serializedRecord = new ProducerRecord<>(topic, partition, timestamp, keyBytes, valBytes, headers);

        streamsProducer.send(serializedRecord, (metadata, exception) -> {
            try {
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

                    if (!topic.endsWith("-changelog")) {
                        // we may not have created a sensor during initialization if the node uses dynamic topic routing,
                        // as all topics are not known up front, so create the sensor for this topic if absent
                        final Sensor topicProducedSensor = producedSensorByTopic.computeIfAbsent(
                            topic,
                            t -> TopicMetrics.producedSensor(
                                Thread.currentThread().getName(),
                                taskId.toString(),
                                processorNodeId,
                                topic,
                                context.metrics()
                            )
                        );
                        final long bytesProduced = producerRecordSizeInBytes(serializedRecord);
                        topicProducedSensor.record(
                            bytesProduced,
                            context.currentSystemTimeMs()
                        );
                    }
                } else {
                    recordSendError(
                        topic,
                        exception,
                        serializedRecord,
                        context,
                        processorNodeId
                    );

                    // KAFKA-7510 only put message key and value in TRACE level log so we don't leak data by default
                    log.trace("Failed record: (key {} value {} timestamp {}) topic=[{}] partition=[{}]", key, value, timestamp, topic, partition);
                }
            } catch (final RuntimeException fatal) {
                sendException.set(new StreamsException("Producer.send `Callback` failed", fatal));
            }
        });
    }

    private <K, V> void handleException(final ProductionExceptionHandler.SerializationExceptionOrigin origin,
                                        final String topic,
                                        final K key,
                                        final V value,
                                        final Headers headers,
                                        final Integer partition,
                                        final Long timestamp,
                                        final String processorNodeId,
                                        final InternalProcessorContext<Void, Void> context,
                                        final Exception serializationException) {
        log.debug(String.format("Error serializing record for topic %s", topic), serializationException);

        final ProducerRecord<K, V> record = new ProducerRecord<>(topic, partition, timestamp, key, value, headers);

        final ProductionExceptionHandlerResponse response;
        try {
            response = Objects.requireNonNull(
                productionExceptionHandler.handleSerializationException(
                    errorHandlerContext(context, processorNodeId),
                    record,
                    serializationException,
                    origin
                ),
                "Invalid ProductionExceptionHandler response."
            );
        } catch (final Exception fatalUserException) {
            // while Java distinguishes checked vs unchecked exceptions, other languages
            // like Scala or Kotlin do not, and thus we need to catch `Exception`
            // (instead of `RuntimeException`) to work well with those languages
            log.error(
                String.format(
                    "Production error callback failed after serialization error for record %s: %s",
                    origin.toString().toLowerCase(Locale.ROOT),
                    errorHandlerContext(context, processorNodeId)
                ),
                serializationException
            );
            throw new FailedProcessingException(
                "Fatal user code error in production error callback",
                processorNodeId,
                fatalUserException
            );
        }

        if (maybeFailResponse(response) == ProductionExceptionHandlerResponse.FAIL) {
            throw new StreamsException(
                String.format(
                    "Unable to serialize record. ProducerRecord(topic=[%s], partition=[%d], timestamp=[%d]",
                    topic,
                    partition,
                    timestamp),
                serializationException
            );
        }

        log.warn("Unable to serialize record, continue processing. " +
                    "ProducerRecord(topic=[{}], partition=[{}], timestamp=[{}])",
                topic,
                partition,
                timestamp);

        droppedRecordsSensor.record();
    }

    private DefaultErrorHandlerContext errorHandlerContext(final InternalProcessorContext<Void, Void> context,
                                                           final String processorNodeId) {
        final RecordContext recordContext = context != null ? context.recordContext() : null;

        return recordContext != null ?
            new DefaultErrorHandlerContext(
                context,
                recordContext.topic(),
                recordContext.partition(),
                recordContext.offset(),
                recordContext.headers(),
                processorNodeId,
                taskId,
                recordContext.timestamp()
            ) :
            new DefaultErrorHandlerContext(
                context,
                null,
                -1,
                -1,
                new RecordHeaders(),
                processorNodeId,
                taskId,
                -1L
            );
    }

    private <KV> StreamsException createStreamsExceptionForClassCastException(final ProductionExceptionHandler.SerializationExceptionOrigin origin,
                                                                              final String topic,
                                                                              final KV keyOrValue,
                                                                              final Serializer<KV> keyOrValueSerializer,
                                                                              final ClassCastException exception) {
        final String keyOrValueClass = keyOrValue == null
            ? String.format("unknown because %s is null", origin.toString().toLowerCase(Locale.ROOT)) : keyOrValue.getClass().getName();

        return new StreamsException(
            MessageFormat.format(
                String.format(
                        "ClassCastException while producing data to topic %s. " +
                            "The {0} serializer %s is not compatible to the actual {0} type: %s. " +
                            "Change the default {0} serde in StreamConfig or provide the correct {0} serde via method parameters " +
                            "(for example if using the DSL, `#to(String topic, Produced<K, V> produced)` with " +
                            "`Produced.{0}Serde(WindowedSerdes.timeWindowedSerdeFrom(String.class))`).",
                        topic,
                        keyOrValueSerializer.getClass().getName(),
                        keyOrValueClass),
                origin.toString().toLowerCase(Locale.ROOT)),
                exception);
    }

    private void recordSendError(final String topic,
                                 final Exception productionException,
                                 final ProducerRecord<byte[], byte[]> serializedRecord,
                                 final InternalProcessorContext<Void, Void> context,
                                 final String processorNodeId) {
        String errorMessage = String.format(SEND_EXCEPTION_MESSAGE, topic, taskId, productionException.toString());

        if (isFatalException(productionException)) {
            errorMessage += "\nWritten offsets would not be recorded and no more records would be sent since this is a fatal error.";
            sendException.set(new StreamsException(errorMessage, productionException));
        } else if (productionException instanceof ProducerFencedException ||
                productionException instanceof InvalidPidMappingException ||
                productionException instanceof InvalidProducerEpochException ||
                productionException instanceof OutOfOrderSequenceException) {
            errorMessage += "\nWritten offsets would not be recorded and no more records would be sent since the producer is fenced, " +
                "indicating the task may be migrated out";
            sendException.set(new TaskMigratedException(errorMessage, productionException));
        } else {
            final ProductionExceptionHandlerResponse response;
            try {
                response = Objects.requireNonNull(
                    productionExceptionHandler.handle(
                        errorHandlerContext(context, processorNodeId),
                        serializedRecord,
                        productionException
                    ),
                    "Invalid ProductionExceptionHandler response."
                );
            } catch (final Exception fatalUserException) {
                // while Java distinguishes checked vs unchecked exceptions, other languages
                // like Scala or Kotlin do not, and thus we need to catch `Exception`
                // (instead of `RuntimeException`) to work well with those languages
                log.error(
                    "Production error callback failed after production error for record {}",
                    serializedRecord,
                    productionException
                );
                sendException.set(new FailedProcessingException(
                    "Fatal user code error in production error callback",
                    processorNodeId,
                    fatalUserException
                    )
                );
                return;
            }

            if (productionException instanceof RetriableException && response == ProductionExceptionHandlerResponse.RETRY) {
                errorMessage += "\nThe broker is either slow or in bad state (like not having enough replicas) in responding the request, " +
                    "or the connection to broker was interrupted sending the request or receiving the response. " +
                    "\nConsider overwriting `max.block.ms` and /or " +
                    "`delivery.timeout.ms` to a larger value to wait longer for such scenarios and avoid timeout errors";
                sendException.set(new TaskCorruptedException(Collections.singleton(taskId)));
            } else {
                if (maybeFailResponse(response) == ProductionExceptionHandlerResponse.FAIL) {
                    errorMessage += "\nException handler choose to FAIL the processing, no more records would be sent.";
                    sendException.set(new StreamsException(errorMessage, productionException));
                } else {
                    errorMessage += "\nException handler choose to CONTINUE processing in spite of this error but written offsets would not be recorded.";
                    droppedRecordsSensor.record();
                }
            }
        }

        log.error(errorMessage, productionException);
    }

    private ProductionExceptionHandlerResponse maybeFailResponse(final ProductionExceptionHandlerResponse response) {
        if (response == ProductionExceptionHandlerResponse.RETRY) {
            log.warn("ProductionExceptionHandler returned RETRY for a non-retriable exception. Will treat it as FAIL.");
            return ProductionExceptionHandlerResponse.FAIL;
        } else {
            return response;
        }
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

        removeAllProducedSensors();

        // No need to abort transaction during a clean close: either we have successfully committed the ongoing
        // transaction during handleRevocation and thus there is no transaction in flight, or else none of the revoked
        // tasks had any data in the current transaction and therefore there is no need to commit or abort it.

        close();
    }

    /**
     * @throws StreamsException fatal error that should cause the thread to die
     * @throws TaskMigratedException recoverable error that would cause the task to be removed
     */
    @Override
    public void closeDirty() {
        log.info("Closing record collector dirty");

        if (streamsProducer.eosEnabled()) {
            // We may be closing dirty because the commit failed, so we must abort the transaction to be safe
            streamsProducer.abortTransaction();
        }

        close();
    }

    private void close() {
        offsets.clear();
        checkForException();
    }

    private void removeAllProducedSensors() {
        for (final Sensor sensor : producedSensorByTopic.values()) {
            streamsMetrics.removeSensor(sensor);
        }
    }

    @Override
    public Map<TopicPartition, Long> offsets() {
        return Collections.unmodifiableMap(new HashMap<>(offsets));
    }

    private void checkForException() {
        final KafkaException exception = sendException.get();

        if (exception != null) {
            sendException.compareAndSet(exception, null);
            throw exception;
        }
    }

    // for testing only
    Producer<byte[], byte[]> producer() {
        return streamsProducer.kafkaProducer();
    }
}
