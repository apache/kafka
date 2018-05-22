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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.OffsetMetadataTooLarge;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.errors.ProductionExceptionHandler.ProductionExceptionHandlerResponse;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecordCollectorImpl implements RecordCollector {
    private final Logger log;
    private final Producer<byte[], byte[]> producer;
    private final Map<TopicPartition, Long> offsets;
    private final String logPrefix;
    private final ProductionExceptionHandler productionExceptionHandler;
    private final Sensor skippedRecordsSensor;

    private final static String LOG_MESSAGE = "Error sending record (key {} value {} timestamp {}) to topic {} due to {}; " +
        "No more records will be sent and no more offsets will be recorded for this task.";
    private final static String EXCEPTION_MESSAGE = "%sAbort sending since %s with a previous record (key %s value %s timestamp %d) to topic %s due to %s";
    private final static String PARAMETER_HINT = "\nYou can increase producer parameter `retries` and `retry.backoff.ms` to avoid this error.";
    private volatile KafkaException sendException;

    public RecordCollectorImpl(final Producer<byte[], byte[]> producer,
                               final String streamTaskId,
                               final LogContext logContext,
                               final ProductionExceptionHandler productionExceptionHandler,
                               final Sensor skippedRecordsSensor) {
        this.producer = producer;
        this.offsets = new HashMap<>();
        this.logPrefix = String.format("task [%s] ", streamTaskId);
        this.log = logContext.logger(getClass());
        this.productionExceptionHandler = productionExceptionHandler;
        this.skippedRecordsSensor = skippedRecordsSensor;
    }

    @Override
    public <K, V> void send(final String topic,
                            final K key,
                            final V value,
                            final Headers headers,
                            final Long timestamp,
                            final Serializer<K> keySerializer,
                            final Serializer<V> valueSerializer,
                            final StreamPartitioner<? super K, ? super V> partitioner) {
        Integer partition = null;

        if (partitioner != null) {
            final List<PartitionInfo> partitions = producer.partitionsFor(topic);
            if (partitions.size() > 0) {
                partition = partitioner.partition(key, value, partitions.size());
            } else {
                throw new StreamsException("Could not get partition information for topic '" + topic + "'." +
                    " This can happen if the topic does not exist.");
            }
        }

        send(topic, key, value, headers, partition, timestamp, keySerializer, valueSerializer);
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

    private <K, V> void recordSendError(
        final K key,
        final V value,
        final Long timestamp,
        final String topic,
        final Exception exception
    ) {
        String errorLogMessage = LOG_MESSAGE;
        String errorMessage = EXCEPTION_MESSAGE;
        if (exception instanceof RetriableException) {
            errorLogMessage += PARAMETER_HINT;
            errorMessage += PARAMETER_HINT;
        }
        log.error(errorLogMessage, key, value, timestamp, topic, exception.toString());
        sendException = new StreamsException(
            String.format(
                errorMessage,
                logPrefix,
                "an error caught",
                key,
                value,
                timestamp,
                topic,
                exception.toString()
            ),
            exception);
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
        final byte[] keyBytes = keySerializer.serialize(topic, key);
        final byte[] valBytes = valueSerializer.serialize(topic, value);

        final ProducerRecord<byte[], byte[]> serializedRecord = new ProducerRecord<>(topic, partition, timestamp, keyBytes, valBytes, headers);

        try {
            producer.send(serializedRecord, new Callback() {
                @Override
                public void onCompletion(final RecordMetadata metadata,
                                         final Exception exception) {
                    if (exception == null) {
                        if (sendException != null) {
                            return;
                        }
                        final TopicPartition tp = new TopicPartition(metadata.topic(), metadata.partition());
                        offsets.put(tp, metadata.offset());
                    } else {
                        if (sendException == null) {
                            if (exception instanceof ProducerFencedException) {
                                log.warn(LOG_MESSAGE, key, value, timestamp, topic, exception.toString());
                                sendException = new ProducerFencedException(
                                    String.format(
                                        EXCEPTION_MESSAGE,
                                        logPrefix,
                                        "producer got fenced",
                                        key,
                                        value,
                                        timestamp,
                                        topic,
                                        exception.toString()
                                    )
                                );
                            } else {
                                if (productionExceptionIsFatal(exception)) {
                                    recordSendError(key, value, timestamp, topic, exception);
                                } else if (productionExceptionHandler.handle(serializedRecord, exception) == ProductionExceptionHandlerResponse.FAIL) {
                                    recordSendError(key, value, timestamp, topic, exception);
                                } else {
                                    log.warn(
                                        "Error sending records (key=[{}] value=[{}] timestamp=[{}]) to topic=[{}] and partition=[{}]; " +
                                            "The exception handler chose to CONTINUE processing in spite of this error.",
                                        key, value, timestamp, topic, partition, exception
                                    );
                                    skippedRecordsSensor.record();
                                }
                            }
                        }
                    }
                }
            });
        } catch (final TimeoutException e) {
            log.error("Timeout exception caught when sending record to topic {}. " +
                "This might happen if the producer cannot send data to the Kafka cluster and thus, " +
                "its internal buffer fills up. " +
                "You can increase producer parameter `max.block.ms` to increase this timeout.", topic);
            throw new StreamsException(String.format("%sFailed to send record to topic %s due to timeout.", logPrefix, topic));
        } catch (final Exception uncaughtException) {
            throw new StreamsException(
                String.format(
                    EXCEPTION_MESSAGE,
                    logPrefix,
                    "an error caught",
                    key,
                    value,
                    timestamp,
                    topic,
                    uncaughtException.toString()
                ),
                uncaughtException);
        }
    }

    private void checkForException() {
        if (sendException != null) {
            throw sendException;
        }
    }

    @Override
    public void flush() {
        log.debug("Flushing producer");
        producer.flush();
        checkForException();
    }

    @Override
    public void close() {
        log.debug("Closing producer");
        producer.close();
        checkForException();
    }

    @Override
    public Map<TopicPartition, Long> offsets() {
        return offsets;
    }

    // for testing only
    Producer<byte[], byte[]> producer() {
        return producer;
    }

}
