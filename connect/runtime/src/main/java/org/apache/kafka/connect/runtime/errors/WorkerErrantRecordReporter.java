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
package org.apache.kafka.connect.runtime.errors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.runtime.InternalSinkRecord;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;

import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class WorkerErrantRecordReporter implements ErrantRecordReporter {

    private static final Logger log = LoggerFactory.getLogger(WorkerErrantRecordReporter.class);

    private final RetryWithToleranceOperator<ConsumerRecord<byte[], byte[]>> retryWithToleranceOperator;
    private final Converter keyConverter;
    private final Converter valueConverter;
    private final HeaderConverter headerConverter;

    // Visible for testing
    protected final ConcurrentMap<TopicPartition, List<Future<Void>>> futures;
    private final AtomicReference<Throwable> taskPutException;

    public WorkerErrantRecordReporter(
        RetryWithToleranceOperator<ConsumerRecord<byte[], byte[]>> retryWithToleranceOperator,
        Converter keyConverter,
        Converter valueConverter,
        HeaderConverter headerConverter
    ) {
        this.retryWithToleranceOperator = retryWithToleranceOperator;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.headerConverter = headerConverter;
        this.futures = new ConcurrentHashMap<>();
        this.taskPutException = new AtomicReference<>();
    }

    @Override
    public Future<Void> report(SinkRecord record, Throwable error) {
        ProcessingContext<ConsumerRecord<byte[], byte[]>> context;

        // Most of the records will be an internal sink record, but the task could potentially
        // report modified or new records, so handle both cases
        if (record instanceof InternalSinkRecord) {
            context = ((InternalSinkRecord) record).context();
        } else {
            // Generate a new consumer record from the modified sink record. We prefer
            // to send the original consumer record (pre-transformed) to the DLQ,
            // but in this case we don't have one and send the potentially transformed
            // record instead
            String topic = record.topic();
            byte[] key = keyConverter.fromConnectData(topic, record.keySchema(), record.key());
            byte[] value = valueConverter.fromConnectData(topic,
                record.valueSchema(), record.value());

            RecordHeaders headers = new RecordHeaders();
            if (record.headers() != null) {
                for (Header header : record.headers()) {
                    String headerKey = header.key();
                    byte[] rawHeader = headerConverter.fromConnectHeader(topic, headerKey,
                        header.schema(), header.value());
                    headers.add(headerKey, rawHeader);
                }
            }

            int keyLength = key != null ? key.length : -1;
            int valLength = value != null ? value.length : -1;

            ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>(record.topic(), record.kafkaPartition(),
                record.kafkaOffset(), record.timestamp(), record.timestampType(), keyLength,
                valLength, key, value, headers, Optional.empty());
            context = new ProcessingContext<>(consumerRecord);
        }

        Future<Void> future = retryWithToleranceOperator.executeFailed(context, Stage.TASK_PUT, SinkTask.class, error);
        taskPutException.compareAndSet(null, error);

        if (!future.isDone()) {
            TopicPartition partition = new TopicPartition(context.original().topic(), context.original().partition());
            futures.computeIfAbsent(partition, p -> new ArrayList<>()).add(future);
        }
        return future;
    }

    /**
     * Awaits the completion of all error reports for a given set of topic partitions
     * @param topicPartitions the topic partitions to await reporter completion for
     */
    public void awaitFutures(Collection<TopicPartition> topicPartitions) {
        futuresFor(topicPartitions).forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                log.error("Encountered an error while awaiting an errant record future's completion.", e);
                throw new ConnectException(e);
            }
        });
    }

    /**
     * Cancels all active error reports for a given set of topic partitions
     * @param topicPartitions the topic partitions to cancel reporting for
     */
    public void cancelFutures(Collection<TopicPartition> topicPartitions) {
        futuresFor(topicPartitions).forEach(future -> {
            try {
                future.cancel(true);
            } catch (Exception e) {
                log.error("Encountered an error while cancelling an errant record future", e);
                // No need to throw the exception here; it's enough to log an error message
            }
        });
    }

    // Removes and returns all futures for the given topic partitions from the set of currently-active futures
    private Collection<Future<Void>> futuresFor(Collection<TopicPartition> topicPartitions) {
        return topicPartitions.stream()
                .map(futures::remove)
                .filter(Objects::nonNull)
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    public synchronized void maybeThrowAsyncError() {
        if (taskPutException.get() != null && !retryWithToleranceOperator.withinToleranceLimits()) {
            throw new ConnectException("Tolerance exceeded in error handler", taskPutException.get());
        }
    }

    /**
     * Wrapper class to aggregate producer futures and abstract away the record metadata from the
     * Connect user.
     */
    public static class ErrantRecordFuture implements Future<Void> {

        private final List<Future<RecordMetadata>> futures;

        public ErrantRecordFuture(List<Future<RecordMetadata>> producerFutures) {
            futures = producerFutures;
        }

        public boolean cancel(boolean mayInterruptIfRunning) {
            throw new UnsupportedOperationException("Reporting an errant record cannot be cancelled.");
        }

        public boolean isCancelled() {
            return false;
        }

        public boolean isDone() {
            return futures.stream().allMatch(Future::isDone);
        }

        public Void get() throws InterruptedException, ExecutionException {
            for (Future<RecordMetadata> future: futures) {
                future.get();
            }
            return null;
        }

        public Void get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
            for (Future<RecordMetadata> future: futures) {
                future.get(timeout, unit);
            }
            return null;
        }
    }
}
