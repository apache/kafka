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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class WorkerErrantRecordReporter implements ErrantRecordReporter {

    private static final Logger log = LoggerFactory.getLogger(WorkerErrantRecordReporter.class);

    private RetryWithToleranceOperator retryWithToleranceOperator;
    private Converter keyConverter;
    private Converter valueConverter;
    private HeaderConverter headerConverter;

    // Visible for testing
    public LinkedList<Future<Void>> futures;

    public WorkerErrantRecordReporter(
        RetryWithToleranceOperator retryWithToleranceOperator,
        Converter keyConverter,
        Converter valueConverter,
        HeaderConverter headerConverter
    ) {
        this.retryWithToleranceOperator = retryWithToleranceOperator;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.headerConverter = headerConverter;
        this.futures = new LinkedList<>();
    }

    @Override
    public Future<Void> report(SinkRecord record, Throwable error) {
        ConsumerRecord<byte[], byte[]> consumerRecord;

        if (record instanceof InternalSinkRecord) {
            consumerRecord = ((InternalSinkRecord) record).originalRecord();
        } else {
            String topic = record.topic();
            byte[] key = keyConverter.fromConnectData(topic, record.keySchema(), record.key());
            byte[] value = valueConverter.fromConnectData(topic, record.valueSchema(),
                record.value());

            RecordHeaders headers = new RecordHeaders();
            if (record.headers() != null) {
                for (Header header : record.headers()) {
                    String headerKey = header.key();
                    byte[] rawHeader = headerConverter.fromConnectHeader(topic, headerKey,
                        header.schema(), header.value());
                    headers.add(headerKey, rawHeader);
                }
            }

            consumerRecord = new ConsumerRecord<>(record.topic(), record.kafkaPartition(),
                record.kafkaOffset(), record.timestamp(), record.timestampType(), -1L, -1,
                -1, key, value, headers);
        }

        Future<Void> future = retryWithToleranceOperator.executeFailed(Stage.TASK_PUT,
            SinkTask.class, consumerRecord, error);

        if (!future.isDone()) {
            futures.add(future);
        }
        return future;
    }

    /**
     * Gets all futures returned by the sink records sent to Kafka by the errant
     * record reporter. This function is intended to be used to block on all the errant record
     * futures.
     */
    public void getAllFutures() {
        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                log.error("Encountered an error while calling ");
                throw new ConnectException(e);
            }
        }
        futures.clear();
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
            for (Future<RecordMetadata> future: futures) {
                if (!future.isDone()) {
                    return false;
                }
            }
            return true;
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
