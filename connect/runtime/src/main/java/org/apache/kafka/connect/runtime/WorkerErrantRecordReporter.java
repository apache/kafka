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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;

import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class WorkerErrantRecordReporter implements ErrantRecordReporter {

    private static final int DLQ_NUM_DESIRED_PARTITIONS = 1;

    private static final Logger log = LoggerFactory.getLogger(WorkerErrantRecordReporter.class);

    private KafkaProducer<byte[], byte[]> producer;
    private String dlqTopic;
    private boolean useDlq;
    private Converter keyConverter;
    private Converter valueConverter;
    private List<ErrantRecordFuture> errantRecordFutures;
    private SinkConnectorConfig sinkConfig;
    private HeaderConverter headerConverter;


    public static WorkerErrantRecordReporter createAndSetup(
        Map<String, Object> adminProps,
        Map<String, Object> producerProps,
        SinkConnectorConfig sinkConnectorConfig,
        Converter workerKeyConverter,
        Converter workerValueConverter,
        HeaderConverter workerHeaderConverter
    ) {

        KafkaProducer<byte[], byte[]> kafkaProducer = DeadLetterQueueReporter.setUpTopicAndProducer(
            adminProps,
            producerProps,
            sinkConnectorConfig,
            DLQ_NUM_DESIRED_PARTITIONS
        );

        return new WorkerErrantRecordReporter(
            kafkaProducer,
            sinkConnectorConfig,
            workerKeyConverter,
            workerValueConverter,
            workerHeaderConverter
        );
    }

    // Visible for testing purposes
    public WorkerErrantRecordReporter(
        KafkaProducer<byte[], byte[]> kafkaProducer,
        SinkConnectorConfig sinkConnectorConfig,
        Converter workerKeyConverter,
        Converter workerValueConverter,
        HeaderConverter workerHeaderConverter
    ) {
        producer = kafkaProducer;
        dlqTopic = sinkConnectorConfig.dlqTopicName();
        useDlq = dlqTopic != null && !dlqTopic.isEmpty();
        keyConverter = workerKeyConverter;
        valueConverter = workerValueConverter;
        errantRecordFutures = new ArrayList<>();
        sinkConfig = sinkConnectorConfig;
        headerConverter = workerHeaderConverter;
    }

    @Override
    public Future<Void> report(SinkRecord record, Throwable error) {

        if (sinkConfig.enableErrorLog()) {
            if (sinkConfig.includeRecordDetailsInErrorLog()) {
                log.error("Error processing record: " + record.toString(), error);
            } else {
                log.error(
                    "Error processing record in topic "
                        + record.topic()
                        + "at offset "
                        + record.kafkaOffset(),
                    error
                );
            }
        }

        Future<RecordMetadata> producerFuture = null;

        if (useDlq) {

            Headers headers = record.headers();
            RecordHeaders result = new RecordHeaders();
            if (headers != null) {
                String topic = record.topic();
                for (Header header : headers) {
                    String key = header.key();
                    byte[] rawHeader = headerConverter.fromConnectHeader(topic, key, header.schema(), header.value());
                    result.add(key, rawHeader);
                }
            }

            ProducerRecord<byte[], byte[]> errantRecord = new ProducerRecord<>(
                dlqTopic,
                null,
                record.timestamp() == RecordBatch.NO_TIMESTAMP ? record.timestamp() : null,
                keyConverter.fromConnectData(dlqTopic, record.keySchema(), record.key()),
                valueConverter.fromConnectData(dlqTopic, record.valueSchema(), record.value()),
                result
            );

            producerFuture = producer.send(errantRecord);
        }

        ErrantRecordFuture errantRecordFuture = new ErrantRecordFuture(producerFuture);
        errantRecordFutures.add(errantRecordFuture);
        return errantRecordFuture;
    }

    public void waitForAllFutures() {
        for (ErrantRecordFuture future : errantRecordFutures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new ConnectException(e);
            }
        }
    }

    // Visible for testing
    public class ErrantRecordFuture implements Future<Void> {

        Future<RecordMetadata> future;

        public ErrantRecordFuture(Future<RecordMetadata> producerFuture) {
            future = producerFuture;
        }

        public boolean cancel(boolean mayInterruptIfRunning) {
            throw new UnsupportedOperationException("Reporting an errant record cannot be cancelled.");
        }

        public boolean isCancelled() {
            return false;
        }

        public boolean isDone() {
            return future == null || future.isDone();
        }

        public Void get() throws InterruptedException, ExecutionException {
            if (future != null) {
                future.get();
            }
            return null;
        }

        public Void get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
            if (future != null) {
                future.get(timeout, unit);
            }
            return null;
        }
    }
}
