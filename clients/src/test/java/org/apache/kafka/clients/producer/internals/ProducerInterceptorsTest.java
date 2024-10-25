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
package org.apache.kafka.clients.producer.internals;


import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ProducerInterceptorsTest {
    private final TopicPartition tp = new TopicPartition("test", 0);
    private final ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>("test", 0, 1, "value");
    private int onAckCount = 0;
    private int onErrorAckCount = 0;
    private int onErrorAckWithTopicSetCount = 0;
    private int onErrorAckWithTopicPartitionSetCount = 0;
    private int onSendCount = 0;

    private class AppendProducerInterceptor implements ProducerInterceptor<Integer, String> {
        private final String appendStr;
        private boolean throwExceptionOnSend = false;
        private boolean throwExceptionOnAck = false;

        public AppendProducerInterceptor(String appendStr) {
            this.appendStr = appendStr;
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }

        @Override
        public ProducerRecord<Integer, String> onSend(ProducerRecord<Integer, String> record) {
            onSendCount++;
            if (throwExceptionOnSend)
                throw new KafkaException("Injected exception in AppendProducerInterceptor.onSend");

            return new ProducerRecord<>(
                    record.topic(), record.partition(), record.key(), record.value().concat(appendStr));
        }

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
            onAckCount++;
            if (exception != null) {
                onErrorAckCount++;
                // the length check is just to call topic() method and let it throw an exception
                // if RecordMetadata.TopicPartition is null
                if (metadata != null && metadata.topic().length() >= 0) {
                    onErrorAckWithTopicSetCount++;
                    if (metadata.partition() >= 0)
                        onErrorAckWithTopicPartitionSetCount++;
                }
            }
            if (throwExceptionOnAck)
                throw new KafkaException("Injected exception in AppendProducerInterceptor.onAcknowledgement");
        }

        @Override
        public void close() {
        }

        // if 'on' is true, onSend will always throw an exception
        public void injectOnSendError(boolean on) {
            throwExceptionOnSend = on;
        }

        // if 'on' is true, onAcknowledgement will always throw an exception
        public void injectOnAcknowledgementError(boolean on) {
            throwExceptionOnAck = on;
        }
    }

    private class AppendNewProducerInterceptor implements ProducerInterceptor<Integer, String> {
        private final String appendStr;
        private boolean throwExceptionOnSend = false;
        private boolean throwExceptionOnAck = false;

        public AppendNewProducerInterceptor(String appendStr) {
            this.appendStr = appendStr;
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }

        @Override
        public ProducerRecord<Integer, String> onSend(ProducerRecord<Integer, String> record) {
            onSendCount++;
            if (throwExceptionOnSend)
                throw new KafkaException("Injected exception in AppendNewProducerInterceptor.onSend");

            return new ProducerRecord<>(
                    record.topic(), record.partition(), record.key(), record.value().concat(appendStr));
        }

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception, Headers headers) {
            onAckCount++;
            if (exception != null) {
                onErrorAckCount++;
                // the length check is just to call topic() method and let it throw an exception
                // if RecordMetadata.TopicPartition is null
                if (metadata != null && metadata.topic().length() >= 0) {
                    onErrorAckWithTopicSetCount++;
                    if (metadata.partition() >= 0)
                        onErrorAckWithTopicPartitionSetCount++;
                }
            }
            if (throwExceptionOnAck)
                throw new KafkaException("Injected exception in AppendNewProducerInterceptor.onAcknowledgement");
        }

        @Override
        public void close() {
        }

        // if 'on' is true, onSend will always throw an exception
        public void injectOnSendError(boolean on) {
            throwExceptionOnSend = on;
        }

        // if 'on' is true, onAcknowledgement will always throw an exception
        public void injectOnAcknowledgementError(boolean on) {
            throwExceptionOnAck = on;
        }
    }

    @Test
    public void testOnSendChain() {
        List<ProducerInterceptor<Integer, String>> interceptorList = new ArrayList<>();
        // we are testing two different interceptors by configuring the same interceptor differently, which is not
        // how it would be done in KafkaProducer, but ok for testing interceptor callbacks
        AppendProducerInterceptor interceptor1 = new AppendProducerInterceptor("One");
        AppendNewProducerInterceptor interceptor2 = new AppendNewProducerInterceptor("Two");
        interceptorList.add(interceptor1);
        interceptorList.add(interceptor2);
        ProducerInterceptors<Integer, String> interceptors = new ProducerInterceptors<>(interceptorList);

        // verify that onSend() mutates the record as expected
        ProducerRecord<Integer, String> interceptedRecord = interceptors.onSend(producerRecord);
        assertEquals(2, onSendCount);
        assertEquals(producerRecord.topic(), interceptedRecord.topic());
        assertEquals(producerRecord.partition(), interceptedRecord.partition());
        assertEquals(producerRecord.key(), interceptedRecord.key());
        assertEquals(interceptedRecord.value(), producerRecord.value().concat("One").concat("Two"));

        // onSend() mutates the same record the same way
        ProducerRecord<Integer, String> anotherRecord = interceptors.onSend(producerRecord);
        assertEquals(4, onSendCount);
        assertEquals(interceptedRecord, anotherRecord);

        // verify that if one of the interceptors throws an exception, other interceptors' callbacks are still called
        interceptor1.injectOnSendError(true);
        ProducerRecord<Integer, String> partInterceptRecord = interceptors.onSend(producerRecord);
        assertEquals(6, onSendCount);
        assertEquals(partInterceptRecord.value(), producerRecord.value().concat("Two"));

        // verify the record remains valid if all onSend throws an exception
        interceptor2.injectOnSendError(true);
        ProducerRecord<Integer, String> noInterceptRecord = interceptors.onSend(producerRecord);
        assertEquals(producerRecord, noInterceptRecord);

        interceptors.close();
    }

    @Test
    public void testOnAcknowledgementChain() {
        List<ProducerInterceptor<Integer, String>> interceptorList = new ArrayList<>();
        // we are testing two different interceptors by configuring the same interceptor differently, which is not
        // how it would be done in KafkaProducer, but ok for testing interceptor callbacks
        AppendProducerInterceptor interceptor1 = new AppendProducerInterceptor("One");
        AppendNewProducerInterceptor interceptor2 = new AppendNewProducerInterceptor("Two");
        interceptorList.add(interceptor1);
        interceptorList.add(interceptor2);
        ProducerInterceptors<Integer, String> interceptors = new ProducerInterceptors<>(interceptorList);

        // verify onAck is called on all interceptors
        RecordMetadata meta = new RecordMetadata(tp, 0, 0, 0, 0, 0);
        interceptors.onAcknowledgement(meta, null, null);
        assertEquals(2, onAckCount);

        // verify that onAcknowledgement exceptions do not propagate
        interceptor1.injectOnAcknowledgementError(true);
        interceptors.onAcknowledgement(meta, null, null);
        assertEquals(4, onAckCount);

        interceptor2.injectOnAcknowledgementError(true);
        interceptors.onAcknowledgement(meta, null, null);
        assertEquals(6, onAckCount);

        interceptors.close();
    }

    @Test
    public void testOnAcknowledgementWithErrorChain() {
        List<ProducerInterceptor<Integer, String>> interceptorList = new ArrayList<>();
        AppendProducerInterceptor interceptor1 = new AppendProducerInterceptor("One");
        AppendNewProducerInterceptor interceptor2 = new AppendNewProducerInterceptor("Two");
        interceptorList.add(interceptor1);
        interceptorList.add(interceptor2);
        ProducerInterceptors<Integer, String> interceptors = new ProducerInterceptors<>(interceptorList);

        // verify that metadata contains both topic and partition
        interceptors.onSendError(producerRecord,
                                 new TopicPartition(producerRecord.topic(), producerRecord.partition()),
                                 new KafkaException("Test"));
        assertEquals(2, onErrorAckCount);
        assertEquals(2, onErrorAckWithTopicPartitionSetCount);

        // verify that metadata contains both topic and partition (because record already contains partition)
        interceptors.onSendError(producerRecord, null, new KafkaException("Test"));
        assertEquals(4, onErrorAckCount);
        assertEquals(4, onErrorAckWithTopicPartitionSetCount);

        // if producer record does not contain partition, interceptor should get partition == -1
        ProducerRecord<Integer, String> record2 = new ProducerRecord<>("test2", null, 1, "value");
        interceptors.onSendError(record2, null, new KafkaException("Test"));
        assertEquals(6, onErrorAckCount);
        assertEquals(6, onErrorAckWithTopicSetCount);
        assertEquals(4, onErrorAckWithTopicPartitionSetCount);

        // if producer record does not contain partition, but topic/partition is passed to
        // onSendError, then interceptor should get valid partition
        int reassignedPartition = producerRecord.partition() + 1;
        interceptors.onSendError(record2,
                                 new TopicPartition(record2.topic(), reassignedPartition),
                                 new KafkaException("Test"));
        assertEquals(8, onErrorAckCount);
        assertEquals(8, onErrorAckWithTopicSetCount);
        assertEquals(6, onErrorAckWithTopicPartitionSetCount);

        // if both record and topic/partition are null, interceptor should not receive metadata
        interceptors.onSendError(null, null, new KafkaException("Test"));
        assertEquals(10, onErrorAckCount);
        assertEquals(8, onErrorAckWithTopicSetCount);
        assertEquals(6, onErrorAckWithTopicPartitionSetCount);

        interceptors.close();
    }
}
