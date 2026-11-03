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

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.runtime.isolation.LoaderSwap;
import org.apache.kafka.connect.sink.ShareSinkTask;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.util.ConnectorTaskId;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SinkRecordConverterTest {
    private static final TopicPartition INPUT = new TopicPartition("input", 2);
    private static final byte[] KEY = "key".getBytes(UTF_8);

    @Mock private Transformation<SinkRecord> transformation;
    @Mock private ErrorHandlingMetrics errorMetrics;
    @Mock private LoaderSwap loaderSwap;
    @Mock private ShareSinkTask task;
    @Mock private ShareConsumer<byte[], byte[]> consumer;

    @Test
    void preservesRoutedTombstoneContextAndHeadersWhileCompletingIntentionalFilters() {
        List<String> stages = new ArrayList<>();
        SinkRecordConverter converter = converter(operator(ToleranceType.NONE, 0), new StringConverter(),
                topic -> stages.add("topic:" + topic));
        when(transformation.apply(any())).thenAnswer(invocation -> {
            SinkRecord record = invocation.getArgument(0);
            stages.add("transform:" + record.kafkaOffset());
            return record.kafkaOffset() == 8L ? null : record.newRecord("routed", 5,
                    record.keySchema(), record.key(), record.valueSchema(), record.value(),
                    record.timestamp(), record.headers());
        });
        Headers headers = new RecordHeaders().add("trace", "first".getBytes(UTF_8)).add("trace", null);
        ConsumerRecord<byte[], byte[]> tombstone = record(7L, null, headers);
        ConsumerRecord<byte[], byte[]> filtered = record(8L, "ignored".getBytes(UTF_8), new RecordHeaders());
        ShareSinkDelivery delivery = new ShareSinkDelivery(records(tombstone, filtered), converter::convertForShare);

        ShareSinkDelivery.CompletedDelivery completed = delivery.complete(task);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<SinkRecord>> delivered = ArgumentCaptor.forClass(Collection.class);
        verify(task).put(delivered.capture());
        assertEquals(1, delivered.getValue().size());
        InternalSinkRecord converted = assertInstanceOf(InternalSinkRecord.class, delivered.getValue().iterator().next());
        assertEquals("routed", converted.topic());
        assertEquals(5, converted.kafkaPartition());
        assertEquals("key", converted.key());
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, converted.valueSchema());
        assertNull(converted.value());
        assertEquals(42L, converted.timestamp());
        assertEquals(TimestampType.CREATE_TIME, converted.timestampType());
        assertEquals("first", converted.headers().iterator().next().value());
        assertNull(converted.headers().lastWithName("trace").value());
        assertEquals(INPUT.topic(), converted.originalTopic());
        assertEquals(INPUT.partition(), converted.originalKafkaPartition());
        assertEquals(7L, converted.originalKafkaOffset());
        assertSame(tombstone, converted.context().original());
        assertEquals(List.of("topic:input", "transform:7", "topic:input", "transform:8"), stages);
        verify(loaderSwap, times(8)).close();
        verifyNoInteractions(consumer);

        Duration timeout = Duration.ofSeconds(5);
        when(consumer.commitSync(timeout)).thenReturn(Map.of(new TopicIdPartition(Uuid.randomUuid(), INPUT), Optional.empty()));
        assertTrue(completed.acknowledge(consumer, timeout).allConfirmed());
        verify(consumer).acknowledge(tombstone, AcknowledgeType.ACCEPT);
        verify(consumer).acknowledge(filtered, AcknowledgeType.ACCEPT);
    }

    @Test
    void failedConversionTransformationAndCancelledRetryCannotCompleteAsFilters() {
        ConsumerRecord<byte[], byte[]> original = record(7L, null, new RecordHeaders());
        Converter failingKey = mock(Converter.class);
        DataException conversionFailure = new DataException("bad key");
        when(failingKey.toConnectData(eq(INPUT.topic()), any(Headers.class), eq(KEY))).thenThrow(conversionFailure);
        SinkRecordConverter failedConversion = converter(operator(ToleranceType.ALL, 0), failingKey, topic -> { });
        assertSame(conversionFailure, failedDelivery(original, failedConversion).getCause());

        DataException transformationFailure = new DataException("bad transform");
        when(transformation.apply(any())).thenThrow(transformationFailure);
        SinkRecordConverter failedTransformation = converter(operator(ToleranceType.ALL, 0), new StringConverter(), topic -> { });
        assertSame(transformationFailure, failedDelivery(original, failedTransformation).getCause());

        RetryWithToleranceOperator<ConsumerRecord<byte[], byte[]>> stoppedOperator = operator(ToleranceType.NONE, -1);
        stoppedOperator.triggerStop();
        Converter retryingKey = mock(Converter.class);
        RetriableException retryFailure = new RetriableException("cancelled retry");
        when(retryingKey.toConnectData(eq(INPUT.topic()), any(Headers.class), eq(KEY))).thenThrow(retryFailure);
        SinkRecordConverter cancelledConversion = converter(stoppedOperator, retryingKey, topic -> { });
        assertSame(retryFailure, failedDelivery(original, cancelledConversion).getCause());
        verify(retryingKey).toConnectData(INPUT.topic(), original.headers(), KEY);
        verifyNoInteractions(task, consumer);
    }

    private ConnectException failedDelivery(ConsumerRecord<byte[], byte[]> record, SinkRecordConverter converter) {
        return assertThrows(ConnectException.class,
                () -> new ShareSinkDelivery(records(record), converter::convertForShare).complete(task));
    }

    private RetryWithToleranceOperator<ConsumerRecord<byte[], byte[]>> operator(ToleranceType tolerance, long retryTimeout) {
        return new RetryWithToleranceOperator<>(retryTimeout, 1L, tolerance, new MockTime(), errorMetrics);
    }

    private SinkRecordConverter converter(RetryWithToleranceOperator<ConsumerRecord<byte[], byte[]>> operator,
                                          Converter keyConverter, Consumer<String> activeTopicRecorder) {
        StringConverter stringConverter = new StringConverter();
        TransformationStage<SinkRecord> stage = new TransformationStage<>(
                Plugin.wrapInstance(transformation, null, "transform"), "transform", "1", ignored -> loaderSwap);
        return new SinkRecordConverter(new ConnectorTaskId("sink", 0),
                Plugin.wrapInstance(keyConverter, null, "key.converter"),
                Plugin.<Converter>wrapInstance(stringConverter, null, "value.converter"),
                Plugin.<HeaderConverter>wrapInstance(stringConverter, null, "header.converter"),
                new TransformationChain<>(List.of(stage), operator), operator, ignored -> loaderSwap, activeTopicRecorder);
    }

    private ConsumerRecord<byte[], byte[]> record(long offset, byte[] value, Headers headers) {
        return new ConsumerRecord<>(INPUT.topic(), INPUT.partition(), offset, offset + 35L, TimestampType.CREATE_TIME,
                KEY.length, value == null ? ConsumerRecord.NULL_SIZE : value.length, KEY, value, headers, Optional.empty());
    }

    @SafeVarargs
    private final ConsumerRecords<byte[], byte[]> records(ConsumerRecord<byte[], byte[]>... records) {
        return new ConsumerRecords<>(Map.of(INPUT, List.of(records)), Map.of());
    }
}
