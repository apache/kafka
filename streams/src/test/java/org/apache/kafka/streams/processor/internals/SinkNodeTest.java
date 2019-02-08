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

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class SinkNodeTest {
    private final Serializer<byte[]> anySerializer = Serdes.ByteArray().serializer();
    private final StateSerdes<Bytes, Bytes> anyStateSerde = StateSerdes.withBuiltinTypes("anyName", Bytes.class, Bytes.class);
    private final RecordCollector recordCollector =  new RecordCollectorImpl(
        null,
        new LogContext("sinknode-test "),
        new DefaultProductionExceptionHandler(),
        new Metrics().sensor("skipped-records")
    );

    private final InternalMockProcessorContext context = new InternalMockProcessorContext(
        anyStateSerde,
        recordCollector
    );
    private final SinkNode<byte[], byte[]> sink = new SinkNode<>("anyNodeName",
            new StaticTopicNameExtractor<>("any-output-topic"), anySerializer, anySerializer, null);

    // Used to verify that the correct exceptions are thrown if the compiler checks are bypassed
    @SuppressWarnings("unchecked")
    private final SinkNode<Object, Object> illTypedSink = (SinkNode) sink;

    @Before
    public void before() {
        recordCollector.init(new MockProducer<>(true, anySerializer, anySerializer));
        sink.init(context);
    }

    @Test
    public void shouldThrowStreamsExceptionOnInputRecordWithInvalidTimestamp() {
        final Bytes anyKey = new Bytes("any key".getBytes());
        final Bytes anyValue = new Bytes("any value".getBytes());

        // When/Then
        context.setTime(-1); // ensures a negative timestamp is set for the record we send next
        try {
            illTypedSink.process(anyKey, anyValue);
            fail("Should have thrown StreamsException");
        } catch (final StreamsException ignored) {
            // expected
        }
    }

    @Test
    public void shouldThrowStreamsExceptionOnKeyValueTypeSerializerMismatch() {
        final String keyOfDifferentTypeThanSerializer = "key with different type";
        final String valueOfDifferentTypeThanSerializer = "value with different type";

        // When/Then
        context.setTime(0);
        try {
            illTypedSink.process(keyOfDifferentTypeThanSerializer, valueOfDifferentTypeThanSerializer);
            fail("Should have thrown StreamsException");
        } catch (final StreamsException e) {
            assertThat(e.getCause(), instanceOf(ClassCastException.class));
        }
    }

    @Test
    public void shouldHandleNullKeysWhenThrowingStreamsExceptionOnKeyValueTypeSerializerMismatch() {
        final String invalidValueToTriggerSerializerMismatch = "";

        // When/Then
        context.setTime(1);
        try {
            illTypedSink.process(null, invalidValueToTriggerSerializerMismatch);
            fail("Should have thrown StreamsException");
        } catch (final StreamsException e) {
            assertThat(e.getCause(), instanceOf(ClassCastException.class));
            assertThat(e.getMessage(), containsString("unknown because key is null"));
        }
    }

    @Test
    public void shouldHandleNullValuesWhenThrowingStreamsExceptionOnKeyValueTypeSerializerMismatch() {
        final String invalidKeyToTriggerSerializerMismatch = "";

        // When/Then
        context.setTime(1);
        try {
            illTypedSink.process(invalidKeyToTriggerSerializerMismatch, null);
            fail("Should have thrown StreamsException");
        } catch (final StreamsException e) {
            assertThat(e.getCause(), instanceOf(ClassCastException.class));
            assertThat(e.getMessage(), containsString("unknown because value is null"));
        }
    }

}
