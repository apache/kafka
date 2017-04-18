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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.MockProcessorContext;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class SinkNodeTest {

    @Test
    @SuppressWarnings("unchecked")
    public void shouldThrowStreamsExceptionOnInputRecordWithInvalidTimestamp() {
        // Given
        final Serializer anySerializer = Serdes.Bytes().serializer();
        final StateSerdes anyStateSerde = StateSerdes.withBuiltinTypes("anyName", Bytes.class, Bytes.class);
        final MockProcessorContext context = new MockProcessorContext(anyStateSerde,
            new RecordCollectorImpl(new MockProducer<byte[], byte[]>(true, anySerializer, anySerializer), null));
        final SinkNode sink = new SinkNode<>("anyNodeName", "any-output-topic", anySerializer, anySerializer, null);
        sink.init(context);
        final Bytes anyKey = new Bytes("any key".getBytes());
        final Bytes anyValue = new Bytes("any value".getBytes());

        // When/Then
        context.setTime(-1); // ensures a negative timestamp is set for the record we send next
        try {
            sink.process(anyKey, anyValue);
            fail("Should have thrown StreamsException");
        } catch (final StreamsException ignored) {
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldThrowStreamsExceptionOnKeyValueTypeSerializerMismatch() {
        // Given
        final Serializer anySerializer = Serdes.Bytes().serializer();
        final StateSerdes anyStateSerde = StateSerdes.withBuiltinTypes("anyName", Bytes.class, Bytes.class);
        final MockProcessorContext context = new MockProcessorContext(anyStateSerde,
            new RecordCollectorImpl(new MockProducer<byte[], byte[]>(true, anySerializer, anySerializer), null));
        context.setTime(0);
        final SinkNode sink = new SinkNode<>("anyNodeName", "any-output-topic", anySerializer, anySerializer, null);
        sink.init(context);
        final String keyOfDifferentTypeThanSerializer = "key with different type";
        final String valueOfDifferentTypeThanSerializer = "value with different type";

        // When/Then
        try {
            sink.process(keyOfDifferentTypeThanSerializer, valueOfDifferentTypeThanSerializer);
            fail("Should have thrown StreamsException");
        } catch (final StreamsException e) {
            assertThat(e.getCause(), instanceOf(ClassCastException.class));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldHandleNullKeysWhenThrowingStreamsExceptionOnKeyValueTypeSerializerMismatch() {
        // Given
        final Serializer anySerializer = Serdes.Bytes().serializer();
        final StateSerdes anyStateSerde = StateSerdes.withBuiltinTypes("anyName", Bytes.class, Bytes.class);
        final MockProcessorContext context = new MockProcessorContext(anyStateSerde,
            new RecordCollectorImpl(new MockProducer<byte[], byte[]>(true, anySerializer, anySerializer), null));
        context.setTime(1);
        final SinkNode sink = new SinkNode<>("anyNodeName", "any-output-topic", anySerializer, anySerializer, null);
        sink.init(context);
        final String invalidValueToTriggerSerializerMismatch = "";

        // When/Then
        try {
            sink.process(null, invalidValueToTriggerSerializerMismatch);
            fail("Should have thrown StreamsException");
        } catch (final StreamsException e) {
            assertThat(e.getCause(), instanceOf(ClassCastException.class));
            assertThat(e.getMessage(), containsString("unknown because key is null"));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldHandleNullValuesWhenThrowingStreamsExceptionOnKeyValueTypeSerializerMismatch() {
        // Given
        final Serializer anySerializer = Serdes.Bytes().serializer();
        final StateSerdes anyStateSerde = StateSerdes.withBuiltinTypes("anyName", Bytes.class, Bytes.class);
        final MockProcessorContext context = new MockProcessorContext(anyStateSerde,
            new RecordCollectorImpl(new MockProducer<byte[], byte[]>(true, anySerializer, anySerializer), null));
        context.setTime(1);
        final SinkNode sink = new SinkNode<>("anyNodeName", "any-output-topic", anySerializer, anySerializer, null);
        sink.init(context);
        final String invalidKeyToTriggerSerializerMismatch = "";

        // When/Then
        try {
            sink.process(invalidKeyToTriggerSerializerMismatch, null);
            fail("Should have thrown StreamsException");
        } catch (final StreamsException e) {
            assertThat(e.getCause(), instanceOf(ClassCastException.class));
            assertThat(e.getMessage(), containsString("unknown because value is null"));
        }
    }

}