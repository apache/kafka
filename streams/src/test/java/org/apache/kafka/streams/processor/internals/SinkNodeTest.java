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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;

public class SinkNodeTest {
    private final StateSerdes<Bytes, Bytes> anyStateSerde = StateSerdes.withBuiltinTypes("anyName", Bytes.class, Bytes.class);
    private final Serializer<byte[]> anySerializer = Serdes.ByteArray().serializer();
    private final RecordCollector recordCollector = new MockRecordCollector();
    private final InternalMockProcessorContext context = new InternalMockProcessorContext(anyStateSerde, recordCollector);
    private final SinkNode<byte[], byte[], ?, ?> sink = new SinkNode<>("anyNodeName",
            new StaticTopicNameExtractor<>("any-output-topic"), anySerializer, anySerializer, null);

    // Used to verify that the correct exceptions are thrown if the compiler checks are bypassed
    @SuppressWarnings("unchecked")
    private final SinkNode<Object, Object, ?, ?> illTypedSink = (SinkNode) sink;

    @Before
    public void before() {
        sink.init(context);
    }

    @Test
    public void shouldThrowStreamsExceptionOnInputRecordWithInvalidTimestamp() {
        // When/Then
        context.setTime(-1); // ensures a negative timestamp is set for the record we send next
        try {
            illTypedSink.process(new Record<>("any key".getBytes(), "any value".getBytes(), -1));
            fail("Should have thrown StreamsException");
        } catch (final StreamsException ignored) {
            // expected
        }
    }

}
