/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.MockProcessorContext;
import org.junit.Test;

import java.util.Properties;

public class SinkNodeTest {

    @Test(expected = StreamsException.class)
    @SuppressWarnings("unchecked")
    public void invalidInputRecordTimestampTest() {
        final Serializer anySerializer = Serdes.Bytes().serializer();
        final StateSerdes anyStateSerde = StateSerdes.withBuiltinTypes("anyName", Bytes.class, Bytes.class);

        final MockProcessorContext context = new MockProcessorContext(anyStateSerde,  new RecordCollector(null, null));
        context.setTime(-1);

        final SinkNode sink = new SinkNode<>("name", "output-topic", anySerializer, anySerializer, null);
        sink.init(context);

        sink.process(null, null);
    }

    @Test(expected = StreamsException.class)
    @SuppressWarnings("unchecked")
    public void shouldThrowStreamsExceptionOnKeyValyeTypeSerializerMissmatch() {
        final Serializer anySerializer = Serdes.Bytes().serializer();
        final StateSerdes anyStateSerde = StateSerdes.withBuiltinTypes("anyName", Bytes.class, Bytes.class);

        Properties config = new Properties();
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        final MockProcessorContext context = new MockProcessorContext(anyStateSerde, new RecordCollector(new MockProducer<byte[], byte[]>(true, anySerializer, anySerializer), null));
        context.setTime(0);

        final SinkNode sink = new SinkNode<>("name", "output-topic", anySerializer, anySerializer, null);
        sink.init(context);

        try {
            sink.process("", "");
        } catch (final StreamsException e) {
            if (e.getCause() instanceof ClassCastException) {
                throw e;
            }
            throw new RuntimeException(e);
        }
    }

}