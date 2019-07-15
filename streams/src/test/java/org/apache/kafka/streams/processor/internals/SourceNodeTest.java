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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.test.MockSourceNode;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class SourceNodeTest {
    @Test
    public void shouldProvideTopicHeadersAndDataToKeyDeserializer() {
        final SourceNode<String, String> sourceNode = new MockSourceNode<>(new String[]{""}, new TheDeserializer(), new TheDeserializer());
        final RecordHeaders headers = new RecordHeaders();
        final String deserializeKey = sourceNode.deserializeKey("topic", headers, "data".getBytes(StandardCharsets.UTF_8));
        assertThat(deserializeKey, is("topic" + headers + "data"));
    }

    @Test
    public void shouldProvideTopicHeadersAndDataToValueDeserializer() {
        final SourceNode<String, String> sourceNode = new MockSourceNode<>(new String[]{""}, new TheDeserializer(), new TheDeserializer());
        final RecordHeaders headers = new RecordHeaders();
        final String deserializedValue = sourceNode.deserializeValue("topic", headers, "data".getBytes(StandardCharsets.UTF_8));
        assertThat(deserializedValue, is("topic" + headers + "data"));
    }

    public static class TheDeserializer implements Deserializer<String> {
        @Override
        public String deserialize(final String topic, final Headers headers, final byte[] data) {
            return topic + headers + new String(data, StandardCharsets.UTF_8);
        }

        @Override
        public String deserialize(final String topic, final byte[] data) {
            return deserialize(topic, null, data);
        }
    }
}
