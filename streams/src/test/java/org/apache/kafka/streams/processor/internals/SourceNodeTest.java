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
import org.apache.kafka.common.serialization.ExtendedDeserializer;
import org.apache.kafka.test.MockSourceNode;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class SourceNodeTest {
    @Test
    public void shouldProvideTopicHeadersAndDataToKeyDeserializer() {
        SourceNode<String, String> sourceNode = new MockSourceNode<>(new String[]{""}, new TheExtendedDeserializer(), new TheExtendedDeserializer());
        RecordHeaders headers = new RecordHeaders();
        String deserializeKey = sourceNode.deserializeKey("topic", headers, "data".getBytes(StandardCharsets.UTF_8));
        assertThat(deserializeKey, is("topic" + headers + "data"));
    }

    @Test
    public void shouldProvideTopicHeadersAndDataToValueDeserializer() {
        SourceNode<String, String> sourceNode = new MockSourceNode<>(new String[]{""}, new TheExtendedDeserializer(), new TheExtendedDeserializer());
        RecordHeaders headers = new RecordHeaders();
        String deserializeKey = sourceNode.deserializeValue("topic", headers, "data".getBytes(StandardCharsets.UTF_8));
        assertThat(deserializeKey, is("topic" + headers + "data"));
    }

    public static class TheExtendedDeserializer implements ExtendedDeserializer<String> {
        @Override
        public String deserialize(String topic, Headers headers, byte[] data) {
            return topic + headers + new String(data, StandardCharsets.UTF_8);
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public String deserialize(String topic, byte[] data) {
            return deserialize(topic, null, data);
        }

        @Override
        public void close() {

        }
    }
}