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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class BaseConsumerTest {
    public static final AtomicInteger updateProducerCount = new AtomicInteger();
    public static final AtomicInteger updateConsumerCount = new AtomicInteger();

    public static class TestClusterResourceListenerSerializer implements Serializer<byte[]>, ClusterResourceListener {
        @Override
        public void onUpdate(ClusterResource clusterResource) {
            updateProducerCount.incrementAndGet();
        }

        @Override
        public byte[] serialize(String topic, byte[] data) {
            return data;
        }
    }

    public static class TestClusterResourceListenerDeserializer implements Deserializer<byte[]>, ClusterResourceListener {
        @Override
        public void onUpdate(ClusterResource clusterResource) {
            updateConsumerCount.incrementAndGet();
        }

        @Override
        public byte[] deserialize(String topic, byte[] data) {
            return data;
        }
    }

    public static class SerializerImpl implements Serializer<byte[]> {
        private ByteArraySerializer serializer = new ByteArraySerializer();

        @Override
        public byte[] serialize(String topic, Headers headers, byte[] data) {
            headers.add("content-type", "application/octet-stream".getBytes());
            return serializer.serialize(topic, data);
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            serializer.configure(configs, isKey);
        }

        @Override
        public void close() {
            serializer.close();
        }

        @Override
        public byte[] serialize(String topic, byte[] data) {
            fail("method should not be invoked");
            return null;
        }
    }

    public static class DeserializerImpl implements Deserializer<byte[]> {
        private ByteArrayDeserializer deserializer = new ByteArrayDeserializer();

        @Override
        public byte[] deserialize(String topic, Headers headers, byte[] data) {
            Header header = headers.lastHeader("content-type");
            assertEquals("application/octet-stream", header == null ? null : new String(header.value()));
            return deserializer.deserialize(topic, data);
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            deserializer.configure(configs, isKey);
        }

        @Override
        public void close() {
            deserializer.close();
        }

        @Override
        public byte[] deserialize(String topic, byte[] data) {
            fail("method should not be invoked");
            return null;
        }
    }
}
