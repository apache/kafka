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
package org.apache.kafka.tools.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.MessageFormatter;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DefaultMessageFormatterTest {

    @Test
    public void testDefaultMessageFormatter() {
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("topic", 0, 123, "key".getBytes(), "value".getBytes());
        MessageFormatter formatter = new DefaultMessageFormatter();
        Map<String, String> configs = new HashMap<>();

        formatter.configure(configs);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        formatter.writeTo(record, new PrintStream(out));
        assertEquals("value\n", out.toString());

        configs.put("print.key", "true");
        formatter.configure(configs);
        out = new ByteArrayOutputStream();
        formatter.writeTo(record, new PrintStream(out));
        assertEquals("key\tvalue\n", out.toString());

        configs.put("print.partition", "true");
        formatter.configure(configs);
        out = new ByteArrayOutputStream();
        formatter.writeTo(record, new PrintStream(out));
        assertEquals("Partition:0\tkey\tvalue\n", out.toString());

        configs.put("print.timestamp", "true");
        formatter.configure(configs);
        out = new ByteArrayOutputStream();
        formatter.writeTo(record, new PrintStream(out));
        assertEquals("NO_TIMESTAMP\tPartition:0\tkey\tvalue\n", out.toString());

        configs.put("print.offset", "true");
        formatter.configure(configs);
        out = new ByteArrayOutputStream();
        formatter.writeTo(record, new PrintStream(out));
        assertEquals("NO_TIMESTAMP\tPartition:0\tOffset:123\tkey\tvalue\n", out.toString());

        configs.put("print.headers", "true");
        formatter.configure(configs);
        out = new ByteArrayOutputStream();
        formatter.writeTo(record, new PrintStream(out));
        assertEquals("NO_TIMESTAMP\tPartition:0\tOffset:123\tNO_HEADERS\tkey\tvalue\n", out.toString());

        configs.put("print.delivery", "true");
        formatter.configure(configs);
        out = new ByteArrayOutputStream();
        formatter.writeTo(record, new PrintStream(out));
        assertEquals("NO_TIMESTAMP\tPartition:0\tOffset:123\tDelivery:NOT_PRESENT\tNO_HEADERS\tkey\tvalue\n", out.toString());

        configs.put("print.epoch", "true");
        formatter.configure(configs);
        out = new ByteArrayOutputStream();
        formatter.writeTo(record, new PrintStream(out));
        assertEquals("NO_TIMESTAMP\tPartition:0\tOffset:123\tDelivery:NOT_PRESENT\tEpoch:NOT_PRESENT\tNO_HEADERS\tkey\tvalue\n", out.toString());

        RecordHeaders headers = new RecordHeaders();
        headers.add("h1", "v1".getBytes());
        headers.add("h2", "v2".getBytes());
        record = new ConsumerRecord<>("topic", 0, 123, 123L, TimestampType.CREATE_TIME, -1, -1, "key".getBytes(), "value".getBytes(),
                headers, Optional.of(58), Optional.of((short) 1));
        out = new ByteArrayOutputStream();
        formatter.writeTo(record, new PrintStream(out));
        assertEquals("CreateTime:123\tPartition:0\tOffset:123\tDelivery:1\tEpoch:58\th1:v1,h2:v2\tkey\tvalue\n", out.toString());

        configs.put("print.value", "false");
        formatter.configure(configs);
        out = new ByteArrayOutputStream();
        formatter.writeTo(record, new PrintStream(out));
        assertEquals("CreateTime:123\tPartition:0\tOffset:123\tDelivery:1\tEpoch:58\th1:v1,h2:v2\tkey\n", out.toString());

        configs.put("key.separator", "<sep>");
        formatter.configure(configs);
        out = new ByteArrayOutputStream();
        formatter.writeTo(record, new PrintStream(out));
        assertEquals("CreateTime:123<sep>Partition:0<sep>Offset:123<sep>Delivery:1<sep>Epoch:58<sep>h1:v1,h2:v2<sep>key\n", out.toString());

        configs.put("print.delivery", "false");
        formatter.configure(configs);
        out = new ByteArrayOutputStream();
        formatter.writeTo(record, new PrintStream(out));
        assertEquals("CreateTime:123<sep>Partition:0<sep>Offset:123<sep>Epoch:58<sep>h1:v1,h2:v2<sep>key\n", out.toString());

        configs.put("print.epoch", "false");
        formatter.configure(configs);
        out = new ByteArrayOutputStream();
        formatter.writeTo(record, new PrintStream(out));
        assertEquals("CreateTime:123<sep>Partition:0<sep>Offset:123<sep>h1:v1,h2:v2<sep>key\n", out.toString());

        configs.put("line.separator", "<end>");
        formatter.configure(configs);
        out = new ByteArrayOutputStream();
        formatter.writeTo(record, new PrintStream(out));
        assertEquals("CreateTime:123<sep>Partition:0<sep>Offset:123<sep>h1:v1,h2:v2<sep>key<end>", out.toString());

        configs.put("headers.separator", "|");
        formatter.configure(configs);
        out = new ByteArrayOutputStream();
        formatter.writeTo(record, new PrintStream(out));
        assertEquals("CreateTime:123<sep>Partition:0<sep>Offset:123<sep>h1:v1|h2:v2<sep>key<end>", out.toString());

        record = new ConsumerRecord<>("topic", 0, 123, 123L, TimestampType.CREATE_TIME, -1, -1, "key".getBytes(), "value".getBytes(),
                headers, Optional.empty());

        configs.put("key.deserializer", UpperCaseDeserializer.class.getName());
        formatter.configure(configs);
        out = new ByteArrayOutputStream();
        formatter.writeTo(record, new PrintStream(out));
        assertEquals("CreateTime:123<sep>Partition:0<sep>Offset:123<sep>h1:v1|h2:v2<sep>KEY<end>", out.toString());

        configs.put("headers.deserializer", UpperCaseDeserializer.class.getName());
        formatter.configure(configs);
        out = new ByteArrayOutputStream();
        formatter.writeTo(record, new PrintStream(out));
        assertEquals("CreateTime:123<sep>Partition:0<sep>Offset:123<sep>h1:V1|h2:V2<sep>KEY<end>", out.toString());

        record = new ConsumerRecord<>("topic", 0, 123, 123L, TimestampType.CREATE_TIME, -1, -1, "key".getBytes(), null,
                headers, Optional.empty());

        configs.put("print.value", "true");
        configs.put("null.literal", "<null>");
        formatter.configure(configs);
        out = new ByteArrayOutputStream();
        formatter.writeTo(record, new PrintStream(out));
        assertEquals("CreateTime:123<sep>Partition:0<sep>Offset:123<sep>h1:V1|h2:V2<sep>KEY<sep><null><end>", out.toString());
        formatter.close();
    }

    static class UpperCaseDeserializer implements Deserializer<String> {

        @Override
        public String deserialize(String topic, byte[] data) {
            return new String(data, StandardCharsets.UTF_8).toUpperCase(Locale.ROOT);
        }
    }
}
