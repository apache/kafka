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
import org.apache.kafka.common.protocol.ApiMessage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class ApiMessageFormatter implements MessageFormatter {

    private static final String VERSION = "version";
    private static final String DATA = "data";
    private static final String KEY = "key";
    private static final String VALUE = "value";
    static final String UNKNOWN = "unknown";

    @Override
    public void writeTo(ConsumerRecord<byte[], byte[]> consumerRecord, PrintStream output) {
        ObjectNode json = new ObjectNode(JsonNodeFactory.instance);

        byte[] key = consumerRecord.key();
        if (Objects.nonNull(key)) {
            short keyVersion = ByteBuffer.wrap(key).getShort();
            JsonNode dataNode = readToKeyNode(ByteBuffer.wrap(key), keyVersion);

            if (dataNode instanceof NullNode) {
                return;
            }
            json.putObject(KEY)
                    .put(VERSION, keyVersion)
                    .set(DATA, dataNode);
        } else {
            json.set(KEY, NullNode.getInstance());
        }

        byte[] value = consumerRecord.value();
        if (Objects.nonNull(value)) {
            short valueVersion = ByteBuffer.wrap(value).getShort();
            JsonNode dataNode = readToValueNode(ByteBuffer.wrap(value), valueVersion);

            json.putObject(VALUE)
                    .put(VERSION, valueVersion)
                    .set(DATA, dataNode);
        } else {
            json.set(VALUE, NullNode.getInstance());
        }

        try {
            output.write(json.toString().getBytes(UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private JsonNode readToKeyNode(ByteBuffer byteBuffer, short version) {
        return readToKeyMessage(byteBuffer)
                .map(logKey -> transferKeyMessageToJsonNode(logKey, version))
                .orElseGet(() -> new TextNode(UNKNOWN));
    }
    
    private JsonNode readToValueNode(ByteBuffer byteBuffer, short version) {
        return readToValueMessage(byteBuffer)
                .map(logValue -> transferValueMessageToJsonNode(logValue, version))
                .orElseGet(() -> new TextNode(UNKNOWN));
    }
    
    abstract Optional<ApiMessage> readToKeyMessage(ByteBuffer byteBuffer);
    abstract JsonNode transferKeyMessageToJsonNode(ApiMessage message, short version);
    abstract Optional<ApiMessage> readToValueMessage(ByteBuffer byteBuffer);
    abstract JsonNode transferValueMessageToJsonNode(ApiMessage message, short version);
}
