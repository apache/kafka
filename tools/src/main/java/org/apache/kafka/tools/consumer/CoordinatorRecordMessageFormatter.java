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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorLoader;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class CoordinatorRecordMessageFormatter implements MessageFormatter {

    private static final String TYPE = "type";
    private static final String VERSION = "version";
    private static final String DATA = "data";
    private static final String KEY = "key";
    private static final String VALUE = "value";

    @Override
    public void writeTo(ConsumerRecord<byte[], byte[]> consumerRecord, PrintStream output) {
        if (Objects.isNull(consumerRecord.key())) return;

        ObjectNode json = new ObjectNode(JsonNodeFactory.instance);
        try {
            CoordinatorRecord record = deserialize(
                consumerRecord.key() != null ? ByteBuffer.wrap(consumerRecord.key()) : null,
                consumerRecord.value() != null ? ByteBuffer.wrap(consumerRecord.value()) : null
            );
            if (!shouldPrint(record.key().apiKey())) return;

            json
                .putObject(KEY)
                .put(TYPE, record.key().apiKey())
                .set(DATA, keyAsJson(record.key()));

            if (Objects.nonNull(record.value())) {
                json
                    .putObject(VALUE)
                    .put(VERSION, record.value().version())
                    .set(DATA, valueAsJson(record.value().message(), record.value().version()));
            } else {
                json.set(VALUE, NullNode.getInstance());
            }
        } catch (CoordinatorLoader.UnknownRecordTypeException ex) {
            return;
        } catch (RuntimeException ex) {
            throw new RuntimeException("Could not read record at offset " + consumerRecord.offset() +
                " due to: " + ex.getMessage(), ex);
        }

        try {
            output.write(json.toString().getBytes(UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract CoordinatorRecord deserialize(ByteBuffer key, ByteBuffer value);
    protected abstract boolean shouldPrint(short recordType);
    protected abstract JsonNode keyAsJson(ApiMessage message);
    protected abstract JsonNode valueAsJson(ApiMessage message, short version);
}  
