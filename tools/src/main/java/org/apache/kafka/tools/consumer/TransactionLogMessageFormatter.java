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
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogKey;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogKeyJsonConverter;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogValue;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogValueJsonConverter;

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

public class TransactionLogMessageFormatter implements MessageFormatter {

    private static final String VERSION = "version";
    private static final String DATA = "data";
    private static final String KEY = "key";
    private static final String VALUE = "value";
    private static final String UNKNOWN = "unknown";

    @Override
    public void writeTo(ConsumerRecord<byte[], byte[]> consumerRecord, PrintStream output) {
        ObjectNode json = new ObjectNode(JsonNodeFactory.instance);

        byte[] key = consumerRecord.key();
        if (Objects.nonNull(key)) {
            short keyVersion = ByteBuffer.wrap(key).getShort();
            JsonNode dataNode = readToTransactionLogKey(ByteBuffer.wrap(key))
                    .map(logKey -> TransactionLogKeyJsonConverter.write(logKey, keyVersion))
                    .orElseGet(() -> new TextNode(UNKNOWN));
            json.putObject(KEY)
                    .put(VERSION, keyVersion)
                    .set(DATA, dataNode);
        } else {
            json.set(KEY, NullNode.getInstance());
        }

        byte[] value = consumerRecord.value();
        if (Objects.nonNull(value)) {
            short valueVersion = ByteBuffer.wrap(value).getShort();
            JsonNode dataNode = readToTransactionLogValue(ByteBuffer.wrap(value))
                    .map(logValue -> TransactionLogValueJsonConverter.write(logValue, valueVersion))
                    .orElseGet(() -> new TextNode(UNKNOWN));
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

    private Optional<TransactionLogKey> readToTransactionLogKey(ByteBuffer byteBuffer) {
        short version = byteBuffer.getShort();
        if (version >= TransactionLogKey.LOWEST_SUPPORTED_VERSION
                && version <= TransactionLogKey.HIGHEST_SUPPORTED_VERSION) {
            return Optional.of(new TransactionLogKey(new ByteBufferAccessor(byteBuffer), version));
        } else {
            return Optional.empty();
        }
    }

    private Optional<TransactionLogValue> readToTransactionLogValue(ByteBuffer byteBuffer) {
        short version = byteBuffer.getShort();
        if (version >= TransactionLogValue.LOWEST_SUPPORTED_VERSION
                && version <= TransactionLogValue.HIGHEST_SUPPORTED_VERSION) {
            return Optional.of(new TransactionLogValue(new ByteBufferAccessor(byteBuffer), version));
        } else {
            return Optional.empty();
        }
    }
}
