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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.MessageFormatter;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogKey;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogKeyJsonConverter;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogValue;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogValueJsonConverter;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

public class TransactionLogMessageFormatter implements MessageFormatter {

    private static final String VERSION = "version";
    private static final String DATA = "data";


    @Override
    public void writeTo(ConsumerRecord<byte[], byte[]> consumerRecord, PrintStream output) {
        Optional.ofNullable(consumerRecord.key())
            .ifPresent(key -> {
                byte[] value = consumerRecord.value();
                short keyVersion = ByteBuffer.wrap(key).getShort();
                short valueVersion = ByteBuffer.wrap(value).getShort();

                ObjectNode json = new ObjectNode(JsonNodeFactory.instance);
                Optional<TransactionLogKey> transactionLogKey = readToTransactionLogKey(ByteBuffer.wrap(key));
                Optional<TransactionLogValue> transactionLogValue = readToTransactionLogValue(ByteBuffer.wrap(value));

                settingKeyNode(json, transactionLogKey, keyVersion);
                settingValueNode(json, transactionLogValue, valueVersion);

                try {
                    output.write(json.toString().getBytes(UTF_8));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
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

    private void settingKeyNode(ObjectNode json, Optional<TransactionLogKey> transactionLogKey, short keyVersion) {
        String key = "key";
        if (transactionLogKey.isPresent()) {
            addDataNode(json, key, keyVersion,
                    TransactionLogKeyJsonConverter.write(transactionLogKey.get(), keyVersion));
        } else {
            addUnknownNode(json, key, keyVersion);
            
        }
    }

    private void settingValueNode(ObjectNode json, Optional<TransactionLogValue> transactionLogValue, short valueVersion) {
        String value = "value";
        if (transactionLogValue.isPresent()) {
            addDataNode(json, value, valueVersion,
                    TransactionLogValueJsonConverter.write(transactionLogValue.get(), valueVersion));
        } else {
            addUnknownNode(json, value, valueVersion);
        }
    }

    private void addUnknownNode(ObjectNode json, String key, short keyVersion) {
        json.putObject(key)
                .put(VERSION, Short.toString(keyVersion))
                .put(DATA, "unknown");
    }

    private static void addDataNode(ObjectNode json, String value,
                                    short valueVersion, JsonNode data) {
        json.putObject(value)
                .put(VERSION, Short.toString(valueVersion))
                .set(DATA, data);
    }
}
