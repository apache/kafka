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
package org.apache.kafka.tools.consumer.group.share;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.MessageFormatter;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotKey;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotKeyJsonConverter;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotValue;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotValueJsonConverter;
import org.apache.kafka.coordinator.share.generated.ShareUpdateKey;
import org.apache.kafka.coordinator.share.generated.ShareUpdateKeyJsonConverter;
import org.apache.kafka.coordinator.share.generated.ShareUpdateValue;
import org.apache.kafka.coordinator.share.generated.ShareUpdateValueJsonConverter;

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

/**
 * Formatter for records of in __share_group_state topic.
 */
public class ShareGroupStateMessageFormatter implements MessageFormatter {

    private static final String VERSION = "version";
    private static final String DATA = "data";
    private static final String KEY = "key";
    private static final String VALUE = "value";
    private static final String UNKNOWN = "unknown";

    @Override
    public void writeTo(ConsumerRecord<byte[], byte[]> consumerRecord, PrintStream output) {
        ObjectNode json = new ObjectNode(JsonNodeFactory.instance);

        byte[] key = consumerRecord.key();
        short keyVersion = -1;
        if (Objects.nonNull(key)) {
            keyVersion = ByteBuffer.wrap(key).getShort();
            JsonNode dataNode = readToKeyJson(ByteBuffer.wrap(key), keyVersion);

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
            JsonNode dataNode = readToValueJson(ByteBuffer.wrap(value), keyVersion, valueVersion);

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

    private JsonNode readToKeyJson(ByteBuffer byteBuffer, short version) {
        return readToSnapshotMessageKey(byteBuffer)
            .map(logKey -> transferKeyMessageToJsonNode(logKey, version))
            .orElseGet(() -> new TextNode(UNKNOWN));
    }

    private Optional<ApiMessage> readToSnapshotMessageKey(ByteBuffer byteBuffer) {
        short version = byteBuffer.getShort();
        if (version >= ShareSnapshotKey.LOWEST_SUPPORTED_VERSION
            && version <= ShareSnapshotKey.HIGHEST_SUPPORTED_VERSION) {
            return Optional.of(new ShareSnapshotKey(new ByteBufferAccessor(byteBuffer), version));
        } else if (version >= ShareUpdateKey.LOWEST_SUPPORTED_VERSION
            && version <= ShareUpdateKey.HIGHEST_SUPPORTED_VERSION) {
            return Optional.of(new ShareUpdateKey(new ByteBufferAccessor(byteBuffer), version));
        } else {
            return Optional.empty();
        }
    }

    private JsonNode transferKeyMessageToJsonNode(ApiMessage logKey, short keyVersion) {
        if (logKey instanceof ShareSnapshotKey) {
            return ShareSnapshotKeyJsonConverter.write((ShareSnapshotKey) logKey, keyVersion);
        } else if (logKey instanceof ShareUpdateKey) {
            return ShareUpdateKeyJsonConverter.write((ShareUpdateKey) logKey, keyVersion);
        }
        return new TextNode(UNKNOWN);
    }

    /**
     * Here the valueVersion is not enough to identity the deserializer for the ByteBuffer.
     * This is because both {@link ShareSnapshotValue} and {@link ShareUpdateValue} have version 0
     * as per RPC spec.
     * To differentiate, we need to use the corresponding key versions. This is acceptable as
     * the records will always appear in pairs (key, value). However, this means that we cannot
     * extend {@link org.apache.kafka.tools.consumer.ApiMessageFormatter} as it requires overriding
     * readToValueJson whose signature does not allow for passing keyversion.
     *
     * @param byteBuffer - Represents the raw data read from the topic
     * @param keyVersion - Version of the actual key component of the data read from topic
     * @param valueVersion - Version of the actual value component of the data read from topic
     * @return JsonNode corresponding to the raw data value component
     */
    protected JsonNode readToValueJson(ByteBuffer byteBuffer, short keyVersion, short valueVersion) {
        return readToSnapshotMessageValue(byteBuffer, keyVersion)
            .map(logValue -> transferValueMessageToJsonNode(logValue, valueVersion))
            .orElseGet(() -> new TextNode(UNKNOWN));
    }

    private JsonNode transferValueMessageToJsonNode(ApiMessage logValue, short version) {
        if (logValue instanceof ShareSnapshotValue) {
            return ShareSnapshotValueJsonConverter.write((ShareSnapshotValue) logValue, version);
        } else if (logValue instanceof ShareUpdateValue) {
            return ShareUpdateValueJsonConverter.write((ShareUpdateValue) logValue, version);
        }
        return new TextNode(UNKNOWN);
    }

    private Optional<ApiMessage> readToSnapshotMessageValue(ByteBuffer byteBuffer, short keyVersion) {
        short version = byteBuffer.getShort();
        // Check the key version here as that will determine which type
        // of value record to fetch. Both share update and share snapshot
        // value records can have the same version.
        if (keyVersion >= ShareSnapshotKey.LOWEST_SUPPORTED_VERSION
            && keyVersion <= ShareSnapshotKey.HIGHEST_SUPPORTED_VERSION) {
            return Optional.of(new ShareSnapshotValue(new ByteBufferAccessor(byteBuffer), version));
        } else if (keyVersion >= ShareUpdateKey.LOWEST_SUPPORTED_VERSION
            && keyVersion <= ShareUpdateKey.HIGHEST_SUPPORTED_VERSION) {
            return Optional.of(new ShareUpdateValue(new ByteBufferAccessor(byteBuffer), version));
        }
        return Optional.empty();
    }
}
