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

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.coordinator.share.generated.CoordinatorRecordType;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotKey;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotKeyJsonConverter;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotValue;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotValueJsonConverter;
import org.apache.kafka.coordinator.share.generated.ShareUpdateKey;
import org.apache.kafka.coordinator.share.generated.ShareUpdateKeyJsonConverter;
import org.apache.kafka.coordinator.share.generated.ShareUpdateValue;
import org.apache.kafka.coordinator.share.generated.ShareUpdateValueJsonConverter;
import org.apache.kafka.tools.consumer.ApiMessageFormatter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * Formatter for records of in __share_group_state topic.
 */
public class ShareGroupStateMessageFormatter extends ApiMessageFormatter {

    @Override
    protected JsonNode readToKeyJson(ByteBuffer byteBuffer) {
        short version = byteBuffer.getShort();
        return readToSnapshotMessageKey(byteBuffer, version)
                .map(logKey -> transferKeyMessageToJsonNode(logKey, version))
                .orElseGet(NullNode::getInstance);
    }

    private Optional<ApiMessage> readToSnapshotMessageKey(ByteBuffer byteBuffer, short version) {
        try {
            switch (CoordinatorRecordType.fromId(version)) {
                case SHARE_SNAPSHOT:
                    return Optional.of(new ShareSnapshotKey(new ByteBufferAccessor(byteBuffer), version));
                case SHARE_UPDATE:
                    return Optional.of(new ShareUpdateKey(new ByteBufferAccessor(byteBuffer), version));
                default:
                    return Optional.empty();
            }
        } catch (UnsupportedVersionException ex) {
            return Optional.empty();
        }
    }

    private JsonNode transferKeyMessageToJsonNode(ApiMessage logKey, short keyVersion) {
        if (logKey instanceof ShareSnapshotKey) {
            return ShareSnapshotKeyJsonConverter.write((ShareSnapshotKey) logKey, keyVersion);
        } else if (logKey instanceof ShareUpdateKey) {
            return ShareUpdateKeyJsonConverter.write((ShareUpdateKey) logKey, keyVersion);
        }
        return null;
    }

    /**
     * Here the valueVersion is not enough to identity the deserializer for the ByteBuffer.
     * This is because both {@link ShareSnapshotValue} and {@link ShareUpdateValue} have version 0
     * as per RPC spec.
     * To differentiate, we need to use the corresponding key versions. This is acceptable as
     * the records will always appear in pairs (key, value).
     *
     * @param byteBuffer - Represents the raw data read from the topic
     * @param keyVersion - Version of the actual key component of the data read from topic
     * @return JsonNode corresponding to the raw data value component
     */
    @Override
    protected JsonNode readToValueJson(ByteBuffer byteBuffer, short keyVersion) {
        short valueVersion = byteBuffer.getShort();
        return readToSnapshotMessageValue(byteBuffer, keyVersion, valueVersion)
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

    private Optional<ApiMessage> readToSnapshotMessageValue(ByteBuffer byteBuffer, short keyVersion, short valueVersion) {
        // Check the key version here as that will determine which type
        // of value record to fetch. Both share update and share snapshot
        // value records can have the same version.
        try {
            switch (CoordinatorRecordType.fromId(keyVersion)) {
                case SHARE_SNAPSHOT:
                    return Optional.of(new ShareSnapshotValue(new ByteBufferAccessor(byteBuffer), valueVersion));
                case SHARE_UPDATE:
                    return Optional.of(new ShareUpdateValue(new ByteBufferAccessor(byteBuffer), valueVersion));
                default:
                    return Optional.empty();
            }
        } catch (UnsupportedVersionException ex) {
            return Optional.empty();
        }
    }
}
