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

import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.coordinator.group.generated.OffsetCommitKey;
import org.apache.kafka.coordinator.group.generated.OffsetCommitKeyJsonConverter;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValue;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValueJsonConverter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.nio.ByteBuffer;

/**
 * Formatter for use with tools such as console consumer: Consumer should also set exclude.internal.topics to false.
 */
public class OffsetsMessageFormatter extends ApiMessageFormatter {
    @Override
    protected JsonNode readToKeyJson(ByteBuffer byteBuffer) {
        short version = byteBuffer.getShort();
        if (version >= OffsetCommitKey.LOWEST_SUPPORTED_VERSION && version <= OffsetCommitKey.HIGHEST_SUPPORTED_VERSION) {
            return OffsetCommitKeyJsonConverter.write(new OffsetCommitKey(new ByteBufferAccessor(byteBuffer), version), version);
        }
        return NullNode.getInstance();
    }

    @Override
    protected JsonNode readToValueJson(ByteBuffer byteBuffer) {
        short version = byteBuffer.getShort();
        if (version >= OffsetCommitValue.LOWEST_SUPPORTED_VERSION && version <= OffsetCommitValue.HIGHEST_SUPPORTED_VERSION) {
            return OffsetCommitValueJsonConverter.write(new OffsetCommitValue(new ByteBufferAccessor(byteBuffer), version), version);
        }
        return new TextNode(UNKNOWN);
    }
}
