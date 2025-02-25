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
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.coordinator.group.generated.CoordinatorRecordType;
import org.apache.kafka.coordinator.group.generated.ShareGroupStatePartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupStatePartitionMetadataKeyJsonConverter;
import org.apache.kafka.coordinator.group.generated.ShareGroupStatePartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupStatePartitionMetadataValueJsonConverter;
import org.apache.kafka.tools.consumer.ApiMessageFormatter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.nio.ByteBuffer;

public class ShareGroupStatePartitionMetadataFormatter extends ApiMessageFormatter {
    @Override
    protected JsonNode readToKeyJson(ByteBuffer byteBuffer) {
        try {
            switch (CoordinatorRecordType.fromId(byteBuffer.getShort())) {
                case SHARE_GROUP_STATE_PARTITION_METADATA:
                    return ShareGroupStatePartitionMetadataKeyJsonConverter.write(
                        new ShareGroupStatePartitionMetadataKey(new ByteBufferAccessor(byteBuffer), (short) 0),
                        (short) 0
                    );

                default:
                    return NullNode.getInstance();
            }
        } catch (UnsupportedVersionException ex) {
            return NullNode.getInstance();
        }
    }

    @Override
    protected JsonNode readToValueJson(ByteBuffer byteBuffer) {
        short version = byteBuffer.getShort();
        if (version >= ShareGroupStatePartitionMetadataValue.LOWEST_SUPPORTED_VERSION && version <= ShareGroupStatePartitionMetadataValue.HIGHEST_SUPPORTED_VERSION) {
            return ShareGroupStatePartitionMetadataValueJsonConverter.write(
                new ShareGroupStatePartitionMetadataValue(new ByteBufferAccessor(byteBuffer), version),
                version
            );
        }
        return new TextNode(UNKNOWN);
    }
}
