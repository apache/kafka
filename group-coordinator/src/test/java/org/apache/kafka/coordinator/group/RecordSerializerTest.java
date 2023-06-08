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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataValue;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class RecordSerializerTest {
    @Test
    public void testSerializeKey() {
        RecordSerializer serializer = new RecordSerializer();
        Record record = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupMetadataKey().setGroupId("group"),
                (short) 1
            ),
            new ApiMessageAndVersion(
                new ConsumerGroupMetadataValue().setEpoch(10),
                (short) 0
            )
        );

        assertArrayEquals(
            MessageUtil.toVersionPrefixedBytes(record.key().version(), record.key().message()),
            serializer.serializeKey(record)
        );
    }

    @Test
    public void testSerializeValue() {
        RecordSerializer serializer = new RecordSerializer();
        Record record = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupMetadataKey().setGroupId("group"),
                (short) 1
            ),
            new ApiMessageAndVersion(
                new ConsumerGroupMetadataValue().setEpoch(10),
                (short) 0
            )
        );

        assertArrayEquals(
            MessageUtil.toVersionPrefixedBytes(record.value().version(), record.value().message()),
            serializer.serializeValue(record)
        );
    }

    @Test
    public void testSerializeNullValue() {
        RecordSerializer serializer = new RecordSerializer();
        Record record = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupMetadataKey().setGroupId("group"),
                (short) 1
            ),
            null
        );

        assertNull(serializer.serializeValue(record));
    }
}
