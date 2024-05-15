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

import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataValue;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CoordinatorRecordTest {
    @Test
    public void testAttributes() {
        ApiMessageAndVersion key = new ApiMessageAndVersion(new ConsumerGroupMetadataKey(), (short) 0);
        ApiMessageAndVersion value = new ApiMessageAndVersion(new ConsumerGroupMetadataValue(), (short) 0);
        CoordinatorRecord record = new CoordinatorRecord(key, value);
        assertEquals(key, record.key());
        assertEquals(value, record.value());
    }

    @Test
    public void testKeyCannotBeNull() {
        assertThrows(NullPointerException.class, () -> new CoordinatorRecord(null, null));
    }

    @Test
    public void testValueCanBeNull() {
        ApiMessageAndVersion key = new ApiMessageAndVersion(new ConsumerGroupMetadataKey(), (short) 0);
        CoordinatorRecord record = new CoordinatorRecord(key, null);
        assertEquals(key, record.key());
        assertNull(record.value());
    }

    @Test
    public void testEquals() {
        ApiMessageAndVersion key = new ApiMessageAndVersion(new ConsumerGroupMetadataKey(), (short) 0);
        ApiMessageAndVersion value = new ApiMessageAndVersion(new ConsumerGroupMetadataValue(), (short) 0);
        CoordinatorRecord record1 = new CoordinatorRecord(key, value);
        CoordinatorRecord record2 = new CoordinatorRecord(key, value);
        assertEquals(record1, record2);
    }
}
