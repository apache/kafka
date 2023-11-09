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
package org.apache.kafka.connect.storage;

import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class OffsetUtilsTest {

    private static final JsonConverter CONVERTER = new JsonConverter();

    static {
        CONVERTER.configure(Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false"), true);
    }

    @Test
    public void testValidateFormatNotMap() {
        DataException e = assertThrows(DataException.class, () -> OffsetUtils.validateFormat(new Object()));
        assertThat(e.getMessage(), containsString("Offsets must be specified as a Map"));
    }

    @Test
    public void testValidateFormatMapWithNonStringKeys() {
        Map<Object, Object> offsetData = new HashMap<>();
        offsetData.put("k1", "v1");
        offsetData.put(1, "v2");
        DataException e = assertThrows(DataException.class, () -> OffsetUtils.validateFormat(offsetData));
        assertThat(e.getMessage(), containsString("Offsets may only use String keys"));
    }

    @Test
    public void testValidateFormatMapWithNonPrimitiveKeys() {
        Map<Object, Object> offsetData = Collections.singletonMap("key", new Object());
        DataException e = assertThrows(DataException.class, () -> OffsetUtils.validateFormat(offsetData));
        assertThat(e.getMessage(), containsString("Offsets may only contain primitive types as values"));

        Map<Object, Object> offsetData2 = Collections.singletonMap("key", new ArrayList<>());
        e = assertThrows(DataException.class, () -> OffsetUtils.validateFormat(offsetData2));
        assertThat(e.getMessage(), containsString("Offsets may only contain primitive types as values"));
    }

    @Test
    public void testValidateFormatWithValidFormat() {
        Map<Object, Object> offsetData = Collections.singletonMap("key", 1);
        // Expect no exception to be thrown
        OffsetUtils.validateFormat(offsetData);
    }

    @Test
    public void testProcessPartitionKeyWithUnknownSerialization() {
        assertInvalidPartitionKey(
                new byte[]{0},
                "Ignoring offset partition key with unknown serialization");
        assertInvalidPartitionKey(
                "i-am-not-json".getBytes(StandardCharsets.UTF_8),
                "Ignoring offset partition key with unknown serialization");
    }

    @Test
    public void testProcessPartitionKeyNotList() {
        assertInvalidPartitionKey(
                new byte[]{},
                "Ignoring offset partition key with an unexpected format");
        assertInvalidPartitionKey(
                serializePartitionKey(new HashMap<>()),
                "Ignoring offset partition key with an unexpected format");
    }

    @Test
    public void testProcessPartitionKeyListWithOneElement() {
        assertInvalidPartitionKey(
                serializePartitionKey(Collections.singletonList("")),
                "Ignoring offset partition key with an unexpected number of elements");
    }

    @Test
    public void testProcessPartitionKeyListWithElementsOfWrongType() {
        assertInvalidPartitionKey(
                serializePartitionKey(Arrays.asList(1, new HashMap<>())),
                "Ignoring offset partition key with an unexpected format for the first element in the partition key list");
        assertInvalidPartitionKey(
                serializePartitionKey(Arrays.asList("connector-name", new ArrayList<>())),
                "Ignoring offset partition key with an unexpected format for the second element in the partition key list");
    }

    public void assertInvalidPartitionKey(byte[] key, String message) {
        try (LogCaptureAppender logCaptureAppender = LogCaptureAppender.createAndRegister(OffsetUtils.class)) {
            Map<String, Set<Map<String, Object>>> connectorPartitions = new HashMap<>();
            OffsetUtils.processPartitionKey(key, new byte[0], CONVERTER, connectorPartitions);
            // Expect no partition to be added to the map since the partition key is of an invalid format
            assertEquals(0, connectorPartitions.size());
            assertEquals(1, logCaptureAppender.getMessages().size());
            assertThat(logCaptureAppender.getMessages().get(0), containsString(message));
        }
    }

    @Test
    public void testProcessPartitionKeyValidList() {
        try (LogCaptureAppender logCaptureAppender = LogCaptureAppender.createAndRegister(OffsetUtils.class)) {
            Map<String, Set<Map<String, Object>>> connectorPartitions = new HashMap<>();
            OffsetUtils.processPartitionKey(serializePartitionKey(Arrays.asList("connector-name", new HashMap<>())), new byte[0], CONVERTER, connectorPartitions);
            assertEquals(1, connectorPartitions.size());
            assertEquals(0, logCaptureAppender.getMessages().size());
        }
    }

    private byte[] serializePartitionKey(Object key) {
        return CONVERTER.fromConnectData("", null, key);
    }
}
