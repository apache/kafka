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

package org.apache.kafka.common.message;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.types.Schema;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Timeout(120)
public class ApiMessageTypeTest {

    @Test
    public void testFromApiKey() {
        for (ApiMessageType type : ApiMessageType.values()) {
            ApiMessageType type2 = ApiMessageType.fromApiKey(type.apiKey());
            assertEquals(type2, type);
        }
    }

    @Test
    public void testInvalidFromApiKey() {
        try {
            ApiMessageType.fromApiKey((short) -1);
            fail("expected to get an UnsupportedVersionException");
        } catch (UnsupportedVersionException uve) {
            // expected
        }
    }

    @Test
    public void testUniqueness() {
        Set<Short> ids = new HashSet<>();
        Set<String> requestNames = new HashSet<>();
        Set<String> responseNames = new HashSet<>();
        for (ApiMessageType type : ApiMessageType.values()) {
            assertFalse(ids.contains(type.apiKey()),
                "found two ApiMessageType objects with id " + type.apiKey());
            ids.add(type.apiKey());
            String requestName = type.newRequest().getClass().getSimpleName();
            assertFalse(requestNames.contains(requestName),
                "found two ApiMessageType objects with requestName " + requestName);
            requestNames.add(requestName);
            String responseName = type.newResponse().getClass().getSimpleName();
            assertFalse(responseNames.contains(responseName),
                "found two ApiMessageType objects with responseName " + responseName);
            responseNames.add(responseName);
        }
        assertEquals(ApiMessageType.values().length, ids.size());
        assertEquals(ApiMessageType.values().length, requestNames.size());
        assertEquals(ApiMessageType.values().length, responseNames.size());
    }

    @Test
    public void testHeaderVersion() {
        assertEquals((short) 1, ApiMessageType.PRODUCE.requestHeaderVersion((short) 0));
        assertEquals((short) 0, ApiMessageType.PRODUCE.responseHeaderVersion((short) 0));

        assertEquals((short) 1, ApiMessageType.PRODUCE.requestHeaderVersion((short) 1));
        assertEquals((short) 0, ApiMessageType.PRODUCE.responseHeaderVersion((short) 1));

        assertEquals((short) 0, ApiMessageType.CONTROLLED_SHUTDOWN.requestHeaderVersion((short) 0));
        assertEquals((short) 0, ApiMessageType.CONTROLLED_SHUTDOWN.responseHeaderVersion((short) 0));

        assertEquals((short) 1, ApiMessageType.CONTROLLED_SHUTDOWN.requestHeaderVersion((short) 1));
        assertEquals((short) 0, ApiMessageType.CONTROLLED_SHUTDOWN.responseHeaderVersion((short) 1));

        assertEquals((short) 1, ApiMessageType.CREATE_TOPICS.requestHeaderVersion((short) 4));
        assertEquals((short) 0, ApiMessageType.CREATE_TOPICS.responseHeaderVersion((short) 4));

        assertEquals((short) 2, ApiMessageType.CREATE_TOPICS.requestHeaderVersion((short) 5));
        assertEquals((short) 1, ApiMessageType.CREATE_TOPICS.responseHeaderVersion((short) 5));
    }

    @Test
    public void testAllVersionsHaveSchemas() {
        for (ApiMessageType type : ApiMessageType.values()) {
            assertTrue(type.lowestSupportedVersion() >= 0);

            assertEquals(type.requestSchemas().length, type.responseSchemas().length,
                    "request and response schemas must be the same length for " + type.name());
            for (int i = 0; i < type.requestSchemas().length; ++i) {
                Schema schema = type.requestSchemas()[i];
                if (i >= type.lowestSupportedVersion())
                    assertNotNull(schema);
                else
                    assertNull(schema);
            }
            for (int i = 0; i < type.responseSchemas().length; ++i) {
                Schema schema = type.responseSchemas()[i];
                if (i >= type.lowestSupportedVersion())
                    assertNotNull(schema);
                else
                    assertNull(schema);
            }

            assertEquals(type.highestSupportedVersion(true) + 1, type.requestSchemas().length);
        }
    }

    @Test
    public void testApiIdsArePositive() {
        for (ApiMessageType type : ApiMessageType.values())
            assertTrue(type.apiKey() >= 0);
    }
}
