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
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.Timeout;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ApiMessageTypeTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

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
            assertFalse("found two ApiMessageType objects with id " + type.apiKey(),
                ids.contains(type.apiKey()));
            ids.add(type.apiKey());
            String requestName = type.newRequest().getClass().getSimpleName();
            assertFalse("found two ApiMessageType objects with requestName " + requestName,
                requestNames.contains(requestName));
            requestNames.add(requestName);
            String responseName = type.newResponse().getClass().getSimpleName();
            assertFalse("found two ApiMessageType objects with responseName " + responseName,
                responseNames.contains(responseName));
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

    /**
     * Kafka currently supports direct upgrades from 0.8 to the latest version. As such, it has to support all apis
     * starting from version 0 and we must have schemas from the oldest version to the latest.
     */
    @Test
    public void testAllVersionsHaveSchemas() {
        for (ApiMessageType type : ApiMessageType.values()) {
            Assertions.assertEquals(0, type.lowestSupportedVersion());

            assertEquals(type.requestSchemas().length, type.responseSchemas().length);
            for (Schema schema : type.requestSchemas())
                assertNotNull(schema);
            for (Schema schema : type.responseSchemas())
                assertNotNull(schema);

            assertEquals(type.highestSupportedVersion() + 1, type.requestSchemas().length);
        }
    }

    @Test
    public void testApiIdsArePositive() {
        for (ApiMessageType type : ApiMessageType.values())
            assertTrue(type.apiKey() >= 0);
    }
}
