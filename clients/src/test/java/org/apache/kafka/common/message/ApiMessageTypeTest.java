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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

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
}
