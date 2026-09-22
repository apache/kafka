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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.BoundField;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.TaggedFields;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        assertThrows(UnsupportedVersionException.class, () -> ApiMessageType.fromApiKey((short) -1));
    }

    @Test
    public void testUniqueness() {
        Set<Short> ids = new HashSet<>();
        Set<String> requestNames = new HashSet<>();
        Set<String> responseNames = new HashSet<>();
        int apiKeysWithNoValidVersionCount = 0;
        for (ApiMessageType type : ApiMessageType.values()) {
            assertFalse(ids.contains(type.apiKey()),
                "found two ApiMessageType objects with id " + type.apiKey());
            ids.add(type.apiKey());
            ApiKeys apiKey = ApiKeys.forId(type.apiKey());
            if (apiKey.hasValidVersion()) {
                String requestName = type.newRequest().getClass().getSimpleName();
                assertFalse(requestNames.contains(requestName),
                        "found two ApiMessageType objects with requestName " + requestName);
                requestNames.add(requestName);
                String responseName = type.newResponse().getClass().getSimpleName();
                assertFalse(responseNames.contains(responseName),
                        "found two ApiMessageType objects with responseName " + responseName);
                responseNames.add(responseName);
            } else
                ++apiKeysWithNoValidVersionCount;
        }
        assertEquals(ApiMessageType.values().length, ids.size());
        int expectedNamesCount = ApiMessageType.values().length - apiKeysWithNoValidVersionCount;
        assertEquals(expectedNamesCount, requestNames.size());
        assertEquals(expectedNamesCount, responseNames.size());
    }

    @Test
    public void testHeaderVersion() {
        assertEquals((short) 1, ApiMessageType.CREATE_TOPICS.requestHeaderVersion((short) 4));
        assertEquals((short) 0, ApiMessageType.CREATE_TOPICS.responseHeaderVersion((short) 4));

        assertEquals((short) 2, ApiMessageType.CREATE_TOPICS.requestHeaderVersion((short) 5));
        assertEquals((short) 1, ApiMessageType.CREATE_TOPICS.responseHeaderVersion((short) 5));

        // SaslHandshake and OffsetDelete are non-flexible: header v1 request / v0 response at every version.
        assertEquals((short) 1, ApiMessageType.SASL_HANDSHAKE.requestHeaderVersion((short) 0));
        assertEquals((short) 0, ApiMessageType.SASL_HANDSHAKE.responseHeaderVersion((short) 0));
        assertEquals((short) 1, ApiMessageType.SASL_HANDSHAKE.requestHeaderVersion((short) 1));
        assertEquals((short) 0, ApiMessageType.SASL_HANDSHAKE.responseHeaderVersion((short) 1));

        assertEquals((short) 1, ApiMessageType.OFFSET_DELETE.requestHeaderVersion((short) 0));
        assertEquals((short) 0, ApiMessageType.OFFSET_DELETE.responseHeaderVersion((short) 0));

        // ApiVersions request follows the flexible rule, but the response always uses a v0 header (KIP-511).
        assertEquals((short) 1, ApiMessageType.API_VERSIONS.requestHeaderVersion((short) 0));
        assertEquals((short) 1, ApiMessageType.API_VERSIONS.requestHeaderVersion((short) 2));
        assertEquals((short) 2, ApiMessageType.API_VERSIONS.requestHeaderVersion((short) 3));
        assertEquals((short) 0, ApiMessageType.API_VERSIONS.responseHeaderVersion((short) 0));
        assertEquals((short) 0, ApiMessageType.API_VERSIONS.responseHeaderVersion((short) 3));

        // Envelope is flexible from v0: header v2 request / v1 response everywhere.
        assertEquals((short) 2, ApiMessageType.ENVELOPE.requestHeaderVersion((short) 0));
        assertEquals((short) 1, ApiMessageType.ENVELOPE.responseHeaderVersion((short) 0));

        // WriteTxnMarkers is flexible across its valid versions (1-2): header v2 request / v1 response.
        assertEquals((short) 2, ApiMessageType.WRITE_TXN_MARKERS.requestHeaderVersion((short) 1));
        assertEquals((short) 1, ApiMessageType.WRITE_TXN_MARKERS.responseHeaderVersion((short) 1));
    }

    /**
     * The header version generated for every existing API and version is consistent with the flexibility of
     * the body: a flexible request/response uses a flexible header (v2+/v1+), a non-flexible one uses header
     * v1/v0, and every header version is one the header schemas define. The sole exception is
     * ApiVersionsResponse, which always uses a v0 header so that older brokers can parse it (KIP-511).
     * Flexible versions are checked with {@code >=} so that a newer flexible header version, such as the v3
     * request header of KIP-1313, can be introduced without changing this test.
     */
    @Test
    public void testHeaderVersionsMatchSchemaFlexibility() {
        for (ApiMessageType type : ApiMessageType.values()) {
            if (type.lowestSupportedVersion() > type.highestSupportedVersion(true))
                continue;
            for (short version = type.lowestSupportedVersion();
                    version <= type.highestSupportedVersion(true); version++) {
                String context = " for " + type.name() + " version " + version;

                short requestHeader = type.requestHeaderVersion(version);
                assertTrue(requestHeader <= RequestHeaderData.HIGHEST_SUPPORTED_VERSION,
                        "Request header version " + requestHeader + " does not exist" + context);
                if (isFlexible(type.requestSchemas()[version])) {
                    assertTrue(requestHeader >= 2, "Flexible request must use a flexible header" + context);
                } else {
                    assertEquals((short) 1, requestHeader, "Non-flexible request must use header v1" + context);
                }

                short responseHeader = type.responseHeaderVersion(version);
                assertTrue(responseHeader <= ResponseHeaderData.HIGHEST_SUPPORTED_VERSION,
                        "Response header version " + responseHeader + " does not exist" + context);
                if (type.apiKey() == ApiKeys.API_VERSIONS.id) {
                    assertEquals((short) 0, responseHeader, "ApiVersionsResponse must use header v0" + context);
                } else if (isFlexible(type.responseSchemas()[version])) {
                    assertTrue(responseHeader >= 1, "Flexible response must use a flexible header" + context);
                } else {
                    assertEquals((short) 0, responseHeader, "Non-flexible response must use header v0" + context);
                }
            }
        }
    }

    private static boolean isFlexible(Schema schema) {
        for (BoundField field : schema.fields()) {
            if (field.def.type instanceof TaggedFields) {
                return true;
            }
        }
        return false;
    }

    @Test
    public void testHeaderVersionWithNoValidVersion() {
        for (ApiMessageType messageType : ApiMessageType.values()) {
            if (messageType.lowestSupportedVersion() > messageType.highestSupportedVersion(true)) {
                assertThrows(UnsupportedVersionException.class, () -> messageType.requestHeaderVersion((short) 0));
                assertThrows(UnsupportedVersionException.class, () -> messageType.responseHeaderVersion((short) 0));
            }
        }
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
