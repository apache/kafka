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

package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionsResponseKey;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ApiVersionsResponseTest {

    @Test
    public void shouldCreateApiResponseThatHasAllApiKeysSupportedByBroker() {
        assertEquals(apiKeysInResponse(ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE), new HashSet<>(ApiKeys.enabledApis()));
        assertTrue(ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE.data().supportedFeatures().isEmpty());
        assertTrue(ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE.data().finalizedFeatures().isEmpty());
        assertEquals(ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH, ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE.data().finalizedFeaturesEpoch());
    }

    @Test
    public void shouldHaveCorrectDefaultApiVersionsResponse() {
        Collection<ApiVersionsResponseKey> apiVersions = ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE.data().apiKeys();
        assertEquals(apiVersions.size(), ApiKeys.enabledApis().size(), "API versions for all API keys must be maintained.");

        for (ApiKeys key : ApiKeys.enabledApis()) {
            ApiVersionsResponseKey version = ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE.apiVersion(key.id);
            assertNotNull(version, "Could not find ApiVersion for API " + key.name);
            assertEquals(version.minVersion(), key.oldestVersion(), "Incorrect min version for Api " + key.name);
            assertEquals(version.maxVersion(), key.latestVersion(), "Incorrect max version for Api " + key.name);

            // Check if versions less than min version are indeed set as null, i.e., deprecated.
            for (int i = 0; i < version.minVersion(); ++i) {
                assertNull(key.messageType.requestSchemas()[i],
                    "Request version " + i + " for API " + version.apiKey() + " must be null");
                assertNull(key.messageType.responseSchemas()[i],
                    "Response version " + i + " for API " + version.apiKey() + " must be null");
            }

            // Check if versions between min and max versions are non null, i.e., valid.
            for (int i = version.minVersion(); i <= version.maxVersion(); ++i) {
                assertNotNull(key.messageType.requestSchemas()[i],
                    "Request version " + i + " for API " + version.apiKey() + " must not be null");
                assertNotNull(key.messageType.responseSchemas()[i],
                    "Response version " + i + " for API " + version.apiKey() + " must not be null");
            }
        }

        assertTrue(ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE.data().supportedFeatures().isEmpty());
        assertTrue(ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE.data().finalizedFeatures().isEmpty());
        assertEquals(ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH, ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE.data().finalizedFeaturesEpoch());
    }

    private Set<ApiKeys> apiKeysInResponse(final ApiVersionsResponse apiVersions) {
        final Set<ApiKeys> apiKeys = new HashSet<>();
        for (final ApiVersionsResponseKey version : apiVersions.data().apiKeys()) {
            apiKeys.add(ApiKeys.forId(version.apiKey()));
        }
        return apiKeys;
    }
}
