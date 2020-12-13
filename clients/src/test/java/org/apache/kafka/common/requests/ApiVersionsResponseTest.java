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
import org.apache.kafka.common.protocol.CommonFields;
import org.apache.kafka.common.protocol.types.BoundField;
import org.apache.kafka.common.protocol.types.Schema;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ApiVersionsResponseTest {

    @Test
    public void shouldCreateApiResponseThatHasAllApiKeysSupportedByBroker() {
        assertEquals(apiKeysInResponse(ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE), new HashSet<>(ApiKeys.enabledApis()));
        assertTrue(ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE.data.supportedFeatures().isEmpty());
        assertTrue(ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE.data.finalizedFeatures().isEmpty());
        assertEquals(ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH, ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE.data.finalizedFeaturesEpoch());
    }

    @Test
    public void shouldHaveCorrectDefaultApiVersionsResponse() {
        Collection<ApiVersionsResponseKey> apiVersions = ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE.data.apiKeys();
        assertEquals("API versions for all API keys must be maintained.", apiVersions.size(), ApiKeys.enabledApis().size());

        for (ApiKeys key : ApiKeys.enabledApis()) {
            ApiVersionsResponseKey version = ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE.apiVersion(key.id);
            assertNotNull("Could not find ApiVersion for API " + key.name, version);
            assertEquals("Incorrect min version for Api " + key.name, version.minVersion(), key.oldestVersion());
            assertEquals("Incorrect max version for Api " + key.name, version.maxVersion(), key.latestVersion());

            // Check if versions less than min version are indeed set as null, i.e., deprecated.
            for (int i = 0; i < version.minVersion(); ++i) {
                assertNull("Request version " + i + " for API " + version.apiKey() + " must be null", key.requestSchemas[i]);
                assertNull("Response version " + i + " for API " + version.apiKey() + " must be null", key.responseSchemas[i]);
            }

            // Check if versions between min and max versions are non null, i.e., valid.
            for (int i = version.minVersion(); i <= version.maxVersion(); ++i) {
                assertNotNull("Request version " + i + " for API " + version.apiKey() + " must not be null", key.requestSchemas[i]);
                assertNotNull("Response version " + i + " for API " + version.apiKey() + " must not be null", key.responseSchemas[i]);
            }
        }

        assertTrue(ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE.data.supportedFeatures().isEmpty());
        assertTrue(ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE.data.finalizedFeatures().isEmpty());
        assertEquals(ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH, ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE.data.finalizedFeaturesEpoch());
    }

    /**
     * All valid client responses which may be throttled should have a field named
     * 'throttle_time_ms' to return the throttle time to the client. Exclusions are
     * <ul>
     *   <li> Cluster actions used only for inter-broker are throttled only if unauthorized
     *   <li> SASL_HANDSHAKE and SASL_AUTHENTICATE are not throttled when used for authentication
     *        when a connection is established or for re-authentication thereafter; these requests
     *        return an error response that may be throttled if they are sent otherwise.
     * </ul>
     */
    @Test
    public void testResponseThrottleTime() {
        List<ApiKeys> authenticationKeys = Arrays.asList(ApiKeys.SASL_HANDSHAKE, ApiKeys.SASL_AUTHENTICATE);
        for (ApiKeys apiKey: ApiKeys.values()) {
            Schema responseSchema = apiKey.responseSchemas[apiKey.latestVersion()];
            BoundField throttleTimeField = responseSchema.get(CommonFields.THROTTLE_TIME_MS.name);
            if (apiKey.clusterAction || authenticationKeys.contains(apiKey))
                assertNull("Unexpected throttle time field: " + apiKey, throttleTimeField);
            else
                assertNotNull("Throttle time field missing: " + apiKey, throttleTimeField);
        }
    }


    private Set<ApiKeys> apiKeysInResponse(final ApiVersionsResponse apiVersions) {
        final Set<ApiKeys> apiKeys = new HashSet<>();
        for (final ApiVersionsResponseKey version : apiVersions.data.apiKeys()) {
            apiKeys.add(ApiKeys.forId(version.apiKey()));
        }
        return apiKeys;
    }
}
