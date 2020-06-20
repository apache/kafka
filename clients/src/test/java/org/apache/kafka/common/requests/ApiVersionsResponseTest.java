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

import org.apache.kafka.common.feature.Features;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.feature.FinalizedVersionRange;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionsResponseKey;
import org.apache.kafka.common.message.ApiVersionsResponseData.FinalizedFeatureKey;
import org.apache.kafka.common.message.ApiVersionsResponseData.SupportedFeatureKey;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.Utils;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class ApiVersionsResponseTest {

    @Test
    public void shouldCreateApiResponseOnlyWithKeysSupportedByMagicValue() {
        final ApiVersionsResponse response = ApiVersionsResponse.apiVersionsResponse(
            10,
            RecordBatch.MAGIC_VALUE_V1,
            Features.emptySupportedFeatures());
        verifyApiKeysForMagic(response, RecordBatch.MAGIC_VALUE_V1);
        assertEquals(10, response.throttleTimeMs());
        assertTrue(response.data.supportedFeatures().isEmpty());
        assertTrue(response.data.finalizedFeatures().isEmpty());
        assertEquals(ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH, response.data.finalizedFeaturesEpoch());
    }

    @Test
    public void shouldCreateApiResponseThatHasAllApiKeysSupportedByBroker() {
        assertEquals(apiKeysInResponse(ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE), Utils.mkSet(ApiKeys.values()));
        assertTrue(ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE.data.supportedFeatures().isEmpty());
        assertTrue(ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE.data.finalizedFeatures().isEmpty());
        assertEquals(ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH, ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE.data.finalizedFeaturesEpoch());
    }

    @Test
    public void shouldReturnAllKeysWhenMagicIsCurrentValueAndThrottleMsIsDefaultThrottle() {
        ApiVersionsResponse response = ApiVersionsResponse.apiVersionsResponse(
            AbstractResponse.DEFAULT_THROTTLE_TIME,
            RecordBatch.CURRENT_MAGIC_VALUE,
            Features.emptySupportedFeatures());
        assertEquals(Utils.mkSet(ApiKeys.values()), apiKeysInResponse(response));
        assertEquals(AbstractResponse.DEFAULT_THROTTLE_TIME, response.throttleTimeMs());
        assertTrue(response.data.supportedFeatures().isEmpty());
        assertTrue(response.data.finalizedFeatures().isEmpty());
        assertEquals(ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH, response.data.finalizedFeaturesEpoch());
    }

    @Test
    public void shouldHaveCorrectDefaultApiVersionsResponse() {
        Collection<ApiVersionsResponseKey> apiVersions = ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE.data.apiKeys();
        assertEquals("API versions for all API keys must be maintained.", apiVersions.size(), ApiKeys.values().length);

        for (ApiKeys key : ApiKeys.values()) {
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

    @Test
    public void shouldReturnFeatureKeysWhenMagicIsCurrentValueAndThrottleMsIsDefaultThrottle() {
        ApiVersionsResponse response = ApiVersionsResponse.apiVersionsResponse(
            10,
            RecordBatch.MAGIC_VALUE_V1,
            Features.supportedFeatures(Utils.mkMap(Utils.mkEntry("feature", new SupportedVersionRange((short) 1, (short) 4)))),
            Features.finalizedFeatures(Utils.mkMap(Utils.mkEntry("feature", new FinalizedVersionRange((short) 2, (short) 3)))),
            10);
        verifyApiKeysForMagic(response, RecordBatch.MAGIC_VALUE_V1);
        assertEquals(10, response.throttleTimeMs());

        assertEquals(1, response.data.supportedFeatures().size());
        SupportedFeatureKey sKey = response.data.supportedFeatures().find("feature");
        assertNotNull(sKey);
        assertEquals(1, sKey.minVersion());
        assertEquals(4, sKey.maxVersion());

        assertEquals(1, response.data.finalizedFeatures().size());
        FinalizedFeatureKey fKey = response.data.finalizedFeatures().find("feature");
        assertNotNull(fKey);
        assertEquals(2, fKey.minVersionLevel());
        assertEquals(3, fKey.maxVersionLevel());

        assertEquals(10, response.data.finalizedFeaturesEpoch());
    }
    
    private void verifyApiKeysForMagic(final ApiVersionsResponse response, final byte maxMagic) {
        for (final ApiVersionsResponseKey version : response.data.apiKeys()) {
            assertTrue(ApiKeys.forId(version.apiKey()).minRequiredInterBrokerMagic <= maxMagic);
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
