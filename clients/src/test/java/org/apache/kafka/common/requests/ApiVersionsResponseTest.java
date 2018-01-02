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
        final ApiVersionsResponse response = ApiVersionsResponse.apiVersionsResponse(10, RecordBatch.MAGIC_VALUE_V1);
        verifyApiKeysForMagic(response, RecordBatch.MAGIC_VALUE_V1);
        assertEquals(10, response.throttleTimeMs());
    }

    @Test
    public void shouldCreateApiResponseThatHasAllApiKeysSupportedByBroker() {
        assertEquals(apiKeysInResponse(ApiVersionsResponse.defaultApiVersionsResponse()), Utils.mkSet(ApiKeys.values()));
    }

    @Test
    public void shouldReturnAllKeysWhenMagicIsCurrentValueAndThrottleMsIsDefaultThrottle() {
        ApiVersionsResponse response = ApiVersionsResponse.apiVersionsResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, RecordBatch.CURRENT_MAGIC_VALUE);
        assertEquals(Utils.mkSet(ApiKeys.values()), apiKeysInResponse(response));
        assertEquals(AbstractResponse.DEFAULT_THROTTLE_TIME, response.throttleTimeMs());
    }

    @Test
    public void shouldHaveCorrectDefaultApiVersionsResponse() {
        Collection<ApiVersionsResponse.ApiVersion> apiVersions = ApiVersionsResponse.defaultApiVersionsResponse().apiVersions();
        assertEquals("API versions for all API keys must be maintained.", apiVersions.size(), ApiKeys.values().length);

        for (ApiKeys key : ApiKeys.values()) {
            ApiVersionsResponse.ApiVersion version = ApiVersionsResponse.defaultApiVersionsResponse().apiVersion(key.id);
            assertNotNull("Could not find ApiVersion for API " + key.name, version);
            assertEquals("Incorrect min version for Api " + key.name, version.minVersion, key.oldestVersion());
            assertEquals("Incorrect max version for Api " + key.name, version.maxVersion, key.latestVersion());

            // Check if versions less than min version are indeed set as null, i.e., deprecated.
            for (int i = 0; i < version.minVersion; ++i) {
                assertNull("Request version " + i + " for API " + version.apiKey + " must be null", key.requestSchemas[i]);
                assertNull("Response version " + i + " for API " + version.apiKey + " must be null", key.responseSchemas[i]);
            }

            // Check if versions between min and max versions are non null, i.e., valid.
            for (int i = version.minVersion; i <= version.maxVersion; ++i) {
                assertNotNull("Request version " + i + " for API " + version.apiKey + " must not be null", key.requestSchemas[i]);
                assertNotNull("Response version " + i + " for API " + version.apiKey + " must not be null", key.responseSchemas[i]);
            }
        }
    }
    
    private void verifyApiKeysForMagic(final ApiVersionsResponse response, final byte maxMagic) {
        for (final ApiVersionsResponse.ApiVersion version : response.apiVersions()) {
            assertTrue(ApiKeys.forId(version.apiKey).minRequiredInterBrokerMagic <= maxMagic);
        }
    }

    private Set<ApiKeys> apiKeysInResponse(final ApiVersionsResponse apiVersions) {
        final Set<ApiKeys> apiKeys = new HashSet<>();
        for (final ApiVersionsResponse.ApiVersion version : apiVersions.apiVersions()) {
            apiKeys.add(ApiKeys.forId(version.apiKey));
        }
        return apiKeys;
    }


}
