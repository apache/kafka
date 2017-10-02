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

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class ApiVersionsResponseTest {

    @Test
    public void shouldCreateApiResponseOnlyWithKeysSupportedByMagicValue() throws Exception {
        final ApiVersionsResponse response = ApiVersionsResponse.apiVersionsResponse(10, RecordBatch.MAGIC_VALUE_V1);
        verifyApiKeysForMagic(response, RecordBatch.MAGIC_VALUE_V1);
        assertEquals(10, response.throttleTimeMs());
    }

    @Test
    public void shouldCreateApiResponseThatHasAllApiKeysSupportedByBroker() throws Exception {
        assertEquals(apiKeysInResponse(ApiVersionsResponse.defaultApiVersionsResponse()), Utils.mkSet(ApiKeys.values()));
    }

    @Test
    public void shouldReturnAllKeysWhenMagicIsCurrentValueAndThrottleMsIsDefaultThrottle() throws Exception {
        ApiVersionsResponse response = ApiVersionsResponse.apiVersionsResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, RecordBatch.CURRENT_MAGIC_VALUE);
        assertEquals(Utils.mkSet(ApiKeys.values()), apiKeysInResponse(response));
        assertEquals(AbstractResponse.DEFAULT_THROTTLE_TIME, response.throttleTimeMs());
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