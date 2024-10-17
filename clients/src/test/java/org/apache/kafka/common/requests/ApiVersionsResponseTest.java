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
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.ApiMessageType.ListenerType;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionCollection;
import org.apache.kafka.common.message.ApiVersionsResponseData.FinalizedFeatureKey;
import org.apache.kafka.common.message.ApiVersionsResponseData.SupportedFeatureKey;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.RecordVersion;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ApiVersionsResponseTest {

    @ParameterizedTest
    @EnumSource(ApiMessageType.ListenerType.class)
    public void shouldHaveCorrectDefaultApiVersionsResponse(ApiMessageType.ListenerType scope) {
        ApiVersionsResponse defaultResponse = TestUtils.defaultApiVersionsResponse(scope);
        assertEquals(ApiKeys.apisForListener(scope).size(), defaultResponse.data().apiKeys().size(),
            "API versions for all API keys must be maintained.");

        for (ApiKeys key : ApiKeys.apisForListener(scope)) {
            ApiVersion version = defaultResponse.apiVersion(key.id);
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

        assertTrue(defaultResponse.data().supportedFeatures().isEmpty());
        assertTrue(defaultResponse.data().finalizedFeatures().isEmpty());
        assertEquals(ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH, defaultResponse.data().finalizedFeaturesEpoch());
    }

    @Test
    public void shouldHaveCommonlyAgreedApiVersionResponseWithControllerOnForwardableAPIs() {
        final ApiKeys forwardableAPIKey = ApiKeys.CREATE_ACLS;
        final ApiKeys nonForwardableAPIKey = ApiKeys.JOIN_GROUP;
        final short minVersion = 0;
        final short maxVersion = 1;
        Map<ApiKeys, ApiVersion> activeControllerApiVersions = Utils.mkMap(
            Utils.mkEntry(forwardableAPIKey, new ApiVersion()
                .setApiKey(forwardableAPIKey.id)
                .setMinVersion(minVersion)
                .setMaxVersion(maxVersion)),
            Utils.mkEntry(nonForwardableAPIKey, new ApiVersion()
                .setApiKey(nonForwardableAPIKey.id)
                .setMinVersion(minVersion)
                .setMaxVersion(maxVersion))
        );

        ApiVersionCollection commonResponse = ApiVersionsResponse.intersectForwardableApis(
            ApiMessageType.ListenerType.ZK_BROKER,
            RecordVersion.current(),
            activeControllerApiVersions,
            true,
            false
        );

        verifyVersions(forwardableAPIKey.id, minVersion, maxVersion, commonResponse);

        verifyVersions(nonForwardableAPIKey.id, ApiKeys.JOIN_GROUP.oldestVersion(),
            ApiKeys.JOIN_GROUP.latestVersion(), commonResponse);
    }

    @Test
    public void shouldCreateApiResponseOnlyWithKeysSupportedByMagicValue() {
        ApiVersionsResponse response = new ApiVersionsResponse.Builder().
            setThrottleTimeMs(10).
            setApiVersions(ApiVersionsResponse.filterApis(
                RecordVersion.V1,
                ListenerType.ZK_BROKER,
                true,
                true)).
            setSupportedFeatures(Features.emptySupportedFeatures()).
            setFinalizedFeatures(Collections.emptyMap()).
            setFinalizedFeaturesEpoch(ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH).
            build();
        verifyApiKeysForMagic(response, RecordBatch.MAGIC_VALUE_V1);
        assertEquals(10, response.throttleTimeMs());
        assertTrue(response.data().supportedFeatures().isEmpty());
        assertTrue(response.data().finalizedFeatures().isEmpty());
        assertEquals(ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH, response.data().finalizedFeaturesEpoch());
    }

    @Test
    public void shouldReturnFeatureKeysWhenMagicIsCurrentValueAndThrottleMsIsDefaultThrottle() {
        ApiVersionsResponse response = new ApiVersionsResponse.Builder().
            setThrottleTimeMs(10).
            setApiVersions(ApiVersionsResponse.filterApis(
                RecordVersion.V1,
                ListenerType.ZK_BROKER,
                true,
                true)).
            setSupportedFeatures(Features.supportedFeatures(
                Utils.mkMap(Utils.mkEntry("feature", new SupportedVersionRange((short) 1, (short) 4))))).
            setFinalizedFeatures(Utils.mkMap(Utils.mkEntry("feature", (short) 3))).
            setFinalizedFeaturesEpoch(10L).
            build();

        verifyApiKeysForMagic(response, RecordBatch.MAGIC_VALUE_V1);
        assertEquals(10, response.throttleTimeMs());
        assertEquals(1, response.data().supportedFeatures().size());
        SupportedFeatureKey sKey = response.data().supportedFeatures().find("feature");
        assertNotNull(sKey);
        assertEquals(1, sKey.minVersion());
        assertEquals(4, sKey.maxVersion());
        assertEquals(1, response.data().finalizedFeatures().size());
        FinalizedFeatureKey fKey = response.data().finalizedFeatures().find("feature");
        assertNotNull(fKey);
        assertEquals(3, fKey.minVersionLevel());
        assertEquals(3, fKey.maxVersionLevel());
        assertEquals(10, response.data().finalizedFeaturesEpoch());
    }

    @ParameterizedTest
    @EnumSource(names = {"ZK_BROKER", "BROKER"})
    public void shouldReturnAllKeysWhenMagicIsCurrentValueAndThrottleMsIsDefaultThrottle(ListenerType listenerType) {
        ApiVersionsResponse response = new ApiVersionsResponse.Builder().
            setThrottleTimeMs(AbstractResponse.DEFAULT_THROTTLE_TIME).
            setApiVersions(ApiVersionsResponse.filterApis(
                RecordVersion.current(),
                listenerType,
                true,
                true)).
            setSupportedFeatures(Features.emptySupportedFeatures()).
            setFinalizedFeatures(Collections.emptyMap()).
            setFinalizedFeaturesEpoch(ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH).
            build();
        assertEquals(new HashSet<>(ApiKeys.apisForListener(listenerType)), apiKeysInResponse(response));
        assertEquals(AbstractResponse.DEFAULT_THROTTLE_TIME, response.throttleTimeMs());
        assertTrue(response.data().supportedFeatures().isEmpty());
        assertTrue(response.data().finalizedFeatures().isEmpty());
        assertEquals(ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH, response.data().finalizedFeaturesEpoch());
    }

    @Test
    public void shouldCreateApiResponseWithTelemetryWhenEnabled() {
        ApiVersionsResponse response = new ApiVersionsResponse.Builder().
            setThrottleTimeMs(10).
            setApiVersions(ApiVersionsResponse.filterApis(
                RecordVersion.V1,
                ListenerType.BROKER,
                true,
                true)).
            setSupportedFeatures(Features.emptySupportedFeatures()).
            setFinalizedFeatures(Collections.emptyMap()).
            setFinalizedFeaturesEpoch(ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH).
            build();
        verifyApiKeysForTelemetry(response, 2);
    }

    @Test
    public void shouldNotCreateApiResponseWithTelemetryWhenDisabled() {
        ApiVersionsResponse response = new ApiVersionsResponse.Builder().
            setThrottleTimeMs(10).
            setApiVersions(ApiVersionsResponse.filterApis(
                RecordVersion.V1,
                ListenerType.BROKER,
                true,
                false)).
            setSupportedFeatures(Features.emptySupportedFeatures()).
            setFinalizedFeatures(Collections.emptyMap()).
            setFinalizedFeaturesEpoch(ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH).
            build();
        verifyApiKeysForTelemetry(response, 0);
    }

    @Test
    public void testMetadataQuorumApisAreDisabled() {
        ApiVersionsResponse response = new ApiVersionsResponse.Builder().
            setThrottleTimeMs(AbstractResponse.DEFAULT_THROTTLE_TIME).
            setApiVersions(ApiVersionsResponse.filterApis(
                RecordVersion.current(),
                ListenerType.ZK_BROKER,
                true,
                true)).
            setSupportedFeatures(Features.emptySupportedFeatures()).
            setFinalizedFeatures(Collections.emptyMap()).
            setFinalizedFeaturesEpoch(ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH).
            build();
        // Ensure that APIs needed for the KRaft mode are not exposed through ApiVersions until we are ready for them
        HashSet<ApiKeys> exposedApis = apiKeysInResponse(response);
        assertFalse(exposedApis.contains(ApiKeys.VOTE));
        assertFalse(exposedApis.contains(ApiKeys.BEGIN_QUORUM_EPOCH));
        assertFalse(exposedApis.contains(ApiKeys.END_QUORUM_EPOCH));
        assertFalse(exposedApis.contains(ApiKeys.DESCRIBE_QUORUM));
    }

    @Test
    public void testIntersect() {
        assertFalse(ApiVersionsResponse.intersect(null, null).isPresent());
        assertThrows(IllegalArgumentException.class,
            () -> ApiVersionsResponse.intersect(new ApiVersion().setApiKey((short) 10), new ApiVersion().setApiKey((short) 3)));

        short min = 0;
        short max = 10;
        ApiVersion thisVersion = new ApiVersion()
                .setApiKey(ApiKeys.FETCH.id)
                .setMinVersion(min)
                .setMaxVersion(Short.MAX_VALUE);

        ApiVersion other = new ApiVersion()
                .setApiKey(ApiKeys.FETCH.id)
                .setMinVersion(Short.MIN_VALUE)
                .setMaxVersion(max);

        ApiVersion expected = new ApiVersion()
                .setApiKey(ApiKeys.FETCH.id)
                .setMinVersion(min)
                .setMaxVersion(max);

        assertFalse(ApiVersionsResponse.intersect(thisVersion, null).isPresent());
        assertFalse(ApiVersionsResponse.intersect(null, other).isPresent());

        assertEquals(expected, ApiVersionsResponse.intersect(thisVersion, other).get());
        // test for symmetric
        assertEquals(expected, ApiVersionsResponse.intersect(other, thisVersion).get());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testAlterV0Features(boolean alterV0Features) {
        Features<SupportedVersionRange> supported =
            Features.supportedFeatures(Collections.singletonMap("my.feature",
                new SupportedVersionRange((short) 0, (short) 1)));
        ApiVersionsResponse response = new ApiVersionsResponse.Builder().
            setApiVersions(ApiVersionsResponse.filterApis(
                RecordVersion.current(),
                ListenerType.BROKER,
                true,
                true)).
            setSupportedFeatures(supported).
            setFinalizedFeatures(Collections.emptyMap()).
            setFinalizedFeaturesEpoch(ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH).
            setAlterFeatureLevel0(alterV0Features).
            build();
        if (alterV0Features) {
            assertNull(response.data().supportedFeatures().find("my.feature"));
        } else {
            assertEquals(new SupportedFeatureKey().
                setName("my.feature").
                setMinVersion((short) 0).
                setMaxVersion((short) 1),
                response.data().supportedFeatures().find("my.feature"));
        }
    }

    private void verifyVersions(short forwardableAPIKey,
                                short minVersion,
                                short maxVersion,
                                ApiVersionCollection commonResponse) {
        ApiVersion expectedVersionsForForwardableAPI =
            new ApiVersion()
                .setApiKey(forwardableAPIKey)
                .setMinVersion(minVersion)
                .setMaxVersion(maxVersion);
        assertEquals(expectedVersionsForForwardableAPI, commonResponse.find(forwardableAPIKey));
    }

    private void verifyApiKeysForMagic(ApiVersionsResponse response, Byte maxMagic) {
        for (ApiVersion version : response.data().apiKeys()) {
            assertTrue(ApiKeys.forId(version.apiKey()).minRequiredInterBrokerMagic <= maxMagic);
        }
    }

    private void verifyApiKeysForTelemetry(ApiVersionsResponse response, int expectedCount) {
        int count = 0;
        for (ApiVersion version : response.data().apiKeys()) {
            if (version.apiKey() == ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS.id || version.apiKey() == ApiKeys.PUSH_TELEMETRY.id) {
                count++;
            }
        }
        assertEquals(expectedCount, count);
    }

    private HashSet<ApiKeys> apiKeysInResponse(ApiVersionsResponse apiVersions) {
        HashSet<ApiKeys> apiKeys = new HashSet<>();
        for (ApiVersion version : apiVersions.data().apiKeys()) {
            apiKeys.add(ApiKeys.forId(version.apiKey()));
        }
        return apiKeys;
    }

}
