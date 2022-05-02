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

package org.apache.kafka.server.common;

import java.util.Arrays;
import java.util.HashSet;
import org.apache.kafka.common.feature.Features;
import org.apache.kafka.common.feature.FinalizedVersionRange;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.message.ApiMessageType.ListenerType;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.ApiVersionsResponseData.FinalizedFeatureKey;
import org.apache.kafka.common.message.ApiVersionsResponseData.SupportedFeatureKey;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.RecordVersion;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.utils.Utils;

import static org.apache.kafka.server.common.MetadataVersion.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

class MetadataVersionTest {

    @Test
    public void testFeatureLevel() {
        int firstFeatureLevelIndex = Arrays.asList(MetadataVersion.VALUES).indexOf(IBP_3_0_IV0);
        for (int i = 0; i < firstFeatureLevelIndex; i++) {
            assertFalse(MetadataVersion.VALUES[i].featureLevel().isPresent());
        }
        short expectedFeatureLevel = 1;
        for (int i = firstFeatureLevelIndex; i < MetadataVersion.VALUES.length; i++) {
            MetadataVersion metadataVersion = MetadataVersion.VALUES[i];
            short featureLevel = metadataVersion.featureLevel().orElseThrow(() ->
                new IllegalArgumentException(
                    String.format("Metadata version %s must have a non-null feature level", metadataVersion.version())));
            assertEquals(expectedFeatureLevel, featureLevel,
                String.format("Metadata version %s should have feature level %s", metadataVersion.version(), expectedFeatureLevel));
            expectedFeatureLevel += 1;
        }
    }

    @Test
    public void testFromVersionString() {
        assertEquals(IBP_0_8_0, MetadataVersion.fromVersionString("0.8.0"));
        assertEquals(IBP_0_8_0, MetadataVersion.fromVersionString("0.8.0.0"));
        assertEquals(IBP_0_8_0, MetadataVersion.fromVersionString("0.8.0.1"));
        // should throw an exception as long as IBP_8_0_IV0 is not defined
        assertThrows(IllegalArgumentException.class, () -> MetadataVersion.fromVersionString("8.0"));

        assertEquals(IBP_0_8_1, MetadataVersion.fromVersionString("0.8.1"));
        assertEquals(IBP_0_8_1, MetadataVersion.fromVersionString("0.8.1.0"));
        assertEquals(IBP_0_8_1, MetadataVersion.fromVersionString("0.8.1.1"));

        assertEquals(IBP_0_8_2, MetadataVersion.fromVersionString("0.8.2"));
        assertEquals(IBP_0_8_2, MetadataVersion.fromVersionString("0.8.2.0"));
        assertEquals(IBP_0_8_2, MetadataVersion.fromVersionString("0.8.2.1"));

        assertEquals(IBP_0_9_0, MetadataVersion.fromVersionString("0.9.0"));
        assertEquals(IBP_0_9_0, MetadataVersion.fromVersionString("0.9.0.0"));
        assertEquals(IBP_0_9_0, MetadataVersion.fromVersionString("0.9.0.1"));

        assertEquals(IBP_0_10_0_IV0, MetadataVersion.fromVersionString("0.10.0-IV0"));

        assertEquals(IBP_0_10_0_IV1, MetadataVersion.fromVersionString("0.10.0"));
        assertEquals(IBP_0_10_0_IV1, MetadataVersion.fromVersionString("0.10.0.0"));
        assertEquals(IBP_0_10_0_IV1, MetadataVersion.fromVersionString("0.10.0.0-IV0"));
        assertEquals(IBP_0_10_0_IV1, MetadataVersion.fromVersionString("0.10.0.1"));

        assertEquals(IBP_0_10_1_IV0, MetadataVersion.fromVersionString("0.10.1-IV0"));
        assertEquals(IBP_0_10_1_IV1, MetadataVersion.fromVersionString("0.10.1-IV1"));

        assertEquals(IBP_0_10_1_IV2, MetadataVersion.fromVersionString("0.10.1"));
        assertEquals(IBP_0_10_1_IV2, MetadataVersion.fromVersionString("0.10.1.0"));
        assertEquals(IBP_0_10_1_IV2, MetadataVersion.fromVersionString("0.10.1-IV2"));
        assertEquals(IBP_0_10_1_IV2, MetadataVersion.fromVersionString("0.10.1.1"));

        assertEquals(IBP_0_10_2_IV0, MetadataVersion.fromVersionString("0.10.2"));
        assertEquals(IBP_0_10_2_IV0, MetadataVersion.fromVersionString("0.10.2.0"));
        assertEquals(IBP_0_10_2_IV0, MetadataVersion.fromVersionString("0.10.2-IV0"));
        assertEquals(IBP_0_10_2_IV0, MetadataVersion.fromVersionString("0.10.2.1"));

        assertEquals(IBP_0_11_0_IV0, MetadataVersion.fromVersionString("0.11.0-IV0"));
        assertEquals(IBP_0_11_0_IV1, MetadataVersion.fromVersionString("0.11.0-IV1"));

        assertEquals(IBP_0_11_0_IV2, MetadataVersion.fromVersionString("0.11.0"));
        assertEquals(IBP_0_11_0_IV2, MetadataVersion.fromVersionString("0.11.0.0"));
        assertEquals(IBP_0_11_0_IV2, MetadataVersion.fromVersionString("0.11.0-IV2"));
        assertEquals(IBP_0_11_0_IV2, MetadataVersion.fromVersionString("0.11.0.1"));

        assertEquals(IBP_1_0_IV0, MetadataVersion.fromVersionString("1.0"));
        assertEquals(IBP_1_0_IV0, MetadataVersion.fromVersionString("1.0.0"));
        assertEquals(IBP_1_0_IV0, MetadataVersion.fromVersionString("1.0.0-IV0"));
        assertEquals(IBP_1_0_IV0, MetadataVersion.fromVersionString("1.0.1"));
        assertThrows(IllegalArgumentException.class, () -> MetadataVersion.fromVersionString("0.1.0"));
        assertThrows(IllegalArgumentException.class, () -> MetadataVersion.fromVersionString("0.1.0.0"));
        assertThrows(IllegalArgumentException.class, () -> MetadataVersion.fromVersionString("0.1.0-IV0"));
        assertThrows(IllegalArgumentException.class, () -> MetadataVersion.fromVersionString("0.1.0.0-IV0"));

        assertEquals(IBP_1_1_IV0, MetadataVersion.fromVersionString("1.1-IV0"));

        assertEquals(IBP_2_0_IV1, MetadataVersion.fromVersionString("2.0"));
        assertEquals(IBP_2_0_IV0, MetadataVersion.fromVersionString("2.0-IV0"));
        assertEquals(IBP_2_0_IV1, MetadataVersion.fromVersionString("2.0-IV1"));

        assertEquals(IBP_2_1_IV2, MetadataVersion.fromVersionString("2.1"));
        assertEquals(IBP_2_1_IV0, MetadataVersion.fromVersionString("2.1-IV0"));
        assertEquals(IBP_2_1_IV1, MetadataVersion.fromVersionString("2.1-IV1"));
        assertEquals(IBP_2_1_IV2, MetadataVersion.fromVersionString("2.1-IV2"));

        assertEquals(IBP_2_2_IV1, MetadataVersion.fromVersionString("2.2"));
        assertEquals(IBP_2_2_IV0, MetadataVersion.fromVersionString("2.2-IV0"));
        assertEquals(IBP_2_2_IV1, MetadataVersion.fromVersionString("2.2-IV1"));

        assertEquals(IBP_2_3_IV1, MetadataVersion.fromVersionString("2.3"));
        assertEquals(IBP_2_3_IV0, MetadataVersion.fromVersionString("2.3-IV0"));
        assertEquals(IBP_2_3_IV1, MetadataVersion.fromVersionString("2.3-IV1"));

        assertEquals(IBP_2_4_IV1, MetadataVersion.fromVersionString("2.4"));
        assertEquals(IBP_2_4_IV0, MetadataVersion.fromVersionString("2.4-IV0"));
        assertEquals(IBP_2_4_IV1, MetadataVersion.fromVersionString("2.4-IV1"));

        assertEquals(IBP_2_5_IV0, MetadataVersion.fromVersionString("2.5"));
        assertEquals(IBP_2_5_IV0, MetadataVersion.fromVersionString("2.5-IV0"));

        assertEquals(IBP_2_6_IV0, MetadataVersion.fromVersionString("2.6"));
        assertEquals(IBP_2_6_IV0, MetadataVersion.fromVersionString("2.6-IV0"));

        assertEquals(IBP_2_7_IV0, MetadataVersion.fromVersionString("2.7-IV0"));
        assertEquals(IBP_2_7_IV1, MetadataVersion.fromVersionString("2.7-IV1"));
        assertEquals(IBP_2_7_IV2, MetadataVersion.fromVersionString("2.7-IV2"));

        assertEquals(IBP_2_8_IV1, MetadataVersion.fromVersionString("2.8"));
        assertEquals(IBP_2_8_IV0, MetadataVersion.fromVersionString("2.8-IV0"));
        assertEquals(IBP_2_8_IV1, MetadataVersion.fromVersionString("2.8-IV1"));

        assertEquals(IBP_3_0_IV1, MetadataVersion.fromVersionString("3.0"));
        assertEquals(IBP_3_0_IV0, MetadataVersion.fromVersionString("3.0-IV0"));
        assertEquals(IBP_3_0_IV1, MetadataVersion.fromVersionString("3.0-IV1"));

        assertEquals(IBP_3_1_IV0, MetadataVersion.fromVersionString("3.1"));
        assertEquals(IBP_3_1_IV0, MetadataVersion.fromVersionString("3.1-IV0"));

        assertEquals(IBP_3_2_IV0, MetadataVersion.fromVersionString("3.2"));
        assertEquals(IBP_3_2_IV0, MetadataVersion.fromVersionString("3.2-IV0"));
    }

    @Test
    public void testMinSupportedVersionFor() {
        assertEquals(IBP_0_8_0, MetadataVersion.minSupportedFor(RecordVersion.V0));
        assertEquals(IBP_0_10_0_IV0, MetadataVersion.minSupportedFor(RecordVersion.V1));
        assertEquals(IBP_0_11_0_IV0, MetadataVersion.minSupportedFor(RecordVersion.V2));

        // Ensure that all record versions have a defined min version so that we remember to update the method
        for (RecordVersion recordVersion : RecordVersion.values()) {
            assertNotNull(MetadataVersion.minSupportedFor(recordVersion));
        }
    }

    @Test
    public void testShortVersion() {
        assertEquals("0.8.0", IBP_0_8_0.shortVersion());
        assertEquals("0.10.0", IBP_0_10_0_IV0.shortVersion());
        assertEquals("0.10.0", IBP_0_10_0_IV1.shortVersion());
        assertEquals("0.11.0", IBP_0_11_0_IV0.shortVersion());
        assertEquals("0.11.0", IBP_0_11_0_IV1.shortVersion());
        assertEquals("0.11.0", IBP_0_11_0_IV2.shortVersion());
        assertEquals("1.0", IBP_1_0_IV0.shortVersion());
        assertEquals("1.1", IBP_1_1_IV0.shortVersion());
        assertEquals("2.0", IBP_2_0_IV0.shortVersion());
        assertEquals("2.0", IBP_2_0_IV1.shortVersion());
        assertEquals("2.1", IBP_2_1_IV0.shortVersion());
        assertEquals("2.1", IBP_2_1_IV1.shortVersion());
        assertEquals("2.1", IBP_2_1_IV2.shortVersion());
        assertEquals("2.2", IBP_2_2_IV0.shortVersion());
        assertEquals("2.2", IBP_2_2_IV1.shortVersion());
        assertEquals("2.3", IBP_2_3_IV0.shortVersion());
        assertEquals("2.3", IBP_2_3_IV1.shortVersion());
        assertEquals("2.4", IBP_2_4_IV0.shortVersion());
        assertEquals("2.5", IBP_2_5_IV0.shortVersion());
        assertEquals("2.6", IBP_2_6_IV0.shortVersion());
        assertEquals("2.7", IBP_2_7_IV2.shortVersion());
        assertEquals("2.8", IBP_2_8_IV0.shortVersion());
        assertEquals("2.8", IBP_2_8_IV1.shortVersion());
        assertEquals("3.0", IBP_3_0_IV0.shortVersion());
        assertEquals("3.0", IBP_3_0_IV1.shortVersion());
        assertEquals("3.1", IBP_3_1_IV0.shortVersion());
        assertEquals("3.2", IBP_3_2_IV0.shortVersion());
    }

    @Test
    public void testVersion() {
        assertEquals("0.8.0", IBP_0_8_0.version());
        assertEquals("0.8.2", IBP_0_8_2.version());
        assertEquals("0.10.0-IV0", IBP_0_10_0_IV0.version());
        assertEquals("0.10.0-IV1", IBP_0_10_0_IV1.version());
        assertEquals("0.11.0-IV0", IBP_0_11_0_IV0.version());
        assertEquals("0.11.0-IV1", IBP_0_11_0_IV1.version());
        assertEquals("0.11.0-IV2", IBP_0_11_0_IV2.version());
        assertEquals("1.0-IV0", IBP_1_0_IV0.version());
        assertEquals("1.1-IV0", IBP_1_1_IV0.version());
        assertEquals("2.0-IV0", IBP_2_0_IV0.version());
        assertEquals("2.0-IV1", IBP_2_0_IV1.version());
        assertEquals("2.1-IV0", IBP_2_1_IV0.version());
        assertEquals("2.1-IV1", IBP_2_1_IV1.version());
        assertEquals("2.1-IV2", IBP_2_1_IV2.version());
        assertEquals("2.2-IV0", IBP_2_2_IV0.version());
        assertEquals("2.2-IV1", IBP_2_2_IV1.version());
        assertEquals("2.3-IV0", IBP_2_3_IV0.version());
        assertEquals("2.3-IV1", IBP_2_3_IV1.version());
        assertEquals("2.4-IV0", IBP_2_4_IV0.version());
        assertEquals("2.5-IV0", IBP_2_5_IV0.version());
        assertEquals("2.6-IV0", IBP_2_6_IV0.version());
        assertEquals("2.7-IV2", IBP_2_7_IV2.version());
        assertEquals("2.8-IV0", IBP_2_8_IV0.version());
        assertEquals("2.8-IV1", IBP_2_8_IV1.version());
        assertEquals("3.0-IV0", IBP_3_0_IV0.version());
        assertEquals("3.0-IV1", IBP_3_0_IV1.version());
        assertEquals("3.1-IV0", IBP_3_1_IV0.version());
        assertEquals("3.2-IV0", IBP_3_2_IV0.version());
    }

    @Test
    public void shouldCreateApiResponseOnlyWithKeysSupportedByMagicValue() {
        ApiVersionsResponse response = ApiVersionsResponse.createApiVersionsResponse(
            10,
            RecordVersion.V1,
            Features.emptySupportedFeatures(),
            Features.emptyFinalizedFeatures(),
            ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH,
            null,
            ListenerType.ZK_BROKER
        );
        verifyApiKeysForMagic(response, RecordBatch.MAGIC_VALUE_V1);
        assertEquals(10, response.throttleTimeMs());
        assertTrue(response.data().supportedFeatures().isEmpty());
        assertTrue(response.data().finalizedFeatures().isEmpty());
        assertEquals(ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH, response.data().finalizedFeaturesEpoch());
    }

    @Test
    public void shouldReturnFeatureKeysWhenMagicIsCurrentValueAndThrottleMsIsDefaultThrottle() {
        ApiVersionsResponse response = ApiVersionsResponse.createApiVersionsResponse(
            10,
            RecordVersion.V1,
            Features.supportedFeatures(
                Utils.mkMap(Utils.mkEntry("feature", new SupportedVersionRange((short) 1, (short) 4)))),
            Features.finalizedFeatures(
                Utils.mkMap(Utils.mkEntry("feature", new FinalizedVersionRange((short) 2, (short) 3)))),
            10L,
            null,
            ListenerType.ZK_BROKER
        );

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
        assertEquals(2, fKey.minVersionLevel());
        assertEquals(3, fKey.maxVersionLevel());
        assertEquals(10, response.data().finalizedFeaturesEpoch());
    }

    private void verifyApiKeysForMagic(ApiVersionsResponse response, Byte maxMagic) {
        for (ApiVersion version : response.data().apiKeys()) {
            assertTrue(ApiKeys.forId(version.apiKey()).minRequiredInterBrokerMagic <= maxMagic);
        }
    }

    @Test
    public void shouldReturnAllKeysWhenMagicIsCurrentValueAndThrottleMsIsDefaultThrottle() {
        ApiVersionsResponse response = ApiVersionsResponse.createApiVersionsResponse(
            AbstractResponse.DEFAULT_THROTTLE_TIME,
            RecordVersion.current(),
            Features.emptySupportedFeatures(),
            Features.emptyFinalizedFeatures(),
            ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH,
            null,
            ListenerType.ZK_BROKER
        );
        assertEquals(new HashSet<ApiKeys>(ApiKeys.zkBrokerApis()), apiKeysInResponse(response));
        assertEquals(AbstractResponse.DEFAULT_THROTTLE_TIME, response.throttleTimeMs());
        assertTrue(response.data().supportedFeatures().isEmpty());
        assertTrue(response.data().finalizedFeatures().isEmpty());
        assertEquals(ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH, response.data().finalizedFeaturesEpoch());
    }

    @Test
    public void testMetadataQuorumApisAreDisabled() {
        ApiVersionsResponse response = ApiVersionsResponse.createApiVersionsResponse(
            AbstractResponse.DEFAULT_THROTTLE_TIME,
            RecordVersion.current(),
            Features.emptySupportedFeatures(),
            Features.emptyFinalizedFeatures(),
            ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH,
            null,
            ListenerType.ZK_BROKER
        );

        // Ensure that APIs needed for the KRaft mode are not exposed through ApiVersions until we are ready for them
        HashSet<ApiKeys> exposedApis = apiKeysInResponse(response);
        assertFalse(exposedApis.contains(ApiKeys.ENVELOPE));
        assertFalse(exposedApis.contains(ApiKeys.VOTE));
        assertFalse(exposedApis.contains(ApiKeys.BEGIN_QUORUM_EPOCH));
        assertFalse(exposedApis.contains(ApiKeys.END_QUORUM_EPOCH));
        assertFalse(exposedApis.contains(ApiKeys.DESCRIBE_QUORUM));
    }

    private HashSet<ApiKeys> apiKeysInResponse(ApiVersionsResponse apiVersions) {
        HashSet<ApiKeys> apiKeys = new HashSet<>();
        for (ApiVersion version : apiVersions.data().apiKeys()) {
            apiKeys.add(ApiKeys.forId(version.apiKey()));
        }
        return apiKeys;
    }
}
