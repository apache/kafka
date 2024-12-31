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
package org.apache.kafka.clients;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionCollection;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NodeApiVersionsTest {

    @Test
    public void testUnsupportedVersionsToString() {
        NodeApiVersions versions = new NodeApiVersions(new ApiVersionCollection(), Collections.emptyList(), false);
        StringBuilder bld = new StringBuilder();
        String prefix = "(";
        for (ApiKeys apiKey : ApiKeys.clientApis()) {
            bld.append(prefix).append(apiKey.name).
                    append("(").append(apiKey.id).append("): UNSUPPORTED");
            prefix = ", ";
        }
        bld.append(")");
        assertEquals(bld.toString(), versions.toString());
    }

    @Test
    public void testUnknownApiVersionsToString() {
        NodeApiVersions versions = NodeApiVersions.create((short) 337, (short) 0, (short) 1);
        assertTrue(versions.toString().endsWith("UNKNOWN(337): 0 to 1)"));
    }

    @Test
    public void testVersionsToString() {
        List<ApiVersion> versionList = new ArrayList<>();
        for (ApiKeys apiKey : ApiKeys.values()) {
            if (apiKey == ApiKeys.DELETE_TOPICS) {
                versionList.add(new ApiVersion()
                        .setApiKey(apiKey.id)
                        .setMinVersion((short) 10000)
                        .setMaxVersion((short) 10001));
            } else versionList.add(ApiVersionsResponse.toApiVersion(apiKey));
        }
        NodeApiVersions versions = new NodeApiVersions(versionList, Collections.emptyList(), false);
        StringBuilder bld = new StringBuilder();
        String prefix = "(";
        for (ApiKeys apiKey : ApiKeys.values()) {
            bld.append(prefix);
            if (apiKey == ApiKeys.DELETE_TOPICS) {
                bld.append("DeleteTopics(20): 10000 to 10001 [unusable: node too new]");
            } else {
                bld.append(apiKey.name).append("(").
                        append(apiKey.id).append("): ");
                if (apiKey.oldestVersion() ==
                        apiKey.latestVersion()) {
                    bld.append(apiKey.oldestVersion());
                } else {
                    bld.append(apiKey.oldestVersion()).
                            append(" to ").
                            append(apiKey.latestVersion());
                }
                bld.append(" [usable: ").append(apiKey.latestVersion()).
                        append("]");
            }
            prefix = ", ";
        }
        bld.append(")");
        assertEquals(bld.toString(), versions.toString());
    }

    @Test
    public void testLatestUsableVersion() {
        NodeApiVersions apiVersions = NodeApiVersions.create(ApiKeys.PRODUCE.id, (short) 8, (short) 10);
        assertEquals(10, apiVersions.latestUsableVersion(ApiKeys.PRODUCE));
        assertEquals(8, apiVersions.latestUsableVersion(ApiKeys.PRODUCE, (short) 7, (short) 8));
        assertEquals(8, apiVersions.latestUsableVersion(ApiKeys.PRODUCE, (short) 8, (short) 8));
        assertEquals(9, apiVersions.latestUsableVersion(ApiKeys.PRODUCE, (short) 8, (short) 9));
        assertEquals(10, apiVersions.latestUsableVersion(ApiKeys.PRODUCE, (short) 8, (short) 10));
        assertEquals(9, apiVersions.latestUsableVersion(ApiKeys.PRODUCE, (short) 9, (short) 9));
        assertEquals(10, apiVersions.latestUsableVersion(ApiKeys.PRODUCE, (short) 9, (short) 10));
        assertEquals(10, apiVersions.latestUsableVersion(ApiKeys.PRODUCE, (short) 10, (short) 10));
        assertEquals(10, apiVersions.latestUsableVersion(ApiKeys.PRODUCE, (short) 10, (short) 11));
    }

    @Test
    public void testLatestUsableVersionOutOfRangeLow() {
        NodeApiVersions apiVersions = NodeApiVersions.create(ApiKeys.PRODUCE.id, (short) 1, (short) 2);
        assertThrows(UnsupportedVersionException.class,
            () -> apiVersions.latestUsableVersion(ApiKeys.PRODUCE, (short) 3, (short) 4));
    }

    @Test
    public void testLatestUsableVersionOutOfRangeHigh() {
        NodeApiVersions apiVersions = NodeApiVersions.create(ApiKeys.PRODUCE.id, (short) 2, (short) 3);
        assertThrows(UnsupportedVersionException.class,
            () -> apiVersions.latestUsableVersion(ApiKeys.PRODUCE, (short) 0, (short) 1));
    }

    @Test
    public void testUsableVersionCalculationNoKnownVersions() {
        NodeApiVersions versions = new NodeApiVersions(new ApiVersionCollection(), Collections.emptyList(), false);
        assertThrows(UnsupportedVersionException.class,
            () -> versions.latestUsableVersion(ApiKeys.FETCH));
    }

    @Test
    public void testLatestUsableVersionOutOfRange() {
        NodeApiVersions apiVersions = NodeApiVersions.create(ApiKeys.PRODUCE.id, (short) 300, (short) 300);
        assertThrows(UnsupportedVersionException.class,
            () -> apiVersions.latestUsableVersion(ApiKeys.PRODUCE));
    }

    @ParameterizedTest
    @EnumSource(ApiMessageType.ListenerType.class)
    public void testUsableVersionLatestVersions(ApiMessageType.ListenerType scope) {
        ApiVersionsResponse defaultResponse = TestUtils.defaultApiVersionsResponse(scope);
        List<ApiVersion> versionList = new LinkedList<>(defaultResponse.data().apiKeys());
        // Add an API key that we don't know about.
        versionList.add(new ApiVersion()
                .setApiKey((short) 100)
                .setMinVersion((short) 0)
                .setMaxVersion((short) 1));
        NodeApiVersions versions = new NodeApiVersions(versionList, Collections.emptyList(), false);
        for (ApiKeys apiKey: ApiKeys.apisForListener(scope)) {
            assertEquals(apiKey.latestVersion(), versions.latestUsableVersion(apiKey));
        }
    }

    @ParameterizedTest
    @EnumSource(ApiMessageType.ListenerType.class)
    public void testConstructionFromApiVersionsResponse(ApiMessageType.ListenerType scope) {
        ApiVersionsResponse apiVersionsResponse = TestUtils.defaultApiVersionsResponse(scope);
        NodeApiVersions versions = new NodeApiVersions(apiVersionsResponse.data().apiKeys(), Collections.emptyList(), false);

        for (ApiVersion apiVersionKey : apiVersionsResponse.data().apiKeys()) {
            ApiVersion apiVersion = versions.apiVersion(ApiKeys.forId(apiVersionKey.apiKey()));
            assertEquals(apiVersionKey.apiKey(), apiVersion.apiKey());
            assertEquals(apiVersionKey.minVersion(), apiVersion.minVersion());
            assertEquals(apiVersionKey.maxVersion(), apiVersion.maxVersion());
        }
    }

    @Test
    public void testFeatures() {
        NodeApiVersions versions = new NodeApiVersions(
            Collections.emptyList(),
            Arrays.asList(new ApiVersionsResponseData.SupportedFeatureKey()
                .setName("transaction.version")
                .setMaxVersion((short) 2)
                .setMinVersion((short) 0)),
            false,
            Arrays.asList(new ApiVersionsResponseData.FinalizedFeatureKey()
                .setName("transaction.version")
                .setMaxVersionLevel((short) 2)
                .setMinVersionLevel((short) 2)),
            0);
        SupportedVersionRange supportedVersionRange = versions.supportedFeatures().get("transaction.version");
        assertEquals(0, supportedVersionRange.min());
        assertEquals(2, supportedVersionRange.max());
        assertEquals((short) 2, versions.finalizedFeatures().get("transaction.version"));
        assertEquals(0L, versions.finalizedFeaturesEpoch());
    }
}
