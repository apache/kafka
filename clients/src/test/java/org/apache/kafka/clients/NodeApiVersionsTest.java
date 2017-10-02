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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.ApiVersionsResponse.ApiVersion;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NodeApiVersionsTest {

    @Test
    public void testUnsupportedVersionsToString() {
        NodeApiVersions versions = new NodeApiVersions(Collections.<ApiVersion>emptyList());
        StringBuilder bld = new StringBuilder();
        String prefix = "(";
        for (ApiKeys apiKey : ApiKeys.values()) {
            bld.append(prefix).append(apiKey.name).
                    append("(").append(apiKey.id).append("): UNSUPPORTED");
            prefix = ", ";
        }
        bld.append(")");
        assertEquals(bld.toString(), versions.toString());
    }

    @Test
    public void testUnknownApiVersionsToString() {
        ApiVersion unknownApiVersion = new ApiVersion((short) 337, (short) 0, (short) 1);
        NodeApiVersions versions = new NodeApiVersions(Collections.singleton(unknownApiVersion));
        assertTrue(versions.toString().endsWith("UNKNOWN(337): 0 to 1)"));
    }

    @Test
    public void testVersionsToString() {
        List<ApiVersion> versionList = new ArrayList<>();
        for (ApiKeys apiKey : ApiKeys.values()) {
            if (apiKey == ApiKeys.DELETE_TOPICS) {
                versionList.add(new ApiVersion(apiKey.id, (short) 10000, (short) 10001));
            } else {
                versionList.add(new ApiVersion(apiKey));
            }
        }
        NodeApiVersions versions = new NodeApiVersions(versionList);
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
        NodeApiVersions apiVersions = NodeApiVersions.create(Collections.singleton(
                new ApiVersion(ApiKeys.PRODUCE.id, (short) 1, (short) 3)));
        assertEquals(3, apiVersions.latestUsableVersion(ApiKeys.PRODUCE));
        assertEquals(1, apiVersions.latestUsableVersion(ApiKeys.PRODUCE, (short) 0, (short) 1));
        assertEquals(1, apiVersions.latestUsableVersion(ApiKeys.PRODUCE, (short) 1, (short) 1));
        assertEquals(2, apiVersions.latestUsableVersion(ApiKeys.PRODUCE, (short) 1, (short) 2));
        assertEquals(3, apiVersions.latestUsableVersion(ApiKeys.PRODUCE, (short) 1, (short) 3));
        assertEquals(2, apiVersions.latestUsableVersion(ApiKeys.PRODUCE, (short) 2, (short) 2));
        assertEquals(3, apiVersions.latestUsableVersion(ApiKeys.PRODUCE, (short) 2, (short) 3));
        assertEquals(3, apiVersions.latestUsableVersion(ApiKeys.PRODUCE, (short) 3, (short) 3));
        assertEquals(3, apiVersions.latestUsableVersion(ApiKeys.PRODUCE, (short) 3, (short) 4));
    }

    @Test(expected = UnsupportedVersionException.class)
    public void testLatestUsableVersionOutOfRangeLow() {
        NodeApiVersions apiVersions = NodeApiVersions.create(Collections.singleton(
                new ApiVersion(ApiKeys.PRODUCE.id, (short) 1, (short) 2)));
        apiVersions.latestUsableVersion(ApiKeys.PRODUCE, (short) 3, (short) 4);
    }

    @Test(expected = UnsupportedVersionException.class)
    public void testLatestUsableVersionOutOfRangeHigh() {
        NodeApiVersions apiVersions = NodeApiVersions.create(Collections.singleton(
                new ApiVersion(ApiKeys.PRODUCE.id, (short) 2, (short) 3)));
        apiVersions.latestUsableVersion(ApiKeys.PRODUCE, (short) 0, (short) 1);
    }

    @Test(expected = UnsupportedVersionException.class)
    public void testUsableVersionCalculationNoKnownVersions() {
        List<ApiVersion> versionList = new ArrayList<>();
        NodeApiVersions versions =  new NodeApiVersions(versionList);
        versions.latestUsableVersion(ApiKeys.FETCH);
    }

    @Test(expected = UnsupportedVersionException.class)
    public void testLatestUsableVersionOutOfRange() {
        NodeApiVersions apiVersions = NodeApiVersions.create(Collections.singleton(
                new ApiVersion(ApiKeys.PRODUCE.id, (short) 300, (short) 300)));
        apiVersions.latestUsableVersion(ApiKeys.PRODUCE);
    }

    @Test
    public void testUsableVersionLatestVersions() {
        List<ApiVersion> versionList = new LinkedList<>();
        for (ApiVersion apiVersion: ApiVersionsResponse.defaultApiVersionsResponse().apiVersions()) {
            versionList.add(apiVersion);
        }
        // Add an API key that we don't know about.
        versionList.add(new ApiVersion((short) 100, (short) 0, (short) 1));
        NodeApiVersions versions =  new NodeApiVersions(versionList);
        for (ApiKeys apiKey: ApiKeys.values()) {
            assertEquals(apiKey.latestVersion(), versions.latestUsableVersion(apiKey));
        }
    }
}
