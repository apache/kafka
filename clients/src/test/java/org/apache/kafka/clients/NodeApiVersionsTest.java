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

import org.apache.kafka.common.ApiKey;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.ApiVersionsResponse.ApiVersion;
import org.junit.Assert;
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
        for (ApiKey api : ApiKey.VALUES) {
            bld.append(prefix).append(api.title()).
                    append("(").append(api.id()).append("): UNSUPPORTED");
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
        for (ApiKey api : ApiKey.values()) {
            if (api == ApiKey.CONTROLLED_SHUTDOWN_KEY) {
                versionList.add(new ApiVersion(api.id(), (short) 0, (short) 0));
            } else if (api == ApiKey.DELETE_TOPICS) {
                versionList.add(new ApiVersion(api.id(), (short) 10000, (short) 10001));
            } else {
                versionList.add(new ApiVersion(api));
            }
        }
        NodeApiVersions versions = new NodeApiVersions(versionList);
        StringBuilder bld = new StringBuilder();
        String prefix = "(";
        for (ApiKey api : ApiKey.VALUES) {
            bld.append(prefix);
            if (api == ApiKey.CONTROLLED_SHUTDOWN_KEY) {
                bld.append("ControlledShutdown(7): 0 [unusable: node too old]");
            } else if (api == ApiKey.DELETE_TOPICS) {
                bld.append("DeleteTopics(20): 10000 to 10001 [unusable: node too new]");
            } else {
                bld.append(api.title()).append("(").append(api.id()).append("): ").append(api.supportedRange());
                bld.append(" [usable: ").append(api.supportedRange().highest()).append("]");
            }
            prefix = ", ";
        }
        bld.append(")");
        assertEquals(bld.toString(), versions.toString());
    }

    @Test
    public void testUsableVersionCalculation() {
        List<ApiVersion> versionList = new ArrayList<>();
        versionList.add(new ApiVersion(ApiKey.CONTROLLED_SHUTDOWN_KEY.id(), (short) 0, (short) 0));
        versionList.add(new ApiVersion(ApiKey.FETCH.id(), (short) 1, (short) 2));
        NodeApiVersions versions =  new NodeApiVersions(versionList);
        try {
            versions.usableVersion(ApiKey.CONTROLLED_SHUTDOWN_KEY);
            Assert.fail("expected UnsupportedVersionException");
        } catch (UnsupportedVersionException e) {
            // pass
        }
        assertEquals(2, versions.usableVersion(ApiKey.FETCH));
    }

    @Test
    public void testUsableVersionNoDesiredVersionReturnsLatestUsable() {
        NodeApiVersions apiVersions = NodeApiVersions.create(Collections.singleton(
                new ApiVersion(ApiKey.PRODUCE.id(), (short) 1, (short) 3)));
        assertEquals(3, apiVersions.usableVersion(ApiKey.PRODUCE, null));
    }

    @Test
    public void testDesiredVersion() {
        NodeApiVersions apiVersions = NodeApiVersions.create(Collections.singleton(
                new ApiVersion(ApiKey.PRODUCE.id(), (short) 1, (short) 3)));
        assertEquals(1, apiVersions.usableVersion(ApiKey.PRODUCE, (short) 1));
        assertEquals(2, apiVersions.usableVersion(ApiKey.PRODUCE, (short) 2));
        assertEquals(3, apiVersions.usableVersion(ApiKey.PRODUCE, (short) 3));
    }

    @Test(expected = UnsupportedVersionException.class)
    public void testDesiredVersionTooLarge() {
        NodeApiVersions apiVersions = NodeApiVersions.create(Collections.singleton(
                new ApiVersion(ApiKey.PRODUCE.id(), (short) 1, (short) 2)));
        apiVersions.usableVersion(ApiKey.PRODUCE, (short) 3);
    }

    @Test(expected = UnsupportedVersionException.class)
    public void testDesiredVersionTooSmall() {
        NodeApiVersions apiVersions = NodeApiVersions.create(Collections.singleton(
                new ApiVersion(ApiKey.PRODUCE.id(), (short) 1, (short) 2)));
        apiVersions.usableVersion(ApiKey.PRODUCE, (short) 0);
    }

    @Test(expected = UnsupportedVersionException.class)
    public void testUsableVersionCalculationNoKnownVersions() {
        List<ApiVersion> versionList = new ArrayList<>();
        NodeApiVersions versions =  new NodeApiVersions(versionList);
        versions.usableVersion(ApiKey.FETCH);
    }

    @Test
    public void testUsableVersionLatestVersions() {
        List<ApiVersion> versionList = new LinkedList<>();
        for (ApiVersion apiVersion: ApiVersionsResponse.API_VERSIONS_RESPONSE.apiVersions()) {
            versionList.add(apiVersion);
        }
        // Add an API key that we don't know about.
        versionList.add(new ApiVersion((short) 100, (short) 0, (short) 1));
        NodeApiVersions versions =  new NodeApiVersions(versionList);
        for (ApiKey api: ApiKey.VALUES) {
            assertEquals(api.supportedRange().highest(), versions.usableVersion(api));
        }
    }
}
