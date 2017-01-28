/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.ApiVersionsResponse.ApiVersion;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

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
    public void testVersionsToString() {
        List<ApiVersion> versionList = new ArrayList<>();
        for (ApiKeys apiKey : ApiKeys.values()) {
            if (apiKey == ApiKeys.CONTROLLED_SHUTDOWN_KEY) {
                versionList.add(new ApiVersion(apiKey.id, (short) 0, (short) 0));
            } else if (apiKey == ApiKeys.DELETE_TOPICS) {
                versionList.add(new ApiVersion(apiKey.id, (short) 10000, (short) 10001));
            } else {
                versionList.add(new ApiVersion(apiKey.id,
                        ProtoUtils.oldestVersion(apiKey.id), ProtoUtils.latestVersion(apiKey.id)));
            }
        }
        NodeApiVersions versions = new NodeApiVersions(versionList);
        StringBuilder bld = new StringBuilder();
        String prefix = "(";
        for (ApiKeys apiKey : ApiKeys.values()) {
            bld.append(prefix);
            if (apiKey == ApiKeys.CONTROLLED_SHUTDOWN_KEY) {
                bld.append("ControlledShutdown(7): 0 [unusable: node too old]");
            } else if (apiKey == ApiKeys.DELETE_TOPICS) {
                bld.append("DeleteTopics(20): 10000 to 10001 [unusable: node too new]");
            } else {
                bld.append(apiKey.name).append("(").
                        append(apiKey.id).append("): ");
                if (ProtoUtils.oldestVersion(apiKey.id) ==
                        ProtoUtils.latestVersion(apiKey.id)) {
                    bld.append(ProtoUtils.oldestVersion(apiKey.id));
                } else {
                    bld.append(ProtoUtils.oldestVersion(apiKey.id)).
                            append(" to ").
                            append(ProtoUtils.latestVersion(apiKey.id));
                }
                bld.append(" [usable: ").append(ProtoUtils.latestVersion(apiKey.id)).
                        append("]");
            }
            prefix = ", ";
        }
        bld.append(")");
        assertEquals(bld.toString(), versions.toString());
    }

    @Test
    public void testUsableVersionCalculation() {
        List<ApiVersion> versionList = new ArrayList<>();
        versionList.add(new ApiVersion(ApiKeys.CONTROLLED_SHUTDOWN_KEY.id, (short) 0, (short) 0));
        versionList.add(new ApiVersion(ApiKeys.FETCH.id, (short) 1, (short) 2));
        NodeApiVersions versions =  new NodeApiVersions(versionList);
        try {
            versions.usableVersion(ApiKeys.CONTROLLED_SHUTDOWN_KEY);
            Assert.fail("expected UnsupportedVersionException");
        } catch (UnsupportedVersionException e) {
            // pass
        }
        assertEquals(2, versions.usableVersion(ApiKeys.FETCH));
    }

    @Test(expected = UnsupportedVersionException.class)
    public void testUsableVersionCalculationNoKnownVersions() {
        List<ApiVersion> versionList = new ArrayList<>();
        NodeApiVersions versions =  new NodeApiVersions(versionList);
        versions.usableVersion(ApiKeys.FETCH);
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
        for (ApiKeys apiKey: ApiKeys.values()) {
            assertEquals(ProtoUtils.latestVersion(apiKey.id), versions.usableVersion(apiKey));
        }
    }
}
