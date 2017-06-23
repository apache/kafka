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
package org.apache.kafka.common;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NodeVersionsTest {
    @Test
    public void testUnsupportedVersionsToString() {
        NodeVersions versions = new NodeVersions(Collections.<Short, ApiVersionRange>emptyMap());
        StringBuilder bld = new StringBuilder();
        String prefix = "(";
        for (ApiKey api : ApiKey.VALUES) {
            bld.append(prefix).append(api.title()).append("(").append(api.id()).append("): UNSUPPORTED");
            prefix = ", ";
        }
        bld.append(")");
        assertEquals(bld.toString(), versions.toString());
    }

    @Test
    public void testUnknownApiVersionsToString() {
        NodeVersions versions = new NodeVersions(Collections.singletonMap((short) 137,
            new ApiVersionRange((short) 0, (short) 1)));
        assertTrue("Expected to see unknown string at the end of " + versions.toString(),
            versions.toString().endsWith("Unknown(137): 0 to 1 [unusable: api too new])"));
    }

    @Test
    public void testVersionsToString() {
        Map<Short, ApiVersionRange> versions = new HashMap<>();
        for (ApiKey api : ApiKey.VALUES) {
            if (api == ApiKey.CONTROLLED_SHUTDOWN_KEY) {
                versions.put(api.id(), new ApiVersionRange((short) 0, (short) 0));
            } else if (api == ApiKey.DELETE_TOPICS) {
                versions.put(api.id(), new ApiVersionRange((short) 10000, (short) 10001));
            } else {
                versions.put(api.id(), new ApiVersionRange(api.supportedRange().lowest(), api.supportedRange().highest()));
            }
        }
        NodeVersions nodeVersions = new NodeVersions(versions);
        StringBuilder bld = new StringBuilder();
        String prefix = "(";
        for (ApiKey apiKey : ApiKey.values()) {
            bld.append(prefix);
            if (apiKey == ApiKey.CONTROLLED_SHUTDOWN_KEY) {
                bld.append("ControlledShutdown(7): 0 [unusable: node too old]");
            } else if (apiKey == ApiKey.DELETE_TOPICS) {
                bld.append("DeleteTopics(20): 10000 to 10001 [unusable: node too new]");
            } else {
                bld.append(apiKey.title()).append("(").append(apiKey.id()).append("): ");
                bld.append(apiKey.supportedRange());
                bld.append(" [usable: ").append(apiKey.supportedRange().highest()).append("]");
            }
            prefix = ", ";
        }
        bld.append(")");
        assertEquals(bld.toString(), nodeVersions.toString());
    }

    @Test
    public void testUsableVersionCalculation() {
        Map<Short, ApiVersionRange> versions = new HashMap<>();
        versions.put(ApiKey.CONTROLLED_SHUTDOWN_KEY.id(), new ApiVersionRange((short) 0, (short) 0));
        versions.put(ApiKey.FETCH.id(), new ApiVersionRange((short) 1, (short) 2));
        NodeVersions nodeVersions =  new NodeVersions(versions);
        try {
            nodeVersions.usableVersion(ApiKey.CONTROLLED_SHUTDOWN_KEY);
            Assert.fail("expected UnsupportedVersionException");
        } catch (UnsupportedVersionException e) {
            // pass
        }
        assertEquals(2, nodeVersions.usableVersion(ApiKey.FETCH));
    }
}
