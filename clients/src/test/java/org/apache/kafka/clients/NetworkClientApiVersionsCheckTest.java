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


import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.Protocol;
import org.apache.kafka.common.requests.AbstractRequestResponse;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.test.DelayedReceive;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NetworkClientApiVersionsCheckTest extends NetworkClientTest {
    
    private static final List<ApiVersionsResponse.ApiVersion> EXPECTED_API_VERSIONS = Collections.singletonList(
            new ApiVersionsResponse.ApiVersion(ApiKeys.METADATA.id, Protocol.MIN_VERSIONS[ApiKeys.METADATA.id],
                    Protocol.CURR_VERSION[ApiKeys.METADATA.id]));

    @Override
    protected List<ApiVersionsResponse.ApiVersion> expectedApiVersions() {
        return EXPECTED_API_VERSIONS;
    }

    @Test
    public void testUnsupportedLesserApiVersions() {
        unsupportedApiVersionsCheck(expectedApiVersions(), (short) (ProtoUtils.latestVersion(ApiKeys.METADATA.id) + 1), Short.MAX_VALUE, "Node 0 does not support required versions for Api " + ApiKeys.METADATA.id);
    }

    @Test
    public void testUnsupportedGreaterApiVersions() {
        unsupportedApiVersionsCheck(expectedApiVersions(), Short.MIN_VALUE, (short) (ProtoUtils.oldestVersion(ApiKeys.METADATA.id) - 1), "Node 0 does not support required versions for Api " + ApiKeys.METADATA.id);
    }

    @Test
    public void testUnsupportedMissingApiVersions() {
        unsupportedApiVersionsCheck(Collections.<ApiVersionsResponse.ApiVersion>emptyList(), Short.MIN_VALUE, Short.MAX_VALUE, "Node 0 does not support Api " + ApiKeys.METADATA.id);
    }

    private void unsupportedApiVersionsCheck(final List<ApiVersionsResponse.ApiVersion> expectedApiVersions,
                                             short minVersion, short maxVersion, String errorMessage) {
        ResponseHeader responseHeader = new ResponseHeader(0);
        List<ApiVersionsResponse.ApiVersion> apiVersions = new ArrayList<>();
        for (ApiVersionsResponse.ApiVersion apiVersion : expectedApiVersions)
            apiVersions.add(new ApiVersionsResponse.ApiVersion(apiVersion.apiKey, minVersion, maxVersion));
        ApiVersionsResponse response = new ApiVersionsResponse(Errors.NONE.code(), apiVersions);
        ByteBuffer buffer = AbstractRequestResponse.serialize(responseHeader, response);

        selector.delayedReceive(new DelayedReceive(node.idString(), new NetworkReceive(node.idString(), buffer)));
        try {
            long deadline = time.milliseconds() + TestUtils.DEFAULT_MAX_WAIT_MS;
            while (time.milliseconds() < deadline && !client.ready(node, time.milliseconds()))
                client.poll(1, time.milliseconds());

            fail("KafkaException should have been thrown for " + expectedApiVersions + ", minVersion: " + minVersion +
                    ", maxVersion: " + maxVersion);
        } catch (KafkaException kex) {
            assertTrue("Exception containing `" + errorMessage + "` should have been thrown due to ApiVersions " +
                    "check, but exception message was: " + kex.getMessage(), kex.getMessage().contains(errorMessage));
        }
    }
}
