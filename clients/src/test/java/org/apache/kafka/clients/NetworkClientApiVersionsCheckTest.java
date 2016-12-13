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
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.Protocol;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.test.DelayedReceive;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.kafka.common.requests.ApiVersionsResponse.API_KEY_NAME;
import static org.apache.kafka.common.requests.ApiVersionsResponse.API_VERSIONS_KEY_NAME;
import static org.apache.kafka.common.requests.ApiVersionsResponse.ERROR_CODE_KEY_NAME;
import static org.apache.kafka.common.requests.ApiVersionsResponse.MAX_VERSION_KEY_NAME;
import static org.apache.kafka.common.requests.ApiVersionsResponse.MIN_VERSION_KEY_NAME;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NetworkClientApiVersionsCheckTest extends NetworkClientTest {

    private static final ApiKeys USED_API_KEY = ApiKeys.METADATA;
    private static final Collection<ApiVersionsResponse.ApiVersion> EXPECTED_API_VERSIONS = createExpectedApiVersions();

    private static Collection<ApiVersionsResponse.ApiVersion> createExpectedApiVersions() {
        List<ApiVersionsResponse.ApiVersion> expectedApiVersions = new ArrayList<>();
        expectedApiVersions.add(new ApiVersionsResponse.ApiVersion(USED_API_KEY.id, Protocol.MIN_VERSIONS[USED_API_KEY.id], Protocol.CURR_VERSION[USED_API_KEY.id]));
        return expectedApiVersions;
    }

    @Override
    protected Collection<ApiVersionsResponse.ApiVersion> getExpectedApiVersions() {
        return EXPECTED_API_VERSIONS;
    }

    @Test
    public void testUnsupportedLesserApiVersions() {
        unsupportedApiVersionsCheck(getExpectedApiVersions(), (short) (ProtoUtils.latestVersion(USED_API_KEY.id) + 1), Short.MAX_VALUE, "Node 0 does not support required versions for Api " + USED_API_KEY.id);
    }

    @Test
    public void testUnsupportedGreaterApiVersions() {
        unsupportedApiVersionsCheck(getExpectedApiVersions(), Short.MIN_VALUE, (short) (ProtoUtils.oldestVersion(USED_API_KEY.id) - 1), "Node 0 does not support required versions for Api " + USED_API_KEY.id);
    }

    @Test
    public void testUnsupportedMissingApiVersions() {
        unsupportedApiVersionsCheck(new ArrayList<ApiVersionsResponse.ApiVersion>(), Short.MIN_VALUE, (short) (Short.MAX_VALUE + 2), "Node 0 does not support Api " + USED_API_KEY.id);
    }

    private void unsupportedApiVersionsCheck(final Collection<ApiVersionsResponse.ApiVersion> expectedApiVersions, short minVersion, short maxVersion, String errorMessage) {
        ResponseHeader respHeader = new ResponseHeader(0);
        Struct resp = new Struct(ProtoUtils.currentResponseSchema(ApiKeys.API_VERSIONS.id));
        resp.set(ERROR_CODE_KEY_NAME, (short) 0);
        List<Struct> apiVersionList = new ArrayList<>();
        for (ApiVersionsResponse.ApiVersion apiVersion : expectedApiVersions) {
            Struct apiVersionStruct = resp.instance(API_VERSIONS_KEY_NAME);
            apiVersionStruct.set(API_KEY_NAME, apiVersion.apiKey);
            apiVersionStruct.set(MIN_VERSION_KEY_NAME, minVersion);
            apiVersionStruct.set(MAX_VERSION_KEY_NAME, maxVersion);
            apiVersionList.add(apiVersionStruct);
        }
        resp.set(API_VERSIONS_KEY_NAME, apiVersionList.toArray());
        int size = respHeader.sizeOf() + resp.sizeOf();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        respHeader.writeTo(buffer);
        resp.writeTo(buffer);
        buffer.flip();
        selector.delayedReceive(new DelayedReceive(node.idString(), new NetworkReceive(node.idString(), buffer)));
        try {
            while (!client.ready(node, time.milliseconds()))
                client.poll(1, time.milliseconds());
            fail("Api versions check failed. Should have thrown KafkaException as required api versions are not supported.");
        } catch (KafkaException kex) {
            assertTrue("Api versions check should have failed.", kex.getMessage().contains(errorMessage));
        }
    }
}
