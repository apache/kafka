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

import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetadataRequestTest {

    @Test
    public void testEmptyMeansAllTopicsV0() {
        MetadataRequestData data = new MetadataRequestData();
        MetadataRequest parsedRequest = new MetadataRequest(data, (short) 0);
        assertTrue(parsedRequest.isAllTopics());
        assertNull(parsedRequest.topics());
    }

    @Test
    public void testEmptyMeansEmptyForVersionsAboveV0() {
        for (int i = 1; i < MetadataRequestData.SCHEMAS.length; i++) {
            MetadataRequestData data = new MetadataRequestData();
            data.setAllowAutoTopicCreation(true);
            MetadataRequest parsedRequest = new MetadataRequest(data, (short) i);
            assertFalse(parsedRequest.isAllTopics());
            assertEquals(Collections.emptyList(), parsedRequest.topics());
        }
    }

    @Test
    public void testMetadataRequestVersion() {
        MetadataRequest.Builder builder = new MetadataRequest.Builder(Collections.singletonList("topic"), false);
        assertEquals(ApiKeys.METADATA.oldestVersion(), builder.oldestAllowedVersion());
        assertEquals(ApiKeys.METADATA.latestVersion(), builder.latestAllowedVersion());

        short version = 5;
        MetadataRequest.Builder builder2 = new MetadataRequest.Builder(Collections.singletonList("topic"), false, version);
        assertEquals(version, builder2.oldestAllowedVersion());
        assertEquals(version, builder2.latestAllowedVersion());

        short minVersion = 1;
        short maxVersion = 6;
        MetadataRequest.Builder builder3 = new MetadataRequest.Builder(Collections.singletonList("topic"), false, minVersion, maxVersion);
        assertEquals(minVersion, builder3.oldestAllowedVersion());
        assertEquals(maxVersion, builder3.latestAllowedVersion());
    }
}
