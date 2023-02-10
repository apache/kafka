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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @Test
    public void testTopicIdAndNullTopicNameRequests() {
        // Construct invalid MetadataRequestTopics. We will build each one separately and ensure the error is thrown.
        List<MetadataRequestData.MetadataRequestTopic> topics = Arrays.asList(
                new MetadataRequestData.MetadataRequestTopic().setName(null).setTopicId(Uuid.randomUuid()),
                new MetadataRequestData.MetadataRequestTopic().setName(null),
                new MetadataRequestData.MetadataRequestTopic().setTopicId(Uuid.randomUuid()),
                new MetadataRequestData.MetadataRequestTopic().setName("topic").setTopicId(Uuid.randomUuid()));

        // if version is 10 or 11, the invalid topic metadata should return an error
        List<Short> invalidVersions = Arrays.asList((short) 10, (short) 11);
        invalidVersions.forEach(version -> topics.forEach(topic -> {
            MetadataRequestData metadataRequestData = new MetadataRequestData().setTopics(Collections.singletonList(topic));
            MetadataRequest.Builder builder = new MetadataRequest.Builder(metadataRequestData);
            assertThrows(UnsupportedVersionException.class, () -> builder.build(version));
        }));
    }

    @Test
    public void testTopicIdWithZeroUuid() {
        List<MetadataRequestData.MetadataRequestTopic> topics = Arrays.asList(
                new MetadataRequestData.MetadataRequestTopic().setName("topic").setTopicId(Uuid.ZERO_UUID),
                new MetadataRequestData.MetadataRequestTopic().setName("topic").setTopicId(new Uuid(0L, 0L)),
                new MetadataRequestData.MetadataRequestTopic().setName("topic"));

        List<Short> invalidVersions = Arrays.asList((short) 10, (short) 11);
        invalidVersions.forEach(version -> topics.forEach(topic -> {
            MetadataRequestData metadataRequestData = new MetadataRequestData().setTopics(Collections.singletonList(topic));
            MetadataRequest.Builder builder = new MetadataRequest.Builder(metadataRequestData);
            assertDoesNotThrow(() -> builder.build(version));
        }));
    }
}
