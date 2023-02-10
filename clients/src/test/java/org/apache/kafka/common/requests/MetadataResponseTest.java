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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class MetadataResponseTest {

    @Test
    void buildClusterTest() {
        Uuid zeroUuid = new Uuid(0L, 0L);
        Uuid randomUuid = Uuid.randomUuid();
        MetadataResponseData.MetadataResponseTopic topicMetadata1 = new MetadataResponseData.MetadataResponseTopic()
                .setName("topic1")
                .setErrorCode(Errors.NONE.code())
                .setPartitions(emptyList())
                .setIsInternal(false);
        MetadataResponseData.MetadataResponseTopic topicMetadata2 = new MetadataResponseData.MetadataResponseTopic()
                .setName("topic2")
                .setErrorCode(Errors.NONE.code())
                .setTopicId(zeroUuid)
                .setPartitions(emptyList())
                .setIsInternal(false);
        MetadataResponseData.MetadataResponseTopic topicMetadata3 = new MetadataResponseData.MetadataResponseTopic()
                .setName("topic3")
                .setErrorCode(Errors.NONE.code())
                .setTopicId(randomUuid)
                .setPartitions(emptyList())
                .setIsInternal(false);

        MetadataResponseData.MetadataResponseTopicCollection topics =
                new MetadataResponseData.MetadataResponseTopicCollection();
        topics.add(topicMetadata1);
        topics.add(topicMetadata2);
        topics.add(topicMetadata3);
        MetadataResponse metadataResponse = new MetadataResponse(new MetadataResponseData().setTopics(topics),
                ApiKeys.METADATA.latestVersion());
        Cluster cluster = metadataResponse.buildCluster();
        assertNull(cluster.topicName(Uuid.ZERO_UUID));
        assertNull(cluster.topicName(zeroUuid));
        assertEquals("topic3", cluster.topicName(randomUuid));
    }
}
