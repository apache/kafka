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

package org.apache.kafka.metadata.migration;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.image.AclsImage;
import org.apache.kafka.image.ClientQuotasImage;
import org.apache.kafka.image.ClusterImage;
import org.apache.kafka.image.ConfigurationsImage;
import org.apache.kafka.image.FeaturesImage;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.ProducerIdsImage;
import org.apache.kafka.image.ScramImage;
import org.apache.kafka.image.TopicsImageTest;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KRaftMigrationZkWriterTest {
    @Test
    public void testExtraneousZkPartitions() {
        CapturingTopicMigrationClient topicClient = new CapturingTopicMigrationClient() {
            @Override
            public void iterateTopics(EnumSet<TopicVisitorInterest> interests, TopicVisitor visitor) {
                Map<Integer, List<Integer>> assignments = new HashMap<>();
                assignments.put(0, Arrays.asList(2, 3, 4));
                assignments.put(1, Arrays.asList(3, 4, 5));
                assignments.put(2, Arrays.asList(2, 4, 5));
                assignments.put(3, Arrays.asList(1, 2, 3)); // This one is not in KRaft
                visitor.visitTopic("foo", TopicsImageTest.FOO_UUID, assignments);

                // Skip partition 1, visit 3 (the extra one)
                IntStream.of(0, 2, 3).forEach(partitionId -> {
                    visitor.visitPartition(
                        new TopicIdPartition(TopicsImageTest.FOO_UUID, new TopicPartition("foo", partitionId)),
                        TopicsImageTest.IMAGE1.getPartition(TopicsImageTest.FOO_UUID, partitionId)
                    );
                });

            }
        };

        CapturingConfigMigrationClient configClient = new CapturingConfigMigrationClient();
        CapturingAclMigrationClient aclClient = new CapturingAclMigrationClient();
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder()
            .setBrokersInZk(0)
            .setTopicMigrationClient(topicClient)
            .setConfigMigrationClient(configClient)
            .build();

        KRaftMigrationZkWriter writer = new KRaftMigrationZkWriter(migrationClient, (opLog, operation) -> {
            System.err.println(opLog);
            operation.apply(ZkMigrationLeadershipState.EMPTY);
        });

        MetadataImage image = new MetadataImage(
            MetadataProvenance.EMPTY,
            FeaturesImage.EMPTY,
            ClusterImage.EMPTY,
            TopicsImageTest.IMAGE1,     // This includes "foo" with 3 partitions
            ConfigurationsImage.EMPTY,
            ClientQuotasImage.EMPTY,
            ProducerIdsImage.EMPTY,
            AclsImage.EMPTY,
            ScramImage.EMPTY
        );

        writer.handleLoadSnapshot(image);
        assertEquals(topicClient.updatedTopics.get("foo").size(), 3);
        assertEquals(topicClient.deletedTopicPartitions.get("foo"), Collections.singleton(3));
        assertEquals(topicClient.updatedTopicPartitions.get("foo"), Collections.singleton(1));
    }
}
