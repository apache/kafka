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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.test.TestUtils;

import java.util.List;
import java.util.Set;

@ClusterTestDefaults(types = {Type.KRAFT})
public class CreateTopicsTest {

    @ClusterTest(brokers = 3)
    public void testCreateClusterAndCreateAndManyTopics(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            // Create many topics
            List<NewTopic> newTopics = List.of(
                new NewTopic("test-topic-1", 2, (short) 3),
                new NewTopic("test-topic-2", 2, (short) 3),
                new NewTopic("test-topic-3", 2, (short) 3)
            );
            CreateTopicsResult createTopicResult = admin.createTopics(newTopics);
            createTopicResult.all().get();

            // List created topics
            Set<String> expectedTopics = Set.of(
                "test-topic-1",
                "test-topic-2",
                "test-topic-3"
            );

            TestUtils.waitForCondition(
                () -> admin.listTopics().names().get().containsAll(expectedTopics),
                "Failed to find topics " + expectedTopics
            );
        }
    }
}