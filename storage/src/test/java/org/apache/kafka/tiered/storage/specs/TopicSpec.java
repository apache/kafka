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
package org.apache.kafka.tiered.storage.specs;

import java.util.List;
import java.util.Map;

/**
 * Specifies a topic-partition with attributes customized for the purpose of tiered-storage tests.
 *
 * @param topicName               The name of the topic.
 * @param partitionCount          The number of partitions for the topic.
 * @param replicationFactor       The replication factor of the topic.
 * @param maxBatchCountPerSegment The maximal number of batch in segments of the topic.
 *                                This allows to obtain a fixed, pre-determined size for the segment, which ease
 *                                reasoning on the expected states of local and tiered storages.
 * @param properties              Configuration of the topic customized for the purpose of tiered-storage tests.
 */
public record TopicSpec(String topicName, int partitionCount, int replicationFactor, int maxBatchCountPerSegment,
                        Map<Integer, List<Integer>> assignment, Map<String, String> properties) {

    @Override
    public String toString() {
        return String.format(
                "Topic[name=%s partition-count=%d replication-factor=%d segment-size=%d assignment=%s properties=%s]",
                topicName, partitionCount, replicationFactor, maxBatchCountPerSegment, assignment, properties);
    }

}
