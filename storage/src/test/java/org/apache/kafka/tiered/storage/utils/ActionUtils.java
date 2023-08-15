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
package org.apache.kafka.tiered.storage.utils;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.tiered.storage.TieredStorageTestContext;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class ActionUtils {

    public static TopicDescription describeTopic(TieredStorageTestContext context, String topic)
            throws ExecutionException, InterruptedException {
        return describeTopics(context, Collections.singletonList(topic)).get(topic);
    }

    public static Map<String, TopicDescription> describeTopics(TieredStorageTestContext context,
                                                                List<String> topics)
            throws ExecutionException, InterruptedException {
        return context.admin()
                .describeTopics(topics)
                .allTopicNames()
                .get();
    }

    /**
     * Get the records found in the local tiered storage.
     * Snapshot does not sort the filesets by base offset.
     * @param context The test context.
     * @param topicPartition The topic-partition of the records.
     * @return The records found in the local tiered storage.
     */
    public static List<Record> tieredStorageRecords(TieredStorageTestContext context,
                                                    TopicPartition topicPartition) {
        return context.takeTieredStorageSnapshot()
                .getFilesets(topicPartition)
                .stream()
                .map(fileset -> {
                    try {
                        return fileset.getRecords();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .sorted(Comparator.comparingLong(records -> records.get(0).offset()))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }
}
