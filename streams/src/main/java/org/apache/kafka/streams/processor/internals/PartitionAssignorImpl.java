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

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamingConfig;
import org.apache.kafka.streams.processor.PartitionGrouper;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PartitionAssignorImpl implements Configurable {

    private PartitionGrouper partitionGrouper;

    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs) {
        Object o = configs.get(StreamingConfig.InternalConfig.PARTITION_GROUPER_INSTANCE);
        if (o == null)
            throw new KafkaException("PartitionGrouper is not specified");

        if (!PartitionGrouper.class.isInstance(o))
            throw new KafkaException(o.getClass().getName() + " is not an instance of " + PartitionGrouper.class.getName());

        partitionGrouper = (PartitionGrouper) o;
    }

    public void partitionToTaskIds(List<TopicPartition> partitions, ByteBuffer data) {
        Map<TopicPartition, Set<Long>> partitionToTaskIds = new HashMap<>();

        int i = 0;
        for (TopicPartition partition : partitions) {
            Set<Long> taskIds = partitionToTaskIds.get(partition);
            if (taskIds == null) {
                taskIds = new HashSet<>();
                partitionToTaskIds.put(partition, taskIds);
            }
            taskIds.add(data.getLong(i * 8));
            i += 4;
        }
        partitionGrouper.partitionToTaskIds(Collections.unmodifiableMap(partitionToTaskIds));
    }

}
