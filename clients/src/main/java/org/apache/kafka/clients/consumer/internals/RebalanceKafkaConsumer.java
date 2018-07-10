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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

/**
 * A class which is used during rebalance that is strictly for the usage of the mode "enable.parallel.rebalance"
 * It implements Runnable so that it can be run in concurrency with the old KafkaConsumer (which does not implement
 * Runnable or extends Thread: the user spawns the process, while Kafka internals spawns this one.)
 */
public class RebalanceKafkaConsumer<K, V> extends KafkaConsumer implements Runnable {
    private final Map<TopicPartition, Long> endOffsets;
    private final Set<TopicPartition> unfinished;

    public RebalanceKafkaConsumer(final Map<String, Object> configs,
                                  final Map<TopicPartition, Long> startOffsets,
                                  final Map<TopicPartition, Long> endOffsets) {
        super(configs, null, null);
        this.endOffsets = endOffsets;
        this.unfinished = new HashSet<>(startOffsets.keySet());
        // send join request -- not done
        //go to start positions i.e. the last committed offsets of the parent consumer
        for (Map.Entry<TopicPartition, Long> entry : startOffsets.entrySet()) {
            super.seek(entry.getKey(), entry.getValue());
        }
    }

    private Set<TopicPartition> findUnfinished() {
        final HashSet<TopicPartition> stillUnfinished = new HashSet<>();
        for (TopicPartition partition : unfinished) {
            if (position(partition) == endOffsets.get(partition)) {
                continue;
            }
            stillUnfinished.add(partition);
        }
        unfinished.retainAll(stillUnfinished);
        return unfinished;
    }

    public boolean terminated() {
        return findUnfinished().size() == 0;
    }

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        if (!terminated()) {
            return super.poll(timeout);
        }
        return null;
    }

    @Override
    public void run() {

    }

    // other methods still needs to be implemented
}
