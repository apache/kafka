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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@InterfaceStability.Evolving
public class DescribeProducersResult {

    private final Map<TopicPartition, KafkaFuture<PartitionProducerState>> futures;

    DescribeProducersResult(Map<TopicPartition, KafkaFuture<PartitionProducerState>> futures) {
        this.futures = futures;
    }

    public KafkaFuture<PartitionProducerState> partitionResult(final TopicPartition partition) {
        KafkaFuture<PartitionProducerState> future = futures.get(partition);
        if (future == null) {
            throw new IllegalArgumentException("Topic partition " + partition +
                " was not included in the request");
        }
        return future;
    }

    public KafkaFuture<Map<TopicPartition, PartitionProducerState>> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]))
            .thenApply(nil -> {
                Map<TopicPartition, PartitionProducerState> results = new HashMap<>(futures.size());
                for (Map.Entry<TopicPartition, KafkaFuture<PartitionProducerState>> entry : futures.entrySet()) {
                    try {
                        results.put(entry.getKey(), entry.getValue().get());
                    } catch (InterruptedException | ExecutionException e) {
                        // This should be unreachable, because allOf ensured that all the futures completed successfully.
                        throw new KafkaException(e);
                    }
                }
                return results;
            });
    }

    public static class PartitionProducerState {
        private final List<ProducerState> activeProducers;

        public PartitionProducerState(List<ProducerState> activeProducers) {
            this.activeProducers = activeProducers;
        }

        public List<ProducerState> activeProducers() {
            return activeProducers;
        }

        @Override
        public String toString() {
            return "PartitionProducerState(" +
                "activeProducers=" + activeProducers +
                ')';
        }
    }

}
