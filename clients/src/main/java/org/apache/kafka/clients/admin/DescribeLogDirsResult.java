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

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.annotation.InterfaceStability;
import java.util.HashMap;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;


/**
 * The result of the {@link Admin#describeLogDirs(Collection)} call.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class DescribeLogDirsResult {
    private final Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> futures;

    DescribeLogDirsResult(Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> futures) {
        this.futures = futures;
    }

    /**
     * Return a map from brokerId to future which can be used to check the information of partitions on each individual broker.
     * @deprecated Deprecated Since Kafka 2.7. Use {@link #descriptions()}.
     */
    @Deprecated
    public Map<Integer, KafkaFuture<Map<String, DescribeLogDirsResponse.LogDirInfo>>> values() {
        return descriptions().entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().thenApply(map -> convertMapValues(map))));
    }

    @SuppressWarnings("deprecation")
    private Map<String, DescribeLogDirsResponse.LogDirInfo> convertMapValues(Map<String, LogDirDescription> map) {
        Stream<Map.Entry<String, LogDirDescription>> stream = map.entrySet().stream();
        return stream.collect(Collectors.toMap(
            Map.Entry::getKey,
            infoEntry -> {
                LogDirDescription logDir = infoEntry.getValue();
                return new DescribeLogDirsResponse.LogDirInfo(logDir.error() == null ? Errors.NONE : Errors.forException(logDir.error()),
                    logDir.replicaInfos().entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        replicaEntry -> new DescribeLogDirsResponse.ReplicaInfo(
                            replicaEntry.getValue().size(),
                            replicaEntry.getValue().offsetLag(),
                            replicaEntry.getValue().isFuture())
                )));
            }));
    }

    /**
     * Return a map from brokerId to future which can be used to check the information of partitions on each individual broker.
     * The result of the future is a map from broker log directory path to a description of that log directory.
     */
    public Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> descriptions() {
        return futures;
    }

    /**
     * Return a future which succeeds only if all the brokers have responded without error
     * @deprecated Deprecated Since Kafka 2.7. Use {@link #allDescriptions()}.
     */
    @Deprecated
    public KafkaFuture<Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>>> all() {
        return allDescriptions().thenApply(map -> map.entrySet().stream().collect(Collectors.toMap(
            entry -> entry.getKey(),
            entry -> convertMapValues(entry.getValue())
        )));
    }

    /**
     * Return a future which succeeds only if all the brokers have responded without error.
     * The result of the future is a map from brokerId to a map from broker log directory path
     * to a description of that log directory.
     */
    public KafkaFuture<Map<Integer, Map<String, LogDirDescription>>> allDescriptions() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0])).
            thenApply(v -> {
                Map<Integer, Map<String, LogDirDescription>> descriptions = new HashMap<>(futures.size());
                for (Map.Entry<Integer, KafkaFuture<Map<String, LogDirDescription>>> entry : futures.entrySet()) {
                    try {
                        descriptions.put(entry.getKey(), entry.getValue().get());
                    } catch (InterruptedException | ExecutionException e) {
                        // This should be unreachable, because allOf ensured that all the futures completed successfully.
                        throw new RuntimeException(e);
                    }
                }
                return descriptions;
            });
    }
}
