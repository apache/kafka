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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import java.util.HashMap;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;


/**
 * The result of the {@link Admin#describeLogDirs(Collection)} call.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
@SuppressWarnings("deprecation")
public class DescribeLogDirsResult {
    private final Map<Integer, KafkaFuture<Map<String, LogDirInfo>>> futures;

    DescribeLogDirsResult(Map<Integer, KafkaFuture<Map<String, LogDirInfo>>> futures) {
        this.futures = futures;
    }

    /**
     * Return a map from brokerId to future which can be used to check the information of partitions on each individual broker.
     * <p>
     * Note: Actually, it returns {@link org.apache.kafka.clients.admin.DescribeLogDirsResult.LogDirInfo} instances instead of deprecated
     * {@link DescribeLogDirsResponse.LogDirInfo}.
     */
    public Map<Integer, KafkaFuture<Map<String, DescribeLogDirsResponse.LogDirInfo>>> values() {
        return futures.entrySet().stream().collect(Collectors.toMap(
            e -> e.getKey(),
            e -> e.getValue().thenApply(new KafkaFuture.BaseFunction<Map<String, LogDirInfo>, Map<String, DescribeLogDirsResponse.LogDirInfo>>() {
                @Override
                public Map<String, DescribeLogDirsResponse.LogDirInfo> apply(Map<String, LogDirInfo> map) {
                    return map.entrySet().stream().collect(Collectors.toMap(f -> f.getKey(), f -> (DescribeLogDirsResponse.LogDirInfo) f.getValue()));
                }
            })
        ));
    }

    /**
     * Return a future which succeeds only if all the brokers have responded without error.
     * <p>
     * Note: Actually, it returns {@link org.apache.kafka.clients.admin.DescribeLogDirsResult.LogDirInfo} instances instead of deprecated
     * {@link DescribeLogDirsResponse.LogDirInfo}.
     */
    public KafkaFuture<Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>>> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0])).
            thenApply(new KafkaFuture.BaseFunction<Void, Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>>>() {
                @Override
                public Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> apply(Void v) {
                    Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> descriptions = new HashMap<>(futures.size());
                    for (Map.Entry<Integer, KafkaFuture<Map<String, LogDirInfo>>> entry : futures.entrySet()) {
                        try {
                            Map<String, LogDirInfo> logDirInfo = entry.getValue().get();
                            descriptions.put(
                                entry.getKey(),
                                logDirInfo.entrySet()
                                    .stream()
                                    .collect(Collectors.toMap(
                                        e -> e.getKey(),
                                        e -> (DescribeLogDirsResponse.LogDirInfo) e.getValue())
                            ));
                        } catch (InterruptedException | ExecutionException e) {
                            // This should be unreachable, because allOf ensured that all the futures completed successfully.
                            throw new RuntimeException(e);
                        }
                    }
                    return descriptions;
                }
            });
    }

    /**
     * State of a LogDir, (possibly) with an error code. Possible error codes are:
     * <p><ul>
     *   <li>KAFKA_STORAGE_ERROR (56)
     *   <li>UNKNOWN (-1)
     * </ul><p>
     */
    static public class LogDirInfo extends DescribeLogDirsResponse.LogDirInfo {
        public LogDirInfo(Errors error, Map<TopicPartition, ReplicaInfo> replicaInfos) {
            super(error, replicaInfos
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                    e -> e.getKey(),
                    e -> (DescribeLogDirsResponse.ReplicaInfo) e.getValue())
                ));
        }
    }


    /**
     * State of a replica.
     **/
    static public class ReplicaInfo extends DescribeLogDirsResponse.ReplicaInfo {
        public ReplicaInfo(long size, long offsetLag, boolean isFuture) {
            super(size, offsetLag, isFuture);
        }
    }
}
