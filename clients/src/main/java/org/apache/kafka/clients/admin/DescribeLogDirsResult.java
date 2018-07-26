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
import org.apache.kafka.common.requests.DescribeLogDirsResponse.LogDirInfo;


/**
 * The result of the {@link AdminClient#describeLogDirs(Collection)} call.
 *
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class DescribeLogDirsResult {
    private final Map<Integer, KafkaFuture<Map<String, LogDirInfo>>> futures;

    DescribeLogDirsResult(Map<Integer, KafkaFuture<Map<String, LogDirInfo>>> futures) {
        this.futures = futures;
    }

    /**
     * Return a map from brokerId to future which can be used to check the information of partitions on each individual broker
     */
    public Map<Integer, KafkaFuture<Map<String, LogDirInfo>>> values() {
        return futures;
    }

    /**
     * Return a future which succeeds only if all the brokers have responded without error
     */
    public KafkaFuture<Map<Integer, Map<String, LogDirInfo>>> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0])).
            thenApply(new KafkaFuture.BaseFunction<Void, Map<Integer, Map<String, LogDirInfo>>>() {
                @Override
                public Map<Integer, Map<String, LogDirInfo>> apply(Void v) {
                    Map<Integer, Map<String, LogDirInfo>> descriptions = new HashMap<>(futures.size());
                    for (Map.Entry<Integer, KafkaFuture<Map<String, LogDirInfo>>> entry : futures.entrySet()) {
                        try {
                            descriptions.put(entry.getKey(), entry.getValue().get());
                        } catch (InterruptedException | ExecutionException e) {
                            // This should be unreachable, because allOf ensured that all the futures completed successfully.
                            throw new RuntimeException(e);
                        }
                    }
                    return descriptions;
                }
            });
    }
}
