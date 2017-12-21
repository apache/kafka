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
import org.apache.kafka.common.requests.QuotaConfigResourceTuple;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@InterfaceStability.Evolving
public class DescribeQuotasResult {
    private final Map<QuotaConfigResourceTuple, KafkaFuture<Config>> resultMap;

    DescribeQuotasResult(Map<QuotaConfigResourceTuple, KafkaFuture<Config>> resultMap) {
        this.resultMap = resultMap;
    }

    /**
     * Return a map from resources to futures which can be used to check the status of the configuration for each
     * resource.
     */
    public Map<QuotaConfigResourceTuple, KafkaFuture<Config>> values() {
        return resultMap;
    }

    /**
     * Return a future which succeeds only if all the config descriptions succeed.
     */
    public KafkaFuture<Map<QuotaConfigResourceTuple, Config>> all() {
        return KafkaFuture.allOf(resultMap.values().toArray(new KafkaFuture[0])).
                thenApply(new KafkaFuture.Function<Void, Map<QuotaConfigResourceTuple, Config>>() {
                    @Override
                    public Map<QuotaConfigResourceTuple, Config> apply(Void v) {
                        Map<QuotaConfigResourceTuple, Config> configs = new HashMap<>(resultMap.size());
                        for (Map.Entry<QuotaConfigResourceTuple, KafkaFuture<Config>> entry : resultMap.entrySet()) {
                            try {
                                configs.put(entry.getKey(), entry.getValue().get());
                            } catch (InterruptedException | ExecutionException e) {
                                // This should be unreachable, because allOf ensured that all the futures
                                // completed successfully.
                                throw new RuntimeException(e);
                            }
                        }
                        return configs;
                    }
                });
    }
}
