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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;


/**
 * The result of the {@link KafkaAdminClient#describeConsumerGroups(Collection, DescribeConsumerGroupsOptions)}} call.
 *
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class DescribeConsumerGroupsResult {

    private final Map<String, KafkaFuture<ConsumerGroupDescription>> futures;

    public DescribeConsumerGroupsResult(final Map<String, KafkaFuture<ConsumerGroupDescription>> futures) {
        this.futures = futures;
    }

    /**
     * Return a map from group id to futures which yield group descriptions.
     */
    public Map<String, KafkaFuture<ConsumerGroupDescription>> describedGroups() {
        return futures;
    }

    /**
     * Return a future which yields all ConsumerGroupDescription objects, if all the describes succeed.
     */
    public KafkaFuture<Map<String, ConsumerGroupDescription>> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0])).thenApply(
            new KafkaFuture.BaseFunction<Void, Map<String, ConsumerGroupDescription>>() {
                @Override
                public Map<String, ConsumerGroupDescription> apply(Void v) {
                    try {
                        Map<String, ConsumerGroupDescription> descriptions = new HashMap<>(futures.size());
                        for (Map.Entry<String, KafkaFuture<ConsumerGroupDescription>> entry : futures.entrySet()) {
                            descriptions.put(entry.getKey(), entry.getValue().get());
                        }
                        return descriptions;
                    } catch (InterruptedException | ExecutionException e) {
                        // This should be unreachable, since the KafkaFuture#allOf already ensured
                        // that all of the futures completed successfully.
                        throw new RuntimeException(e);
                    }
                }
            });
    }
}
