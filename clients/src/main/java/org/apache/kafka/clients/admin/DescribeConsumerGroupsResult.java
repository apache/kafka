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
import java.util.Map;


/**
 * The result of the {@link KafkaAdminClient#describeConsumerGroups(Collection, DescribeConsumerGroupsOptions)}} call.
 *
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class DescribeConsumerGroupsResult {

    private final KafkaFuture<Map<String, KafkaFuture<ConsumerGroupDescription>>> futures;

    public DescribeConsumerGroupsResult(KafkaFuture<Map<String, KafkaFuture<ConsumerGroupDescription>>> futures) {
        this.futures = futures;
    }

    /**
     * Return a map from group name to futures which can be used to check the description of a consumer group.
     */
    public KafkaFuture<Map<String, KafkaFuture<ConsumerGroupDescription>>> values() {
        return futures;
    }

    public KafkaFuture<Collection<String>> names() {
        return futures.thenApply(new KafkaFuture.Function<Map<String, KafkaFuture<ConsumerGroupDescription>>, Collection<String>>() {
            @Override
            public Collection<String> apply(Map<String, KafkaFuture<ConsumerGroupDescription>> stringKafkaFutureMap) {
                return stringKafkaFutureMap.keySet();
            }
        });
    }

    /**
     * Return a future which succeeds only if all the consumer group descriptions succeed.
     */
    public KafkaFuture<Collection<KafkaFuture<ConsumerGroupDescription>>> all() {
        return futures.thenApply(new KafkaFuture.Function<Map<String, KafkaFuture<ConsumerGroupDescription>>, Collection<KafkaFuture<ConsumerGroupDescription>>>() {
            @Override
            public Collection<KafkaFuture<ConsumerGroupDescription>> apply(Map<String, KafkaFuture<ConsumerGroupDescription>> stringKafkaFutureMap) {
                return stringKafkaFutureMap.values();
            }
        });
    }

}
