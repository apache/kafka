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
package org.apache.kafka.tiered.storage;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.common.test.ClusterInstance;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class TieredStorageTestPlan {

    private final List<TieredStorageTestAction> actions;

    TieredStorageTestPlan(List<TieredStorageTestAction> actions) {
        this.actions = List.copyOf(actions);
    }

    public void execute(ClusterInstance clusterInstance, GroupProtocol groupProtocol) throws Exception {
        execute(clusterInstance, groupProtocol, Map.of());
    }

    public void execute(ClusterInstance clusterInstance,
                        GroupProtocol groupProtocol,
                        Map<String, Object> extraConsumerProps) throws Exception {
        Map<String, Object> consumerProps = new HashMap<>(extraConsumerProps);
        consumerProps.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT));
        execute(clusterInstance, consumerProps);
    }

    public void execute(ClusterInstance clusterInstance, Map<String, Object> extraConsumerProps) throws Exception {
        try (TieredStorageTestContext context = new TieredStorageTestContext(clusterInstance, Map.copyOf(extraConsumerProps))) {
            try {
                for (TieredStorageTestAction action : actions) {
                    action.execute(context);
                }
            } finally {
                context.printReport(System.out);
            }
        }
    }
}
