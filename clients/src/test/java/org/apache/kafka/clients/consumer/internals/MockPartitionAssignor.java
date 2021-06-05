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

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;

public class MockPartitionAssignor extends AbstractPartitionAssignor {

    private final List<RebalanceProtocol> supportedProtocols;

    private int numAssignment;

    private Map<String, List<TopicPartition>> result = null;

    MockPartitionAssignor(final List<RebalanceProtocol> supportedProtocols) {
        this.supportedProtocols = supportedProtocols;
        numAssignment = 0;
    }

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        if (result == null)
            throw new IllegalStateException("Call to assign with no result prepared");
        return result;
    }

    @Override
    public String name() {
        return "consumer-mock-assignor";
    }

    @Override
    public List<RebalanceProtocol> supportedProtocols() {
        return supportedProtocols;
    }

    public void clear() {
        this.result = null;
    }

    public void prepare(Map<String, List<TopicPartition>> result) {
        this.result = result;
    }

    @Override
    public void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
        numAssignment += 1;
    }

    int numAssignment() {
        return numAssignment;
    }
}
