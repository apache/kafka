/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.kafka.server.policy;

import org.apache.kafka.common.errors.PolicyViolationException;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public interface CreateTopicPolicy {

    class RequestMetadata {
        private final String topic;
        private final int numPartitions;
        private final short replicationFactor;
        private final Map<Integer, List<Integer>> replicasAssignments;
        private final Map<String, String> configs;

        public RequestMetadata(String topic, int numPartitions, short replicationFactor,
                               Map<Integer, List<Integer>> replicasAssignments, Map<String, String> configs) {
            this.topic = topic;
            this.numPartitions = numPartitions;
            this.replicationFactor = replicationFactor;
            this.replicasAssignments = Collections.unmodifiableMap(replicasAssignments);
            this.configs = Collections.unmodifiableMap(configs);
        }

        public String topic() {
            return topic;
        }

        public int numPartitions() {
            return numPartitions;
        }

        public Map<Integer, List<Integer>> replicasAssignments() {
            return replicasAssignments;
        }

        public Map<String, String> configs() {
            return configs;
        }

        @Override
        public String toString() {
            return "RequestMetadata(topic=" + topic +
                    ", numPartitions=" + numPartitions +
                    ", replicationFactor=" + replicationFactor +
                    ", replicasAssignments=" + replicasAssignments +
                    ", configs=" + configs + ")";
        }
    }

    void validate(RequestMetadata requestMetadata) throws PolicyViolationException;
}
