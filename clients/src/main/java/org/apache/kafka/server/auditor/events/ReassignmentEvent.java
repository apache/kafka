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

package org.apache.kafka.server.auditor.events;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.server.auditor.AuditEvent;
import org.apache.kafka.server.auditor.AuditInfo;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ReassignmentEvent extends AuditEvent {

    public enum EventType {
        ALTER, DESCRIBE
    }

    public static class ReassignedTopic {
        private final String name;
        private final List<ReassignedPartition> partitions;

        public ReassignedTopic(String name, List<ReassignedPartition> partitions) {
            this.name = name;
            this.partitions = Collections.unmodifiableList(partitions);
        }

        public String name() {
            return name;
        }

        public List<ReassignedPartition> partitions() {
            return partitions;
        }
    }

    public static class ReassignedPartition {
        private final int partitionIndex;
        private final List<Integer> reassignedReplicas;

        public ReassignedPartition(int partitionIndex, List<Integer> reassignedReplicas) {
            this.partitionIndex = partitionIndex;
            this.reassignedReplicas = Collections.unmodifiableList(reassignedReplicas);
        }

        public int partitionIndex() {
            return partitionIndex;
        }

        public List<Integer> reassignedReplicas() {
            return reassignedReplicas;
        }
    }

    private final List<ReassignedTopic> topicReassignments;
    private final AuditInfo auditInfo;
    private final Map<TopicPartition, Integer> results;
    private final EventType eventType;

    public static ReassignmentEvent alterReassignmentEvent(List<ReassignedTopic> topicReassignments,
                                                           AuditInfo auditInfo,
                                                           Map<TopicPartition, Integer> results) {
        return new ReassignmentEvent(topicReassignments, auditInfo, results, EventType.ALTER);
    }

    public static ReassignmentEvent describeReassignmentEvent(List<ReassignedTopic> topicReassignments,
                                                           AuditInfo auditInfo,
                                                           Map<TopicPartition, Integer> results) {
        return new ReassignmentEvent(topicReassignments, auditInfo, results, EventType.ALTER);
    }

    public ReassignmentEvent(List<ReassignedTopic> topicReassignments, AuditInfo auditInfo,
                             Map<TopicPartition, Integer> results, EventType eventType) {
        this.topicReassignments = Collections.unmodifiableList(topicReassignments);
        this.auditInfo = auditInfo;
        this.results = Collections.unmodifiableMap(results);
        this.eventType = eventType;
    }

    public List<ReassignedTopic> topicReassignments() {
        return topicReassignments;
    }

    public AuditInfo auditInfo() {
        return auditInfo;
    }

    public Map<TopicPartition, Integer> results() {
        return results;
    }
}
