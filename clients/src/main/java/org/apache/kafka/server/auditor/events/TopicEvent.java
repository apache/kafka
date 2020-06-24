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

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.server.auditor.AuditEvent;
import org.apache.kafka.server.auditor.AuditInfo;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

@InterfaceStability.Evolving
public class TopicEvent extends AuditEvent {

    public static class AuditedTopic {
        private final String topicName;
        private final int numPartitions;
        private final int replicationFactor;

        private static final int NO_PARTITION_NUMBER = -1;
        private static final int NO_REPLICATION_FACTOR = -1;

        public AuditedTopic(String topicName) {
            this.topicName = topicName;
            this.numPartitions = NO_PARTITION_NUMBER;
            this.replicationFactor = NO_REPLICATION_FACTOR;
        }

        public AuditedTopic(String topicName, int numPartitions, int replicationFactor) {
            this.topicName = topicName;
            this.numPartitions = numPartitions;
            this.replicationFactor = replicationFactor;
        }

        public String name() {
            return topicName;
        }

        public int numPartitions() {
            return numPartitions;
        }

        public int replicationFactor() {
            return replicationFactor;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AuditedTopic that = (AuditedTopic) o;
            return numPartitions == that.numPartitions &&
                replicationFactor == that.replicationFactor &&
                topicName.equals(that.topicName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicName, numPartitions, replicationFactor);
        }
    }

    public enum EventType {
        CREATE, DELETE
    }

    private final Map<AuditedTopic, AuditInfo> auditInfo;
    private final EventType eventType;

    public static TopicEvent topicCreateEvent(Map<AuditedTopic, AuditInfo> auditInfo) {
        return new TopicEvent(auditInfo, EventType.CREATE);
    }

    public static TopicEvent topicDeleteEvent(Map<AuditedTopic, AuditInfo> auditInfo) {
        return new TopicEvent(auditInfo, EventType.DELETE);
    }

    public TopicEvent(Map<AuditedTopic, AuditInfo> auditInfo, EventType eventType) {
        this.auditInfo = Collections.unmodifiableMap(auditInfo);
        this.eventType = eventType;
    }

    public Map<AuditedTopic, AuditInfo> auditInfo() {
        return auditInfo;
    }

    public EventType eventType() {
        return eventType;
    }
}
