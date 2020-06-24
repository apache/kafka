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
import java.util.Map;
import java.util.Set;

public class DeleteOffsetsEvent extends AuditEvent {

    private final String groupId;
    private final AuditInfo groupIdAuditInfo;
    private final Map<TopicPartition, AuditInfo> topicAuditInfos;

    public DeleteOffsetsEvent(String groupId, AuditInfo groupIdAuditInfo, Map<TopicPartition, AuditInfo> topicAuditInfos) {
        this.groupId = groupId;
        this.groupIdAuditInfo = groupIdAuditInfo;
        this.topicAuditInfos = Collections.unmodifiableMap(topicAuditInfos);
    }

    public String groupId() {
        return groupId;
    }

    public AuditInfo groupIdAuditInfo() {
        return groupIdAuditInfo;
    }

    public AuditInfo topicAuditInfo(TopicPartition entity) {
        return topicAuditInfos.get(entity);
    }

    public Set<TopicPartition> partitionsToDelete() {
        return topicAuditInfos.keySet();
    }
}
