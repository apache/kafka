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

public class DeleteRecordsEvent extends AuditEvent {

    private final Map<TopicPartition, Long> deleteOffsets;
    private final Map<TopicPartition, AuditInfo> auditInfo;

    public DeleteRecordsEvent(Map<TopicPartition, Long> deleteOffsets, Map<TopicPartition, AuditInfo> auditInfo) {
        this.deleteOffsets = Collections.unmodifiableMap(deleteOffsets);
        this.auditInfo = Collections.unmodifiableMap(auditInfo);
    }

    public Set<TopicPartition> auditedTopics() {
        return auditInfo.keySet();
    }

    public AuditInfo auditInfo(TopicPartition entity) {
        return auditInfo.get(entity);
    }

    public long deleteOffset(TopicPartition entity) {
        return deleteOffsets.get(entity);
    }
}
