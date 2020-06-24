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
import java.util.Set;

@InterfaceStability.Evolving
public class SyncGroupEvent extends AuditEvent {

    private final Set<String> assignedTopics;
    private final String clientId;
    private final String groupId;
    private final AuditInfo auditInfo;

    public SyncGroupEvent(Set<String> assignedTopics, String clientId, String groupId, AuditInfo auditInfo) {
        this.assignedTopics = Collections.unmodifiableSet(assignedTopics);
        this.clientId = clientId;
        this.groupId = groupId;
        this.auditInfo = auditInfo;
    }

    public String groupId() {
        return groupId;
    }

    public AuditInfo groupIdAuditInfo() {
        return auditInfo;
    }

    public String clientId() {
        return clientId;
    }

    public Set<String> assignedTopics() {
        return assignedTopics;
    }
}
