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
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.server.auditor.AuditEvent;
import org.apache.kafka.server.auditor.AuditInfo;

import java.util.Collections;
import java.util.Map;

@InterfaceStability.Evolving
public class ProduceEvent extends AuditEvent {

    private final Map<TopicPartition, AuditInfo> auditInfo;
    private final String clientId;

    public ProduceEvent(Map<TopicPartition, AuditInfo> auditInfo, String clientId) {
        this.auditInfo = Collections.unmodifiableMap(auditInfo);
        this.clientId = clientId;
    }

    public Map<TopicPartition, AuditInfo> topicAuditInfo() {
        return auditInfo;
    }

    public String clientId() {
        return clientId;
    }
}
