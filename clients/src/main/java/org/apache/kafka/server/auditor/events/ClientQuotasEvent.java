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

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.server.auditor.AuditEvent;
import org.apache.kafka.server.auditor.AuditInfo;
import org.apache.kafka.server.authorizer.AclDeleteResult;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ClientQuotasEvent<T, R> extends AuditEvent {

    private final AuditInfo clusterAuditInfo;
    private final Set<T> auditedEntities;
    private final AclEvent.EventType eventType;
    private final Map<R, Integer> operationResults;

    public static ClientQuotasEvent<AclBinding, AclBinding> clientQuotaCreateEvent(Set<AclBinding> auditedEntities, AuditInfo clusterAuditInfo) {
        return new ClientQuotasEvent<>(auditedEntities, clusterAuditInfo, AclEvent.EventType.CREATE);
    }

    public static ClientQuotasEvent<AclBindingFilter, AclDeleteResult> clientQuotaDeleteEvent(Set<AclBindingFilter> auditedEntities,
                                                                             AuditInfo clusterAuditInfo,
                                                                             Map<AclDeleteResult, Integer> results) {
        return new ClientQuotasEvent<>(auditedEntities, clusterAuditInfo, results, AclEvent.EventType.DELETE);
    }

    public static ClientQuotasEvent<AclBindingFilter, AclBinding> clientQuotaDescribeEvent(Set<AclBindingFilter> auditedEntities,
                                                                          AuditInfo clusterAuditInfo,
                                                                          Map<AclBinding, Integer> results) {
        return new ClientQuotasEvent<>(auditedEntities, clusterAuditInfo, results, AclEvent.EventType.DESCRIBE);
    }

    public ClientQuotasEvent(Set<T> auditedEntities, AuditInfo clusterAuditInfo, AclEvent.EventType eventType) {
        this.auditedEntities = auditedEntities;
        this.clusterAuditInfo = clusterAuditInfo;
        this.eventType = eventType;
        this.operationResults = new HashMap<>();
    }

    public ClientQuotasEvent(Set<T> auditedEntities, AuditInfo clusterAuditInfo, Map<R, Integer> operationResults, AclEvent.EventType eventType) {
        this.auditedEntities = Collections.unmodifiableSet(auditedEntities);
        this.clusterAuditInfo = clusterAuditInfo;
        this.eventType = eventType;
        this.operationResults = operationResults;
    }

    public Set<T> auditedEntities() {
        return auditedEntities;
    }

    public AuditInfo clusterAuditInfo() {
        return clusterAuditInfo;
    }

    public Map<R, Integer> operationResults() {
        return operationResults;
    }

    public AclEvent.EventType eventType() {
        return eventType;
    }
}
