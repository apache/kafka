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

package org.apache.kafka.server.auditor;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.server.authorizer.AuthorizationResult;

/**
 * This class encapsulates the authorization information with the result of the operation. It is used in specific ways
 * in the {@link AuditEvent} implementations. For instance a {@link org.apache.kafka.server.auditor.events.TopicEvent}
 * will have an AuditInfo for every topic as each topic is authorized but in case of an
 * {@link org.apache.kafka.server.auditor.events.AclEvent} authorization only happens for the cluster resource,
 * therefore there will be only one instance of this.
 */
@InterfaceStability.Evolving
public class AuditInfo {

    private final ResourcePattern resourcePattern;
    private final AclOperation operation;
    private final AuthorizationResult allowed;
    private final int error;

    public AuditInfo(AclOperation operation, ResourcePattern resourcePattern) {
        this.operation = operation;
        this.resourcePattern = resourcePattern;
        this.allowed = AuthorizationResult.ALLOWED;
        this.error = 0;
    }

    public AuditInfo(AclOperation operation, ResourcePattern resourcePattern, AuthorizationResult allowed, int error) {
        this.operation = operation;
        this.resourcePattern = resourcePattern;
        this.allowed = allowed;
        this.error = error;
    }

    public AclOperation operation() {
        return operation;
    }

    public ResourcePattern resource() {
        return resourcePattern;
    }

    public AuthorizationResult allowed() {
        return allowed;
    }

    public int errorCode() {
        return error;
    }
}
