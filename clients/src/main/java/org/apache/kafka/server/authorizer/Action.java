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

package org.apache.kafka.server.authorizer;

import java.util.Objects;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.resource.ResourcePattern;

@InterfaceStability.Evolving
public class Action {

    private final ResourcePattern resourcePattern;
    private final AclOperation operation;
    private final int resourceReferenceCount;
    private final boolean logIfAllowed;
    private final boolean logIfDenied;

    public Action(AclOperation operation,
                  ResourcePattern resourcePattern,
                  int resourceReferenceCount,
                  boolean logIfAllowed,
                  boolean logIfDenied) {
        this.operation = operation;
        this.resourcePattern = resourcePattern;
        this.logIfAllowed = logIfAllowed;
        this.logIfDenied = logIfDenied;
        this.resourceReferenceCount = resourceReferenceCount;
    }

    /**
     * Resource on which action is being performed.
     */
    public ResourcePattern resourcePattern() {
        return resourcePattern;
    }

    /**
     * Operation being performed.
     */
    public AclOperation operation() {
        return operation;
    }

    /**
     * Indicates if audit logs tracking ALLOWED access should include this action if result is
     * ALLOWED. The flag is true if access to a resource is granted while processing the request as a
     * result of this authorization. The flag is false only for requests used to describe access where
     * no operation on the resource is actually performed based on the authorization result.
     */
    public boolean logIfAllowed() {
        return logIfAllowed;
    }

    /**
     * Indicates if audit logs tracking DENIED access should include this action if result is
     * DENIED. The flag is true if access to a resource was explicitly requested and request
     * is denied as a result of this authorization request. The flag is false if request was
     * filtering out authorized resources (e.g. to subscribe to regex pattern). The flag is also
     * false if this is an optional authorization where an alternative resource authorization is
     * applied if this fails (e.g. Cluster:Create which is subsequently overridden by Topic:Create).
     */
    public boolean logIfDenied() {
        return logIfDenied;
    }

    /**
     * Number of times the resource being authorized is referenced within the request. For example, a single
     * request may reference `n` topic partitions of the same topic. Brokers will authorize the topic once
     * with `resourceReferenceCount=n`. Authorizers may include the count in audit logs.
     */
    public int resourceReferenceCount() {
        return resourceReferenceCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Action)) {
            return false;
        }

        Action that = (Action) o;
        return Objects.equals(this.resourcePattern, that.resourcePattern) &&
            Objects.equals(this.operation, that.operation) &&
            this.resourceReferenceCount == that.resourceReferenceCount &&
            this.logIfAllowed == that.logIfAllowed &&
            this.logIfDenied == that.logIfDenied;

    }

    @Override
    public int hashCode() {
        return Objects.hash(resourcePattern, operation, resourceReferenceCount, logIfAllowed, logIfDenied);
    }

    @Override
    public String toString() {
        return "Action(" +
            ", resourcePattern='" + resourcePattern + '\'' +
            ", operation='" + operation + '\'' +
            ", resourceReferenceCount='" + resourceReferenceCount + '\'' +
            ", logIfAllowed='" + logIfAllowed + '\'' +
            ", logIfDenied='" + logIfDenied + '\'' +
            ')';
    }
}
