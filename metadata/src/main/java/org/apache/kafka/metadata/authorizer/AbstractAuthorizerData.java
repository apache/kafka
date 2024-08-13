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
package org.apache.kafka.metadata.authorizer;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.server.authorizer.AuthorizationResult.ALLOWED;

/**
 * An abstract implementation of the audit logging info for Authorizer data.
 */
public abstract class AbstractAuthorizerData implements AuthorizerData {
    /**
     * The audig logger to use.  May not be static or testing breaks
     */
    private final Logger auditLog = LoggerFactory.getLogger("kafka.authorizer.logger");

    /**
     * Log audit messages
     *
     * @param principal      the principal that made the request.
     * @param requestContext the request context
     * @param action         the action that was requested
     * @param rule           the rule that was returned.
     */
    protected final void logAuditMessage(
            KafkaPrincipal principal,
            AuthorizableRequestContext requestContext,
            Action action,
            MatchingRule rule
    ) {
        switch (rule.result()) {
            case ALLOWED:
                // logIfAllowed is true if access is granted to the resource as a result of this authorization.
                // In this case, log at debug level. If false, no access is actually granted, the result is used
                // only to determine authorized operations. So log only at trace level.
                if (action.logIfAllowed() && auditLog.isDebugEnabled()) {
                    auditLog.debug(buildAuditMessage(principal, requestContext, action, rule));
                } else if (auditLog.isTraceEnabled()) {
                    auditLog.trace(buildAuditMessage(principal, requestContext, action, rule));
                }
                return;

            case DENIED:
                // logIfDenied is true if access to the resource was explicitly requested. Since this is an attempt
                // to access unauthorized resources, log at info level. If false, this is either a request to determine
                // authorized operations or a filter (e.g for regex subscriptions) to filter out authorized resources.
                // In this case, log only at trace level.
                if (action.logIfDenied()) {
                    auditLog.info(buildAuditMessage(principal, requestContext, action, rule));
                } else if (auditLog.isTraceEnabled()) {
                    auditLog.trace(buildAuditMessage(principal, requestContext, action, rule));
                }
        }
    }

    /**
     * Log audit messages.
     * @param principal the principal that made the request.
     * @param host the host from which the request was made
     * @param operation the operation that was requested.
     * @param resourceType the resource type that was requested.
     * @param rule the rule that was returned.
     */
    protected final void logAuditMessage(
            KafkaPrincipal principal,
            String host,
            AclOperation operation,
            ResourceType resourceType,
            MatchingRule rule
    ) {
        switch (rule.result()) {
            case ALLOWED:
                if (auditLog.isTraceEnabled()) {
                    auditLog.trace(buildAuditMessage(principal, host, operation, resourceType, rule));
                }
                return;

            case DENIED:
                if (auditLog.isTraceEnabled()) {
                    auditLog.trace(buildAuditMessage(principal, host, operation, resourceType, rule));
                }
        }
    }

    private String buildAuditMessage(
            KafkaPrincipal principal,
            String host,
            AclOperation op,
            ResourceType resourceType,
            MatchingRule rule
    ) {
        StringBuilder bldr = new StringBuilder();
        bldr.append("Principal = ").append(principal);
        bldr.append(" is ").append(rule.result() == ALLOWED ? "Allowed" : "Denied");
        bldr.append(" operation = ").append(op);
        bldr.append(" from host = ").append(host);
        bldr.append(" for resource type = ").append(resourceType.name());
        bldr.append(" based on rule ").append(rule);
        return bldr.toString();
    }

    private String buildAuditMessage(
            KafkaPrincipal principal,
            AuthorizableRequestContext context,
            Action action,
            MatchingRule rule
    ) {
        StringBuilder bldr = new StringBuilder();
        bldr.append("Principal = ").append(principal);
        bldr.append(" is ").append(rule.result() == ALLOWED ? "Allowed" : "Denied");
        bldr.append(" operation = ").append(action.operation());
        bldr.append(" from host = ").append(context.clientAddress().getHostAddress());
        bldr.append(" on resource = ");
        appendResourcePattern(action.resourcePattern(), bldr);
        bldr.append(" for request = ").append(ApiKeys.forId(context.requestType()).name);
        bldr.append(" with resourceRefCount = ").append(action.resourceReferenceCount());
        bldr.append(" based on rule ").append(rule);
        return bldr.toString();
    }

    private void appendResourcePattern(ResourcePattern resourcePattern, StringBuilder bldr) {
        bldr.append(SecurityUtils.resourceTypeName(resourcePattern.resourceType()))
                .append(":")
                .append(resourcePattern.patternType())
                .append(":")
                .append(resourcePattern.name());
    }

}
