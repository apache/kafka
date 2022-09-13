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

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.slf4j.Logger;

import static org.apache.kafka.server.authorizer.AuthorizationResult.ALLOWED;


interface MatchingRule {
    AuthorizationResult result();

    default void logAuditMessage(
        Logger auditLog,
        KafkaPrincipal principal,
        AuthorizableRequestContext requestContext,
        Action action
    ) {
        switch (result()) {
            case ALLOWED:
                // logIfAllowed is true if access is granted to the resource as a result of this authorization.
                // In this case, log at debug level. If false, no access is actually granted, the result is used
                // only to determine authorized operations. So log only at trace level.
                if (action.logIfAllowed() && auditLog.isDebugEnabled()) {
                    auditLog.debug(buildAuditMessage(principal, requestContext, action));
                } else if (auditLog.isTraceEnabled()) {
                    auditLog.trace(buildAuditMessage(principal, requestContext, action));
                }
                break;

            case DENIED:
                // logIfDenied is true if access to the resource was explicitly requested. Since this is an attempt
                // to access unauthorized resources, log at info level. If false, this is either a request to determine
                // authorized operations or a filter (e.g for regex subscriptions) to filter out authorized resources.
                // In this case, log only at trace level.
                if (action.logIfDenied()) {
                    auditLog.info(buildAuditMessage(principal, requestContext, action));
                } else if (auditLog.isTraceEnabled()) {
                    auditLog.trace(buildAuditMessage(principal, requestContext, action));
                }
                break;
        }
    }

    default String buildAuditMessage(
        KafkaPrincipal principal,
        AuthorizableRequestContext context,
        Action action
    ) {
        StringBuilder bldr = new StringBuilder();
        bldr.append("Principal = ").append(principal);
        bldr.append(" is ").append(result() == ALLOWED ? "Allowed" : "Denied");
        bldr.append(" operation = ").append(action.operation());
        bldr.append(" from host = ").append(context.clientAddress().getHostAddress());
        bldr.append(" on resource = ");
        appendResourcePattern(action.resourcePattern(), bldr);
        bldr.append(" for request = ").append(ApiKeys.forId(context.requestType()).name);
        bldr.append(" with resourceRefCount = ").append(action.resourceReferenceCount());
        bldr.append(" based on rule ").append(this);
        return bldr.toString();
    }

    static void appendResourcePattern(
        ResourcePattern resourcePattern,
        StringBuilder bldr
    ) {
        bldr.append(SecurityUtils.resourceTypeName(resourcePattern.resourceType()))
                .append(":")
                .append(resourcePattern.patternType())
                .append(":")
                .append(resourcePattern.name());
    }
}