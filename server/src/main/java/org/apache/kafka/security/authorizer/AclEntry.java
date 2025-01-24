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
package org.apache.kafka.security.authorizer;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.common.acl.AclOperation.ALTER;
import static org.apache.kafka.common.acl.AclOperation.ALTER_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.CLUSTER_ACTION;
import static org.apache.kafka.common.acl.AclOperation.CREATE;
import static org.apache.kafka.common.acl.AclOperation.CREATE_TOKENS;
import static org.apache.kafka.common.acl.AclOperation.DELETE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE_TOKENS;
import static org.apache.kafka.common.acl.AclOperation.IDEMPOTENT_WRITE;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;

public class AclEntry {

    public static final KafkaPrincipal WILDCARD_PRINCIPAL = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "*");
    public static final String WILDCARD_PRINCIPAL_STRING = WILDCARD_PRINCIPAL.toString();
    public static final String WILDCARD_HOST = "*";
    public static final String WILDCARD_RESOURCE = ResourcePattern.WILDCARD_RESOURCE;
    public static final Set<AclOperation> ACL_OPERATIONS = Arrays.stream(AclOperation.values())
        .filter(t -> !(t == AclOperation.UNKNOWN || t == AclOperation.ANY))
        .collect(Collectors.toSet());

    public static Set<AclOperation> supportedOperations(ResourceType resourceType) {
        switch (resourceType) {
            case TOPIC:
                return new HashSet<>(Arrays.asList(READ, WRITE, CREATE, DESCRIBE, DELETE, ALTER, DESCRIBE_CONFIGS, ALTER_CONFIGS));
            case GROUP:
                return new HashSet<>(Arrays.asList(READ, DESCRIBE, DELETE, DESCRIBE_CONFIGS, ALTER_CONFIGS));
            case CLUSTER:
                return new HashSet<>(Arrays.asList(CREATE, CLUSTER_ACTION, DESCRIBE_CONFIGS, ALTER_CONFIGS, IDEMPOTENT_WRITE, ALTER, DESCRIBE));
            case TRANSACTIONAL_ID:
                return new HashSet<>(Arrays.asList(DESCRIBE, WRITE));
            case DELEGATION_TOKEN:
                return Collections.singleton(DESCRIBE);
            case USER:
                return new HashSet<>(Arrays.asList(CREATE_TOKENS, DESCRIBE_TOKENS));
            default:
                throw new IllegalArgumentException("Not a concrete resource type");
        }
    }

    public static Errors authorizationError(ResourceType resourceType) {
        switch (resourceType) {
            case TOPIC:
                return Errors.TOPIC_AUTHORIZATION_FAILED;
            case GROUP:
                return Errors.GROUP_AUTHORIZATION_FAILED;
            case CLUSTER:
                return Errors.CLUSTER_AUTHORIZATION_FAILED;
            case TRANSACTIONAL_ID:
                return Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED;
            case DELEGATION_TOKEN:
                return Errors.DELEGATION_TOKEN_AUTHORIZATION_FAILED;
            default:
                throw new IllegalArgumentException("Authorization error type not known");
        }
    }
}
