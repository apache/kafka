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

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.Json;
import org.apache.kafka.server.util.json.DecodeJson;
import org.apache.kafka.server.util.json.JsonObject;
import org.apache.kafka.server.util.json.JsonValue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
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

public class AclEntry extends AccessControlEntry {
    private static final DecodeJson.DecodeInteger INT = new DecodeJson.DecodeInteger();
    private static final DecodeJson.DecodeString STRING = new DecodeJson.DecodeString();

    public static final KafkaPrincipal WILDCARD_PRINCIPAL = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "*");
    public static final String WILDCARD_PRINCIPAL_STRING = WILDCARD_PRINCIPAL.toString();
    public static final String WILDCARD_HOST = "*";
    public static final String WILDCARD_RESOURCE = ResourcePattern.WILDCARD_RESOURCE;
    public static final String RESOURCE_SEPARATOR = ":";
    public static final Set<ResourceType> RESOURCE_TYPES = Arrays.stream(ResourceType.values())
        .filter(t -> !(t == ResourceType.UNKNOWN || t == ResourceType.ANY))
        .collect(Collectors.toSet());
    public static final Set<AclOperation> ACL_OPERATIONS = Arrays.stream(AclOperation.values())
        .filter(t -> !(t == AclOperation.UNKNOWN || t == AclOperation.ANY))
        .collect(Collectors.toSet());

    private static final String PRINCIPAL_KEY = "principal";
    private static final String PERMISSION_TYPE_KEY = "permissionType";
    private static final String OPERATION_KEY = "operation";
    private static final String HOSTS_KEY = "host";
    public static final String VERSION_KEY = "version";
    public static final int CURRENT_VERSION = 1;
    private static final String ACLS_KEY = "acls";

    public final AccessControlEntry ace;
    public final KafkaPrincipal kafkaPrincipal;

    public AclEntry(AccessControlEntry ace) {
        super(ace.principal(), ace.host(), ace.operation(), ace.permissionType());
        this.ace = ace;

        kafkaPrincipal = ace.principal() == null
            ? null
            : SecurityUtils.parseKafkaPrincipal(ace.principal());
    }

    /**
     * Parse JSON representation of ACLs
     * @param bytes of acls json string
     *
     * <p>
        {
            "version": 1,
            "acls": [
                {
                    "host":"host1",
                    "permissionType": "Deny",
                    "operation": "Read",
                    "principal": "User:alice"
                }
            ]
        }
     * </p>
     *
     * @return set of AclEntry objects from the JSON string
     */
    public static Set<AclEntry> fromBytes(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0)
            return Collections.emptySet();

        Optional<JsonValue> jsonValue = Json.parseBytes(bytes);
        if (!jsonValue.isPresent())
            return Collections.emptySet();

        JsonObject js = jsonValue.get().asJsonObject();

        //the acl json version.
        Utils.require(js.apply(VERSION_KEY).to(INT) == CURRENT_VERSION);

        Set<AclEntry> res = new HashSet<>();

        Iterator<JsonValue> aclsIter = js.apply(ACLS_KEY).asJsonArray().iterator();
        while (aclsIter.hasNext()) {
            JsonObject itemJs = aclsIter.next().asJsonObject();
            KafkaPrincipal principal = SecurityUtils.parseKafkaPrincipal(itemJs.apply(PRINCIPAL_KEY).to(STRING));
            AclPermissionType permissionType = SecurityUtils.permissionType(itemJs.apply(PERMISSION_TYPE_KEY).to(STRING));
            String host = itemJs.apply(HOSTS_KEY).to(STRING);
            AclOperation operation = SecurityUtils.operation(itemJs.apply(OPERATION_KEY).to(STRING));

            res.add(new AclEntry(new AccessControlEntry(principal.toString(),
                host, operation, permissionType)));
        }

        return res;
    }

    public static Map<String, Object> toJsonCompatibleMap(Set<AclEntry> acls) {
        Map<String, Object> res = new HashMap<>();
        res.put(AclEntry.VERSION_KEY, AclEntry.CURRENT_VERSION);
        res.put(AclEntry.ACLS_KEY, acls.stream().map(AclEntry::toMap).collect(Collectors.toList()));
        return res;
    }

    public static Set<AclOperation> supportedOperations(ResourceType resourceType) {
        switch (resourceType) {
            case TOPIC:
                return new HashSet<>(Arrays.asList(READ, WRITE, CREATE, DESCRIBE, DELETE, ALTER, DESCRIBE_CONFIGS, ALTER_CONFIGS));
            case GROUP:
                return new HashSet<>(Arrays.asList(READ, DESCRIBE, DELETE));
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

    public Map<String, Object> toMap() {
        Map<String, Object> res = new HashMap<>();
        res.put(AclEntry.PRINCIPAL_KEY, principal());
        res.put(AclEntry.PERMISSION_TYPE_KEY, SecurityUtils.permissionTypeName(permissionType()));
        res.put(AclEntry.OPERATION_KEY, SecurityUtils.operationName(operation()));
        res.put(AclEntry.HOSTS_KEY, host());
        return res;
    }

    @Override
    public int hashCode() {
        return ace.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o); // to keep spotbugs happy
    }

    @Override
    public String toString() {
        return String.format("%s has %s permission for operations: %s from hosts: %s", principal(), permissionType().name(), operation(), host());
    }
}
