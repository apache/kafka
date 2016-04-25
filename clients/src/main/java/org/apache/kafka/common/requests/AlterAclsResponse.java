/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.AlterAclsRequest.Action;
import org.apache.kafka.common.security.auth.Acl;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.Operation;
import org.apache.kafka.common.security.auth.PermissionType;
import org.apache.kafka.common.security.auth.Resource;
import org.apache.kafka.common.security.auth.ResourceType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlterAclsResponse extends AbstractRequestResponse {

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.ALTER_ACLS.id);

    public static final String RESPONSES_KEY_NAME = "responses";

    public static final String RESOURCE_KEY_NAME = "resource";
    public static final String RESOURCE_TYPE_KEY_NAME = "resource_type";
    public static final String RESOURCE_NAME_KEY_NAME = "resource_name";

    public static final String RESULTS_KEY_NAME = "results";
    public static final String ACTION_KEY_NAME = "action";

    public static final String ACL_KEY_NAME = "acl";
    public static final String ACL_PRINCIPLE_KEY_NAME = "acl_principle";
    public static final String ACL_PERMISSION_TYPE_KEY_NAME = "acl_permission_type";
    public static final String ACL_HOST_KEY_NAME = "acl_host";
    public static final String ACL_OPERATION_KEY_NAME = "acl_operation";

    public static final String ERROR_CODE_KEY_NAME = "error_code";

    public static final class ActionResponse {
        public final Action action;
        public final Acl acl;
        public final Errors error;

        public ActionResponse(Action action, Acl acl, Errors error) {
            this.action = action;
            this.acl = acl;
            this.error = error;
        }
    }

    /**
     * Possible error codes:
     *
     * CLUSTER_AUTHORIZATION_FAILED(31)
     */

    private final Map<Resource, List<ActionResponse>> results;

    public AlterAclsResponse(Map<Resource, List<ActionResponse>> results) {
        super(new Struct(CURRENT_SCHEMA));

        List<Struct> responsesArray = new ArrayList<>();
        for (Map.Entry<Resource, List<ActionResponse>> result : results.entrySet()) {
            Struct response = struct.instance(RESPONSES_KEY_NAME);

            Struct resource = response.instance(RESOURCE_KEY_NAME);
            resource.set(RESOURCE_TYPE_KEY_NAME, result.getKey().getResourceType().id);
            resource.set(RESOURCE_NAME_KEY_NAME, result.getKey().getName());

            List<Struct> actionResults = new ArrayList<>();
            for (ActionResponse actionResult : result.getValue()) {
                Struct actionResultStruct = response.instance(RESULTS_KEY_NAME);

                actionResultStruct.set(ACTION_KEY_NAME, actionResult.action.id);

                Struct aclStruct = actionResultStruct.instance(ACL_KEY_NAME);
                aclStruct.set(ACL_PRINCIPLE_KEY_NAME, actionResult.acl.getPrincipal().toString());
                aclStruct.set(ACL_PERMISSION_TYPE_KEY_NAME, actionResult.acl.getPermissionType().id);
                aclStruct.set(ACL_HOST_KEY_NAME, actionResult.acl.getHost());
                aclStruct.set(ACL_OPERATION_KEY_NAME, actionResult.acl.getOperation().id);

                actionResultStruct.set(ACL_KEY_NAME, aclStruct);

                actionResultStruct.set(ERROR_CODE_KEY_NAME, actionResult.error.code());

                actionResults.add(actionResultStruct);
            }

            response.set(RESOURCE_KEY_NAME, resource);
            response.set(RESULTS_KEY_NAME, actionResults.toArray());

            responsesArray.add(response);
        }

        struct.set(RESPONSES_KEY_NAME, responsesArray.toArray());

        this.results = results;
    }

    public AlterAclsResponse(Struct struct) {
        super(struct);

        Map<Resource, List<ActionResponse>> results = new HashMap<>();

        Object[] responses = (Object[]) struct.get(RESPONSES_KEY_NAME);
        for (Object responseObj : responses) {
            Struct response = (Struct) responseObj;

            Struct resourceStruct = response.getStruct(RESOURCE_KEY_NAME);
            ResourceType resourceType = ResourceType.forId(resourceStruct.getByte(RESOURCE_TYPE_KEY_NAME));
            String resourceName = resourceStruct.getString(RESOURCE_NAME_KEY_NAME);
            Resource resource = new Resource(resourceType, resourceName);

            List<ActionResponse> actionResults = new ArrayList<>();
            Object[] actionResultsArray = (Object[]) response.get(RESULTS_KEY_NAME);
            for (Object actionResultObj : actionResultsArray) {
                Struct actionResultStruct = (Struct) actionResultObj;

                Action action = Action.forId(actionResultStruct.getByte(ACTION_KEY_NAME));

                Struct aclStruct = actionResultStruct.getStruct(ACL_KEY_NAME);
                KafkaPrincipal principal = KafkaPrincipal.fromString(aclStruct.getString(ACL_PRINCIPLE_KEY_NAME));
                PermissionType permissionType = PermissionType.forId(aclStruct.getByte(ACL_PERMISSION_TYPE_KEY_NAME));
                String host = aclStruct.getString(ACL_HOST_KEY_NAME);
                Operation operation = Operation.forId(aclStruct.getByte(ACL_OPERATION_KEY_NAME));
                Acl acl = new Acl(principal, permissionType, host, operation);

                Errors error = Errors.forCode(actionResultStruct.getShort(ERROR_CODE_KEY_NAME));

                actionResults.add(new ActionResponse(action, acl, error));
            }
            results.put(resource, actionResults);
        }

        this.results = results;
    }

    public Map<Resource, List<ActionResponse>> results() {
        return this.results;
    }

    public static AlterAclsResponse parse(ByteBuffer buffer) {
        return new AlterAclsResponse(CURRENT_SCHEMA.read(buffer));
    }
}
