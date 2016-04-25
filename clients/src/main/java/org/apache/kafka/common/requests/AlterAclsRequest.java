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
import org.apache.kafka.common.requests.AlterAclsResponse.ActionResponse;
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

public class AlterAclsRequest extends AbstractRequest {

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.ALTER_ACLS.id);

    public static final String REQUESTS_KEY_NAME = "requests";

    public static final String RESOURCE_KEY_NAME = "resource";
    public static final String RESOURCE_TYPE_KEY_NAME = "resource_type";
    public static final String RESOURCE_NAME_KEY_NAME = "resource_name";

    public static final String ACTIONS_KEY_NAME = "actions";
    public static final String ACTION_KEY_NAME = "action";

    public static final String ACL_KEY_NAME = "acl";
    public static final String ACL_PRINCIPLE_KEY_NAME = "acl_principle";
    public static final String ACL_PERMISSION_TYPE_KEY_NAME = "acl_permission_type";
    public static final String ACL_HOST_KEY_NAME = "acl_host";
    public static final String ACL_OPERATION_KEY_NAME = "acl_operation";

    public static enum Action {
        DELETE((byte) 0, "Delete"),
        ADD((byte) 1, "Add");

        private static Action[] idToAction;
        private static Map<String, Action> nameToAction;
        public static final int MAX_ID;

        static {
            int maxId = -1;
            for (ResourceType key : ResourceType.values()) {
                maxId = Math.max(maxId, key.id);
            }
            idToAction = new Action[maxId + 1];
            nameToAction = new HashMap<>();
            for (Action key : Action.values()) {
                idToAction[key.id] = key;
                nameToAction.put(key.name.toUpperCase(), key);
            }
            MAX_ID = maxId;
        }

        public final byte id;
        public final String name;

        private Action(byte id, String name) {
            this.id = id;
            this.name = name;
        }

        /** Case insensitive lookup by name */
        public static Action forName(String name) {
            Action action = nameToAction.get(name.toUpperCase());
            if (action == null) {
                throw new IllegalArgumentException(String.format("No enum constant with name %s", name));
            }
            return action;
        }

        public static Action forId(byte id) {
            return idToAction[id];
        }
    }

    public static final class ActionRequest {
        public final Action action;
        public final Acl acl;

        public ActionRequest(Action action, Acl acl) {
            this.action = action;
            this.acl = acl;
        }
    }

    /**
     * Possible error codes:
     *
     * CLUSTER_AUTHORIZATION_FAILED(31)
     */

    private final Map<Resource, List<ActionRequest>> requests;

    public AlterAclsRequest(Map<Resource, List<ActionRequest>> requests) {
        super(new Struct(CURRENT_SCHEMA));

        List<Struct> requestsList = new ArrayList<>();
        for (Map.Entry<Resource, List<ActionRequest>> request : requests.entrySet()) {
            Struct requestStruct = struct.instance(REQUESTS_KEY_NAME);

            Struct resource = requestStruct.instance(RESOURCE_KEY_NAME);
            resource.set(RESOURCE_TYPE_KEY_NAME, request.getKey().getResourceType().id);
            resource.set(RESOURCE_NAME_KEY_NAME, request.getKey().getName());

            List<Struct> actionRequests = new ArrayList<>();
            for (ActionRequest actionRequest : request.getValue()) {
                Struct actionRequestStruct = requestStruct.instance(ACTIONS_KEY_NAME);

                actionRequestStruct.set(ACTION_KEY_NAME, actionRequest.action.id);

                Struct aclStruct = actionRequestStruct.instance(ACL_KEY_NAME);
                aclStruct.set(ACL_PRINCIPLE_KEY_NAME, actionRequest.acl.getPrincipal().toString());
                aclStruct.set(ACL_PERMISSION_TYPE_KEY_NAME, actionRequest.acl.getPermissionType().id);
                aclStruct.set(ACL_HOST_KEY_NAME, actionRequest.acl.getHost());
                aclStruct.set(ACL_OPERATION_KEY_NAME, actionRequest.acl.getOperation().id);

                actionRequestStruct.set(ACL_KEY_NAME, aclStruct);

                actionRequests.add(actionRequestStruct);
            }

            requestStruct.set(RESOURCE_KEY_NAME, resource);
            requestStruct.set(ACTIONS_KEY_NAME, actionRequests.toArray());

            requestsList.add(requestStruct);
        }

        struct.set(REQUESTS_KEY_NAME, requestsList.toArray());

        this.requests = requests;
    }

    public AlterAclsRequest(Struct struct) {
        super(struct);

        Map<Resource, List<ActionRequest>> requests = new HashMap<>();

        Object[] requestsArray = (Object[]) struct.get(REQUESTS_KEY_NAME);
        for (Object requestObj : requestsArray) {
            Struct request = (Struct) requestObj;

            Struct resourceStruct = request.getStruct(RESOURCE_KEY_NAME);
            ResourceType resourceType = ResourceType.forId(resourceStruct.getByte(RESOURCE_TYPE_KEY_NAME));
            String resourceName = resourceStruct.getString(RESOURCE_NAME_KEY_NAME);
            Resource resource = new Resource(resourceType, resourceName);

            List<ActionRequest> actionRequests = new ArrayList<>();
            Object[] actionRequestArray = (Object[]) request.get(ACTIONS_KEY_NAME);
            for (Object actionRequestObj : actionRequestArray) {
                Struct actionRequestStruct = (Struct) actionRequestObj;

                Action action = Action.forId(actionRequestStruct.getByte(ACTION_KEY_NAME));

                Struct aclStruct = actionRequestStruct.getStruct(ACL_KEY_NAME);
                KafkaPrincipal principal = KafkaPrincipal.fromString(aclStruct.getString(ACL_PRINCIPLE_KEY_NAME));
                PermissionType permissionType = PermissionType.forId(aclStruct.getByte(ACL_PERMISSION_TYPE_KEY_NAME));
                String host = aclStruct.getString(ACL_HOST_KEY_NAME);
                Operation operation = Operation.forId(aclStruct.getByte(ACL_OPERATION_KEY_NAME));
                Acl acl = new Acl(principal, permissionType, host, operation);

                actionRequests.add(new ActionRequest(action, acl));
            }
            requests.put(resource, actionRequests);
        }

        this.requests = requests;
    }

    public Map<Resource, List<ActionRequest>> requests() {
        return this.requests;
    }

    @Override
    public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
        Errors error = Errors.forException(e);

        Map<Resource, List<ActionResponse>> results = new HashMap<>();
        for (Map.Entry<Resource, List<ActionRequest>> request : requests.entrySet()) {
            List<ActionResponse> actionResults = new ArrayList<>();
            for (ActionRequest actionRequest : request.getValue()) {
                actionResults.add(new ActionResponse(actionRequest.action, actionRequest.acl, error));
            }
            results.put(request.getKey(), actionResults);
        }

        switch (versionId) {
            case 0:
                return new AlterAclsResponse(results);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                    versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.ALTER_ACLS.id)));
        }
    }

    public static AlterAclsRequest parse(ByteBuffer buffer, int versionId) {
        return new AlterAclsRequest(ProtoUtils.parseRequest(ApiKeys.ALTER_ACLS.id, versionId, buffer));
    }

    public static AlterAclsRequest parse(ByteBuffer buffer) {
        return new AlterAclsRequest(CURRENT_SCHEMA.read(buffer));
    }
}
