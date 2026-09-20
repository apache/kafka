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

package org.apache.kafka.server;

import org.apache.kafka.clients.admin.EndpointType;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.DescribeClusterResponseData.DescribeClusterBrokerCollection;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeClusterRequest;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.network.Request;
import org.apache.kafka.security.authorizer.AclEntry;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class AuthHelper {

    private final Optional<Authorizer> authorizer;

    public AuthHelper(Optional<Plugin<Authorizer>> authorizer) {
        this.authorizer = authorizer.map(Plugin::get);
    }

    public boolean authorize(
        RequestContext requestContext,
        AclOperation operation,
        ResourceType resourceType,
        String resourceName,
        boolean logIfAllowed,
        boolean logIfDenied,
        int refCount
    ) {
        if (authorizer.isEmpty()) {
            return true;
        }
        ResourcePattern resource = new ResourcePattern(resourceType, resourceName, PatternType.LITERAL);
        List<Action> actions = List.of(
            new Action(operation, resource, refCount, logIfAllowed, logIfDenied)
        );
        return authorizer.get().authorize(requestContext, actions).get(0) == AuthorizationResult.ALLOWED;
    }

    public boolean authorize(
        RequestContext requestContext,
        AclOperation operation,
        ResourceType resourceType,
        String resourceName
    ) {
        return authorize(requestContext, operation, resourceType, resourceName, true, true, 1);
    }

    public void authorizeClusterOperation(Request request, AclOperation operation) {
        if (!authorize(request.context(), operation, ResourceType.CLUSTER, Resource.CLUSTER_NAME)) {
            throw new ClusterAuthorizationException("Request " + request + " needs " + operation + " permission.");
        }
    }

    public int authorizedOperations(Request request, Resource resource) {
        List<AclOperation> supportedOps = new ArrayList<>(AclEntry.supportedOperations(resource.resourceType()));
        Set<AclOperation> authorizedOps;
        if (authorizer.isPresent()) {
            ResourcePattern resourcePattern = new ResourcePattern(resource.resourceType(), resource.name(), PatternType.LITERAL);
            List<Action> actions = supportedOps.stream()
                .map(op -> new Action(op, resourcePattern, 1, false, false))
                .toList();
            List<AuthorizationResult> results = authorizer.get().authorize(request.context(), actions);
            authorizedOps = new HashSet<>();
            // Authorizer.authorize returns one result per action in the same order, so the i-th
            // result corresponds to the i-th supported operation. Iterate up to the smaller size to
            // stay within bounds.
            int count = Math.min(results.size(), supportedOps.size());
            for (int i = 0; i < count; i++) {
                if (results.get(i) == AuthorizationResult.ALLOWED) {
                    authorizedOps.add(supportedOps.get(i));
                }
            }
        } else {
            authorizedOps = new HashSet<>(supportedOps);
        }
        Set<Byte> opCodes = authorizedOps.stream()
            .map(AclOperation::code)
            .collect(Collectors.toSet());
        return Utils.to32BitField(opCodes);
    }

    public boolean authorizeByResourceType(
        RequestContext requestContext,
        AclOperation operation,
        ResourceType resourceType
    ) {
        return authorizer.map(authorizerInstance -> authorizerInstance.authorizeByResourceType(requestContext, operation, resourceType) == AuthorizationResult.ALLOWED).orElse(true);
    }

    public <T> Set<String> filterByAuthorized(
        RequestContext requestContext,
        AclOperation operation,
        ResourceType resourceType,
        Collection<T> resources,
        boolean logIfAllowed,
        boolean logIfDenied,
        Function<T, String> resourceName
    ) {
        if (resources.isEmpty()) {
            return Set.of();
        }
        if (authorizer.isEmpty()) {
            Set<String> result = new HashSet<>();
            for (T resource : resources) {
                result.add(resourceName.apply(resource));
            }
            return result;
        }
        // Count occurrences of each resource name
        Map<String, Integer> resourceNameToCount = new HashMap<>();
        for (T resource : resources) {
            String name = resourceName.apply(resource);
            resourceNameToCount.merge(name, 1, Integer::sum);
        }

        List<String> names = new ArrayList<>(resourceNameToCount.keySet());
        List<Action> actions = names.stream()
            .map(name -> new Action(
                operation,
                new ResourcePattern(resourceType, name, PatternType.LITERAL),
                resourceNameToCount.get(name),
                logIfAllowed,
                logIfDenied
            ))
            .toList();
        List<AuthorizationResult> results = authorizer.get().authorize(requestContext, actions);
        Set<String> authorized = new HashSet<>();
        // Authorizer.authorize returns one result per action in the same order, so the i-th
        // result corresponds to the i-th resource name. Iterate up to the smaller size to
        // stay within bounds.
        int count = Math.min(results.size(), names.size());
        for (int i = 0; i < count; i++) {
            if (results.get(i) == AuthorizationResult.ALLOWED) {
                authorized.add(names.get(i));
            }
        }
        return authorized;
    }

    public <T> Set<String> filterByAuthorized(
        RequestContext requestContext,
        AclOperation operation,
        ResourceType resourceType,
        Collection<T> resources,
        Function<T, String> resourceName
    ) {
        return filterByAuthorized(requestContext, operation, resourceType, resources, true, true, resourceName);
    }

    public record PartitionResult<T>(List<T> authorized, List<T> unauthorized) {
    }

    public <T> PartitionResult<T> partitionByAuthorized(
        RequestContext requestContext,
        AclOperation operation,
        ResourceType resourceType,
        List<T> resources,
        Function<T, String> resourceName
    ) {
        if (authorizer.isEmpty()) {
            return new PartitionResult<>(resources, List.of());
        }
        Set<String> authorizedResourceNames = filterByAuthorized(
            requestContext, operation, resourceType, resources, resourceName
        );
        List<T> authorized = new ArrayList<>();
        List<T> unauthorized = new ArrayList<>();
        for (T resource : resources) {
            if (authorizedResourceNames.contains(resourceName.apply(resource))) {
                authorized.add(resource);
            } else {
                unauthorized.add(resource);
            }
        }
        return new PartitionResult<>(authorized, unauthorized);
    }

    public DescribeClusterResponseData computeDescribeClusterResponse(
        Request request,
        EndpointType expectedEndpointType,
        String clusterId,
        Supplier<DescribeClusterBrokerCollection> getNodes,
        Supplier<Integer> getControllerId
    ) {
        DescribeClusterRequest describeClusterRequest = request.body(DescribeClusterRequest.class);
        EndpointType requestEndpointType = EndpointType.fromId(describeClusterRequest.data().endpointType());
        if (requestEndpointType.equals(EndpointType.UNKNOWN)) {
            return new DescribeClusterResponseData()
                .setErrorCode(request.header().data().requestApiVersion() == 0
                    ? Errors.INVALID_REQUEST.code()
                    : Errors.UNSUPPORTED_ENDPOINT_TYPE.code())
                .setErrorMessage("Unsupported endpoint type " + (int) describeClusterRequest.data().endpointType());
        } else if (!expectedEndpointType.equals(requestEndpointType)) {
            return new DescribeClusterResponseData()
                .setErrorCode(request.header().data().requestApiVersion() == 0
                    ? Errors.INVALID_REQUEST.code()
                    : Errors.MISMATCHED_ENDPOINT_TYPE.code())
                .setErrorMessage("The request was sent to an endpoint of type " + expectedEndpointType +
                    ", but we wanted an endpoint of type " + requestEndpointType);
        }

        int clusterAuthorizedOperations = Integer.MIN_VALUE; // Default value in the schema
        // get cluster authorized operations
        if (describeClusterRequest.data().includeClusterAuthorizedOperations()) {
            if (authorize(request.context(), AclOperation.DESCRIBE, ResourceType.CLUSTER, Resource.CLUSTER_NAME)) {
                clusterAuthorizedOperations = authorizedOperations(request, Resource.CLUSTER);
            } else {
                clusterAuthorizedOperations = 0;
            }
        }

        // Get the node list and the controller ID.
        DescribeClusterBrokerCollection nodes = getNodes.get();
        int controllerId = getControllerId.get();
        // If the provided controller ID is not in the node list, return -1 instead
        // to avoid confusing the client. This could happen in a case where we know
        // the controller ID, but we don't yet have KIP-919 information about that controller.
        int effectiveControllerId = (nodes.find(controllerId) == null) ? -1 : controllerId;

        return new DescribeClusterResponseData()
            .setClusterId(clusterId)
            .setControllerId(effectiveControllerId)
            .setClusterAuthorizedOperations(clusterAuthorizedOperations)
            .setBrokers(nodes)
            .setEndpointType(expectedEndpointType.id());
    }
}
