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

import org.apache.kafka.clients.admin.EndpointType;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeClusterRequest;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.network.Request;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class JAuthHelper {
    private final Optional<Plugin<Authorizer>> authorizerOpt;

    public JAuthHelper(Optional<Plugin<Authorizer>> authorizer) {
        this.authorizerOpt = authorizer;
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
        return authorizerOpt.map(plugin -> {
            ResourcePattern resource = new ResourcePattern(resourceType, resourceName, PatternType.LITERAL);
            List<Action> actions = List.of(new Action(operation, resource, refCount, logIfAllowed, logIfDenied));
            return plugin.get().authorize(requestContext, actions).get(0) == AuthorizationResult.ALLOWED;
        }).orElse(true);
    }

    public boolean authorize(
        RequestContext requestContext,
        AclOperation operation,
        ResourceType resourceType,
        String resourceName
    ) {
        return authorize(requestContext, operation, resourceType, resourceName, true, true, 1);
    }

    public void authorizeClusterOperation(
        Request request,
        AclOperation operation
    ) {
        if (!authorize(request.context(), operation, ResourceType.CLUSTER, Resource.CLUSTER_NAME, true, true, 1)) {
            throw new ClusterAuthorizationException(String.format("Request %s needs %s permission.", request, operation));
        }
    }

    public int authorizedOperations(
        Request request,
        Resource resource
    ) {
        List<AclOperation> supportedOps = AclEntry.supportedOperations(resource.resourceType()).stream().toList();
        if (authorizerOpt.isPresent()) {
            ResourcePattern resourcePattern = new ResourcePattern(resource.resourceType(), resource.name(), PatternType.LITERAL);
            List<Action> actions = supportedOps.stream()
                .map(op -> new Action(op, resourcePattern, 1, false, false))
                .toList();
            List<AuthorizationResult> authorizationResults = authorizerOpt.get().get().authorize(request.context(), actions);
            Set<Byte> finalSupportedOperations = new HashSet<>();
            int size = Math.min(supportedOps.size(), authorizationResults.size());
            for (int index = 0; index < size; index++) {
                if (authorizationResults.get(index) == AuthorizationResult.ALLOWED) {
                    finalSupportedOperations.add(supportedOps.get(index).code());
                }
            }
            return Utils.to32BitField(finalSupportedOperations);
        }
        return Utils.to32BitField(supportedOps.stream().map(AclOperation::code).collect(Collectors.toSet()));
    }

    public boolean authorizeByResourceType(
        RequestContext requestContext,
        AclOperation operation,
        ResourceType resourceType
    ) {
        return authorizerOpt.map(plugin -> plugin.get().authorizeByResourceType(requestContext, operation, resourceType) == AuthorizationResult.ALLOWED)
            .orElse(true);
    }

    public record PartitionedSeq<T>(
        List<T> authorizedPartition,
        List<T> unauthorizedPartition
    ) {
    }

    public <T> PartitionedSeq<T> partitionSeqByAuthorized(
        RequestContext requestContext,
        AclOperation operation,
        ResourceType resourceType,
        List<T> resources,
        boolean logIfAllowed,
        boolean logIfDenied,
        Function<T, String> resourceName
    ) {
        return authorizerOpt.map(plugin -> {
            Set<String> authorizedResourceNames = filterByAuthorized(requestContext, operation, resourceType,
                resources, logIfAllowed, logIfDenied, resourceName);
            Map<Boolean, List<T>> partitioned = resources.stream()
                .collect(Collectors.partitioningBy(r -> authorizedResourceNames.contains(resourceName.apply(r))));
            return new PartitionedSeq<>(partitioned.get(true), partitioned.get(false));
        }).orElse(new PartitionedSeq<>(resources, List.of()));
    }

    public <T> PartitionedSeq<T> partitionSeqByAuthorized(
        RequestContext requestContext,
        AclOperation operation,
        ResourceType resourceType,
        List<T> resources,
        Function<T, String> resourceName
    ) {
        return partitionSeqByAuthorized(requestContext, operation, resourceType, resources, true, true, resourceName);
    }

    public <T> Set<String> filterByAuthorized(
        RequestContext requestContext,
        AclOperation operation,
        ResourceType resourceType,
        Iterable<T> resources,
        boolean logIfAllowed,
        boolean logIfDenied,
        Function<T, String> resourceName
    ) {
        return authorizerOpt.map(plugin -> {
            Map<String, Integer> resourceNameToCount = new TreeMap<>();
            for (T resource : resources) {
                resourceNameToCount.compute(resourceName.apply(resource), (k, v) -> v == null ? 1 : v + 1);
            }

            List<Action> actions = new ArrayList<>(resourceNameToCount.size());
            List<String> resourceNames = new ArrayList<>();
            resourceNameToCount.forEach((rName, count) -> {
                ResourcePattern resource = new ResourcePattern(resourceType, rName, PatternType.LITERAL);
                actions.add(new Action(operation, resource, count, logIfAllowed, logIfDenied));
                resourceNames.add(rName);
            });

            List<AuthorizationResult> authorizationResults = plugin.get().authorize(requestContext, actions);
            Set<String> finalResourceNames = new HashSet<>();
            for (int i = 0; i < resourceNames.size(); i++) {
                if (authorizationResults.get(i) == AuthorizationResult.ALLOWED) {
                    finalResourceNames.add(resourceNames.get(i));
                }
            }
            return finalResourceNames;
        }).orElseGet(() -> {
            Set<String> finalResourceNames = new HashSet<>();
            for (T resource : resources) {
                finalResourceNames.add(resourceName.apply(resource));
            }
            return finalResourceNames;
        });
    }

    public <T> Set<String> filterByAuthorized(
        RequestContext requestContext,
        AclOperation operation,
        ResourceType resourceType,
        Iterable<T> resources,
        Function<T, String> resourceName
    ) {
        return filterByAuthorized(requestContext, operation, resourceType, resources, true, true, resourceName);
    }

    public DescribeClusterResponseData computeDescribeClusterResponse(
        Request request,
        EndpointType expectedEndpointType,
        String clusterId,
        Supplier<DescribeClusterResponseData.DescribeClusterBrokerCollection> getNodes,
        Supplier<Integer> getControllerId
    ) {
        DescribeClusterRequest describeClusterRequest = request.body(DescribeClusterRequest.class);
        EndpointType requestEndpointType = EndpointType.fromId(describeClusterRequest.data().endpointType());

        if (requestEndpointType.equals(EndpointType.UNKNOWN)) {
            return new DescribeClusterResponseData()
                .setErrorCode(request.header().data().requestApiVersion() == 0 ? Errors.INVALID_REQUEST.code() : Errors.UNSUPPORTED_ENDPOINT_TYPE.code())
                .setErrorMessage("Unsupported endpoint type " + (int) describeClusterRequest.data().endpointType());
        } else if (!expectedEndpointType.equals(requestEndpointType)) {
            return new DescribeClusterResponseData()
                .setErrorCode(request.header().data().requestApiVersion() == 0 ? Errors.INVALID_REQUEST.code() : Errors.MISMATCHED_ENDPOINT_TYPE.code())
                .setErrorMessage("The request was sent to an endpoint of type " + expectedEndpointType +
                    ", but we wanted an endpoint of type " + requestEndpointType);
        }

        var clusterAuthorizedOperations = Integer.MIN_VALUE;    // Default value in the schema

        // get cluster authorized operations
        if (describeClusterRequest.data().includeClusterAuthorizedOperations()) {
            if (authorize(request.context(), AclOperation.DESCRIBE, ResourceType.CLUSTER, Resource.CLUSTER_NAME, true, true, 1))
                clusterAuthorizedOperations = authorizedOperations(request, Resource.CLUSTER);
            else
                clusterAuthorizedOperations = 0;
        }

        // get the node list and the controller ID.
        DescribeClusterResponseData.DescribeClusterBrokerCollection nodes = getNodes.get();
        int controllerId = getControllerId.get();

        // If the provided controller ID is not in the node list, return -1 instead
        // to avoid confusing the client. This could happen in a case where we know
        // the controller ID, but we don't yet have KIP-919 information about that
        // controller.
        int effectiveControllerId = nodes.find(controllerId) == null ? -1 : controllerId;
        return new DescribeClusterResponseData().
            setClusterId(clusterId).
            setControllerId(effectiveControllerId).
            setClusterAuthorizedOperations(clusterAuthorizedOperations).
            setBrokers(nodes).
            setEndpointType(expectedEndpointType.id());
    }
}