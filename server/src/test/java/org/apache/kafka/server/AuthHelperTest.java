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
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.message.DescribeClusterRequestData;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.DescribeClusterResponseData.DescribeClusterBroker;
import org.apache.kafka.common.message.DescribeClusterResponseData.DescribeClusterBrokerCollection;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeClusterRequest;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.network.Request;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AuthHelperTest {

    private Authorizer authorizer;
    private Plugin<Authorizer> authorizerPlugin;

    @BeforeEach
    public void setUp() {
        authorizer = mock(Authorizer.class);
        authorizerPlugin = Plugin.wrapInstance(authorizer, null, "authorizer.class.name");
    }

    private static Request newMockDescribeClusterRequest(DescribeClusterRequestData data, int requestVersion)
        throws UnknownHostException {
        RequestContext requestContext = new RequestContext(
            new RequestHeader(ApiKeys.DESCRIBE_CLUSTER, (short) requestVersion, "", 0),
            "",
            InetAddress.getLocalHost(),
            KafkaPrincipal.ANONYMOUS,
            new ListenerName("PLAINTEXT"),
            SecurityProtocol.PLAINTEXT,
            ClientInformation.EMPTY,
            false);
        Request request = mock(Request.class);
        when(request.body(DescribeClusterRequest.class)).thenReturn(
            new DescribeClusterRequest(data, (short) requestVersion));
        when(request.context()).thenReturn(requestContext);
        when(request.header()).thenReturn(requestContext.header);
        return request;
    }

    @Test
    public void testAuthorize() throws UnknownHostException {
        AclOperation operation = AclOperation.WRITE;
        ResourceType resourceType = ResourceType.TOPIC;
        String resourceName = "topic-1";
        RequestHeader requestHeader = new RequestHeader(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion(), "", 0);
        RequestContext requestContext = new RequestContext(requestHeader, "1", InetAddress.getLocalHost(),
            KafkaPrincipal.ANONYMOUS, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false);

        List<Action> expectedActions = List.of(
            new Action(operation, new ResourcePattern(resourceType, resourceName, PatternType.LITERAL), 1, true, true)
        );

        when(authorizer.authorize(requestContext, expectedActions))
            .thenReturn(List.of(AuthorizationResult.ALLOWED));

        boolean result = new AuthHelper(Optional.of(authorizerPlugin)).authorize(
            requestContext, operation, resourceType, resourceName);

        verify(authorizer).authorize(requestContext, expectedActions);
        assertTrue(result);
    }

    @Test
    public void testFilterByAuthorized() throws UnknownHostException {
        AclOperation operation = AclOperation.WRITE;
        ResourceType resourceType = ResourceType.TOPIC;
        String resourceName1 = "topic-1";
        String resourceName2 = "topic-2";
        String resourceName3 = "topic-3";
        RequestHeader requestHeader = new RequestHeader(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion(), "", 0);
        RequestContext requestContext = new RequestContext(requestHeader, "1", InetAddress.getLocalHost(),
            KafkaPrincipal.ANONYMOUS, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false);

        List<Action> expectedActions = List.of(
            new Action(operation, new ResourcePattern(resourceType, resourceName1, PatternType.LITERAL), 2, true, true),
            new Action(operation, new ResourcePattern(resourceType, resourceName2, PatternType.LITERAL), 1, true, true),
            new Action(operation, new ResourcePattern(resourceType, resourceName3, PatternType.LITERAL), 1, true, true)
        );

        when(authorizer.authorize(
            ArgumentMatchers.eq(requestContext),
            argThat(t -> t.containsAll(expectedActions))
        )).thenAnswer(invocation -> {
            List<Action> actions = invocation.getArgument(1);
            return actions.stream().map(action -> {
                String name = action.resourcePattern().name();
                if (name.equals(resourceName1) || name.equals(resourceName3))
                    return AuthorizationResult.ALLOWED;
                else
                    return AuthorizationResult.DENIED;
            }).toList();
        });

        // Duplicate resource names should not trigger multiple calls to authorize
        Set<String> result = new AuthHelper(Optional.of(authorizerPlugin)).filterByAuthorized(
            requestContext,
            operation,
            resourceType,
            List.of(resourceName1, resourceName2, resourceName1, resourceName3),
            s -> s
        );

        verify(authorizer).authorize(
            ArgumentMatchers.eq(requestContext),
            argThat(t -> t.containsAll(expectedActions))
        );

        assertEquals(Set.of(resourceName1, resourceName3), result);
    }

    @Test
    public void testFilterByAuthorizedIsResilientToMismatchedAuthorizeResults() throws UnknownHostException {
        AclOperation operation = AclOperation.WRITE;
        ResourceType resourceType = ResourceType.TOPIC;
        RequestHeader requestHeader = new RequestHeader(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion(), "", 0);
        RequestContext requestContext = new RequestContext(requestHeader, "1", InetAddress.getLocalHost(),
            KafkaPrincipal.ANONYMOUS, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false);

        // A single distinct resource produces a single action, but a misbehaving authorizer may return
        // a result list of a different size. The original Scala implementation used `zip`, which tolerated
        // this by truncating to the shorter sequence. Verify we keep that behavior instead of failing with
        // IndexOutOfBoundsException.
        when(authorizer.authorize(ArgumentMatchers.eq(requestContext), ArgumentMatchers.any()))
            .thenReturn(List.of(AuthorizationResult.ALLOWED, AuthorizationResult.DENIED));

        Set<String> result = new AuthHelper(Optional.of(authorizerPlugin)).filterByAuthorized(
            requestContext, operation, resourceType, List.of("topic-1"), s -> s);

        assertEquals(Set.of("topic-1"), result);
    }

    @Test
    public void testPartitionByAuthorized() throws UnknownHostException {
        AclOperation operation = AclOperation.DESCRIBE;
        ResourceType resourceType = ResourceType.TOPIC;
        RequestHeader requestHeader = new RequestHeader(ApiKeys.METADATA, ApiKeys.METADATA.latestVersion(), "", 0);
        RequestContext requestContext = new RequestContext(requestHeader, "1", InetAddress.getLocalHost(),
            KafkaPrincipal.ANONYMOUS, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false);

        when(authorizer.authorize(ArgumentMatchers.eq(requestContext), ArgumentMatchers.any()))
            .thenAnswer(invocation -> {
                List<Action> actions = invocation.getArgument(1);
                return actions.stream().map(action -> {
                    String name = action.resourcePattern().name();
                    if (name.equals("topic-1") || name.equals("topic-3"))
                        return AuthorizationResult.ALLOWED;
                    else
                        return AuthorizationResult.DENIED;
                }).toList();
            });

        AuthHelper.PartitionResult<String> result = new AuthHelper(Optional.of(authorizerPlugin)).partitionByAuthorized(
            requestContext, operation, resourceType, List.of("topic-1", "topic-2", "topic-3"), s -> s);

        // The original input order should be preserved within each partition.
        assertEquals(List.of("topic-1", "topic-3"), result.authorized());
        assertEquals(List.of("topic-2"), result.unauthorized());
    }

    @Test
    public void testPartitionByAuthorizedWithoutAuthorizer() {
        List<String> resources = List.of("topic-1", "topic-2", "topic-3");
        // The request context is unused when there is no authorizer, so it can be null here.
        AuthHelper.PartitionResult<String> result = new AuthHelper(Optional.empty()).partitionByAuthorized(
            null, AclOperation.DESCRIBE, ResourceType.TOPIC, resources, s -> s);

        // Without an authorizer everything is authorized.
        assertEquals(resources, result.authorized());
        assertTrue(result.unauthorized().isEmpty());
    }

    @Test
    public void testComputeDescribeClusterResponseV1WithUnknownEndpointType() throws UnknownHostException {
        AuthHelper authHelper = new AuthHelper(Optional.of(authorizerPlugin));
        Request request = newMockDescribeClusterRequest(
            new DescribeClusterRequestData().setEndpointType((byte) 123), 1);
        DescribeClusterResponseData responseData = authHelper.computeDescribeClusterResponse(request,
            EndpointType.BROKER,
            "ltCWoi9wRhmHSQCIgAznEg",
            DescribeClusterBrokerCollection::new,
            () -> 1);
        assertEquals(new DescribeClusterResponseData()
            .setErrorCode(Errors.UNSUPPORTED_ENDPOINT_TYPE.code())
            .setErrorMessage("Unsupported endpoint type 123"), responseData);
    }

    @Test
    public void testComputeDescribeClusterResponseV0WithUnknownEndpointType() throws UnknownHostException {
        AuthHelper authHelper = new AuthHelper(Optional.of(authorizerPlugin));
        Request request = newMockDescribeClusterRequest(
            new DescribeClusterRequestData().setEndpointType((byte) 123), 0);
        DescribeClusterResponseData responseData = authHelper.computeDescribeClusterResponse(request,
            EndpointType.BROKER,
            "ltCWoi9wRhmHSQCIgAznEg",
            DescribeClusterBrokerCollection::new,
            () -> 1);
        assertEquals(new DescribeClusterResponseData()
            .setErrorCode(Errors.INVALID_REQUEST.code())
            .setErrorMessage("Unsupported endpoint type 123"), responseData);
    }

    @Test
    public void testComputeDescribeClusterResponseV1WithUnexpectedEndpointType() throws UnknownHostException {
        AuthHelper authHelper = new AuthHelper(Optional.of(authorizerPlugin));
        Request request = newMockDescribeClusterRequest(
            new DescribeClusterRequestData().setEndpointType(EndpointType.BROKER.id()), 1);
        DescribeClusterResponseData responseData = authHelper.computeDescribeClusterResponse(request,
            EndpointType.CONTROLLER,
            "ltCWoi9wRhmHSQCIgAznEg",
            DescribeClusterBrokerCollection::new,
            () -> 1);
        assertEquals(new DescribeClusterResponseData()
                .setErrorCode(Errors.MISMATCHED_ENDPOINT_TYPE.code())
                .setErrorMessage("The request was sent to an endpoint of type CONTROLLER, but we wanted an endpoint of type BROKER"),
            responseData);
    }

    @Test
    public void testComputeDescribeClusterResponseV0WithUnexpectedEndpointType() throws UnknownHostException {
        AuthHelper authHelper = new AuthHelper(Optional.of(authorizerPlugin));
        Request request = newMockDescribeClusterRequest(
            new DescribeClusterRequestData().setEndpointType(EndpointType.BROKER.id()), 0);
        DescribeClusterResponseData responseData = authHelper.computeDescribeClusterResponse(request,
            EndpointType.CONTROLLER,
            "ltCWoi9wRhmHSQCIgAznEg",
            DescribeClusterBrokerCollection::new,
            () -> 1);
        assertEquals(new DescribeClusterResponseData()
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage("The request was sent to an endpoint of type CONTROLLER, but we wanted an endpoint of type BROKER"),
            responseData);
    }

    @Test
    public void testComputeDescribeClusterResponseWhereControllerIsNotFound() throws UnknownHostException {
        AuthHelper authHelper = new AuthHelper(Optional.of(authorizerPlugin));
        Request request = newMockDescribeClusterRequest(
            new DescribeClusterRequestData().setEndpointType(EndpointType.CONTROLLER.id()), 1);
        DescribeClusterResponseData responseData = authHelper.computeDescribeClusterResponse(request,
            EndpointType.CONTROLLER,
            "ltCWoi9wRhmHSQCIgAznEg",
            DescribeClusterBrokerCollection::new,
            () -> 1);
        assertEquals(new DescribeClusterResponseData()
            .setClusterId("ltCWoi9wRhmHSQCIgAznEg")
            .setControllerId(-1)
            .setClusterAuthorizedOperations(Integer.MIN_VALUE)
            .setEndpointType((byte) 2), responseData);
    }

    @Test
    public void testComputeDescribeClusterResponseSuccess() throws UnknownHostException {
        AuthHelper authHelper = new AuthHelper(Optional.of(authorizerPlugin));
        Request request = newMockDescribeClusterRequest(
            new DescribeClusterRequestData().setEndpointType(EndpointType.CONTROLLER.id()), 1);
        DescribeClusterBrokerCollection nodes = new DescribeClusterBrokerCollection(
            List.of(new DescribeClusterBroker().setBrokerId(1)).iterator());
        DescribeClusterResponseData responseData = authHelper.computeDescribeClusterResponse(request,
            EndpointType.CONTROLLER,
            "ltCWoi9wRhmHSQCIgAznEg",
            () -> nodes,
            () -> 1);
        assertEquals(new DescribeClusterResponseData()
            .setClusterId("ltCWoi9wRhmHSQCIgAznEg")
            .setControllerId(1)
            .setClusterAuthorizedOperations(Integer.MIN_VALUE)
            .setBrokers(nodes)
            .setEndpointType((byte) 2), responseData);
    }
}
