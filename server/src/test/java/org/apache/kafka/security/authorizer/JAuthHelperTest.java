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
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.message.DescribeClusterRequestData;
import org.apache.kafka.common.message.DescribeClusterResponseData;
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

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JAuthHelperTest {

    private static final String CLIENT_ID = "";

    private static Request newMockDescribeClusterRequest(
        DescribeClusterRequestData data,
        int requestVersion
    ) throws UnknownHostException {
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
        Authorizer authorizer = mock(Authorizer.class);
        Plugin<Authorizer> authorizerPlugin = Plugin.wrapInstance(authorizer, null, "authorizer.class.name");

        AclOperation operation = AclOperation.WRITE;
        ResourceType resourceType = ResourceType.TOPIC;
        String resourceName = "topic-1";
        RequestHeader requestHeader = new RequestHeader(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion(), CLIENT_ID, 0);
        RequestContext requestContext = new RequestContext(requestHeader, "1", InetAddress.getLocalHost(),
            KafkaPrincipal.ANONYMOUS, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false);

        List<Action> expectedActions = List.of(
            new Action(operation, new ResourcePattern(resourceType, resourceName, PatternType.LITERAL),
                1, true, true)
        );

        when(authorizer.authorize(requestContext, expectedActions))
            .thenReturn(List.of(AuthorizationResult.ALLOWED));

        boolean result = new JAuthHelper(Optional.of(authorizerPlugin)).authorize(
            requestContext, operation, resourceType, resourceName);

        verify(authorizer).authorize(requestContext, expectedActions);

        assertEquals(true, result);
    }

    @Test
    public void testFilterByAuthorized() throws UnknownHostException {
        Authorizer authorizer = mock(Authorizer.class);
        Plugin<Authorizer> authorizerPlugin = Plugin.wrapInstance(authorizer, null, "authorizer.class.name");

        AclOperation operation = AclOperation.WRITE;
        ResourceType resourceType = ResourceType.TOPIC;
        String resourceName1 = "topic-1";
        String resourceName2 = "topic-2";
        String resourceName3 = "topic-3";
        RequestHeader requestHeader = new RequestHeader(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion(),
            CLIENT_ID, 0);
        RequestContext requestContext = new RequestContext(requestHeader, "1", InetAddress.getLocalHost(),
            KafkaPrincipal.ANONYMOUS, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false);

        List<Action> expectedActions = List.of(
            new Action(operation, new ResourcePattern(resourceType, resourceName1, PatternType.LITERAL),
                2, true, true),
            new Action(operation, new ResourcePattern(resourceType, resourceName2, PatternType.LITERAL),
                1, true, true),
            new Action(operation, new ResourcePattern(resourceType, resourceName3, PatternType.LITERAL),
                1, true, true)
        );

        when(authorizer.authorize(
            eq(requestContext), argThat(actions -> actions != null && actions.containsAll(expectedActions))
        )).thenAnswer(invocation -> {
            List<Action> actions = invocation.getArgument(1);
            return actions.stream().map(action -> {
                if (Set.of(resourceName1, resourceName3).contains(action.resourcePattern().name()))
                    return AuthorizationResult.ALLOWED;
                else
                    return AuthorizationResult.DENIED;
            }).collect(Collectors.toList());
        });

        Set<String> result = new JAuthHelper(Optional.of(authorizerPlugin)).filterByAuthorized(
            requestContext,
            operation,
            resourceType,
            // Duplicate resource names should not trigger multiple calls to authorize
            List.of(resourceName1, resourceName2, resourceName1, resourceName3),
            true,
            true,
            Function.identity()
        );

        verify(authorizer).authorize(
            eq(requestContext), argThat(actions -> actions != null && actions.containsAll(expectedActions))
        );

        assertEquals(Set.of(resourceName1, resourceName3), result);
    }

    @Test
    public void testFilterByAuthorizedWithEmptyResources() throws UnknownHostException {
        Authorizer authorizer = mock(Authorizer.class);
        Plugin<Authorizer> authorizerPlugin = Plugin.wrapInstance(authorizer, null, "authorizer.class.name");

        RequestHeader requestHeader = new RequestHeader(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion(), CLIENT_ID, 0);
        RequestContext requestContext = new RequestContext(requestHeader, "1", InetAddress.getLocalHost(),
            KafkaPrincipal.ANONYMOUS, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false);

        // Mock returns a non-empty list even for empty actions — simulating a poorly-behaved mock/authorizer.
        when(authorizer.authorize(any(RequestContext.class), any()))
            .thenReturn(List.of(AuthorizationResult.ALLOWED));

        Set<String> result = new JAuthHelper(Optional.of(authorizerPlugin)).filterByAuthorized(
            requestContext,
            AclOperation.WRITE,
            ResourceType.TOPIC,
            Collections.emptyList(),
            Function.identity()
        );

        assertEquals(Set.of(), result);
    }

    @Test
    public void testAuthorizedOperationsWithMismatchedAuthorizerResults() throws UnknownHostException {
        Authorizer authorizer = mock(Authorizer.class);
        Plugin<Authorizer> authorizerPlugin = Plugin.wrapInstance(authorizer, null, "authorizer.class.name");

        RequestHeader requestHeader = new RequestHeader(ApiKeys.DESCRIBE_CLUSTER, (short) 1, CLIENT_ID, 0);
        RequestContext requestContext = new RequestContext(requestHeader, "1", InetAddress.getLocalHost(),
            KafkaPrincipal.ANONYMOUS, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false);
        Request request = mock(Request.class);
        when(request.context()).thenReturn(requestContext);

        // Mock returns an empty list regardless of how many actions are sent.
        when(authorizer.authorize(any(RequestContext.class), any()))
            .thenReturn(Collections.emptyList());

        org.apache.kafka.common.resource.Resource resource =
            new org.apache.kafka.common.resource.Resource(ResourceType.TOPIC, "test-topic");

        int result = new JAuthHelper(Optional.of(authorizerPlugin)).authorizedOperations(request, resource);

        // No operations should be authorized since the authorizer returned an empty list.
        assertEquals(0, result);
    }

    @Test
    public void testComputeDescribeClusterResponseV1WithUnknownEndpointType() throws UnknownHostException {
        Authorizer authorizer = mock(Authorizer.class);
        Plugin<Authorizer> authorizerPlugin = Plugin.wrapInstance(authorizer, null, "authorizer.class.name");
        JAuthHelper authHelper = new JAuthHelper(Optional.of(authorizerPlugin));
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
        Authorizer authorizer = mock(Authorizer.class);
        Plugin<Authorizer> authorizerPlugin = Plugin.wrapInstance(authorizer, null, "authorizer.class.name");
        JAuthHelper authHelper = new JAuthHelper(Optional.of(authorizerPlugin));
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
        Authorizer authorizer = mock(Authorizer.class);
        Plugin<Authorizer> authorizerPlugin = Plugin.wrapInstance(authorizer, null, "authorizer.class.name");
        JAuthHelper authHelper = new JAuthHelper(Optional.of(authorizerPlugin));
        Request request = newMockDescribeClusterRequest(
            new DescribeClusterRequestData().setEndpointType(EndpointType.BROKER.id()), 1);
        DescribeClusterResponseData responseData = authHelper.computeDescribeClusterResponse(request,
            EndpointType.CONTROLLER,
            "ltCWoi9wRhmHSQCIgAznEg",
            DescribeClusterBrokerCollection::new,
            () -> 1);
        assertEquals(new DescribeClusterResponseData()
            .setErrorCode(Errors.MISMATCHED_ENDPOINT_TYPE.code())
            .setErrorMessage("The request was sent to an endpoint of type CONTROLLER, but we wanted an endpoint of type BROKER"), responseData);
    }

    @Test
    public void testComputeDescribeClusterResponseV0WithUnexpectedEndpointType() throws UnknownHostException {
        Authorizer authorizer = mock(Authorizer.class);
        Plugin<Authorizer> authorizerPlugin = Plugin.wrapInstance(authorizer, null, "authorizer.class.name");
        JAuthHelper authHelper = new JAuthHelper(Optional.of(authorizerPlugin));
        Request request = newMockDescribeClusterRequest(
            new DescribeClusterRequestData().setEndpointType(EndpointType.BROKER.id()), 0);
        DescribeClusterResponseData responseData = authHelper.computeDescribeClusterResponse(request,
            EndpointType.CONTROLLER,
            "ltCWoi9wRhmHSQCIgAznEg",
            DescribeClusterBrokerCollection::new,
            () -> 1);
        assertEquals(new DescribeClusterResponseData()
            .setErrorCode(Errors.INVALID_REQUEST.code())
            .setErrorMessage("The request was sent to an endpoint of type CONTROLLER, but we wanted an endpoint of type BROKER"), responseData);
    }

    @Test
    public void testComputeDescribeClusterResponseWhereControllerIsNotFound() throws UnknownHostException {
        Authorizer authorizer = mock(Authorizer.class);
        Plugin<Authorizer> authorizerPlugin = Plugin.wrapInstance(authorizer, null, "authorizer.class.name");
        JAuthHelper authHelper = new JAuthHelper(Optional.of(authorizerPlugin));
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
        Authorizer authorizer = mock(Authorizer.class);
        Plugin<Authorizer> authorizerPlugin = Plugin.wrapInstance(authorizer, null, "authorizer.class.name");
        JAuthHelper authHelper = new JAuthHelper(Optional.of(authorizerPlugin));
        Request request = newMockDescribeClusterRequest(
            new DescribeClusterRequestData().setEndpointType(EndpointType.CONTROLLER.id()), 1);
        DescribeClusterBrokerCollection nodes = new DescribeClusterBrokerCollection(
            Arrays.asList(
                new DescribeClusterResponseData.DescribeClusterBroker().setBrokerId(1)));
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
