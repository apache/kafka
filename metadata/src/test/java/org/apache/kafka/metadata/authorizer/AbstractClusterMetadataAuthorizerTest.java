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

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.errors.AuthorizerNotReadyException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.Action;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.singletonList;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.common.security.auth.KafkaPrincipal.USER_TYPE;
import static org.apache.kafka.server.authorizer.AuthorizationResult.ALLOWED;
import static org.apache.kafka.server.authorizer.AuthorizationResult.DENIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public abstract class AbstractClusterMetadataAuthorizerTest<T extends ClusterMetadataAuthorizer> extends AbstractAuthorizerTest<T> {

    private static final AtomicLong NEXT_ID = new AtomicLong(0);

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDenyAuditLogging(boolean logIfDenied) throws Exception {
        try (MockedStatic<LoggerFactory> mockedLoggerFactory = Mockito.mockStatic(LoggerFactory.class)) {
            Logger otherLog = Mockito.mock(Logger.class);
            Logger auditLog = Mockito.mock(Logger.class);
            mockedLoggerFactory
                .when(() -> LoggerFactory.getLogger("kafka.authorizer.logger"))
                .thenReturn(auditLog);

            mockedLoggerFactory
                .when(() -> LoggerFactory.getLogger(Mockito.any(Class.class)))
                .thenReturn(otherLog);

            Mockito.when(auditLog.isDebugEnabled()).thenReturn(true);
            Mockito.when(auditLog.isTraceEnabled()).thenReturn(true);

            Builder builder = addManyAcls(getTestingWrapperBuilder().superUser("User:superMan"));
            T authorizer = builder.get().getAuthorizer();

            ResourcePattern topicResource = new ResourcePattern(TOPIC, "alpha", LITERAL);
            Action action = new Action(READ, topicResource, 1, false, logIfDenied);
            MockAuthorizableRequestContext requestContext = new MockAuthorizableRequestContext.Builder()
                .setPrincipal(new KafkaPrincipal(USER_TYPE, "bob"))
                .setClientAddress(InetAddress.getByName("127.0.0.1"))
                .build();

            assertEquals(singletonList(DENIED), authorizer.authorize(requestContext, singletonList(action)));

            String expectedAuditLog = "Principal = User:bob is Denied operation = READ " +
                "from host = 127.0.0.1 on resource = Topic:LITERAL:alpha for request = Fetch " +
                "with resourceRefCount = 1 based on rule MatchingAcl(acl=StandardAcl(resourceType=TOPIC, " +
                "resourceName=alp, patternType=PREFIXED, principal=User:bob, host=*, operation=READ, " +
                "permissionType=DENY))";

            if (logIfDenied) {
                Mockito.verify(auditLog).info(expectedAuditLog);
            } else {
                Mockito.verify(auditLog).trace(expectedAuditLog);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testAllowAuditLogging(boolean logIfAllowed) throws Exception {
        try (MockedStatic<LoggerFactory> mockedLoggerFactory = Mockito.mockStatic(LoggerFactory.class)) {
            Logger otherLog = Mockito.mock(Logger.class);
            Logger auditLog = Mockito.mock(Logger.class);
            mockedLoggerFactory
                .when(() -> LoggerFactory.getLogger("kafka.authorizer.logger"))
                .thenReturn(auditLog);

            mockedLoggerFactory
                .when(() -> LoggerFactory.getLogger(Mockito.any(Class.class)))
                .thenReturn(otherLog);

            Mockito.when(auditLog.isDebugEnabled()).thenReturn(true);
            Mockito.when(auditLog.isTraceEnabled()).thenReturn(true);


            Builder builder = addManyAcls(getTestingWrapperBuilder().superUser("User:superMan"));
            T authorizer = builder.get().getAuthorizer();

            ResourcePattern topicResource = new ResourcePattern(TOPIC, "green1", LITERAL);
            Action action = new Action(READ, topicResource, 1, logIfAllowed, false);
            MockAuthorizableRequestContext requestContext = new MockAuthorizableRequestContext.Builder()
                .setPrincipal(new KafkaPrincipal(USER_TYPE, "bob"))
                .setClientAddress(InetAddress.getByName("127.0.0.1"))
                .build();

            assertEquals(singletonList(ALLOWED), authorizer.authorize(requestContext, singletonList(action)));

            String expectedAuditLog = "Principal = User:bob is Allowed operation = READ " +
                "from host = 127.0.0.1 on resource = Topic:LITERAL:green1 for request = Fetch " +
                "with resourceRefCount = 1 based on rule MatchingAcl(acl=StandardAcl(resourceType=TOPIC, " +
                "resourceName=green, patternType=PREFIXED, principal=User:bob, host=*, operation=READ, " +
                "permissionType=ALLOW))";

            if (logIfAllowed) {
                Mockito.verify(auditLog).debug(expectedAuditLog);
            } else {
                Mockito.verify(auditLog).trace(expectedAuditLog);
            }
        }
    }

    /**
     * Test attempts to authorize prior to completeInitialLoad. During this time, only
     * superusers can be authorized. Other users will get an AuthorizerNotReadyException
     * exception. Not even an authorization result, just an exception thrown for the whole
     * batch.
     */
    @Test
    public void testAuthorizationPriorToCompleteInitialLoad() throws Exception {

        Builder builder = addManyAcls(getTestingWrapperBuilder().superUser("User:superMan"));
        TestingWrapper<T> wrapper = builder.get();
        T authorizer = wrapper.configure(wrapper.getUnconfiguredAuthorizer());

        List<Action> actions =  Arrays.asList(AuthorizerTestUtils.newAction(READ, TOPIC, "green1"),
                AuthorizerTestUtils.newAction(WRITE, GROUP, "wheel"));

        assertThrows(AuthorizerNotReadyException.class, () ->
            authorizer.authorize(AuthorizerTestUtils.newRequestContext("bob"), actions));

        assertEquals(Arrays.asList(ALLOWED, ALLOWED),
            authorizer.authorize(AuthorizerTestUtils.newRequestContext("superMan"), actions));
    }

    @Test
    public void testCompleteInitialLoad() throws Exception {
        Builder builder = addManyAcls(getTestingWrapperBuilder().superUser("User:superMan"));
        TestingWrapper<T> wrapper = builder.get();
        T authorizer = wrapper.configure(wrapper.getUnconfiguredAuthorizer());
        Map<Endpoint, ? extends CompletionStage<Void>> futures = authorizer.start(new AuthorizerTestServerInfo(Collections.singleton(PLAINTEXT)));
        assertEquals(Collections.singleton(PLAINTEXT), futures.keySet());
        assertFalse(futures.get(PLAINTEXT).toCompletableFuture().isDone());
        authorizer.completeInitialLoad();
        assertTrue(futures.get(PLAINTEXT).toCompletableFuture().isDone());
        assertFalse(futures.get(PLAINTEXT).toCompletableFuture().isCompletedExceptionally());
    }

    @Test
    public void testCompleteInitialLoadWithException() throws Exception {
        Builder builder = addManyAcls(getTestingWrapperBuilder().superUser("User:superMan"));
        TestingWrapper<T> wrapper = builder.get();
        T authorizer = wrapper.configure(wrapper.getUnconfiguredAuthorizer());

        Map<Endpoint, ? extends CompletionStage<Void>> futures = authorizer.start(new AuthorizerTestServerInfo(Arrays.asList(PLAINTEXT, CONTROLLER)));
        assertEquals(new HashSet<>(Arrays.asList(PLAINTEXT, CONTROLLER)), futures.keySet());
        assertFalse(futures.get(PLAINTEXT).toCompletableFuture().isDone());
        assertTrue(futures.get(CONTROLLER).toCompletableFuture().isDone());
        authorizer.completeInitialLoad(new TimeoutException("timed out"));
        assertTrue(futures.get(PLAINTEXT).toCompletableFuture().isDone());
        assertTrue(futures.get(PLAINTEXT).toCompletableFuture().isCompletedExceptionally());
        assertTrue(futures.get(CONTROLLER).toCompletableFuture().isDone());
        assertFalse(futures.get(CONTROLLER).toCompletableFuture().isCompletedExceptionally());
    }
}
