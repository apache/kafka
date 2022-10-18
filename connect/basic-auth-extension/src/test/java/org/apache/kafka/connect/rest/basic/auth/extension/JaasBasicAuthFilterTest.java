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

package org.apache.kafka.connect.rest.basic.auth.extension;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.ChoiceCallback;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

import org.apache.kafka.common.security.authenticator.TestJaasConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class JaasBasicAuthFilterTest {

    private static final String LOGIN_MODULE =
        "org.apache.kafka.connect.rest.basic.auth.extension.PropertyFileLoginModule";

    @Test
    public void testSuccess() throws IOException {
        File credentialFile = setupPropertyLoginFile(true);
        JaasBasicAuthFilter jaasBasicAuthFilter = setupJaasFilter("KafkaConnect", credentialFile.getPath());
        ContainerRequestContext requestContext = setMock("Basic", "user", "password");
        jaasBasicAuthFilter.filter(requestContext);

        verify(requestContext, atLeastOnce()).getMethod();
        verify(requestContext).getHeaderString(JaasBasicAuthFilter.AUTHORIZATION);
    }

    @Test
    public void testEmptyCredentialsFile() throws IOException {
        File credentialFile = setupPropertyLoginFile(false);
        JaasBasicAuthFilter jaasBasicAuthFilter = setupJaasFilter("KafkaConnect", credentialFile.getPath());
        ContainerRequestContext requestContext = setMock("Basic", "user", "password");
        jaasBasicAuthFilter.filter(requestContext);

        verify(requestContext, atLeastOnce()).getMethod();
        verify(requestContext).getHeaderString(JaasBasicAuthFilter.AUTHORIZATION);
    }

    @Test
    public void testBadCredential() throws IOException {
        File credentialFile = setupPropertyLoginFile(true);
        JaasBasicAuthFilter jaasBasicAuthFilter = setupJaasFilter("KafkaConnect", credentialFile.getPath());
        ContainerRequestContext requestContext = setMock("Basic", "user1", "password");
        jaasBasicAuthFilter.filter(requestContext);

        verify(requestContext).abortWith(any(Response.class));
        verify(requestContext, atLeastOnce()).getMethod();
        verify(requestContext).getHeaderString(JaasBasicAuthFilter.AUTHORIZATION);
    }

    @Test
    public void testBadPassword() throws IOException {
        File credentialFile = setupPropertyLoginFile(true);
        JaasBasicAuthFilter jaasBasicAuthFilter = setupJaasFilter("KafkaConnect", credentialFile.getPath());
        ContainerRequestContext requestContext = setMock("Basic", "user", "password1");
        jaasBasicAuthFilter.filter(requestContext);

        verify(requestContext).abortWith(any(Response.class));
        verify(requestContext, atLeastOnce()).getMethod();
        verify(requestContext).getHeaderString(JaasBasicAuthFilter.AUTHORIZATION);
    }

    @Test
    public void testUnknownBearer() throws IOException {
        File credentialFile = setupPropertyLoginFile(true);
        JaasBasicAuthFilter jaasBasicAuthFilter = setupJaasFilter("KafkaConnect", credentialFile.getPath());
        ContainerRequestContext requestContext = setMock("Unknown", "user", "password");
        jaasBasicAuthFilter.filter(requestContext);

        verify(requestContext).abortWith(any(Response.class));
        verify(requestContext, atLeastOnce()).getMethod();
        verify(requestContext).getHeaderString(JaasBasicAuthFilter.AUTHORIZATION);
    }

    @Test
    public void testUnknownLoginModule() throws IOException {
        File credentialFile = setupPropertyLoginFile(true);
        JaasBasicAuthFilter jaasBasicAuthFilter = setupJaasFilter("KafkaConnect1", credentialFile.getPath());
        ContainerRequestContext requestContext = setMock("Basic", "user", "password");
        jaasBasicAuthFilter.filter(requestContext);

        verify(requestContext).abortWith(any(Response.class));
        verify(requestContext, atLeastOnce()).getMethod();
        verify(requestContext).getHeaderString(JaasBasicAuthFilter.AUTHORIZATION);
    }

    @Test
    public void testUnknownCredentialsFile() throws IOException {
        JaasBasicAuthFilter jaasBasicAuthFilter = setupJaasFilter("KafkaConnect", "/tmp/testcrednetial");
        ContainerRequestContext requestContext = setMock("Basic", "user", "password");
        jaasBasicAuthFilter.filter(requestContext);

        verify(requestContext).abortWith(any(Response.class));
        verify(requestContext, atLeastOnce()).getMethod();
        verify(requestContext).getHeaderString(JaasBasicAuthFilter.AUTHORIZATION);
    }

    @Test
    public void testNoFileOption() throws IOException {
        JaasBasicAuthFilter jaasBasicAuthFilter = setupJaasFilter("KafkaConnect", null);
        ContainerRequestContext requestContext = setMock("Basic", "user", "password");
        jaasBasicAuthFilter.filter(requestContext);

        verify(requestContext).abortWith(any(Response.class));
        verify(requestContext, atLeastOnce()).getMethod();
        verify(requestContext).getHeaderString(JaasBasicAuthFilter.AUTHORIZATION);
    }

    @Test
    public void testInternalTaskConfigEndpointSkipped() throws IOException {
        testInternalEndpointSkipped(HttpMethod.POST, "connectors/connName/tasks");
    }

    @Test
    public void testInternalZombieFencingEndpointSkipped() throws IOException {
        testInternalEndpointSkipped(HttpMethod.PUT, "connectors/connName/fence");
    }

    private void testInternalEndpointSkipped(String method, String endpoint) throws IOException {
        UriInfo uriInfo = mock(UriInfo.class);
        when(uriInfo.getPath()).thenReturn(endpoint);

        ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
        when(requestContext.getMethod()).thenReturn(method);
        when(requestContext.getUriInfo()).thenReturn(uriInfo);

        File credentialFile = setupPropertyLoginFile(true);
        JaasBasicAuthFilter jaasBasicAuthFilter = setupJaasFilter("KafkaConnect1", credentialFile.getPath());

        jaasBasicAuthFilter.filter(requestContext);

        verify(uriInfo).getPath();
        verify(requestContext, atLeastOnce()).getMethod();
        verify(requestContext).getUriInfo();
        verifyNoMoreInteractions(requestContext);
    }

    @Test
    public void testPostNotChangingConnectorTask() throws IOException {
        UriInfo uriInfo = mock(UriInfo.class);
        when(uriInfo.getPath()).thenReturn("local:randomport/connectors/connName");

        ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
        when(requestContext.getMethod()).thenReturn(HttpMethod.POST);
        when(requestContext.getUriInfo()).thenReturn(uriInfo);
        String authHeader = "Basic" + Base64.getEncoder().encodeToString(("user" + ":" + "password").getBytes());
        when(requestContext.getHeaderString(JaasBasicAuthFilter.AUTHORIZATION))
            .thenReturn(authHeader);

        File credentialFile = setupPropertyLoginFile(true);
        JaasBasicAuthFilter jaasBasicAuthFilter = setupJaasFilter("KafkaConnect", credentialFile.getPath());

        jaasBasicAuthFilter.filter(requestContext);

        verify(requestContext).abortWith(any(Response.class));
        verify(requestContext).getUriInfo();
        verify(requestContext).getUriInfo();
    }

    @Test
    public void testUnsupportedCallback() {
        CallbackHandler callbackHandler = new JaasBasicAuthFilter.BasicAuthCallBackHandler(
                new JaasBasicAuthFilter.BasicAuthCredentials(authHeader("basic", "user", "pwd")));
        Callback unsupportedCallback = new ChoiceCallback(
            "You take the blue pill... the story ends, you wake up in your bed and believe whatever you want to believe. " 
                + "You take the red pill... you stay in Wonderland, and I show you how deep the rabbit hole goes.",
            new String[] {"blue pill", "red pill"},
            1,
            true
        );
        assertThrows(ConnectException.class, () -> callbackHandler.handle(new Callback[] {unsupportedCallback}));
    }

    @Test
    public void testSecurityContextSet() throws IOException, URISyntaxException {
        File credentialFile = setupPropertyLoginFile(true);
        JaasBasicAuthFilter jaasBasicAuthFilter = setupJaasFilter("KafkaConnect", credentialFile.getPath());
        ContainerRequestContext requestContext = setMock("Basic", "user1", "password1");

        when(requestContext.getUriInfo()).thenReturn(mock(UriInfo.class));
        when(requestContext.getUriInfo().getRequestUri()).thenReturn(new URI("https://foo.bar"));

        jaasBasicAuthFilter.filter(requestContext);

        ArgumentCaptor<SecurityContext> capturedContext = ArgumentCaptor.forClass(SecurityContext.class);
        verify(requestContext).setSecurityContext(capturedContext.capture());
        assertEquals("user1", capturedContext.getValue().getUserPrincipal().getName());
        assertEquals(true, capturedContext.getValue().isSecure());
    }

    private String authHeader(String authorization, String username, String password) {
        return authorization + " " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes());
    }

    private ContainerRequestContext setMock(String authorization, String username, String password) {
        ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
        when(requestContext.getMethod()).thenReturn(HttpMethod.GET);
        when(requestContext.getHeaderString(JaasBasicAuthFilter.AUTHORIZATION))
            .thenReturn(authHeader(authorization, username, password));
        return requestContext;
    }

    private File setupPropertyLoginFile(boolean includeUsers) throws IOException {
        File credentialFile = TestUtils.tempFile("credential", ".properties");
        if (includeUsers) {
            List<String> lines = new ArrayList<>();
            lines.add("user=password");
            lines.add("user1=password1");
            Files.write(credentialFile.toPath(), lines, StandardCharsets.UTF_8);
        }
        return credentialFile;
    }

    private JaasBasicAuthFilter setupJaasFilter(String name, String credentialFilePath) {
        TestJaasConfig configuration = new TestJaasConfig();
        Map<String, Object> moduleOptions = credentialFilePath != null
            ? Collections.singletonMap("file", credentialFilePath)
            : Collections.emptyMap();
        configuration.addEntry(name, LOGIN_MODULE, moduleOptions);
        return new JaasBasicAuthFilter(configuration);
    }

}
