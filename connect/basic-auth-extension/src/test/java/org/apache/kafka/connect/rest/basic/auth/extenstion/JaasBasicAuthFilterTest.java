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

package org.apache.kafka.connect.rest.basic.auth.extenstion;

import org.apache.kafka.common.security.JaasUtils;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.annotation.MockStrict;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import javax.security.auth.login.Configuration;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;

import static org.powermock.api.easymock.PowerMock.replayAll;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.*")
public class JaasBasicAuthFilterTest {

    @MockStrict
    private ContainerRequestContext requestContext;

    private JaasBasicAuthFilter jaasBasicAuthFilter = new JaasBasicAuthFilter();

    @Before
    public void setup() throws IOException {
        EasyMock.reset(requestContext);
        File credentialFile = File.createTempFile("credential", ".properties");
        credentialFile.deleteOnExit();
        List<String> lines = new ArrayList<>();
        lines.add("user=password");
        lines.add("user1=password1");
        Files.write(credentialFile.toPath(), lines, StandardCharsets.UTF_8);

        File jaasConfigFile = File.createTempFile("ks-jaas-", ".conf");
        jaasConfigFile.deleteOnExit();
        System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, jaasConfigFile.getPath());
        lines = new ArrayList<>();
        lines.add("KafkaConnect { org.apache.kafka.connect.rest.basic.auth.extenstion.PropertyFileLoginModule required ");
        lines.add("file=\"" + credentialFile.getPath() + "\"");
        lines.add(";};");
        Files.write(jaasConfigFile.toPath(), lines, StandardCharsets.UTF_8);
        Configuration.setConfiguration(null);
    }

    @Test
    public void testSuccess() throws IOException {
        String authHeader = "Basic " + Base64.getEncoder().encodeToString("user:password".getBytes());
        EasyMock.expect(requestContext.getHeaderString(JaasBasicAuthFilter.AUTHORIZATION))
            .andReturn(authHeader);
        replayAll();
        jaasBasicAuthFilter.filter(requestContext);
    }


    @Test
    public void testBadCredential() throws IOException {
        String authHeader = "Basic " + Base64.getEncoder().encodeToString("user1:password".getBytes());
        EasyMock.expect(requestContext.getHeaderString(JaasBasicAuthFilter.AUTHORIZATION))
            .andReturn(authHeader);
        requestContext.abortWith(EasyMock.anyObject(Response.class));
        EasyMock.expectLastCall();
        replayAll();
        jaasBasicAuthFilter.filter(requestContext);
    }

    @Test
    public void testBadPassword() throws IOException {
        String authHeader = "Basic " + Base64.getEncoder().encodeToString("user:password1".getBytes());
        EasyMock.expect(requestContext.getHeaderString(JaasBasicAuthFilter.AUTHORIZATION))
            .andReturn(authHeader);
        requestContext.abortWith(EasyMock.anyObject(Response.class));
        EasyMock.expectLastCall();
        replayAll();
        jaasBasicAuthFilter.filter(requestContext);
    }

    @Test
    public void testUnknownBearer() throws IOException {
        String authHeader =
            "Unknown " + Base64.getEncoder().encodeToString("user:password".getBytes());
        EasyMock.expect(requestContext.getHeaderString(JaasBasicAuthFilter.AUTHORIZATION))
            .andReturn(authHeader);
        requestContext.abortWith(EasyMock.anyObject(Response.class));
        EasyMock.expectLastCall();
        replayAll();
        jaasBasicAuthFilter.filter(requestContext);
    }

    @Test
    public void testUnknownLoginModule() throws IOException {

        File jaasConfigFile = File.createTempFile("ks-jaas-", ".conf");
        jaasConfigFile.deleteOnExit();

        System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, jaasConfigFile.getPath());
        List<String> lines = new ArrayList<>();
        lines.add("KafkaConnect1 { org.apache.kafka.connect.rest.extension"
                  + ".PropertyFileLoginModule required ;};");
        Files.write(jaasConfigFile.toPath(), lines, StandardCharsets.UTF_8);
        Configuration.setConfiguration(null);

        String authHeader =
            "Basic " + Base64.getEncoder().encodeToString("user:password".getBytes());
        EasyMock.expect(requestContext.getHeaderString(JaasBasicAuthFilter.AUTHORIZATION))
            .andReturn(authHeader);
        requestContext.abortWith(EasyMock.anyObject(Response.class));
        EasyMock.expectLastCall();
        replayAll();
        jaasBasicAuthFilter.filter(requestContext);
    }

}
