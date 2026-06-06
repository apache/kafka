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

package org.apache.kafka.common.security.oauthbearer.internals.expiring;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerRefreshingLogin;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;

import javax.security.auth.Subject;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OAuthBearerExpiringCredentialRefreshingLoginTest {

    private OAuthBearerExpiringCredentialRefreshingLogin login;
    private Subject mockSubject;
    private OAuthBearerToken mockToken;

    @BeforeEach
    public void setup() throws LoginException {
        AuthenticateCallbackHandler mockCallbackHandler = mock(AuthenticateCallbackHandler.class);
        Configuration mockConfiguration = mock(Configuration.class);
        mockToken = mock(OAuthBearerToken.class);
        mockSubject = mock(Subject.class);

        login = new OAuthBearerExpiringCredentialRefreshingLogin(
                "KafkaClient",
                mockConfiguration,
                new ExpiringCredentialRefreshConfig(
                        new ConfigDef().withClientSaslSupport().parse(Collections.emptyMap()),
                        true),
                mockCallbackHandler,
                OAuthBearerRefreshingLogin.class,
                new ExpiringCredentialRefreshingLogin.LoginContextFactory() {
                    @Override
                    public LoginContext createLoginContext(ExpiringCredentialRefreshingLogin expiringCredentialRefreshingLogin) {
                        LoginContext mockLoginContext = mock(LoginContext.class);
                        when(mockLoginContext.getSubject()).thenReturn(mockSubject);
                        return mockLoginContext;
                    }
                },
                Time.SYSTEM
        );

        login.login();
    }

    @Test
    public void testExpiringCredentialSubjectContainsNoTokens() {
        when(mockSubject.getPrivateCredentials(Mockito.any())).thenReturn(Collections.emptySet());

        assertNull(login.expiringCredential());
    }

    @Test
    public void testExpiringCredentialMapsTokenFieldsCorrectly() {
        when(mockToken.principalName()).thenReturn("test-user");
        when(mockToken.startTimeMs()).thenReturn(1000L);
        when(mockToken.lifetimeMs()).thenReturn(9000L);
        when(mockSubject.getPrivateCredentials(OAuthBearerToken.class))
                .thenReturn(Collections.singleton(mockToken));

        ExpiringCredential result = login.expiringCredential();

        assertNotNull(result);
        assertEquals("test-user", result.principalName());
        assertEquals(1000L, result.startTimeMs());
        assertEquals(9000L, result.expireTimeMs());
        assertNull(result.absoluteLastRefreshTimeMs());
    }
}