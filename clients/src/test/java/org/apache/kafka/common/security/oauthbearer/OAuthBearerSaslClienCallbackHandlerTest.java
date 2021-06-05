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
package org.apache.kafka.common.security.oauthbearer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;

import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerSaslClientCallbackHandler;
import org.junit.jupiter.api.Test;

public class OAuthBearerSaslClienCallbackHandlerTest {
    private static OAuthBearerToken createTokenWithLifetimeMillis(final long lifetimeMillis) {
        return new OAuthBearerToken() {
            @Override
            public String value() {
                return null;
            }

            @Override
            public Long startTimeMs() {
                return null;
            }

            @Override
            public Set<String> scope() {
                return null;
            }

            @Override
            public String principalName() {
                return null;
            }

            @Override
            public long lifetimeMs() {
                return lifetimeMillis;
            }
        };
    }

    @Test
    public void testWithZeroTokens() {
        OAuthBearerSaslClientCallbackHandler handler = createCallbackHandler();
        PrivilegedActionException e = assertThrows(PrivilegedActionException.class, () -> Subject.doAs(new Subject(),
            (PrivilegedExceptionAction<Void>) () -> {
                OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
                handler.handle(new Callback[] {callback});
                return null;
            }
        ));
        assertEquals(IOException.class, e.getCause().getClass());
    }

    @Test()
    public void testWithPotentiallyMultipleTokens() throws Exception {
        OAuthBearerSaslClientCallbackHandler handler = createCallbackHandler();
        Subject.doAs(new Subject(), (PrivilegedExceptionAction<Void>) () -> {
            final int maxTokens = 4;
            final Set<Object> privateCredentials = Subject.getSubject(AccessController.getContext())
                    .getPrivateCredentials();
            privateCredentials.clear();
            for (int num = 1; num <= maxTokens; ++num) {
                privateCredentials.add(createTokenWithLifetimeMillis(num));
                OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
                handler.handle(new Callback[] {callback});
                assertEquals(num, callback.token().lifetimeMs());
            }
            return null;
        });
    }

    private static OAuthBearerSaslClientCallbackHandler createCallbackHandler() {
        OAuthBearerSaslClientCallbackHandler handler = new OAuthBearerSaslClientCallbackHandler();
        handler.configure(Collections.emptyMap(), OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
                Collections.emptyList());
        return handler;
    }
}
