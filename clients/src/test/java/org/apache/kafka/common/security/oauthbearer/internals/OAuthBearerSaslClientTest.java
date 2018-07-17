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
package org.apache.kafka.common.security.oauthbearer.internals;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.SaslExtensionsCallback;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredJws;
import org.easymock.EasyMockSupport;
import org.junit.Test;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.SaslException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class OAuthBearerSaslClientTest extends EasyMockSupport {

    private Map<String, String> TEST_EXTENSIONS = new LinkedHashMap<String, String>()
    {{
        put("One", "1");
        put("Two", "2");
        put("Three", "3");
    }};
    private final String ERROR_MESSAGE = "Error as expected!";

    public class ExtensionsCallbackHandler implements AuthenticateCallbackHandler {
        private boolean configured = false;
        private boolean toThrow;

        ExtensionsCallbackHandler(boolean toThrow) {
            this.toThrow = toThrow;
        }

        public boolean configured() {
            return configured;
        }

        @Override
        public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
            configured = true;
        }

        @Override
        public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof OAuthBearerTokenCallback)
                    ((OAuthBearerTokenCallback) callback).token(createMock(OAuthBearerUnsecuredJws.class));
                else if (callback instanceof SaslExtensionsCallback) {
                    if (toThrow)
                        throw new ConfigException(ERROR_MESSAGE);
                    else
                        ((SaslExtensionsCallback) callback).extensions(TEST_EXTENSIONS);
                } else
                    throw new UnsupportedCallbackException(callback);
            }
        }

        @Override
        public void close() {
        }
    }

    @Test
    public void testAttachesExtensionsToFirstClientMessage() throws Exception {
        String expectedToken = "n,,auth=Bearer null,One=1,Two=2,Three=3";
        OAuthBearerSaslClient client = new OAuthBearerSaslClient(new ExtensionsCallbackHandler(false));

        String message = new String(client.evaluateChallenge("".getBytes()), StandardCharsets.UTF_8);

        assertEquals(expectedToken, message);
    }

    @Test
    public void testNoExtensionsDoesNotAttachAnythingToFirstClientMessage() throws Exception {
        TEST_EXTENSIONS.clear();
        String expectedToken = "n,,auth=Bearer null";
        OAuthBearerSaslClient client = new OAuthBearerSaslClient(new ExtensionsCallbackHandler(false));

        String message = new String(client.evaluateChallenge("".getBytes()), StandardCharsets.UTF_8);

        assertEquals(expectedToken, message);
    }

    @Test
    public void testWrapsExtensionsCallbackHandlingErrorInSaslExceptionInFirstClientMessage() {
        OAuthBearerSaslClient client = new OAuthBearerSaslClient(new ExtensionsCallbackHandler(true));
        try {
            client.evaluateChallenge("".getBytes());
            fail("Should have failed with " + SaslException.class.getName());
        } catch (SaslException e) {
            // assert it has caught our expected exception
            assertEquals(ConfigException.class, e.getCause().getClass());
            assertEquals(ERROR_MESSAGE, e.getCause().getMessage());
        }

    }
}
