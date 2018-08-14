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

import org.apache.kafka.common.security.auth.SaslExtensions;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OAuthBearerExtensionsValidatorCallbackTest {
    private static final OAuthBearerToken TOKEN = new OAuthBearerToken() {
        @Override
        public String value() {
            return "value";
        }

        @Override
        public Long startTimeMs() {
            return null;
        }

        @Override
        public Set<String> scope() {
            return Collections.emptySet();
        }

        @Override
        public String principalName() {
            return "principalName";
        }

        @Override
        public long lifetimeMs() {
            return 0;
        }
    };

    @Test
    public void testValidatedExtensionsAreReturned() {
        Map<String, String> extensions = new HashMap<>();
        extensions.put("hello", "bye");

        OAuthBearerExtensionsValidatorCallback callback = new OAuthBearerExtensionsValidatorCallback(TOKEN, new SaslExtensions(extensions));

        assertTrue(callback.validatedExtensions().isEmpty());
        assertTrue(callback.invalidExtensions().isEmpty());
        callback.validate("hello");
        assertFalse(callback.validatedExtensions().isEmpty());
        assertEquals("bye", callback.validatedExtensions().get("hello"));
        assertTrue(callback.invalidExtensions().isEmpty());
    }

    @Test
    public void testInvalidExtensionsAndErrorMessagesAreReturned() {
        Map<String, String> extensions = new HashMap<>();
        extensions.put("hello", "bye");

        OAuthBearerExtensionsValidatorCallback callback = new OAuthBearerExtensionsValidatorCallback(TOKEN, new SaslExtensions(extensions));

        assertTrue(callback.validatedExtensions().isEmpty());
        assertTrue(callback.invalidExtensions().isEmpty());
        callback.error("hello", "error");
        assertFalse(callback.invalidExtensions().isEmpty());
        assertEquals("error", callback.invalidExtensions().get("hello"));
        assertTrue(callback.validatedExtensions().isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCannotValidateExtensionWhichWasNotGiven() {
        Map<String, String> extensions = new HashMap<>();
        extensions.put("hello", "bye");

        OAuthBearerExtensionsValidatorCallback callback = new OAuthBearerExtensionsValidatorCallback(TOKEN, new SaslExtensions(extensions));

        callback.validate("???");
    }
}
