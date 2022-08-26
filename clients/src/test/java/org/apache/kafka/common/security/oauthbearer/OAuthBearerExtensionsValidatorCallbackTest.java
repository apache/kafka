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
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OAuthBearerExtensionsValidatorCallbackTest {
    private static final OAuthBearerToken TOKEN = new OAuthBearerTokenMock();

    @Test
    public void testValidatedExtensionsAreReturned() {
        Map<String, String> extensions = new HashMap<>();
        extensions.put("hello", "bye");

        OAuthBearerExtensionsValidatorCallback callback = new OAuthBearerExtensionsValidatorCallback(TOKEN, new SaslExtensions(extensions));

        assertTrue(callback.validatedExtensions().isEmpty());
        assertTrue(callback.invalidExtensions().isEmpty());
        callback.valid("hello");
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

    /**
     * Extensions that are neither validated or invalidated must not be present in either maps
     */
    @Test
    public void testUnvalidatedExtensionsAreIgnored() {
        Map<String, String> extensions = new HashMap<>();
        extensions.put("valid", "valid");
        extensions.put("error", "error");
        extensions.put("nothing", "nothing");

        OAuthBearerExtensionsValidatorCallback callback = new OAuthBearerExtensionsValidatorCallback(TOKEN, new SaslExtensions(extensions));
        callback.error("error", "error");
        callback.valid("valid");

        assertFalse(callback.validatedExtensions().containsKey("nothing"));
        assertFalse(callback.invalidExtensions().containsKey("nothing"));
        assertEquals("nothing", callback.ignoredExtensions().get("nothing"));
    }

    @Test
    public void testCannotValidateExtensionWhichWasNotGiven() {
        Map<String, String> extensions = new HashMap<>();
        extensions.put("hello", "bye");

        OAuthBearerExtensionsValidatorCallback callback = new OAuthBearerExtensionsValidatorCallback(TOKEN, new SaslExtensions(extensions));

        assertThrows(IllegalArgumentException.class, () -> callback.valid("???"));
    }
}
