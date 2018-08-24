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

import javax.security.auth.callback.Callback;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.common.utils.CollectionUtils.subtractMap;

/**
 * A {@code Callback} for use by the {@code SaslServer} implementation when it
 * needs to validate the SASL extensions for the OAUTHBEARER mechanism
 * Callback handlers should use the {@link #valid(String)}
 * method to communicate valid extensions back to the SASL server.
 * Callback handlers should use the
 * {@link #error(String, String)} method to communicate validation errors back to
 * the SASL Server.
 * As per RFC-7628 (https://tools.ietf.org/html/rfc7628#section-3.1), unknown extensions must be ignored by the server.
 * The callback handler implementation should simply ignore unknown extensions,
 * not calling {@link #error(String, String)} nor {@link #valid(String)}.
 * Callback handlers should communicate other problems by raising an {@code IOException}.
 * <p>
 * The OAuth bearer token is provided in the callback for better context in extension validation.
 * It is very important that token validation is done in its own {@link OAuthBearerValidatorCallback}
 * irregardless of provided extensions, as they are inherently insecure.
 */
public class OAuthBearerExtensionsValidatorCallback implements Callback {
    private final OAuthBearerToken token;
    private final SaslExtensions inputExtensions;
    private final Map<String, String> validatedExtensions = new HashMap<>();
    private final Map<String, String> invalidExtensions = new HashMap<>();

    public OAuthBearerExtensionsValidatorCallback(OAuthBearerToken token, SaslExtensions extensions) {
        this.token = Objects.requireNonNull(token);
        this.inputExtensions = Objects.requireNonNull(extensions);
    }

    /**
     * @return {@link OAuthBearerToken} the OAuth bearer token of the client
     */
    public OAuthBearerToken token() {
        return token;
    }

    /**
     * @return {@link SaslExtensions} consisting of the unvalidated extension names and values that were sent by the client
     */
    public SaslExtensions inputExtensions() {
        return inputExtensions;
    }

    /**
     * @return an unmodifiable {@link Map} consisting of the validated and recognized by the server extension names and values
     */
    public Map<String, String> validatedExtensions() {
        return Collections.unmodifiableMap(validatedExtensions);
    }

    /**
     * @return An immutable {@link Map} consisting of the name->error messages of extensions which failed validation
     */
    public Map<String, String> invalidExtensions() {
        return Collections.unmodifiableMap(invalidExtensions);
    }

    /**
     * @return An immutable {@link Map} consisting of the extensions that have neither been validated nor invalidated
     */
    public Map<String, String> ignoredExtensions() {
        return Collections.unmodifiableMap(subtractMap(subtractMap(inputExtensions.map(), invalidExtensions), validatedExtensions));
    }

    /**
     * Validates a specific extension in the original {@code inputExtensions} map
     * @param extensionName - the name of the extension which was validated
     */
    public void valid(String extensionName) {
        if (!inputExtensions.map().containsKey(extensionName))
            throw new IllegalArgumentException(String.format("Extension %s was not found in the original extensions", extensionName));
        validatedExtensions.put(extensionName, inputExtensions.map().get(extensionName));
    }
    /**
     * Set the error value for a specific extension key-value pair if validation has failed
     *
     * @param invalidExtensionName
     *            the mandatory extension name which caused the validation failure
     * @param errorMessage
     *            error message describing why the validation failed
     */
    public void error(String invalidExtensionName, String errorMessage) {
        if (Objects.requireNonNull(invalidExtensionName).isEmpty())
            throw new IllegalArgumentException("extension name must not be empty");
        this.invalidExtensions.put(invalidExtensionName, errorMessage);
    }
}
