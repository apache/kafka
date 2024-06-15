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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.auth.SaslExtensions;
import org.apache.kafka.common.security.authenticator.SaslInternalConfigs;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerExtensionsValidatorCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallback;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code SaslServer} implementation for SASL/OAUTHBEARER in Kafka. An instance
 * of {@link OAuthBearerToken} is available upon successful authentication via
 * the negotiated property "{@code OAUTHBEARER.token}"; the token could be used
 * in a custom authorizer (to authorize based on JWT claims rather than ACLs,
 * for example).
 */
public class OAuthBearerSaslServer implements SaslServer {

    private static final Logger log = LoggerFactory.getLogger(OAuthBearerSaslServer.class);
    private static final String NEGOTIATED_PROPERTY_KEY_TOKEN = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM + ".token";
    private static final String INTERNAL_ERROR_ON_SERVER = "Authentication could not be performed due to an internal error on the server";

    private final AuthenticateCallbackHandler callbackHandler;

    private boolean complete;
    private OAuthBearerToken tokenForNegotiatedProperty = null;
    private String errorMessage = null;
    private SaslExtensions extensions;

    public OAuthBearerSaslServer(CallbackHandler callbackHandler) {
        if (!(Objects.requireNonNull(callbackHandler) instanceof AuthenticateCallbackHandler))
            throw new IllegalArgumentException(String.format("Callback handler must be castable to %s: %s",
                    AuthenticateCallbackHandler.class.getName(), callbackHandler.getClass().getName()));
        this.callbackHandler = (AuthenticateCallbackHandler) callbackHandler;
    }

    /**
     * @throws SaslAuthenticationException
     *             if access token cannot be validated
     *             <p>
     *             <b>Note:</b> This method may throw
     *             {@link SaslAuthenticationException} to provide custom error
     *             messages to clients. But care should be taken to avoid including
     *             any information in the exception message that should not be
     *             leaked to unauthenticated clients. It may be safer to throw
     *             {@link SaslException} in some cases so that a standard error
     *             message is returned to clients.
     *             </p>
     */
    @Override
    public byte[] evaluateResponse(byte[] response) throws SaslException, SaslAuthenticationException {
        if (response.length == 1 && response[0] == OAuthBearerSaslClient.BYTE_CONTROL_A && errorMessage != null) {
            log.debug("Received %x01 response from client after it received our error");
            throw new SaslAuthenticationException(errorMessage);
        }
        errorMessage = null;

        OAuthBearerClientInitialResponse clientResponse;
        try {
            clientResponse = new OAuthBearerClientInitialResponse(response);
        } catch (SaslException e) {
            log.debug(e.getMessage());
            throw e;
        }

        return process(clientResponse.tokenValue(), clientResponse.authorizationId(), clientResponse.extensions());
    }

    @Override
    public String getAuthorizationID() {
        if (!complete)
            throw new IllegalStateException("Authentication exchange has not completed");
        return tokenForNegotiatedProperty.principalName();
    }

    @Override
    public String getMechanismName() {
        return OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
    }

    @Override
    public Object getNegotiatedProperty(String propName) {
        if (!complete)
            throw new IllegalStateException("Authentication exchange has not completed");
        if (NEGOTIATED_PROPERTY_KEY_TOKEN.equals(propName))
            return tokenForNegotiatedProperty;
        if (SaslInternalConfigs.CREDENTIAL_LIFETIME_MS_SASL_NEGOTIATED_PROPERTY_KEY.equals(propName))
            return tokenForNegotiatedProperty.lifetimeMs();
        return extensions.map().get(propName);
    }

    @Override
    public boolean isComplete() {
        return complete;
    }

    @Override
    public byte[] unwrap(byte[] incoming, int offset, int len) {
        if (!complete)
            throw new IllegalStateException("Authentication exchange has not completed");
        return Arrays.copyOfRange(incoming, offset, offset + len);
    }

    @Override
    public byte[] wrap(byte[] outgoing, int offset, int len) {
        if (!complete)
            throw new IllegalStateException("Authentication exchange has not completed");
        return Arrays.copyOfRange(outgoing, offset, offset + len);
    }

    @Override
    public void dispose() {
        complete = false;
        tokenForNegotiatedProperty = null;
        extensions = null;
    }

    private byte[] process(String tokenValue, String authorizationId, SaslExtensions extensions) throws SaslException {
        OAuthBearerValidatorCallback callback = new OAuthBearerValidatorCallback(tokenValue);
        try {
            callbackHandler.handle(new Callback[] {callback});
        } catch (IOException | UnsupportedCallbackException e) {
            handleCallbackError(e);
        }
        OAuthBearerToken token = callback.token();
        if (token == null) {
            errorMessage = jsonErrorResponse(callback.errorStatus(), callback.errorScope(),
                    callback.errorOpenIDConfiguration());
            log.debug(errorMessage);
            return errorMessage.getBytes(StandardCharsets.UTF_8);
        }
        /*
         * We support the client specifying an authorization ID as per the SASL
         * specification, but it must match the principal name if it is specified.
         */
        if (!authorizationId.isEmpty() && !authorizationId.equals(token.principalName()))
            throw new SaslAuthenticationException(String.format(
                    "Authentication failed: Client requested an authorization id (%s) that is different from the token's principal name (%s)",
                    authorizationId, token.principalName()));

        Map<String, String> validExtensions = processExtensions(token, extensions);

        tokenForNegotiatedProperty = token;
        this.extensions = new SaslExtensions(validExtensions);
        complete = true;
        log.debug("Successfully authenticate User={}", token.principalName());
        return new byte[0];
    }

    private Map<String, String> processExtensions(OAuthBearerToken token, SaslExtensions extensions) throws SaslException {
        OAuthBearerExtensionsValidatorCallback extensionsCallback = new OAuthBearerExtensionsValidatorCallback(token, extensions);
        try {
            callbackHandler.handle(new Callback[] {extensionsCallback});
        } catch (UnsupportedCallbackException e) {
            // backwards compatibility - no extensions will be added
        } catch (IOException e) {
            handleCallbackError(e);
        }
        if (!extensionsCallback.invalidExtensions().isEmpty()) {
            String errorMessage = String.format("Authentication failed: %d extensions are invalid! They are: %s",
                    extensionsCallback.invalidExtensions().size(),
                    Utils.mkString(extensionsCallback.invalidExtensions(), "", "", ": ", "; "));
            log.debug(errorMessage);
            throw new SaslAuthenticationException(errorMessage);
        }

        return extensionsCallback.validatedExtensions();
    }

    private static String jsonErrorResponse(String errorStatus, String errorScope, String errorOpenIDConfiguration) {
        String jsonErrorResponse = String.format("{\"status\":\"%s\"", errorStatus);
        if (errorScope != null)
            jsonErrorResponse = String.format("%s, \"scope\":\"%s\"", jsonErrorResponse, errorScope);
        if (errorOpenIDConfiguration != null)
            jsonErrorResponse = String.format("%s, \"openid-configuration\":\"%s\"", jsonErrorResponse,
                    errorOpenIDConfiguration);
        jsonErrorResponse = String.format("%s}", jsonErrorResponse);
        return jsonErrorResponse;
    }

    private void handleCallbackError(Exception e) throws SaslException {
        String msg = String.format("%s: %s", INTERNAL_ERROR_ON_SERVER, e.getMessage());
        log.debug(msg, e);
        throw new SaslException(msg);
    }

    public static String[] mechanismNamesCompatibleWithPolicy(Map<String, ?> props) {
        return props != null && "true".equals(String.valueOf(props.get(Sasl.POLICY_NOPLAINTEXT))) ? new String[] {}
                : new String[] {OAuthBearerLoginModule.OAUTHBEARER_MECHANISM};
    }

    public static class OAuthBearerSaslServerFactory implements SaslServerFactory {
        @Override
        public SaslServer createSaslServer(String mechanism, String protocol, String serverName, Map<String, ?> props,
                CallbackHandler callbackHandler) {
            String[] mechanismNamesCompatibleWithPolicy = getMechanismNames(props);
            for (String name : mechanismNamesCompatibleWithPolicy) {
                if (name.equals(mechanism)) {
                    return new OAuthBearerSaslServer(callbackHandler);
                }
            }
            return null;
        }

        @Override
        public String[] getMechanismNames(Map<String, ?> props) {
            return OAuthBearerSaslServer.mechanismNamesCompatibleWithPolicy(props);
        }
    }
}
