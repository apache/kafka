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
package org.apache.kafka.common.security.oauthbearer.internals.secured;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET;

/**
 * {@code ClientSecretRequestFormatter} is an {@link HttpRequestFormatter} that formats HTTP requests
 * for OAuth 2.0 token requests using the <code>client_credentials</code> grant type with client secret
 * authentication.
 *
 * <p>
 * This formatter implements the traditional OAuth 2.0 client authentication method where the client
 * authenticates using a client ID and client secret. The credentials are sent in the HTTP Authorization
 * header using HTTP Basic authentication (Base64-encoded).
 * </p>
 *
 * <p>
 * The formatter creates HTTP requests with:
 * <ul>
 *   <li>Authorization header: Basic &lt;base64(clientId:clientSecret)&gt;</li>
 *   <li>Content-Type: application/x-www-form-urlencoded</li>
 *   <li>grant_type=client_credentials</li>
 *   <li>Optional scope parameter</li>
 * </ul>
 * </p>
 *
 * <p>
 * This class was renamed from {@code ClientCredentialsRequestFormatter} to better distinguish it from
 * client assertion-based authentication ({@link ClientAssertionRequestFormatter}).
 * </p>
 *
 * @see ClientAssertionRequestFormatter
 * @see <a href="https://datatracker.ietf.org/doc/html/rfc6749#section-2.3.1">RFC 6749 Section 2.3.1: Client Password</a>
 */
public class ClientSecretRequestFormatter implements HttpRequestFormatter {

    public static final String GRANT_TYPE = "client_credentials";

    private final String clientId;

    private final String clientSecret;

    private final String scope;

    /**
     * Creates a new {@code ClientSecretRequestFormatter} instance.
     *
     * @param clientId     The OAuth client ID (required, non-blank)
     * @param clientSecret The OAuth client secret (required, non-blank)
     * @param scope        The OAuth scope to request; may be {@code null} for default scope
     * @param urlEncode    If {@code true}, URL-encodes the client ID, client secret, and scope according to
     *                     RFC 6749 Section 2.3.1; if {@code false}, uses them as-is
     * @throws ConfigException if the client ID or client secret is blank
     */
    public ClientSecretRequestFormatter(String clientId, String clientSecret, String scope, boolean urlEncode) {
        if (Utils.isBlank(clientId))
            throw new ConfigException(SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID, clientId);

        if (Utils.isBlank(clientSecret))
            throw new ConfigException(SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET, clientSecret);

        clientId = clientId.trim();
        clientSecret = clientSecret.trim();
        scope = Utils.isBlank(scope) ? null : scope.trim();

        // according to RFC-6749 clientId & clientSecret must be urlencoded, see https://tools.ietf.org/html/rfc6749#section-2.3.1
        if (urlEncode) {
            clientId = URLEncoder.encode(clientId, StandardCharsets.UTF_8);
            clientSecret = URLEncoder.encode(clientSecret, StandardCharsets.UTF_8);

            if (scope != null)
                scope = URLEncoder.encode(scope, StandardCharsets.UTF_8);
        }

        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.scope = scope;
    }

    @Override
    public Map<String, String> formatHeaders() {
        String s = String.format("%s:%s", clientId, clientSecret);
        // Per RFC-7617, we need to use the *non-URL safe* base64 encoder. See KAFKA-14496.
        String encoded = Base64.getEncoder().encodeToString(Utils.utf8(s));
        String authorizationHeader = String.format("Basic %s", encoded);

        Map<String, String> headers = new HashMap<>();
        headers.put("Accept", "application/json");
        headers.put("Authorization", authorizationHeader);
        headers.put("Cache-Control", "no-cache");
        headers.put("Content-Type", "application/x-www-form-urlencoded");
        return headers;
    }

    @Override
    public String formatBody() {
        StringBuilder requestParameters = new StringBuilder();
        requestParameters.append("grant_type=").append(GRANT_TYPE);

        if (scope != null)
            requestParameters.append("&scope=").append(scope);

        return requestParameters.toString();
    }
}
