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

import org.apache.kafka.common.security.oauthbearer.internals.secured.assertion.CloseableSupplier;
import org.apache.kafka.common.utils.Utils;

import java.io.Closeable;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * {@code ClientAssertionRequestFormatter} is an {@link HttpRequestFormatter} that formats HTTP requests
 * for OAuth 2.0 token requests using the <code>client_credentials</code> grant type with client assertion
 * authentication (RFC 7521 and RFC 7523).
 *
 * <p>
 * This formatter is used when authenticating to an OAuth provider using a client assertion (JWT)
 * instead of a client secret. The assertion is typically a self-signed JWT that proves the client's
 * identity. This method is more secure than client secrets as the assertion is short-lived and the
 * private key used to sign it never leaves the client.
 * </p>
 *
 * <p>
 * The formatter creates HTTP requests with:
 * <ul>
 *   <li>Content-Type: application/x-www-form-urlencoded</li>
 *   <li>grant_type=client_credentials</li>
 *   <li>client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer</li>
 *   <li>client_assertion=&lt;JWT assertion&gt;</li>
 *   <li>Optional client_id parameter</li>
 *   <li>Optional scope parameter</li>
 * </ul>
 * </p>
 *
 * @see ClientSecretRequestFormatter
 * @see <a href="https://datatracker.ietf.org/doc/html/rfc7521">RFC 7521: Assertion Framework for OAuth 2.0</a>
 * @see <a href="https://datatracker.ietf.org/doc/html/rfc7523">RFC 7523: JWT Profile for OAuth 2.0 Client Authentication</a>
 */
public class ClientAssertionRequestFormatter implements HttpRequestFormatter, Closeable {
    public static final String GRANT_TYPE = "client_credentials";
    private final String clientId;
    private final String scope;
    private final CloseableSupplier<String> assertionSupplier;

    /**
     * Creates a new {@code ClientAssertionRequestFormatter} instance.
     *
     * @param clientId          The OAuth client ID; may be {@code null} if the assertion contains the client ID
     * @param scope             The OAuth scope to request; may be {@code null} for default scope
     * @param assertionSupplier A closeable supplier that provides the JWT assertion string when invoked;
     *                          this formatter takes ownership and will close it when {@link #close()} is called
     */
    public ClientAssertionRequestFormatter(String clientId, String scope, CloseableSupplier<String> assertionSupplier) {
        this.clientId = clientId;
        this.scope = scope;
        this.assertionSupplier = assertionSupplier;
    }

    @Override
    public Map<String, String> formatHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put("Accept", "application/json");
        headers.put("Cache-Control", "no-cache");
        headers.put("Content-Type", "application/x-www-form-urlencoded");
        return headers;
    }

    @Override
    public String formatBody() {
        StringBuilder requestParameters = new StringBuilder();
        // client_assertion_type: Pre-encoded URN (colons encoded as %3A per RFC 6749)
        requestParameters.append("client_assertion_type=").append("urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer");
        // client_assertion: JWT is Base64url-encoded (URL-safe per RFC 7515); encoding is a no-op but
        // applied defensively for consistency with JwtBearerRequestFormatter
        requestParameters.append("&client_assertion=").append(URLEncoder.encode(assertionSupplier.get(), StandardCharsets.UTF_8));
        // grant_type: Encoded per RFC 6749 Section 4.4.2
        requestParameters.append("&grant_type=").append(URLEncoder.encode(GRANT_TYPE, StandardCharsets.UTF_8));
        if (!Utils.isBlank(clientId)) {
            // client_id: Encoded for consistency with other parameters per RFC 6749
            requestParameters.append("&client_id=").append(URLEncoder.encode(clientId, StandardCharsets.UTF_8));
        }
        if (!Utils.isBlank(scope)) {
            // scope: May contain special characters, encoded per RFC 6749 Section 3.3
            String encodedScope = URLEncoder.encode(scope.trim(), StandardCharsets.UTF_8);
            requestParameters.append("&scope=").append(encodedScope);
        }

        return requestParameters.toString();
    }

    @Override
    public void close() throws IOException {
        Utils.closeQuietly(assertionSupplier, "JWT assertion supplier");
    }
}
