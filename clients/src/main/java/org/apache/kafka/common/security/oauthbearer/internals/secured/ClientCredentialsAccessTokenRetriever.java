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

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.utils.Utils;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_HEADER_URLENCODE;

/**
 * <code>HttpAccessTokenRetriever</code> is an {@link AccessTokenRetriever} that will
 * communicate with an OAuth/OIDC provider directly via HTTP to post client credentials
 * ({@link #CLIENT_ID_CONFIG}/{@link #CLIENT_SECRET_CONFIG})
 * to a publicized token endpoint URL
 * ({@link SaslConfigs#SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL}).
 *
 * @see AccessTokenRetriever
 * @see SaslConfigs#SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL
 */

public class ClientCredentialsAccessTokenRetriever extends HttpAccessTokenRetriever {

    public static final String GRANT_TYPE = "client_credentials";

    private static final String CLIENT_ID_CONFIG = "clientId";
    private static final String CLIENT_SECRET_CONFIG = "clientSecret";
    private static final String SCOPE_CONFIG = "scope";

    private String clientId;

    private String clientSecret;

    private String scope;

    private boolean urlencodeHeader;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        super.configure(configs, saslMechanism, jaasConfigEntries);

        JaasOptionsUtils jou = new JaasOptionsUtils(saslMechanism, jaasConfigEntries);
        ConfigurationUtils cu = new ConfigurationUtils(configs, saslMechanism);
        clientId = jou.validateString(CLIENT_ID_CONFIG);
        clientSecret = jou.validateString(CLIENT_SECRET_CONFIG);
        scope = jou.validateString(SCOPE_CONFIG, false);
        urlencodeHeader = validateUrlencodeHeader(cu);
    }

    @Override
    protected byte[] formatRequestBody() {
        String body = formatRequestBody(scope);
        return body.getBytes(StandardCharsets.UTF_8);
    }

    static String formatRequestBody(String scope) {
        StringBuilder requestParameters = new StringBuilder();
        requestParameters.append("grant_type=").append(GRANT_TYPE);

        if (scope != null && !scope.trim().isEmpty()) {
            scope = scope.trim();
            String encodedScope = URLEncoder.encode(scope, StandardCharsets.UTF_8);
            requestParameters.append("&scope=").append(encodedScope);
        }

        return requestParameters.toString();
    }

    @Override
    protected Map<String, String> formatRequestHeaders(int contentLength) {
        Map<String, String> headers = new HashMap<>(super.formatRequestHeaders(contentLength));
        headers.put("Authorization", formatAuthorizationHeader(clientId, clientSecret, urlencodeHeader));
        return headers;
    }

    static String formatAuthorizationHeader(String clientId, String clientSecret, boolean urlencodeHeader) {
        clientId = sanitizeString("the token endpoint request client ID parameter", clientId);
        clientSecret = sanitizeString("the token endpoint request client secret parameter", clientSecret);

        // according to RFC-6749 clientId & clientSecret must be urlencoded, see https://tools.ietf.org/html/rfc6749#section-2.3.1
        if (urlencodeHeader) {
            clientId = URLEncoder.encode(clientId, StandardCharsets.UTF_8);
            clientSecret = URLEncoder.encode(clientSecret, StandardCharsets.UTF_8);
        }

        String s = String.format("%s:%s", clientId, clientSecret);
        // Per RFC-7617, we need to use the *non-URL safe* base64 encoder. See KAFKA-14496.
        String encoded = Base64.getEncoder().encodeToString(Utils.utf8(s));
        return String.format("Basic %s", encoded);
    }

    private static String sanitizeString(String name, String value) {
        if (value == null)
            throw new IllegalArgumentException(String.format("The value for %s must be non-null", name));

        if (value.isEmpty())
            throw new IllegalArgumentException(String.format("The value for %s must be non-empty", name));

        value = value.trim();

        if (value.isEmpty())
            throw new IllegalArgumentException(String.format("The value for %s must not contain only whitespace", name));

        return value;
    }

    /**
     * In some cases, the incoming {@link Map} doesn't contain a value for
     * {@link SaslConfigs#SASL_OAUTHBEARER_HEADER_URLENCODE}. Returning {@code null} from {@link Map#get(Object)}
     * will cause a {@link NullPointerException} when it is later unboxed.
     *
     * <p/>
     *
     * This utility method ensures that we have a non-{@code null} value to use in the
     * {@link ClientCredentialsAccessTokenRetriever} constructor.
     */
    public static boolean validateUrlencodeHeader(ConfigurationUtils configurationUtils) {
        Boolean urlencodeHeader = configurationUtils.validateBoolean(SASL_OAUTHBEARER_HEADER_URLENCODE, false);
        return Objects.requireNonNullElse(urlencodeHeader, DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE);
    }
}
