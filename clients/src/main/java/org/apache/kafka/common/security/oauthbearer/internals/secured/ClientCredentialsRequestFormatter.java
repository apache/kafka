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

public class ClientCredentialsRequestFormatter implements HttpRequestFormatter {

    public static final String GRANT_TYPE = "client_credentials";

    private final String clientId;

    private final String clientSecret;

    private final String scope;

    public ClientCredentialsRequestFormatter(String clientId, String clientSecret, String scope, boolean urlencode) {
        if (Utils.isBlank(clientId))
            throw new ConfigException(SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID, clientId);

        if (Utils.isBlank(clientSecret))
            throw new ConfigException(SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET, clientId);

        clientId = clientId.trim();
        clientSecret = clientSecret.trim();
        scope = Utils.isBlank(scope) ? null : scope.trim();

        // according to RFC-6749 clientId & clientSecret must be urlencoded, see https://tools.ietf.org/html/rfc6749#section-2.3.1
        if (urlencode) {
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
