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

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.kafka.common.utils.Utils;

public class PrivateKeyRequestFormatter implements HttpRequestFormatter {

    private final String scope;
    private final Supplier<String> assertionSupplier;
    private final String clientId;

    private static final String CLIENT_ASSERTION_TYPE = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";
    private static final String GRANT_TYPE = "client_credentials";

    public PrivateKeyRequestFormatter(String scope, Supplier<String> assertionSupplier, Optional<String> clientId) {
        this.scope = scope;
        this.assertionSupplier = assertionSupplier;
        this.clientId = clientId.orElse(null);
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
        String assertion = assertionSupplier.get();
        StringBuilder requestParameters = new StringBuilder();
        requestParameters.append("grant_type=")
                .append(URLEncoder.encode(GRANT_TYPE, StandardCharsets.UTF_8));
        requestParameters.append("&client_assertion=")
                .append(URLEncoder.encode(assertion, StandardCharsets.UTF_8));
        requestParameters.append("&client_assertion_type=")
                .append(URLEncoder.encode(CLIENT_ASSERTION_TYPE, StandardCharsets.UTF_8));

        if (clientId != null) {
            requestParameters.append("&client_id=")
                    .append(URLEncoder.encode(clientId, StandardCharsets.UTF_8));
        }

        if (!Utils.isBlank(scope))
            requestParameters.append("&scope=")
                    .append(URLEncoder.encode(scope.trim(), StandardCharsets.UTF_8));

        return requestParameters.toString();
    }

    public String getGrantType() {
        return "client_credentials";
    }

    public String getClientAssertionType() {
        return "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";
    }

}
