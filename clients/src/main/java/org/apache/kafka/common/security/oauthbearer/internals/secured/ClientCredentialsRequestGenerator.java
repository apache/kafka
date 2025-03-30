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

import org.apache.kafka.common.utils.Utils;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ClientCredentialsRequestGenerator implements HttpRequestGenerator {

    public static final String GRANT_TYPE = "client_credentials";

    private final URI tokenEndpoint;
    private final String clientId;
    private final String clientSecret;
    private final Optional<String> scope;

    public ClientCredentialsRequestGenerator(URI tokenEndpoint,
                                             String clientId,
                                             String clientSecret,
                                             String scope,
                                             boolean urlencodeHeader) {
        this.tokenEndpoint = tokenEndpoint;

        // according to RFC-6749 clientId & clientSecret must be urlencoded, see https://tools.ietf.org/html/rfc6749#section-2.3.1
        this.clientId = urlencodeHeader ? URLEncoder.encode(clientId, StandardCharsets.UTF_8) : clientId;
        this.clientSecret = urlencodeHeader ? URLEncoder.encode(clientSecret, StandardCharsets.UTF_8) : clientSecret;
        this.scope = Utils.isBlank(scope) ? Optional.empty() : Optional.of(URLEncoder.encode(scope, StandardCharsets.UTF_8));
    }

    @Override
    public String generateBody() {
        StringBuilder requestParameters = new StringBuilder();
        requestParameters.append("grant_type=").append(GRANT_TYPE);
        scope.ifPresent(s -> requestParameters.append("&scope=").append(s));
        return requestParameters.toString();
    }

    @Override
    public Map<String, String> generateHeaders() {
        String s = String.format("%s:%s", clientId, clientSecret);
        // Per RFC-7617, we need to use the *non-URL safe* base64 encoder. See KAFKA-14496.
        String encoded = Base64.getEncoder().encodeToString(Utils.utf8(s));
        String header = String.format("Basic %s", encoded);

        Map<String, String> headers = new HashMap<>();
        headers.put("Accept", "application/json");
        headers.put("Authorization", header);
        headers.put("Cache-Control", "no-cache");
        headers.put("Content-Type", "application/x-www-form-urlencoded");
        return headers;
    }

    @Override
    public HttpRequest generateRequest() {
        HttpRequest.BodyPublisher bodyPublisher = HttpRequest.BodyPublishers.ofString(generateBody());

        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(tokenEndpoint)
            .POST(bodyPublisher);

        for (Map.Entry<String, String> header : generateHeaders().entrySet())
            builder = builder.header(header.getKey(), header.getValue());

        return builder.build();
    }
}
