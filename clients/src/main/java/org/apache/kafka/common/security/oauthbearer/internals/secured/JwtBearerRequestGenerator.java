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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.oauthbearer.AssertionCreator;
import org.apache.kafka.common.security.oauthbearer.AssertionJwtTemplate;
import org.apache.kafka.common.security.oauthbearer.JwtRetrieverException;
import org.apache.kafka.common.utils.Utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class JwtBearerRequestGenerator implements HttpRequestGenerator {

    public static final String GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer";

    private final URL tokenEndpoint;
    private final Optional<String> scope;
    private final AssertionCreator assertionCreator;
    private final AssertionJwtTemplate assertionJwtTemplate;

    public JwtBearerRequestGenerator(URL tokenEndpoint,
                                     Optional<String> scope,
                                     AssertionCreator assertionCreator,
                                     AssertionJwtTemplate assertionJwtTemplate) {
        this.tokenEndpoint = tokenEndpoint;
        this.scope = scope;
        this.assertionCreator = assertionCreator;
        this.assertionJwtTemplate = assertionJwtTemplate;
    }

    @Override
    public String generateBody() {
        String assertion;

        try {
            assertion = assertionCreator.create(assertionJwtTemplate);
        } catch (Exception e) {
            throw new JwtRetrieverException("Error signing OAuth assertion with private key", e);
        }

        StringBuilder requestParameters = new StringBuilder();
        requestParameters.append("grant_type=").append(URLEncoder.encode(GRANT_TYPE, StandardCharsets.UTF_8));
        requestParameters.append("&assertion=").append(URLEncoder.encode(assertion, StandardCharsets.UTF_8));
        scope.ifPresent(s -> requestParameters.append("&scope=").append(URLEncoder.encode(s, StandardCharsets.UTF_8)));
        return requestParameters.toString();
    }

    @Override
    public Map<String, String> generateHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put("Accept", "application/json");
        headers.put("Cache-Control", "no-cache");
        headers.put("Content-Type", "application/x-www-form-urlencoded");
        return headers;
    }

    @Override
    public HttpRequest generateRequest() {
        HttpRequest.BodyPublisher bodyPublisher = HttpRequest.BodyPublishers.ofString(generateBody());

        URI uri;

        try {
            uri = tokenEndpoint.toURI();
        } catch (URISyntaxException e) {
            throw new KafkaException("An error occurred formatting the OAuth token retrieval request", e);
        }

        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(uri)
            .POST(bodyPublisher);

        for (Map.Entry<String, String> header : generateHeaders().entrySet())
            builder = builder.header(header.getKey(), header.getValue());

        return builder.build();
    }

    @Override
    public void close() {
        Utils.closeQuietly(assertionCreator, "assertionCreator");
        Utils.closeQuietly(assertionJwtTemplate, "assertionJwtTemplate");
    }
}
