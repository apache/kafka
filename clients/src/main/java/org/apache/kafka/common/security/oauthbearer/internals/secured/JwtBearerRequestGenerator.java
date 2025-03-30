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

import org.apache.kafka.common.security.oauthbearer.AssertionCreator;
import org.apache.kafka.common.security.oauthbearer.AssertionJwtTemplate;
import org.apache.kafka.common.security.oauthbearer.JwtRetrieverException;
import org.apache.kafka.common.utils.Utils;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class JwtBearerRequestGenerator implements HttpRequestGenerator {

    public static final String GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer";

    private final URI tokenEndpoint;
    private final AssertionCreator assertionCreator;
    private final AssertionJwtTemplate assertionJwtTemplate;

    public JwtBearerRequestGenerator(URI tokenEndpoint,
                                     AssertionCreator assertionCreator,
                                     AssertionJwtTemplate assertionJwtTemplate) {
        this.tokenEndpoint = tokenEndpoint;
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

        String encodedGrantType = URLEncoder.encode(GRANT_TYPE, StandardCharsets.UTF_8);
        String encodedAssertion = URLEncoder.encode(assertion, StandardCharsets.UTF_8);
        return String.format("grant_type=%s&assertion=%s", encodedGrantType, encodedAssertion);
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

        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(tokenEndpoint)
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
