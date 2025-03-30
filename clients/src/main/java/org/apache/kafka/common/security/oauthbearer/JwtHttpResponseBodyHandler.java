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

import org.apache.kafka.common.utils.Utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.List;

public class JwtHttpResponseBodyHandler implements HttpResponse.BodyHandler<String> {

    public static final List<String> DEFAULT_JSON_PATHS = List.of("/id_token", "/access_token");

    private static final Logger log = LoggerFactory.getLogger(JwtHttpResponseBodyHandler.class);
    private static final int MAX_RESPONSE_BODY_LENGTH = 1000;

    private final List<String> jsonPaths;

    public JwtHttpResponseBodyHandler() {
        this(DEFAULT_JSON_PATHS);
    }

    public JwtHttpResponseBodyHandler(List<String> jsonPaths) {
        this.jsonPaths = Collections.unmodifiableList(jsonPaths);
    }

    @Override
    public HttpResponse.BodySubscriber<String> apply(HttpResponse.ResponseInfo responseInfo) {
        return HttpResponse.BodySubscribers.mapping(
            HttpResponse.BodyHandlers.ofString().apply(responseInfo),
            this::extractJwt
        );
    }

    public String extractJwt(String responseBody) throws JwtRetrieverException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode;

        try {
            rootNode = mapper.readTree(responseBody);
        } catch (Exception e) {
            throw new JwtRetrieverException("An unexpected error occurred parsing the JSON from the OAuth token retrieval response", e);
        }

        for (String jsonPath : jsonPaths) {
            JsonNode node = rootNode.at(jsonPath);

            if (node == null) {
                log.debug("The JSON path {} did not yield a node in the OAuth token retrieval response", jsonPath);
                continue;
            }

            String jwt = node.textValue();

            if (Utils.isBlank(jwt)) {
                log.debug("The JSON path {} yielded a node in the OAuth token retrieval response, but the value was null, blank, or whitespace", jsonPath);
                continue;
            }

            return jwt.trim();
        }

        // Only grab the first N characters so that if the response body is huge, we don't
        // blow up.
        String snippet = responseBody;

        if (snippet.length() > MAX_RESPONSE_BODY_LENGTH) {
            int actualLength = responseBody.length();
            String s = responseBody.substring(0, MAX_RESPONSE_BODY_LENGTH);
            snippet = String.format("%s (trimmed to first %d characters out of %d total)", s, MAX_RESPONSE_BODY_LENGTH, actualLength);
        }

        throw new JwtRetrieverException(String.format("The token endpoint response did not contain a JWT value. Response: (%s)", snippet));
    }
}