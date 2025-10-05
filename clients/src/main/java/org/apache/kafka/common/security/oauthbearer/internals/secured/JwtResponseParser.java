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

import org.apache.kafka.common.security.oauthbearer.JwtRetrieverException;
import org.apache.kafka.common.utils.Utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class JwtResponseParser {

    private static final String[] JSON_PATHS = new String[] {"/access_token", "/id_token"};
    private static final int MAX_RESPONSE_BODY_LENGTH = 1000;

    public String parseJwt(String responseBody) throws JwtRetrieverException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode;

        try {
            rootNode = mapper.readTree(responseBody);
        } catch (IOException e) {
            throw new JwtRetrieverException(e);
        }

        for (String jsonPath : JSON_PATHS) {
            JsonNode node = rootNode.at(jsonPath);

            if (node != null && !node.isMissingNode()) {
                String value = node.textValue();

                if (!Utils.isBlank(value)) {
                    return value.trim();
                }
            }
        }

        // Only grab the first N characters so that if the response body is huge, we don't blow up.
        String snippet = responseBody;

        if (snippet.length() > MAX_RESPONSE_BODY_LENGTH) {
            int actualLength = responseBody.length();
            String s = responseBody.substring(0, MAX_RESPONSE_BODY_LENGTH);
            snippet = String.format("%s (trimmed to first %d characters out of %d total)", s, MAX_RESPONSE_BODY_LENGTH, actualLength);
        }

        throw new JwtRetrieverException(String.format("The token endpoint response did not contain a valid JWT. Response: (%s)", snippet));
    }
}
