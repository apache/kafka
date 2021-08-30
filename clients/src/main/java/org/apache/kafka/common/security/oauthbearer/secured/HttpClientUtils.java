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

package org.apache.kafka.common.security.oauthbearer.secured;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.kafka.common.utils.Utils;

public class HttpClientUtils {

    public static final String AUTHORIZATION_HEADER = "Authorization";

    public static void copy(InputStream is, OutputStream os) throws IOException {
        byte[] buf = new byte[4096];
        int b;

        while ((b = is.read(buf)) != -1)
            os.write(buf, 0, b);
    }

    public static String parseAccessToken(String responseBody) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readTree(responseBody);
        JsonNode accessTokenNode = rootNode.at("/access_token");

        if (accessTokenNode == null)
            throw new IOException("The token endpoint response did not contain an access_token value");

        return sanitizeString("The token endpoint response access_token", accessTokenNode.textValue());
    }

    public static String formatAuthorizationHeader(String clientId, String clientSecret) throws IOException {
        clientId = sanitizeString("The token endpoint request clientId", clientId);
        clientSecret = sanitizeString("The token endpoint request clientId", clientSecret);

        String s = String.format("%s:%s", clientId, clientSecret);
        String encoded = Base64.getUrlEncoder().encodeToString(Utils.utf8(s));
        return String.format("Basic %s", encoded);
    }

    public static String formatRequestBody(String scope) throws IOException {
        try {
            StringBuilder requestParameters = new StringBuilder();
            requestParameters.append("grant_type=client_credentials");

            if (scope != null && !scope.trim().isEmpty()) {
                scope = scope.trim();
                String encodedScope = URLEncoder.encode(scope, StandardCharsets.UTF_8.name());
                requestParameters.append("&scope=").append(encodedScope);
            }

            return requestParameters.toString();
        } catch (UnsupportedEncodingException e) {
            // The world has gone crazy!
            throw new IOException(String.format("Encoding %s not supported", StandardCharsets.UTF_8.name()));
        }
    }

    private static String sanitizeString(String name, String value) throws IOException {
        if (value == null)
            throw new IOException(String.format("%s value must be non-null", name));

        if (value.isEmpty())
            throw new IOException(String.format("%s value must be non-empty", name));

        value = value.trim();

        if (value.isEmpty())
            throw new IOException(String.format("%s value must not contain only whitespace", name));

        return value;
    }

}
