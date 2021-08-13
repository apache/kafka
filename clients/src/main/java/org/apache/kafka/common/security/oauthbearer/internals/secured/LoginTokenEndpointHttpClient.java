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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.kafka.common.security.oauthbearer.internals.secured.httpclient.Retry;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoginTokenEndpointHttpClient {

    private static final Logger log = LoggerFactory.getLogger(LoginTokenEndpointHttpClient.class);

    private final String clientId;

    private final String clientSecret;

    private final String scope;

    private final String tokenEndpointUri;

    private final long connectTimeoutMs;

    private final long readTimeoutMs;

    private final Retry<String> retry;

    public LoginTokenEndpointHttpClient(String clientId,
        String clientSecret,
        String scope,
        String tokenEndpointUri,
        long connectTimeoutMs,
        long readTimeoutMs,
        Retry<String> retry) {
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.scope = scope;
        this.tokenEndpointUri = tokenEndpointUri;
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
        this.retry = retry;
    }

    public String getAccessToken() throws IOException {
        String authorizationHeader = formatAuthorizationHeader(clientId, clientSecret);
        log.debug("getAccessToken - authorizationHeader: {}", authorizationHeader);

        String requestBody = formatRequestBody(scope);
        log.debug("getAccessToken - requestBody: {}", requestBody);

        String responseBody = retry.execute(() -> {
            HttpURLConnection con = writeRequest(tokenEndpointUri, authorizationHeader, requestBody);
            return readResponse(con);
        });

        log.debug("getAccessToken - responseBody: {}", responseBody);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readTree(responseBody);
        JsonNode accessTokenNode = rootNode.at("/access_token");

        if (accessTokenNode == null)
            throw new IllegalStateException("The token endpoint response did not contain an access_token value");

        return accessTokenNode.textValue();
    }

    private HttpURLConnection writeRequest(String uri,
        String authorizationHeader,
        String requestBody)
        throws IOException {
        URL url = new URL(uri);
        HttpURLConnection con = (HttpURLConnection)url.openConnection();
        con.setRequestMethod("POST");
        con.setRequestProperty("Accept", "application/json");
        con.setRequestProperty("Authorization", authorizationHeader);
        con.setRequestProperty("Cache-Control", "no-cache");
        con.setRequestProperty("Content-Length", String.valueOf(requestBody.length()));
        con.setUseCaches(false);
        con.setDoOutput(true);
        con.setConnectTimeout((int)connectTimeoutMs);
        con.setReadTimeout((int)readTimeoutMs);

        try {
            log.debug("writeRequest - preparing to connect to {}", uri);
            con.connect();
        } catch (ConnectException e) {
            throw new IOException("Failed to connect to: " + uri, e);
        }

        try (OutputStream os = con.getOutputStream()) {
            ByteArrayInputStream is = new ByteArrayInputStream(requestBody.getBytes(
                StandardCharsets.UTF_8));
            log.debug("writeRequest - preparing to write request body to {}", uri);
            copy(is, os);
        }

        return con;
    }

    static String readResponse(HttpURLConnection con) throws IOException {
        int responseCode = con.getResponseCode();
        log.warn("readResponse - responseCode: {}", responseCode);

        if (responseCode == HttpURLConnection.HTTP_OK || responseCode == HttpURLConnection.HTTP_CREATED) {
            byte[] responseBody;

            try (InputStream is = con.getInputStream()) {
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                copy(is, os);
                responseBody = os.toByteArray();
            }

            String body = new String(responseBody, StandardCharsets.UTF_8);

            if (body.isEmpty())
                throw new IllegalStateException("The token endpoint response was unexpectedly empty");

            return body;
        } else {
            throw new IllegalStateException(String.format("The unexpected response code %s was encountered reading the token endpoint response", responseCode));
        }
    }

    static void copy(InputStream is, OutputStream os) throws IOException {
        byte[] buf = new byte[4096];
        int b;

        while ((b = is.read(buf)) != -1)
            os.write(buf, 0, b);
    }

    static String formatAuthorizationHeader(String clientId, String clientSecret) {
        String s = String.format("%s:%s", clientId, clientSecret);
        String encoded = Base64.getUrlEncoder().encodeToString(Utils.utf8(s));
        return String.format("Basic %s", encoded);
    }

    static String formatRequestBody(String scope) {
        try {
            StringBuilder requestParameters = new StringBuilder();
            requestParameters.append("grant_type=client_credentials");

            if (scope != null) {
                String encodedScope = URLEncoder.encode(scope, StandardCharsets.UTF_8.name());
                requestParameters.append("&scope=").append(encodedScope);
            }

            return requestParameters.toString();
        } catch (UnsupportedEncodingException e) {
            // The world has gone crazy!
            throw new IllegalStateException(String.format("Encoding %s not supported", StandardCharsets.UTF_8.name()));
        }
    }

}
