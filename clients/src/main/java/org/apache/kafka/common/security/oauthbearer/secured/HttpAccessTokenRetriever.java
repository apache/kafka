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
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <code>HttpAccessTokenRetriever</code> is an {@link AccessTokenRetriever} that will
 * communicate with an OAuth/OIDC provider directly via HTTP to post client credentials
 * ({@link LoginCallbackHandlerConfiguration#CLIENT_ID_CONFIG}/{@link LoginCallbackHandlerConfiguration#CLIENT_SECRET_CONFIG})
 * to a publicized token endpoint URL ({@link LoginCallbackHandlerConfiguration#TOKEN_ENDPOINT_URI_CONFIG}).
 *
 * @see AccessTokenRetriever
 * @see LoginCallbackHandlerConfiguration#CLIENT_ID_CONFIG
 * @see LoginCallbackHandlerConfiguration#CLIENT_SECRET_CONFIG
 * @see LoginCallbackHandlerConfiguration#TOKEN_ENDPOINT_URI_CONFIG
 */

public class HttpAccessTokenRetriever implements AccessTokenRetriever {

    private static final Logger log = LoggerFactory.getLogger(HttpAccessTokenRetriever.class);

    public static final String AUTHORIZATION_HEADER = "Authorization";

    private final String clientId;

    private final String clientSecret;

    private final String scope;

    private final SSLSocketFactory sslSocketFactory;

    private final String tokenEndpointUri;

    private final int loginAttempts;

    private final long loginRetryWaitMs;

    private final long loginMaxWaitMs;

    private final Integer loginConnectTimeoutMs;

    private final Integer loginReadTimeoutMs;

    public HttpAccessTokenRetriever(String clientId,
        String clientSecret,
        String scope,
        SSLSocketFactory sslSocketFactory,
        String tokenEndpointUri,
        int loginAttempts,
        long loginRetryWaitMs,
        long loginMaxWaitMs,
        Integer loginConnectTimeoutMs,
        Integer loginReadTimeoutMs) {
        this.clientId = Objects.requireNonNull(clientId);
        this.clientSecret = Objects.requireNonNull(clientSecret);
        this.scope = scope;
        this.sslSocketFactory = sslSocketFactory;
        this.tokenEndpointUri = Objects.requireNonNull(tokenEndpointUri);
        this.loginAttempts = loginAttempts;
        this.loginRetryWaitMs = loginRetryWaitMs;
        this.loginMaxWaitMs = loginMaxWaitMs;
        this.loginConnectTimeoutMs = loginConnectTimeoutMs;
        this.loginReadTimeoutMs = loginReadTimeoutMs;
    }

    /**
     * Retrieves a JWT access token in its serialized three-part form. The implementation
     * is free to determine how it should be retrieved but should not perform validation
     * on the result.
     *
     * <b>Note</b>: This is a blocking function and callers should be aware that the
     * implementation communicates over a network. The facility in the
     * {@link javax.security.auth.spi.LoginModule} from which this is ultimately called
     * does not provide an asynchronous approach.
     *
     * @return Non-<code>null</code> JWT access token string
     *
     * @throws IOException Thrown on errors related to IO during retrieval
     */

    @Override
    public String retrieve() throws IOException {
        String authorizationHeader = formatAuthorizationHeader(clientId, clientSecret);
        String requestBody = formatRequestBody(scope);

        Retry<String> retry = new Retry<>(Time.SYSTEM,
            loginAttempts,
            loginRetryWaitMs,
            loginMaxWaitMs);

        HttpURLConnection con = (HttpURLConnection) new URL(tokenEndpointUri).openConnection();

        if (sslSocketFactory != null && con instanceof HttpsURLConnection)
            ((HttpsURLConnection) con).setSSLSocketFactory(sslSocketFactory);

        Map<String, String> headers = Collections.singletonMap(AUTHORIZATION_HEADER, authorizationHeader);

        String responseBody = retry.execute(() -> post(con,
            headers,
            requestBody,
            loginConnectTimeoutMs,
            loginReadTimeoutMs));
        log.debug("retrieve - responseBody: {}", responseBody);

        return parseAccessToken(responseBody);
    }

    public static String post(HttpURLConnection con,
        Map<String, String> headers,
        String requestBody,
        Integer connectTimeoutMs,
        Integer readTimeoutMs)
        throws IOException {
        con.setRequestMethod("POST");
        con.setRequestProperty("Accept", "application/json");

        if (headers != null) {
            for (Map.Entry<String, String> header : headers.entrySet())
                con.setRequestProperty(header.getKey(), header.getValue());
        }

        con.setRequestProperty("Cache-Control", "no-cache");

        if (requestBody != null) {
            con.setRequestProperty("Content-Length", String.valueOf(requestBody.length()));
            con.setDoOutput(true);
        }

        con.setUseCaches(false);

        if (connectTimeoutMs != null)
            con.setConnectTimeout(connectTimeoutMs);

        if (readTimeoutMs != null)
            con.setReadTimeout(readTimeoutMs);

        try {
            log.debug("post - preparing to connect to {}", con.getURL());
            con.connect();
        } catch (ConnectException e) {
            throw new IOException("Failed to connect to: " + con.getURL(), e);
        }

        if (requestBody != null) {
            try (OutputStream os = con.getOutputStream()) {
                ByteArrayInputStream is = new ByteArrayInputStream(requestBody.getBytes(
                    StandardCharsets.UTF_8));
                log.debug("post - preparing to write request body to {}", con.getURL());
                copy(is, os);
            }
        }

        int responseCode = con.getResponseCode();
        log.debug("post - responseCode: {}", responseCode);

        if (responseCode == HttpURLConnection.HTTP_OK || responseCode == HttpURLConnection.HTTP_CREATED) {
            String responseBody;

            try (InputStream is = con.getInputStream()) {
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                log.debug("post - preparing to read request body to {}", con.getURL());
                copy(is, os);
                responseBody = os.toString(StandardCharsets.UTF_8.name());
            }

            if (responseBody.isEmpty())
                throw new IOException("The token endpoint response was unexpectedly empty");

            return responseBody;
        } else {
            throw new IOException(String.format("The unexpected response code %s was encountered reading the token endpoint response", responseCode));
        }
    }

    static void copy(InputStream is, OutputStream os) throws IOException {
        byte[] buf = new byte[4096];
        int b;

        while ((b = is.read(buf)) != -1)
            os.write(buf, 0, b);
    }

    static String parseAccessToken(String responseBody) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readTree(responseBody);
        JsonNode accessTokenNode = rootNode.at("/access_token");

        if (accessTokenNode == null)
            throw new IOException("The token endpoint response did not contain an access_token value");

        return sanitizeString("The token endpoint response access_token", accessTokenNode.textValue());
    }

    static String formatAuthorizationHeader(String clientId, String clientSecret) throws IOException {
        clientId = sanitizeString("The token endpoint request clientId", clientId);
        clientSecret = sanitizeString("The token endpoint request clientId", clientSecret);

        String s = String.format("%s:%s", clientId, clientSecret);
        String encoded = Base64.getUrlEncoder().encodeToString(Utils.utf8(s));
        return String.format("Basic %s", encoded);
    }

    static String formatRequestBody(String scope) throws IOException {
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