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
import org.apache.kafka.common.config.SaslConfigs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.net.ssl.SSLSocketFactory;
import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_HEADER_URLENCODE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;

/**
 * <code>HttpAccessTokenRetriever</code> is an {@link AccessTokenRetriever} that will
 * communicate with an OAuth/OIDC provider directly via HTTP.
 *
 * @see AccessTokenRetriever
 */

public abstract class HttpAccessTokenRetriever implements AccessTokenRetriever {

    private static final Logger log = LoggerFactory.getLogger(HttpAccessTokenRetriever.class);

    private static final Set<Integer> UNRETRYABLE_HTTP_CODES;

    private static final int MAX_RESPONSE_BODY_LENGTH = 1000;

    private String tokenEndpointUrl;

    private long retryBackoffMs;

    private long retryBackoffMaxMs;

    private OAuthBearerHttpClient client;

    static {
        // This does not have to be an exhaustive list. There are other HTTP codes that
        // are defined in different RFCs (e.g. https://datatracker.ietf.org/doc/html/rfc6585)
        // that we won't worry about yet. The worst case if a status code is missing from
        // this set is that the request will be retried.
        UNRETRYABLE_HTTP_CODES = new HashSet<>();
        UNRETRYABLE_HTTP_CODES.add(HttpURLConnection.HTTP_BAD_REQUEST);
        UNRETRYABLE_HTTP_CODES.add(HttpURLConnection.HTTP_UNAUTHORIZED);
        UNRETRYABLE_HTTP_CODES.add(HttpURLConnection.HTTP_PAYMENT_REQUIRED);
        UNRETRYABLE_HTTP_CODES.add(HttpURLConnection.HTTP_FORBIDDEN);
        UNRETRYABLE_HTTP_CODES.add(HttpURLConnection.HTTP_NOT_FOUND);
        UNRETRYABLE_HTTP_CODES.add(HttpURLConnection.HTTP_BAD_METHOD);
        UNRETRYABLE_HTTP_CODES.add(HttpURLConnection.HTTP_NOT_ACCEPTABLE);
        UNRETRYABLE_HTTP_CODES.add(HttpURLConnection.HTTP_PROXY_AUTH);
        UNRETRYABLE_HTTP_CODES.add(HttpURLConnection.HTTP_CONFLICT);
        UNRETRYABLE_HTTP_CODES.add(HttpURLConnection.HTTP_GONE);
        UNRETRYABLE_HTTP_CODES.add(HttpURLConnection.HTTP_LENGTH_REQUIRED);
        UNRETRYABLE_HTTP_CODES.add(HttpURLConnection.HTTP_PRECON_FAILED);
        UNRETRYABLE_HTTP_CODES.add(HttpURLConnection.HTTP_ENTITY_TOO_LARGE);
        UNRETRYABLE_HTTP_CODES.add(HttpURLConnection.HTTP_REQ_TOO_LONG);
        UNRETRYABLE_HTTP_CODES.add(HttpURLConnection.HTTP_UNSUPPORTED_TYPE);
        UNRETRYABLE_HTTP_CODES.add(HttpURLConnection.HTTP_NOT_IMPLEMENTED);
        UNRETRYABLE_HTTP_CODES.add(HttpURLConnection.HTTP_VERSION);
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        JaasOptionsUtils jou = new JaasOptionsUtils(saslMechanism, jaasConfigEntries);
        ConfigurationUtils cu = new ConfigurationUtils(saslMechanism, configs);

        URL url = cu.validateUrl(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);
        tokenEndpointUrl = url.toString();
        retryBackoffMs = cu.validateLong(SASL_LOGIN_RETRY_BACKOFF_MS);
        retryBackoffMaxMs = cu.validateLong(SASL_LOGIN_RETRY_BACKOFF_MAX_MS);

        Optional<SSLSocketFactory> sslSocketFactory = jou.maybeCreateSSLSocketFactory(url);
        Optional<Integer> connectTimeoutMs = Optional.ofNullable(cu.validateInteger(SASL_LOGIN_CONNECT_TIMEOUT_MS, false));
        Optional<Integer> readTimeoutMs = Optional.ofNullable(cu.validateInteger(SASL_LOGIN_READ_TIMEOUT_MS, false));

        client = new OAuthBearerHttpClient(tokenEndpointUrl, sslSocketFactory, connectTimeoutMs, readTimeoutMs);
    }

    protected abstract byte[] formatRequestBody();

    protected Map<String, String> formatRequestHeaders(int contentLength) {
        Map<String, String> headers = new HashMap<>();
        headers.put("Accept", "application/json");
        headers.put("Cache-Control", "no-cache");
        headers.put("Content-Length", String.valueOf(contentLength));
        return headers;
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
        byte[] requestBody = formatRequestBody();
        Map<String, String> headers = formatRequestHeaders(requestBody.length);

        Retry<String> retry = new Retry<>(retryBackoffMs, retryBackoffMaxMs);
        String responseBody = null;

        try {
            responseBody = retry.execute(() -> {
                HttpURLConnection con = null;

                try {
                    con = client.connect(tokenEndpointUrl, "POST", headers);
                    OAuthBearerHttpClient.HttpResponse httpResponse = client.post(headers, requestBody);
                    return handleOutput(tokenEndpointUrl, httpResponse);
                } catch (IOException e) {
                    throw new ExecutionException(e);
                } finally {
                    if (con != null) {
                        con.disconnect();
                    }
                }
            });
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException)
                throw (IOException) e.getCause();
            else
                throw new KafkaException(e.getCause());
        }

        return parseAccessToken(responseBody);
    }

    static String handleOutput(String url, OAuthBearerHttpClient.HttpResponse httpResponse) throws IOException {
        int responseCode = httpResponse.responseCode;
        Optional<String> responseBodyOpt = httpResponse.responseBody.map(b -> new String(b, StandardCharsets.UTF_8));
        String errorMessage = formatErrorMessage(
            httpResponse.errorResponseBody.map(b -> new String(b, StandardCharsets.UTF_8))
        );

        if (responseCode == HttpURLConnection.HTTP_OK || responseCode == HttpURLConnection.HTTP_CREATED) {
            if (responseBodyOpt.isPresent())
                return responseBodyOpt.get();

            throw new IOException(
                String.format(
                    "The token endpoint response was unexpectedly empty despite response code %d from %s and error message %s",
                    responseCode,
                    url,
                    errorMessage
                )
            );
        }

        log.warn("handleOutput - error response code: {}, error response body: {}", responseCode, errorMessage);

        if (UNRETRYABLE_HTTP_CODES.contains(responseCode)) {
            // We know that this is a non-transient error, so let's not keep retrying the
            // request unnecessarily.
            throw new UnretryableException(
                new IOException(
                    String.format(
                        "The response code %s and error response %s was encountered reading the token endpoint response; will not attempt further retries",
                        responseCode,
                        errorMessage
                    )
                )
            );
        } else {
            // We don't know if this is a transient (retryable) error or not, so let's assume
            // it is.
            throw new IOException(
                String.format(
                    "The unexpected response code %s and error message %s was encountered reading the token endpoint response",
                    responseCode,
                    errorMessage
                )
            );
        }
    }

    static String formatErrorMessage(Optional<String> errorResponseBodyOpt) {
        // See https://www.ietf.org/rfc/rfc6749.txt, section 5.2 for the format
        // of this error message.
        if (errorResponseBodyOpt.isEmpty()) {
            return "{}";
        }
        String errorResponseBody = errorResponseBodyOpt.get();
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode rootNode = mapper.readTree(errorResponseBody);
            if (!rootNode.at("/error").isMissingNode()) {
                return String.format("{%s - %s}", rootNode.at("/error"), rootNode.at("/error_description"));
            } else if (!rootNode.at("/errorCode").isMissingNode()) {
                return String.format("{%s - %s}", rootNode.at("/errorCode"), rootNode.at("/errorSummary"));
            } else {
                return errorResponseBody;
            }
        } catch (Exception e) {
            log.warn("Error parsing error response", e);
        }
        return String.format("{%s}", errorResponseBody);
    }

    static String parseAccessToken(String responseBody) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readTree(responseBody);
        JsonNode accessTokenNode = rootNode.at("/access_token");

        if (accessTokenNode == null) {
            // Only grab the first N characters so that if the response body is huge, we don't
            // blow up.
            String snippet = responseBody;

            if (snippet.length() > MAX_RESPONSE_BODY_LENGTH) {
                int actualLength = responseBody.length();
                String s = responseBody.substring(0, MAX_RESPONSE_BODY_LENGTH);
                snippet = String.format("%s (trimmed to first %d characters out of %d total)", s, MAX_RESPONSE_BODY_LENGTH, actualLength);
            }

            throw new IOException(String.format("The token endpoint response did not contain an access_token value. Response: (%s)", snippet));
        }

        String name = "the token endpoint response's access_token JSON attribute";
        String value = accessTokenNode.textValue();

        if (value == null)
            throw new IllegalArgumentException(String.format("The value for %s must be non-null", name));

        if (value.isEmpty())
            throw new IllegalArgumentException(String.format("The value for %s must be non-empty", name));

        value = value.trim();

        if (value.isEmpty())
            throw new IllegalArgumentException(String.format("The value for %s must not contain only whitespace", name));

        return value;
    }

    /**
     * In some cases, the incoming {@link Map} doesn't contain a value for
     * {@link SaslConfigs#SASL_OAUTHBEARER_HEADER_URLENCODE}. Returning {@code null} from {@link Map#get(Object)}
     * will cause a {@link NullPointerException} when it is later unboxed.
     *
     * <p/>
     *
     * This utility method ensures that we have a non-{@code null} value to use in the
     * {@link HttpAccessTokenRetriever} constructor.
     */
    public static boolean validateUrlencodeHeader(ConfigurationUtils configurationUtils) {
        Boolean urlencodeHeader = configurationUtils.validateBoolean(SASL_OAUTHBEARER_HEADER_URLENCODE, false);

        if (urlencodeHeader != null)
            return urlencodeHeader;
        else
            return DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE;
    }

}
