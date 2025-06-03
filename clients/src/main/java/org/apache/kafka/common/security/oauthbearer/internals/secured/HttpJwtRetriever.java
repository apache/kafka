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
import org.apache.kafka.common.security.oauthbearer.JwtRetriever;
import org.apache.kafka.common.security.oauthbearer.JwtRetrieverException;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;

/**
 * <code>HttpJwtRetriever</code> is a {@link JwtRetriever} that will communicate with an OAuth/OIDC
 * provider directly via HTTP to post client credentials
 * ({@link OAuthBearerLoginCallbackHandler#CLIENT_ID_CONFIG}/{@link OAuthBearerLoginCallbackHandler#CLIENT_SECRET_CONFIG})
 * to a publicized token endpoint URL ({@link SaslConfigs#SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL}).
 */
public class HttpJwtRetriever implements JwtRetriever {

    private static final Logger log = LoggerFactory.getLogger(HttpJwtRetriever.class);

    private static final Set<Integer> UNRETRYABLE_HTTP_CODES;

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

    private final HttpRequestFormatter requestFormatter;

    private SSLSocketFactory sslSocketFactory;

    private URL tokenEndpointUrl;

    private long loginRetryBackoffMs;

    private long loginRetryBackoffMaxMs;

    private Integer loginConnectTimeoutMs;

    private Integer loginReadTimeoutMs;

    public HttpJwtRetriever(HttpRequestFormatter requestFormatter) {
        this.requestFormatter = Objects.requireNonNull(requestFormatter);
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        ConfigurationUtils cu = new ConfigurationUtils(configs, saslMechanism);
        JaasOptionsUtils jou = new JaasOptionsUtils(saslMechanism, jaasConfigEntries);

        tokenEndpointUrl = cu.validateUrl(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);

        if (jou.shouldCreateSSLSocketFactory(tokenEndpointUrl))
            sslSocketFactory = jou.createSSLSocketFactory();

        this.loginRetryBackoffMs = cu.validateLong(SASL_LOGIN_RETRY_BACKOFF_MS);
        this.loginRetryBackoffMaxMs = cu.validateLong(SASL_LOGIN_RETRY_BACKOFF_MAX_MS);
        this.loginConnectTimeoutMs = cu.validateInteger(SASL_LOGIN_CONNECT_TIMEOUT_MS, false);
        this.loginReadTimeoutMs = cu.validateInteger(SASL_LOGIN_READ_TIMEOUT_MS, false);
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
     * @throws JwtRetrieverException Thrown on errors related to IO, parsing, etc. during retrieval
     */
    public String retrieve() throws JwtRetrieverException {
        String requestBody = requestFormatter.formatBody();
        Retry<String> retry = new Retry<>(loginRetryBackoffMs, loginRetryBackoffMaxMs);
        Map<String, String> headers = requestFormatter.formatHeaders();

        String responseBody;

        try {
            responseBody = retry.execute(() -> {
                HttpURLConnection con = null;

                try {
                    con = (HttpURLConnection) tokenEndpointUrl.openConnection();

                    if (sslSocketFactory != null && con instanceof HttpsURLConnection)
                        ((HttpsURLConnection) con).setSSLSocketFactory(sslSocketFactory);

                    return post(con, headers, requestBody, loginConnectTimeoutMs, loginReadTimeoutMs);
                } catch (IOException e) {
                    throw new ExecutionException(e);
                } finally {
                    if (con != null)
                        con.disconnect();
                }
            });
        } catch (ExecutionException e) {
            if (e.getCause() instanceof JwtRetrieverException)
                throw (JwtRetrieverException) e.getCause();
            else
                throw new KafkaException(e.getCause());
        }

        JwtResponseParser responseParser = new JwtResponseParser();
        return responseParser.parseJwt(responseBody);
    }

    public static String post(HttpURLConnection con,
        Map<String, String> headers,
        String requestBody,
        Integer connectTimeoutMs,
        Integer readTimeoutMs)
        throws IOException, UnretryableException {
        handleInput(con, headers, requestBody, connectTimeoutMs, readTimeoutMs);
        return handleOutput(con);
    }

    private static void handleInput(HttpURLConnection con,
        Map<String, String> headers,
        String requestBody,
        Integer connectTimeoutMs,
        Integer readTimeoutMs)
        throws IOException, UnretryableException {
        log.debug("handleInput - starting post for {}", con.getURL());
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

        log.debug("handleInput - preparing to connect to {}", con.getURL());
        con.connect();

        if (requestBody != null) {
            try (OutputStream os = con.getOutputStream()) {
                ByteArrayInputStream is = new ByteArrayInputStream(requestBody.getBytes(StandardCharsets.UTF_8));
                log.debug("handleInput - preparing to write request body to {}", con.getURL());
                copy(is, os);
            }
        }
    }

    static String handleOutput(final HttpURLConnection con) throws IOException {
        int responseCode = con.getResponseCode();
        log.debug("handleOutput - responseCode: {}", responseCode);

        // NOTE: the contents of the response should not be logged so that we don't leak any
        // sensitive data.
        String responseBody = null;

        // NOTE: It is OK to log the error response body and/or its formatted version as
        // per the OAuth spec, it doesn't include sensitive information.
        // See https://www.ietf.org/rfc/rfc6749.txt, section 5.2
        String errorResponseBody = null;

        try (InputStream is = con.getInputStream()) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            log.debug("handleOutput - preparing to read response body from {}", con.getURL());
            copy(is, os);
            responseBody = os.toString(StandardCharsets.UTF_8);
        } catch (Exception e) {
            // there still can be useful error response from the servers, lets get it
            try (InputStream is = con.getErrorStream()) {
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                log.debug("handleOutput - preparing to read error response body from {}", con.getURL());
                copy(is, os);
                errorResponseBody = os.toString(StandardCharsets.UTF_8);
            } catch (Exception e2) {
                log.warn("handleOutput - error retrieving error information", e2);
            }
            log.warn("handleOutput - error retrieving data", e);
        }

        if (responseCode == HttpURLConnection.HTTP_OK || responseCode == HttpURLConnection.HTTP_CREATED) {
            log.debug("handleOutput - responseCode: {}, error response: {}", responseCode,
                errorResponseBody);

            if (responseBody == null || responseBody.isEmpty())
                throw new IOException(String.format("The token endpoint response was unexpectedly empty despite response code %d from %s and error message %s",
                    responseCode, con.getURL(), formatErrorMessage(errorResponseBody)));

            return responseBody;
        } else {
            log.warn("handleOutput - error response code: {}, error response body: {}", responseCode,
                formatErrorMessage(errorResponseBody));

            if (UNRETRYABLE_HTTP_CODES.contains(responseCode)) {
                // We know that this is a non-transient error, so let's not keep retrying the
                // request unnecessarily.
                throw new UnretryableException(new IOException(String.format("The response code %s and error response %s was encountered reading the token endpoint response; will not attempt further retries",
                    responseCode, formatErrorMessage(errorResponseBody))));
            } else {
                // We don't know if this is a transient (retryable) error or not, so let's assume
                // it is.
                throw new IOException(String.format("The unexpected response code %s and error message %s was encountered reading the token endpoint response",
                    responseCode, formatErrorMessage(errorResponseBody)));
            }
        }
    }

    static void copy(InputStream is, OutputStream os) throws IOException {
        byte[] buf = new byte[4096];
        int b;

        while ((b = is.read(buf)) != -1)
            os.write(buf, 0, b);
    }

    static String formatErrorMessage(String errorResponseBody) {
        // See https://www.ietf.org/rfc/rfc6749.txt, section 5.2 for the format
        // of this error message.
        if (errorResponseBody == null || errorResponseBody.trim().isEmpty()) {
            return "{}";
        }
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
}
