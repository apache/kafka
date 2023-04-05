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

package org.apache.kafka.connect.runtime.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.runtime.distributed.Crypto;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.runtime.rest.entities.ErrorMessage;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.runtime.rest.util.SSLUtils;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Client for outbound REST requests to other members of a Connect cluster
 * This class is thread-safe.
 */
public class RestClient {
    private static final Logger log = LoggerFactory.getLogger(RestClient.class);
    private static final ObjectMapper JSON_SERDE = new ObjectMapper();

    private final AbstractConfig config;

    public RestClient(AbstractConfig config) {
        this.config = config;
    }

    // VisibleForTesting
    HttpClient httpClient(SslContextFactory sslContextFactory) {
        return sslContextFactory != null ? new HttpClient(sslContextFactory) : new HttpClient();
    }

    /**
     * Sends HTTP request to remote REST server
     *
     * @param url             HTTP connection will be established with this url.
     * @param method          HTTP method ("GET", "POST", "PUT", etc.)
     * @param headers         HTTP headers from REST endpoint
     * @param requestBodyData Object to serialize as JSON and send in the request body.
     * @param responseFormat  Expected format of the response to the HTTP request.
     * @param <T>             The type of the deserialized response to the HTTP request.
     * @return The deserialized response to the HTTP request, or null if no data is expected.
     */
    public <T> HttpResponse<T> httpRequest(String url, String method, HttpHeaders headers, Object requestBodyData,
                                                  TypeReference<T> responseFormat) {
        return httpRequest(url, method, headers, requestBodyData, responseFormat, null, null);
    }

    /**
     * Sends HTTP request to remote REST server
     *
     * @param url                       HTTP connection will be established with this url.
     * @param method                    HTTP method ("GET", "POST", "PUT", etc.)
     * @param headers                   HTTP headers from REST endpoint
     * @param requestBodyData           Object to serialize as JSON and send in the request body.
     * @param responseFormat            Expected format of the response to the HTTP request.
     * @param <T>                       The type of the deserialized response to the HTTP request.
     * @param sessionKey                The key to sign the request with (intended for internal requests only);
     *                                  may be null if the request doesn't need to be signed
     * @param requestSignatureAlgorithm The algorithm to sign the request with (intended for internal requests only);
     *                                  may be null if the request doesn't need to be signed
     * @return The deserialized response to the HTTP request, or null if no data is expected.
     */
    public <T> HttpResponse<T> httpRequest(String url, String method, HttpHeaders headers, Object requestBodyData,
                                                  TypeReference<T> responseFormat,
                                                  SecretKey sessionKey, String requestSignatureAlgorithm) {
        // Only try to load SSL configs if we have to (see KAFKA-14816)
        SslContextFactory sslContextFactory = url.startsWith("https://")
                ? SSLUtils.createClientSideSslContextFactory(config)
                : null;
        HttpClient client = httpClient(sslContextFactory);
        client.setFollowRedirects(false);

        try {
            client.start();
        } catch (Exception e) {
            log.error("Failed to start RestClient: ", e);
            throw new ConnectRestException(Response.Status.INTERNAL_SERVER_ERROR, "Failed to start RestClient: " + e.getMessage(), e);
        }

        try {
            return httpRequest(client, url, method, headers, requestBodyData, responseFormat, sessionKey, requestSignatureAlgorithm);
        } finally {
            try {
                client.stop();
            } catch (Exception e) {
                log.error("Failed to stop HTTP client", e);
            }
        }
    }

    private <T> HttpResponse<T> httpRequest(HttpClient client, String url, String method,
                                           HttpHeaders headers, Object requestBodyData,
                                           TypeReference<T> responseFormat, SecretKey sessionKey,
                                           String requestSignatureAlgorithm) {
        try {
            String serializedBody = requestBodyData == null ? null : JSON_SERDE.writeValueAsString(requestBodyData);
            log.trace("Sending {} with input {} to {}", method, serializedBody, url);

            Request req = client.newRequest(url);
            req.method(method);
            req.accept("application/json");
            req.agent("kafka-connect");
            addHeadersToRequest(headers, req);

            if (serializedBody != null) {
                req.content(new StringContentProvider(serializedBody, StandardCharsets.UTF_8), "application/json");
            }

            if (sessionKey != null && requestSignatureAlgorithm != null) {
                InternalRequestSignature.addToRequest(
                    Crypto.SYSTEM,
                    sessionKey,
                    serializedBody != null ? serializedBody.getBytes(StandardCharsets.UTF_8) : null,
                    requestSignatureAlgorithm,
                    req
                );
            }

            ContentResponse res = req.send();

            int responseCode = res.getStatus();
            log.debug("Request's response code: {}", responseCode);
            if (responseCode == HttpStatus.NO_CONTENT_204) {
                return new HttpResponse<>(responseCode, convertHttpFieldsToMap(res.getHeaders()), null);
            } else if (responseCode >= 400) {
                ErrorMessage errorMessage = JSON_SERDE.readValue(res.getContentAsString(), ErrorMessage.class);
                throw new ConnectRestException(responseCode, errorMessage.errorCode(), errorMessage.message());
            } else if (responseCode >= 200 && responseCode < 300) {
                T result = JSON_SERDE.readValue(res.getContentAsString(), responseFormat);
                return new HttpResponse<>(responseCode, convertHttpFieldsToMap(res.getHeaders()), result);
            } else {
                throw new ConnectRestException(Response.Status.INTERNAL_SERVER_ERROR,
                        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
                        "Unexpected status code when handling forwarded request: " + responseCode);
            }
        } catch (IOException | InterruptedException | TimeoutException | ExecutionException e) {
            log.error("IO error forwarding REST request: ", e);
            throw new ConnectRestException(Response.Status.INTERNAL_SERVER_ERROR, "IO Error trying to forward REST request: " + e.getMessage(), e);
        } catch (ConnectRestException e) {
            // catching any explicitly thrown ConnectRestException-s to preserve its status code
            // and to avoid getting it overridden by the more generic catch (Throwable) clause down below
            log.error("Error forwarding REST request", e);
            throw e;
        } catch (Throwable t) {
            log.error("Error forwarding REST request", t);
            throw new ConnectRestException(Response.Status.INTERNAL_SERVER_ERROR, "Error trying to forward REST request: " + t.getMessage(), t);
        }
    }


    /**
     * Extract headers from REST call and add to client request
     * @param headers         Headers from REST endpoint
     * @param req             The client request to modify
     */
    private static void addHeadersToRequest(HttpHeaders headers, Request req) {
        if (headers != null) {
            String credentialAuthorization = headers.getHeaderString(HttpHeaders.AUTHORIZATION);
            if (credentialAuthorization != null) {
                req.header(HttpHeaders.AUTHORIZATION, credentialAuthorization);
            }
        }
    }

    /**
     * Convert response headers from Jetty format ({@link HttpFields}) to a simple {@link Map}
     * @param httpFields the response headers
     * @return a {@link Map} containing the response headers
     */
    private static Map<String, String> convertHttpFieldsToMap(HttpFields httpFields) {
        Map<String, String> headers = new HashMap<>();

        if (httpFields == null || httpFields.size() == 0)
            return headers;

        for (HttpField field : httpFields) {
            headers.put(field.getName(), field.getValue());
        }

        return headers;
    }

    public static class HttpResponse<T> {
        private final int status;
        private final Map<String, String> headers;
        private final T body;

        public HttpResponse(int status, Map<String, String> headers, T body) {
            this.status = status;
            this.headers = headers;
            this.body = body;
        }

        public int status() {
            return status;
        }

        public Map<String, String> headers() {
            return headers;
        }

        public T body() {
            return body;
        }
    }
}
