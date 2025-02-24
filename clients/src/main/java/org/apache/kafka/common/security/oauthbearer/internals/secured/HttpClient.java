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

import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.Optional;

import javax.net.ssl.HttpsURLConnection;

/**
 * <code>HttpClient</code> is a lightweight client that can be used by callback handlers to
 * communicate with an OAuth/OIDC provider directly via HTTP.
 */
public class HttpClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(HttpClient.class);

    private final String url;

    private final Optional<SslResource> sslResource;

    private final Optional<Integer> connectTimeoutMs;

    private final Optional<Integer> readTimeoutMs;

    public HttpClient(String url) {
        this(url, Optional.empty(), Optional.empty(), Optional.empty());
    }

    public HttpClient(String url,
                      Optional<SslResource> sslResource,
                      Optional<Integer> connectTimeoutMs,
                      Optional<Integer> readTimeoutMs) {
        this.url = url;
        this.sslResource = sslResource;
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
    }

    @Override
    public void close() throws IOException {
        sslResource.ifPresent(r -> Utils.closeQuietly(r, "sslResource"));
    }

    public HttpURLConnection connect(String url,
                                     Map<String, String> headers,
                                     Optional<Integer> contentLength) throws IOException, UnretryableException {
        log.debug("connect - starting connect for {}", url);

        HttpURLConnection con = (HttpURLConnection) new URL(url).openConnection();

        if (sslResource.isPresent() && con instanceof HttpsURLConnection)
            ((HttpsURLConnection) con).setSSLSocketFactory(sslResource.get().sslSocketFactory());

        if (contentLength.isPresent()) {
            con.setRequestMethod("POST");
            con.setDoOutput(true);
            headers.put("Content-Length", String.valueOf(contentLength.get()));
        } else {
            con.setRequestMethod("GET");
            con.setDoOutput(false);
        }

        for (Map.Entry<String, String> header : headers.entrySet())
            con.setRequestProperty(header.getKey(), header.getValue());

        headers.put("Accept", "application/json");
        headers.put("Cache-Control", "no-cache");

        con.setUseCaches(false);
        connectTimeoutMs.ifPresent(con::setConnectTimeout);
        readTimeoutMs.ifPresent(con::setReadTimeout);

        log.debug("connect - preparing to connect to {}", con.getURL());
        con.connect();
        return con;
    }

    public HttpResponse post(Map<String, String> requestHeaders, byte[] requestBody) throws IOException {
        HttpURLConnection con = null;

        try {
            con = connect(url, requestHeaders, Optional.of(requestBody.length));
            write(con, requestBody);
            return read(con);
        } finally {
            if (con != null)
                con.disconnect();
        }
    }

    public void write(HttpURLConnection con, byte[] requestBody) throws IOException {
        try (OutputStream os = con.getOutputStream()) {
            ByteArrayInputStream is = new ByteArrayInputStream(requestBody);
            log.trace("write - preparing to write request body to {}", con.getURL());
            copy(is, os);
        }
    }

    public HttpResponse read(HttpURLConnection con) throws IOException {
        int responseCode = con.getResponseCode();
        log.debug("read - responseCode: {}", responseCode);

        // NOTE: the contents of the response should not be logged so that we don't leak any
        // sensitive data.
        byte[] responseBody = null;

        // NOTE: It is OK to log the error response body and/or its formatted version as
        // per the OAuth spec, it doesn't include sensitive information.
        // See https://www.ietf.org/rfc/rfc6749.txt, section 5.2
        byte[] errorResponseBody = null;

        try (InputStream is = con.getInputStream()) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            log.debug("read - preparing to read response body from {}", con.getURL());
            copy(is, os);
            responseBody = os.toByteArray();
        } catch (Exception e) {
            // There still can be useful error response from the servers, lets get it from the error stream.
            try (InputStream is = con.getErrorStream()) {
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                log.debug("read - preparing to read error response body from {}", con.getURL());
                copy(is, os);
                errorResponseBody = os.toByteArray();
            } catch (Exception e2) {
                log.warn("read - error retrieving error information", e2);
            }

            log.warn("read - error retrieving data", e);
        }

        Optional<byte[]> responseBodyOpt = responseBody != null && responseBody.length > 0 ? Optional.of(responseBody) : Optional.empty();
        Optional<byte[]> errorResponseBodyOpt = errorResponseBody != null && errorResponseBody.length > 0 ? Optional.of(errorResponseBody) : Optional.empty();
        return new HttpResponse(responseCode, responseBodyOpt, errorResponseBodyOpt);
    }

    static void copy(InputStream is, OutputStream os) throws IOException {
        byte[] buf = new byte[4096];
        int b;

        while ((b = is.read(buf)) != -1)
            os.write(buf, 0, b);
    }

    public static class HttpResponse {

        public final int responseCode;

        public final Optional<byte[]> responseBody;

        public final Optional<byte[]> errorResponseBody;

        public HttpResponse(int responseCode, Optional<byte[]> responseBody, Optional<byte[]> errorResponseBody) {
            this.responseCode = responseCode;
            this.responseBody = responseBody;
            this.errorResponseBody = errorResponseBody;
        }
    }
}
