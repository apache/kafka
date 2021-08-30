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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpClient {

    private static final Logger log = LoggerFactory.getLogger(HttpClient.class);

    private final HttpURLConnectionSupplier connectionSupplier;

    private final Integer connectTimeoutMs;

    private final Integer readTimeoutMs;

    public HttpClient(HttpURLConnectionSupplier connectionSupplier,
        Integer connectTimeoutMs,
        Integer readTimeoutMs) {
        this.connectionSupplier = connectionSupplier;
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
    }

    public String post(Map<String, String> headers, String requestBody) throws IOException {
        HttpURLConnection con = connectionSupplier.get();
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
                ByteArrayInputStream is = new ByteArrayInputStream(requestBody.getBytes(StandardCharsets.UTF_8));
                log.debug("post - preparing to write request body to {}", con.getURL());
                HttpClientUtils.copy(is, os);
            }
        }

        int responseCode = con.getResponseCode();
        log.warn("post - responseCode: {}", responseCode);

        if (responseCode == HttpURLConnection.HTTP_OK || responseCode == HttpURLConnection.HTTP_CREATED) {
            String responseBody;

            try (InputStream is = con.getInputStream()) {
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                log.debug("post - preparing to read request body to {}", con.getURL());
                HttpClientUtils.copy(is, os);
                responseBody = os.toString(StandardCharsets.UTF_8.name());
            }

            if (responseBody.isEmpty())
                throw new IOException("The token endpoint response was unexpectedly empty");

            return responseBody;
        } else {
            throw new IOException(String.format("The unexpected response code %s was encountered reading the token endpoint response", responseCode));
        }
    }

}
