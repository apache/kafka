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

package org.apache.kafka.connect.openlineage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Locale;

/**
 * Transport abstraction for emitting OpenLineage JSON events.
 *
 * <p>Three built-in implementations are provided:
 * <ul>
 *   <li>{@link HttpTransport} &ndash; POSTs events to a configurable HTTP
 *       endpoint (e.g. Marquez, Atlan).</li>
 *   <li>{@link FileTransport} &ndash; appends NDJSON lines to a local
 *       file.</li>
 *   <li>{@link ConsoleTransport} &ndash; logs events via SLF4J at
 *       {@code INFO} level.</li>
 * </ul>
 *
 * <p>Use {@link #create(OpenLineageConfig)} to obtain the appropriate
 * transport based on the configuration.
 */
public interface OpenLineageTransport extends Closeable {

    /**
     * Emit an OpenLineage event.
     *
     * @param eventJson the JSON string representing a RunEvent
     */
    void emit(String eventJson);

    /**
     * Factory method that creates a transport based on the given
     * configuration.
     *
     * @param config the OpenLineage configuration
     * @return a transport instance; never {@code null}
     */
    static OpenLineageTransport create(OpenLineageConfig config) {
        String type = config.transportType();
        switch (type.toLowerCase(Locale.ROOT)) {
            case "http":
                return new HttpTransport(config);
            case "file":
                return new FileTransport(config);
            case "console":
                return new ConsoleTransport();
            default:
                LoggerFactory.getLogger(OpenLineageTransport.class)
                    .warn("Unknown transport type '{}', falling back to console", type);
                return new ConsoleTransport();
        }
    }

    // ---------------------------------------------------------------
    // HttpTransport
    // ---------------------------------------------------------------

    /**
     * POSTs OpenLineage JSON events to a configurable HTTP endpoint.
     */
    final class HttpTransport implements OpenLineageTransport {

        private static final Logger log = LoggerFactory.getLogger(HttpTransport.class);
        private static final Duration TIMEOUT = Duration.ofSeconds(30);

        private final HttpClient httpClient;
        private final URI endpoint;
        private final String authHeader;

        HttpTransport(OpenLineageConfig config) {
            this.httpClient = HttpClient.newBuilder()
                .connectTimeout(TIMEOUT)
                .build();

            String baseUrl = config.transportUrl();
            if (baseUrl == null || baseUrl.isEmpty()) {
                throw new IllegalArgumentException(
                    "openlineage.transport.url must be set when using HTTP transport");
            }
            // Strip trailing slash from base URL before appending endpoint
            if (baseUrl.endsWith("/")) {
                baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
            }
            this.endpoint = URI.create(baseUrl + config.transportEndpoint());

            if ("api_key".equals(config.authType()) && config.authApiKey() != null) {
                this.authHeader = "Bearer " + config.authApiKey();
            } else {
                this.authHeader = null;
            }
            log.info("OpenLineage HTTP transport initialized: endpoint={}", endpoint);
        }

        @Override
        public void emit(String eventJson) {
            try {
                HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                    .uri(endpoint)
                    .timeout(TIMEOUT)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(eventJson, StandardCharsets.UTF_8));

                if (authHeader != null) {
                    reqBuilder.header("Authorization", authHeader);
                }

                HttpResponse<String> response = httpClient.send(
                    reqBuilder.build(),
                    HttpResponse.BodyHandlers.ofString()
                );

                if (response.statusCode() >= 200 && response.statusCode() < 300) {
                    log.debug("OpenLineage event emitted successfully (HTTP {})",
                        response.statusCode());
                } else {
                    log.warn("OpenLineage HTTP transport received status {}: {}",
                        response.statusCode(), response.body());
                }
            } catch (IOException | InterruptedException e) {
                log.error("Failed to emit OpenLineage event via HTTP", e);
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public void close() {
            // HttpClient does not require explicit close in JDK 11
        }
    }

    // ---------------------------------------------------------------
    // FileTransport
    // ---------------------------------------------------------------

    /**
     * Appends OpenLineage events as newline-delimited JSON (NDJSON) to a
     * local file.
     */
    final class FileTransport implements OpenLineageTransport {

        private static final Logger log = LoggerFactory.getLogger(FileTransport.class);

        private final Path filePath;

        FileTransport(OpenLineageConfig config) {
            String path = config.filePath();
            if (path == null || path.isEmpty()) {
                path = "/tmp/openlineage-events.ndjson";
                log.info("No file path configured, defaulting to {}", path);
            }
            this.filePath = Paths.get(path);
            log.info("OpenLineage file transport initialized: path={}", filePath);
        }

        @Override
        public void emit(String eventJson) {
            try (BufferedWriter writer = Files.newBufferedWriter(
                    filePath,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND)) {
                writer.write(eventJson);
                writer.newLine();
            } catch (IOException e) {
                log.error("Failed to write OpenLineage event to file {}", filePath, e);
            }
        }

        @Override
        public void close() {
            // Nothing to close
        }
    }

    // ---------------------------------------------------------------
    // ConsoleTransport
    // ---------------------------------------------------------------

    /**
     * Logs OpenLineage events via SLF4J at {@code INFO} level.
     */
    final class ConsoleTransport implements OpenLineageTransport {

        private static final Logger log = LoggerFactory.getLogger(ConsoleTransport.class);

        ConsoleTransport() {
            log.info("OpenLineage console transport initialized");
        }

        @Override
        public void emit(String eventJson) {
            log.info("OpenLineage event: {}", eventJson);
        }

        @Override
        public void close() {
            // Nothing to close
        }
    }
}
