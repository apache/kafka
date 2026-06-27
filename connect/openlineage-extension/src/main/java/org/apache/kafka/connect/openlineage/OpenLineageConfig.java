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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;

/**
 * Reads OpenLineage configuration from either Connect worker properties
 * (prefixed with {@code openlineage.}) or from a YAML file pointed to by
 * the {@code OPENLINEAGE_CONFIG} environment variable.
 *
 * <p>Worker properties take precedence over the YAML file when both are
 * present.
 *
 * <p>The YAML file follows the standard OpenLineage client configuration
 * format used by Spark and Flink integrations:
 * <pre>
 * transport:
 *   type: http
 *   url: https://your-backend:5000
 *   endpoint: /api/v1/lineage
 *   auth:
 *     type: api_key
 *     apiKey: your-api-key
 * </pre>
 * or for file transport:
 * <pre>
 * transport:
 *   type: file
 *   location: /path/to/events.json
 * </pre>
 *
 * <h3>Supported worker properties</h3>
 * <ul>
 *   <li>{@code openlineage.transport.type} &ndash; {@code http}, {@code file},
 *       or {@code console} (default: {@code console})</li>
 *   <li>{@code openlineage.transport.url} &ndash; base URL for the HTTP
 *       transport</li>
 *   <li>{@code openlineage.transport.endpoint} &ndash; path appended to the
 *       URL (default: {@code /api/v1/lineage})</li>
 *   <li>{@code openlineage.transport.auth.type} &ndash; {@code api_key} or
 *       empty</li>
 *   <li>{@code openlineage.transport.auth.apiKey} &ndash; bearer token</li>
 *   <li>{@code openlineage.transport.location} &ndash; file path for file
 *       transport</li>
 *   <li>{@code openlineage.namespace} &ndash; the job namespace
 *       (default: {@code kafka-connect})</li>
 *   <li>{@code openlineage.poll.interval.ms} &ndash; how often to poll
 *       cluster state, in milliseconds (default: {@code 10000})</li>
 *   <li>{@code openlineage.running.interval.ms} &ndash; how often to emit
 *       RUNNING heartbeat events for active connectors, in milliseconds
 *       (default: {@code 300000} = 5 minutes, matching Flink&rsquo;s pattern)</li>
 * </ul>
 */
public final class OpenLineageConfig {

    private static final Logger log = LoggerFactory.getLogger(OpenLineageConfig.class);

    static final String PREFIX = "openlineage.";
    static final String ENV_VAR = "OPENLINEAGE_CONFIG";
    static final String ENV_NAMESPACE = "OPENLINEAGE_NAMESPACE";

    static final String DEFAULT_NAMESPACE = "kafka-connect";
    static final String DEFAULT_TRANSPORT_TYPE = "console";
    static final String DEFAULT_ENDPOINT = "/api/v1/lineage";
    static final long DEFAULT_POLL_INTERVAL_MS = 10_000L;
    static final long DEFAULT_RUNNING_INTERVAL_MS = 300_000L; // 5 minutes

    private final String transportType;
    private final String transportUrl;
    private final String transportEndpoint;
    private final String authType;
    private final String authApiKey;
    private final String filePath;
    private final String namespace;
    private final long pollIntervalMs;
    private final long runningIntervalMs;
    private final String bootstrapServers;

    public OpenLineageConfig(Map<String, ?> workerProps) {
        // Try to load YAML config from env var
        JsonNode yamlRoot = loadYamlConfig();

        // Worker properties override YAML, YAML overrides defaults
        this.transportType = resolve(workerProps, "transport.type",
            yamlString(yamlRoot, "transport", "type"), DEFAULT_TRANSPORT_TYPE)
            .toLowerCase(Locale.ROOT);
        this.transportUrl = resolve(workerProps, "transport.url",
            yamlString(yamlRoot, "transport", "url"), null);
        this.transportEndpoint = resolve(workerProps, "transport.endpoint",
            yamlString(yamlRoot, "transport", "endpoint"), DEFAULT_ENDPOINT);
        this.authType = resolve(workerProps, "transport.auth.type",
            yamlString(yamlRoot, "transport", "auth", "type"), null);
        this.authApiKey = resolve(workerProps, "transport.auth.apiKey",
            yamlString(yamlRoot, "transport", "auth", "apiKey"), null);

        // File transport: support both "location" (standard OL) and "file.path"
        String yamlLocation = yamlString(yamlRoot, "transport", "location");
        String yamlFilePath = yamlString(yamlRoot, "transport", "file", "path");
        this.filePath = resolve(workerProps, "transport.location",
            yamlLocation != null ? yamlLocation : yamlFilePath, null);

        // Namespace: env var > worker prop > YAML > default
        String envNs = System.getenv(ENV_NAMESPACE);
        String resolvedNs = resolve(workerProps, "namespace",
            yamlString(yamlRoot, "namespace"), DEFAULT_NAMESPACE);
        this.namespace = (envNs != null && !envNs.isEmpty()) ? envNs : resolvedNs;

        String pollStr = resolve(workerProps, "poll.interval.ms", null,
            String.valueOf(DEFAULT_POLL_INTERVAL_MS));
        this.pollIntervalMs = Long.parseLong(pollStr);

        String runningStr = resolve(workerProps, "running.interval.ms", null,
            String.valueOf(DEFAULT_RUNNING_INTERVAL_MS));
        this.runningIntervalMs = Long.parseLong(runningStr);

        // Worker-level bootstrap.servers (not prefixed with openlineage.) — used
        // to build the Kafka dataset namespace so topics are named
        // kafka://<broker>:<port> rather than the kafka://localhost:9092 fallback.
        Object bootstrap = workerProps.get("bootstrap.servers");
        this.bootstrapServers = (bootstrap != null && !bootstrap.toString().trim().isEmpty())
            ? bootstrap.toString().trim() : null;

        log.info("OpenLineage config: transport={}, url={}, endpoint={}, namespace={}, poll={}ms, running={}ms",
            transportType, transportUrl, transportEndpoint, namespace, pollIntervalMs, runningIntervalMs);
    }

    public String transportType() {
        return transportType;
    }

    public String transportUrl() {
        return transportUrl;
    }

    public String transportEndpoint() {
        return transportEndpoint;
    }

    public String authType() {
        return authType;
    }

    public String authApiKey() {
        return authApiKey;
    }

    public String filePath() {
        return filePath;
    }

    public String namespace() {
        return namespace;
    }

    public long pollIntervalMs() {
        return pollIntervalMs;
    }

    public long runningIntervalMs() {
        return runningIntervalMs;
    }

    /**
     * The Connect worker's {@code bootstrap.servers}, used to build the Kafka
     * dataset namespace ({@code kafka://<bootstrap>}); {@code null} if unset.
     */
    public String bootstrapServers() {
        return bootstrapServers;
    }

    private static String resolve(Map<String, ?> workerProps, String key,
                                  String yamlValue, String defaultValue) {
        Object wpVal = workerProps.get(PREFIX + key);
        if (wpVal != null) {
            String s = wpVal.toString().trim();
            if (!s.isEmpty()) {
                return s;
            }
        }
        if (yamlValue != null && !yamlValue.isEmpty()) {
            return yamlValue;
        }
        return defaultValue;
    }

    private static String yamlString(JsonNode root, String... path) {
        if (root == null) {
            return null;
        }
        JsonNode node = root;
        for (String field : path) {
            node = node.get(field);
            if (node == null) {
                return null;
            }
        }
        return node.isTextual() ? node.asText() : node.toString();
    }

    /**
     * Load YAML config. Tries Jackson YAML factory first; if the YAML module
     * is not on the classpath, falls back to plain JSON ObjectMapper (which
     * can still parse simple YAML that is also valid JSON).
     */
    private static JsonNode loadYamlConfig() {
        String envPath = System.getenv(ENV_VAR);
        if (envPath == null || envPath.trim().isEmpty()) {
            return null;
        }
        File configFile = new File(envPath);
        if (!configFile.exists()) {
            log.warn("OpenLineage config file not found: {}", envPath);
            return null;
        }
        try {
            // Try YAML factory (jackson-dataformat-yaml on classpath)
            ObjectMapper yamlMapper = createYamlMapper();
            return yamlMapper.readTree(configFile);
        } catch (IOException e) {
            log.warn("Failed to read OpenLineage config from {}: {}", envPath, e.getMessage());
            return null;
        }
    }

    private static ObjectMapper createYamlMapper() {
        try {
            // Reflectively load YAMLFactory to avoid hard dependency
            Class<?> yamlFactoryClass = Class.forName("com.fasterxml.jackson.dataformat.yaml.YAMLFactory");
            Object yamlFactory = yamlFactoryClass.getDeclaredConstructor().newInstance();
            return new ObjectMapper((com.fasterxml.jackson.core.JsonFactory) yamlFactory);
        } catch (ReflectiveOperationException e) {
            log.debug("jackson-dataformat-yaml not on classpath, falling back to JSON parser");
            return new ObjectMapper();
        }
    }
}
