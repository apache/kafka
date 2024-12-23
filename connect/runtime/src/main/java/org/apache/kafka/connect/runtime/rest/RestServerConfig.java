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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslClientAuth;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.WorkerConfig;

import org.eclipse.jetty.util.StringUtil;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

/**
 * Defines the configuration surface for a {@link RestServer} instance, with support for both
 * {@link #forInternal(Map) internal-only} and {@link #forPublic(Integer, Map) user-facing}
 * servers. An internal-only server will expose only the endpoints and listeners necessary for
 * intra-cluster communication; these include the task-write and zombie-fencing endpoints. A
 * user-facing server will expose these endpoints and, in addition, all endpoints that are part of
 * the public REST API for Kafka Connect; these include the connector creation, connector
 * status, configuration validation, and logging endpoints. In addition, a user-facing server will
 * instantiate any user-configured
 * {@link RestServerConfig#REST_EXTENSION_CLASSES_CONFIG REST extensions}.
 */
public abstract class RestServerConfig extends AbstractConfig {

    public static final String LISTENERS_CONFIG = "listeners";
    private static final String LISTENERS_DOC
            = "List of comma-separated URIs the REST API will listen on. The supported protocols are HTTP and HTTPS.\n" +
            " Specify hostname as 0.0.0.0 to bind to all interfaces.\n" +
            " Leave hostname empty to bind to default interface.\n" +
            " Examples of legal listener lists: HTTP://myhost:8083,HTTPS://myhost:8084";
    // Visible for testing
    static final List<String> LISTENERS_DEFAULT = Collections.singletonList("http://:8083");

    public static final String REST_ADVERTISED_HOST_NAME_CONFIG = "rest.advertised.host.name";
    private static final String REST_ADVERTISED_HOST_NAME_DOC
            = "If this is set, this is the hostname that will be given out to other workers to connect to.";

    public static final String REST_ADVERTISED_PORT_CONFIG = "rest.advertised.port";
    private static final String REST_ADVERTISED_PORT_DOC
            = "If this is set, this is the port that will be given out to other workers to connect to.";

    public static final String REST_ADVERTISED_LISTENER_CONFIG = "rest.advertised.listener";
    private static final String REST_ADVERTISED_LISTENER_DOC
            = "Sets the advertised listener (HTTP or HTTPS) which will be given to other workers to use.";

    public static final String ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG = "access.control.allow.origin";
    private static final String ACCESS_CONTROL_ALLOW_ORIGIN_DOC =
            "Value to set the Access-Control-Allow-Origin header to for REST API requests." +
                    "To enable cross origin access, set this to the domain of the application that should be permitted" +
                    " to access the API, or '*' to allow access from any domain. The default value only allows access" +
                    " from the domain of the REST API.";
    protected static final String ACCESS_CONTROL_ALLOW_ORIGIN_DEFAULT = "";

    public static final String ACCESS_CONTROL_ALLOW_METHODS_CONFIG = "access.control.allow.methods";
    private static final String ACCESS_CONTROL_ALLOW_METHODS_DOC =
            "Sets the methods supported for cross origin requests by setting the Access-Control-Allow-Methods header. "
                    + "The default value of the Access-Control-Allow-Methods header allows cross origin requests for GET, POST and HEAD.";
    private static final String ACCESS_CONTROL_ALLOW_METHODS_DEFAULT = "";

    public static final String ADMIN_LISTENERS_CONFIG = "admin.listeners";
    private static final String ADMIN_LISTENERS_DOC = "List of comma-separated URIs the Admin REST API will listen on." +
            " The supported protocols are HTTP and HTTPS." +
            " An empty or blank string will disable this feature." +
            " The default behavior is to use the regular listener (specified by the 'listeners' property).";
    public static final String ADMIN_LISTENERS_HTTPS_CONFIGS_PREFIX = "admin.listeners.https.";

    public static final String REST_EXTENSION_CLASSES_CONFIG = "rest.extension.classes";
    private static final String REST_EXTENSION_CLASSES_DOC =
            "Comma-separated names of <code>ConnectRestExtension</code> classes, loaded and called "
                    + "in the order specified. Implementing the interface  "
                    + "<code>ConnectRestExtension</code> allows you to inject into Connect's REST API user defined resources like filters. "
                    + "Typically used to add custom capability like logging, security, etc. ";

    // Visible for testing
    static final String RESPONSE_HTTP_HEADERS_CONFIG = "response.http.headers.config";
    // Visible for testing
    static final String RESPONSE_HTTP_HEADERS_DOC = "Rules for REST API HTTP response headers";
    // Visible for testing
    static final String RESPONSE_HTTP_HEADERS_DEFAULT = "";
    private static final Collection<String> HEADER_ACTIONS = List.of("set", "add", "setDate", "addDate");


    /**
     * @return the listeners to use for this server, or empty if no admin endpoints should be exposed,
     * or null if the admin endpoints should be exposed on the {@link #listeners() regular listeners} for
     * this server
     */
    public abstract List<String> adminListeners();

    /**
     * @return a list of {@link #REST_EXTENSION_CLASSES_CONFIG REST extension} classes
     * to instantiate and use with the server
     */
    public abstract List<String> restExtensions();

    /**
     * @return whether {@link WorkerConfig#TOPIC_TRACKING_ENABLE_CONFIG topic tracking}
     * is enabled on this worker
     */
    public abstract boolean topicTrackingEnabled();

    /**
     * @return whether {@link WorkerConfig#TOPIC_TRACKING_ALLOW_RESET_CONFIG topic tracking resets}
     * are enabled on this worker
     */
    public abstract boolean topicTrackingResetEnabled();

    /**
     * Add the properties related to a user-facing server to the given {@link ConfigDef}.
     * </p>
     * This automatically adds the properties for intra-cluster communication; it is not necessary to
     * invoke both {@link #addInternalConfig(ConfigDef)} and this method on the same {@link ConfigDef}.
     * @param configDef the {@link ConfigDef} to add the properties to; may not be null
     */
    public static void addPublicConfig(ConfigDef configDef) {
        addInternalConfig(configDef);
        configDef
                .define(
                        REST_EXTENSION_CLASSES_CONFIG,
                        ConfigDef.Type.LIST,
                        "",
                        ConfigDef.Importance.LOW, REST_EXTENSION_CLASSES_DOC
                ).define(ADMIN_LISTENERS_CONFIG,
                        ConfigDef.Type.LIST,
                        null,
                        new AdminListenersValidator(),
                        ConfigDef.Importance.LOW,
                        ADMIN_LISTENERS_DOC);
    }

    /**
     * Add the properties related to an internal-only server to the given {@link ConfigDef}.
     * @param configDef the {@link ConfigDef} to add the properties to; may not be null
     */
    public static void addInternalConfig(ConfigDef configDef) {
        configDef
                .define(
                        LISTENERS_CONFIG,
                        ConfigDef.Type.LIST,
                        LISTENERS_DEFAULT,
                        new ListenersValidator(),
                        ConfigDef.Importance.LOW,
                        LISTENERS_DOC
                ).define(
                        REST_ADVERTISED_HOST_NAME_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.LOW,
                        REST_ADVERTISED_HOST_NAME_DOC
                ).define(
                        REST_ADVERTISED_PORT_CONFIG,
                        ConfigDef.Type.INT,
                        null,
                        ConfigDef.Importance.LOW,
                        REST_ADVERTISED_PORT_DOC
                ).define(
                        REST_ADVERTISED_LISTENER_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.LOW,
                        REST_ADVERTISED_LISTENER_DOC
                ).define(
                        ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG,
                        ConfigDef.Type.STRING,
                        ACCESS_CONTROL_ALLOW_ORIGIN_DEFAULT,
                        ConfigDef.Importance.LOW,
                        ACCESS_CONTROL_ALLOW_ORIGIN_DOC
                ).define(
                        ACCESS_CONTROL_ALLOW_METHODS_CONFIG,
                        ConfigDef.Type.STRING,
                        ACCESS_CONTROL_ALLOW_METHODS_DEFAULT,
                        ConfigDef.Importance.LOW,
                        ACCESS_CONTROL_ALLOW_METHODS_DOC
                ).define(
                        RESPONSE_HTTP_HEADERS_CONFIG,
                        ConfigDef.Type.STRING,
                        RESPONSE_HTTP_HEADERS_DEFAULT,
                        new ResponseHttpHeadersValidator(),
                        ConfigDef.Importance.LOW,
                        RESPONSE_HTTP_HEADERS_DOC
                ).define(
                        BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG,
                        ConfigDef.Type.STRING,
                        BrokerSecurityConfigs.SSL_CLIENT_AUTH_DEFAULT,
                        in(Utils.enumOptions(SslClientAuth.class)),
                        ConfigDef.Importance.LOW,
                        BrokerSecurityConfigs.SSL_CLIENT_AUTH_DOC);
    }

    public static RestServerConfig forPublic(Integer rebalanceTimeoutMs, Map<?, ?> props) {
        return new PublicConfig(rebalanceTimeoutMs, props);
    }

    public static RestServerConfig forInternal(Map<?, ?> props) {
        return new InternalConfig(props);
    }

    public List<String> listeners() {
        return getList(LISTENERS_CONFIG);
    }

    public String rawListeners() {
        return (String) originals().get(LISTENERS_CONFIG);
    }

    public String allowedOrigins() {
        return getString(ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG);
    }

    public String allowedMethods() {
        return getString(ACCESS_CONTROL_ALLOW_METHODS_CONFIG);
    }

    public String responseHeaders() {
        return getString(RESPONSE_HTTP_HEADERS_CONFIG);
    }

    public String advertisedListener() {
        return getString(RestServerConfig.REST_ADVERTISED_LISTENER_CONFIG);
    }

    public String advertisedHostName() {
        return getString(REST_ADVERTISED_HOST_NAME_CONFIG);
    }

    public Integer advertisedPort() {
        return getInt(REST_ADVERTISED_PORT_CONFIG);
    }

    public Integer rebalanceTimeoutMs() {
        return null;
    }

    protected RestServerConfig(ConfigDef configDef, Map<?, ?> props) {
        super(configDef, props, Utils.castToStringObjectMap(props), true);
    }

    // Visible for testing
    static void validateHttpResponseHeaderConfig(String config) {
        try {
            // validate format
            String[] configTokens = config.trim().split("\\s+", 2);
            if (configTokens.length != 2) {
                throw new ConfigException(String.format("Invalid format of header config '%s'. "
                        + "Expected: '[action] [header name]:[header value]'", config));
            }

            // validate action
            String method = configTokens[0].trim();
            validateHeaderConfigAction(method);

            // validate header name and header value pair
            String header = configTokens[1];
            String[] headerTokens = header.trim().split(":");
            if (headerTokens.length != 2) {
                throw new ConfigException(
                        String.format("Invalid format of header name and header value pair '%s'. "
                                + "Expected: '[header name]:[header value]'", header));
            }

            // validate header name
            String headerName = headerTokens[0].trim();
            if (headerName.isEmpty() || headerName.matches(".*\\s+.*")) {
                throw new ConfigException(String.format("Invalid header name '%s'. "
                        + "The '[header name]' cannot contain whitespace", headerName));
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new ConfigException(String.format("Invalid header config '%s'.", config), e);
        }
    }

    // Visible for testing
    static void validateHeaderConfigAction(String action) {
        if (HEADER_ACTIONS.stream().noneMatch(action::equalsIgnoreCase)) {
            throw new ConfigException(String.format("Invalid header config action: '%s'. "
                    + "Expected one of %s", action, HEADER_ACTIONS));
        }
    }

    private static class ListenersValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object value) {
            if (!(value instanceof List)) {
                throw new ConfigException("Invalid value type for listeners (expected list of URLs , ex: http://localhost:8080,https://localhost:8443).");
            }

            List<?> items = (List<?>) value;
            if (items.isEmpty()) {
                throw new ConfigException("Invalid value for listeners, at least one URL is expected, ex: http://localhost:8080,https://localhost:8443.");
            }

            for (Object item : items) {
                if (!(item instanceof String)) {
                    throw new ConfigException("Invalid type for listeners (expected String).");
                }
                if (Utils.isBlank((String) item)) {
                    throw new ConfigException("Empty URL found when parsing listeners list.");
                }
            }
        }

        @Override
        public String toString() {
            return "List of comma-separated URLs, ex: http://localhost:8080,https://localhost:8443.";
        }
    }

    private static class AdminListenersValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object value) {
            if (value == null) {
                return;
            }

            if (!(value instanceof List)) {
                throw new ConfigException("Invalid value type for admin.listeners (expected list).");
            }

            List<?> items = (List<?>) value;
            if (items.isEmpty()) {
                return;
            }

            for (Object item : items) {
                if (!(item instanceof String)) {
                    throw new ConfigException("Invalid type for admin.listeners (expected String).");
                }
                if (Utils.isBlank((String) item)) {
                    throw new ConfigException("Empty URL found when parsing admin.listeners list.");
                }
            }
        }

        @Override
        public String toString() {
            return "List of comma-separated URLs, ex: http://localhost:8080,https://localhost:8443.";
        }
    }

    private static class ResponseHttpHeadersValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object value) {
            String strValue = (String) value;
            if (Utils.isBlank(strValue)) {
                return;
            }

            String[] configs = StringUtil.csvSplit(strValue); // handles and removed surrounding quotes
            Arrays.stream(configs).forEach(RestServerConfig::validateHttpResponseHeaderConfig);
        }

        @Override
        public String toString() {
            return "Comma-separated header rules, where each header rule is of the form "
                    + "'[action] [header name]:[header value]' and optionally surrounded by double quotes "
                    + "if any part of a header rule contains a comma";
        }
    }

    private static class InternalConfig extends RestServerConfig {

        private static ConfigDef config() {
            ConfigDef result = new ConfigDef().withClientSslSupport();
            addInternalConfig(result);
            return result;
        }

        @Override
        public List<String> adminListeners() {
            // Disable admin resources (such as the logging resource)
            return Collections.emptyList();
        }

        @Override
        public List<String> restExtensions() {
            // Disable the use of REST extensions
            return null;
        }

        @Override
        public boolean topicTrackingEnabled() {
            // Topic tracking is unnecessary if we don't expose a public REST API
            return false;
        }

        @Override
        public boolean topicTrackingResetEnabled() {
            // Topic tracking is unnecessary if we don't expose a public REST API
            return false;
        }

        public InternalConfig(Map<?, ?> props) {
            super(config(), props);
        }
    }

    private static class PublicConfig extends RestServerConfig {

        private final Integer rebalanceTimeoutMs;
        private static ConfigDef config() {
            ConfigDef result = new ConfigDef().withClientSslSupport();
            addPublicConfig(result);
            WorkerConfig.addTopicTrackingConfig(result);
            return result;
        }

        @Override
        public List<String> adminListeners() {
            return getList(ADMIN_LISTENERS_CONFIG);
        }

        @Override
        public List<String> restExtensions() {
            return getList(REST_EXTENSION_CLASSES_CONFIG);
        }

        @Override
        public Integer rebalanceTimeoutMs() {
            return rebalanceTimeoutMs;
        }

        @Override
        public boolean topicTrackingEnabled() {
            return getBoolean(WorkerConfig.TOPIC_TRACKING_ENABLE_CONFIG);
        }

        @Override
        public boolean topicTrackingResetEnabled() {
            return getBoolean(WorkerConfig.TOPIC_TRACKING_ALLOW_RESET_CONFIG);
        }

        public PublicConfig(Integer rebalanceTimeoutMs, Map<?, ?> props) {
            super(config(), props);
            this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        }
    }
}
