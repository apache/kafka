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

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.health.ConnectClusterDetails;
import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.rest.ConnectRestExtensionContext;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.health.ConnectClusterDetailsImpl;
import org.apache.kafka.connect.runtime.health.ConnectClusterStateImpl;
import org.apache.kafka.connect.runtime.rest.errors.ConnectExceptionMapper;
import org.apache.kafka.connect.runtime.rest.util.SSLUtils;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.Slf4jRequestLogWriter;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.eclipse.jetty.servlets.HeaderFilter;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.glassfish.hk2.utilities.Binder;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.DispatcherType;
import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Embedded server for the REST API that provides the control plane for Kafka Connect workers.
 */
public abstract class RestServer {

    // TODO: This should not be so long. However, due to potentially long rebalances that may have to wait a full
    // session timeout to complete, during which we cannot serve some requests. Ideally we could reduce this, but
    // we need to consider all possible scenarios this could fail. It might be ok to fail with a timeout in rare cases,
    // but currently a worker simply leaving the group can take this long as well.
    public static final long DEFAULT_REST_REQUEST_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(90);

    private static final Logger log = LoggerFactory.getLogger(RestServer.class);

    // Used to distinguish between Admin connectors and regular REST API connectors when binding admin handlers
    private static final String ADMIN_SERVER_CONNECTOR_NAME = "Admin";

    private static final Pattern LISTENER_PATTERN = Pattern.compile("^(.*)://\\[?([0-9a-zA-Z\\-%._:]*)\\]?:(-?[0-9]+)");
    private static final long GRACEFUL_SHUTDOWN_TIMEOUT_MS = 60 * 1000;

    private static final String PROTOCOL_HTTP = "http";
    private static final String PROTOCOL_HTTPS = "https";

    protected final RestServerConfig config;
    private final ContextHandlerCollection handlers;
    private final Server jettyServer;
    private final RequestTimeout requestTimeout;

    private List<ConnectRestExtension> connectRestExtensions = Collections.emptyList();

    /**
     * Create a REST server for this herder using the specified configs.
     */
    protected RestServer(RestServerConfig config) {
        this.config = config;

        List<String> listeners = config.listeners();
        List<String> adminListeners = config.adminListeners();

        jettyServer = new Server();
        handlers = new ContextHandlerCollection();
        requestTimeout = new RequestTimeout(DEFAULT_REST_REQUEST_TIMEOUT_MS);

        createConnectors(listeners, adminListeners);
    }

    /**
     * Adds Jetty connector for each configured listener
     */
    public final void createConnectors(List<String> listeners, List<String> adminListeners) {
        List<Connector> connectors = new ArrayList<>();

        for (String listener : listeners) {
            Connector connector = createConnector(listener);
            connectors.add(connector);
            log.info("Added connector for {}", listener);
        }

        jettyServer.setConnectors(connectors.toArray(new Connector[0]));

        if (adminListeners != null && !adminListeners.isEmpty()) {
            for (String adminListener : adminListeners) {
                Connector conn = createConnector(adminListener, true);
                jettyServer.addConnector(conn);
                log.info("Added admin connector for {}", adminListener);
            }
        }
    }

    /**
     * Creates regular (non-admin) Jetty connector according to configuration
     */
    public final Connector createConnector(String listener) {
        return createConnector(listener, false);
    }

    /**
     * Creates Jetty connector according to configuration
     */
    public final Connector createConnector(String listener, boolean isAdmin) {
        Matcher listenerMatcher = LISTENER_PATTERN.matcher(listener);

        if (!listenerMatcher.matches())
            throw new ConfigException("Listener doesn't have the right format (protocol://hostname:port).");

        String protocol = listenerMatcher.group(1).toLowerCase(Locale.ENGLISH);

        if (!PROTOCOL_HTTP.equals(protocol) && !PROTOCOL_HTTPS.equals(protocol))
            throw new ConfigException(String.format("Listener protocol must be either \"%s\" or \"%s\".", PROTOCOL_HTTP, PROTOCOL_HTTPS));

        String hostname = listenerMatcher.group(2);
        int port = Integer.parseInt(listenerMatcher.group(3));

        ServerConnector connector;

        if (PROTOCOL_HTTPS.equals(protocol)) {
            SslContextFactory.Server ssl;
            if (isAdmin) {
                ssl = SSLUtils.createServerSideSslContextFactory(config, RestServerConfig.ADMIN_LISTENERS_HTTPS_CONFIGS_PREFIX);
            } else {
                ssl = SSLUtils.createServerSideSslContextFactory(config);
            }
            connector = new ServerConnector(jettyServer, ssl);
            if (!isAdmin) {
                connector.setName(String.format("%s_%s%d", PROTOCOL_HTTPS, hostname, port));
            }
        } else {
            connector = new ServerConnector(jettyServer);
            if (!isAdmin) {
                connector.setName(String.format("%s_%s%d", PROTOCOL_HTTP, hostname, port));
            }
        }

        if (isAdmin) {
            connector.setName(ADMIN_SERVER_CONNECTOR_NAME);
        }

        if (!hostname.isEmpty())
            connector.setHost(hostname);

        connector.setPort(port);

        return connector;
    }

    public void initializeServer() {
        log.info("Initializing REST server");

        Slf4jRequestLogWriter slf4jRequestLogWriter = new Slf4jRequestLogWriter();
        slf4jRequestLogWriter.setLoggerName(RestServer.class.getCanonicalName());
        CustomRequestLog requestLog = new CustomRequestLog(slf4jRequestLogWriter, CustomRequestLog.EXTENDED_NCSA_FORMAT + " %{ms}T");
        jettyServer.setRequestLog(requestLog);

        /* Needed for graceful shutdown as per `setStopTimeout` documentation */
        StatisticsHandler statsHandler = new StatisticsHandler();
        statsHandler.setHandler(handlers);
        jettyServer.setHandler(statsHandler);
        jettyServer.setStopTimeout(GRACEFUL_SHUTDOWN_TIMEOUT_MS);
        jettyServer.setStopAtShutdown(true);

        try {
            jettyServer.start();
        } catch (Exception e) {
            throw new ConnectException("Unable to initialize REST server", e);
        }

        log.info("REST server listening at " + jettyServer.getURI() + ", advertising URL " + advertisedUrl());
        URI adminUrl = adminUrl();
        if (adminUrl != null)
            log.info("REST admin endpoints at " + adminUrl);
    }

    protected final void initializeResources() {
        log.info("Initializing REST resources");

        ResourceConfig resourceConfig = newResourceConfig();
        Collection<Class<?>> regularResources = regularResources();
        regularResources.forEach(resourceConfig::register);
        configureRegularResources(resourceConfig);

        List<String> adminListeners = config.adminListeners();
        ResourceConfig adminResourceConfig;
        if (adminListeners != null && adminListeners.isEmpty()) {
            log.info("Skipping adding admin resources");
            // set up adminResource but add no handlers to it
            adminResourceConfig = resourceConfig;
        } else {
            if (adminListeners == null) {
                log.info("Adding admin resources to main listener");
                adminResourceConfig = resourceConfig;
            } else {
                // TODO: we need to check if these listeners are same as 'listeners'
                // TODO: the following code assumes that they are different
                log.info("Adding admin resources to admin listener");
                adminResourceConfig = newResourceConfig();
            }
            Collection<Class<?>> adminResources = adminResources();
            adminResources.forEach(adminResourceConfig::register);
            configureAdminResources(adminResourceConfig);
        }

        ServletContainer servletContainer = new ServletContainer(resourceConfig);
        ServletHolder servletHolder = new ServletHolder(servletContainer);
        List<Handler> contextHandlers = new ArrayList<>();

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.addServlet(servletHolder, "/*");
        contextHandlers.add(context);

        ServletContextHandler adminContext = null;
        if (adminResourceConfig != resourceConfig) {
            adminContext = new ServletContextHandler(ServletContextHandler.SESSIONS);
            ServletHolder adminServletHolder = new ServletHolder(new ServletContainer(adminResourceConfig));
            adminContext.setContextPath("/");
            adminContext.addServlet(adminServletHolder, "/*");
            adminContext.setVirtualHosts(new String[]{"@" + ADMIN_SERVER_CONNECTOR_NAME});
            contextHandlers.add(adminContext);
        }

        String allowedOrigins = config.allowedOrigins();
        if (!Utils.isBlank(allowedOrigins)) {
            FilterHolder filterHolder = new FilterHolder(new CrossOriginFilter());
            filterHolder.setName("cross-origin");
            filterHolder.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, allowedOrigins);
            String allowedMethods = config.allowedMethods();
            if (!Utils.isBlank(allowedMethods)) {
                filterHolder.setInitParameter(CrossOriginFilter.ALLOWED_METHODS_PARAM, allowedMethods);
            }
            context.addFilter(filterHolder, "/*", EnumSet.of(DispatcherType.REQUEST));
        }

        String headerConfig = config.responseHeaders();
        if (!Utils.isBlank(headerConfig)) {
            configureHttpResponseHeaderFilter(context, headerConfig);
        }

        handlers.setHandlers(contextHandlers.toArray(new Handler[0]));
        try {
            context.start();
        } catch (Exception e) {
            throw new ConnectException("Unable to initialize REST resources", e);
        }

        if (adminResourceConfig != resourceConfig) {
            try {
                log.debug("Starting admin context");
                adminContext.start();
            } catch (Exception e) {
                throw new ConnectException("Unable to initialize Admin REST resources", e);
            }
        }

        log.info("REST resources initialized; server is started and ready to handle requests");
    }

    private ResourceConfig newResourceConfig() {
        ResourceConfig result = new ResourceConfig();
        result.register(new JacksonJsonProvider());
        result.register(requestTimeout.binder());
        result.register(ConnectExceptionMapper.class);
        result.property(ServerProperties.WADL_FEATURE_DISABLE, true);
        return result;
    }

    /**
     * @return the resources that should be registered with the
     * standard (i.e., non-admin) listener for this server; may be empty, but not null
     */
    protected abstract Collection<Class<?>> regularResources();

    /**
     * @return the resources that should be registered with the
     * admin listener for this server; may be empty, but not null
     */
    protected abstract Collection<Class<?>> adminResources();

    /**
     * Pluggable hook to customize the regular (i.e., non-admin) resources on this server
     * after they have been instantiated and registered with the given {@link ResourceConfig}.
     * This may be used to, for example, add REST extensions via {@link #registerRestExtensions(Herder, ResourceConfig)}.
     * <p>
     * <em>N.B.: Classes do <b>not</b> need to register the resources provided in {@link #regularResources()} with
     * the {@link ResourceConfig} parameter in this method; they are automatically registered by the parent class.</em>
     * @param resourceConfig the {@link ResourceConfig} that the server's regular listeners are registered with; never null
     */
    protected void configureRegularResources(ResourceConfig resourceConfig) {
        // No-op by default
    }

    /**
     * Pluggable hook to customize the admin resources on this server after they have been instantiated and registered
     * with the given {@link ResourceConfig}. This may be used to, for example, add REST extensions via
     * {@link #registerRestExtensions(Herder, ResourceConfig)}.
     * <p>
     * <em>N.B.: Classes do <b>not</b> need to register the resources provided in {@link #adminResources()} with
     * the {@link ResourceConfig} parameter in this method; they are automatically registered by the parent class.</em>
     * @param adminResourceConfig the {@link ResourceConfig} that the server's admin listeners are registered with; never null
     */
    protected void configureAdminResources(ResourceConfig adminResourceConfig) {
        // No-op by default
    }

    public URI serverUrl() {
        return jettyServer.getURI();
    }

    public void stop() {
        log.info("Stopping REST server");

        try {
            if (handlers.isRunning()) {
                for (Handler handler : handlers.getHandlers()) {
                    if (handler != null) {
                        Utils.closeQuietly(handler::stop, handler.toString());
                    }
                }
            }
            for (ConnectRestExtension connectRestExtension : connectRestExtensions) {
                try {
                    connectRestExtension.close();
                } catch (IOException e) {
                    log.warn("Error while invoking close on " + connectRestExtension.getClass(), e);
                }
            }
            jettyServer.stop();
            jettyServer.join();
        } catch (Exception e) {
            throw new ConnectException("Unable to stop REST server", e);
        } finally {
            try {
                jettyServer.destroy();
            } catch (Exception e) {
                log.error("Unable to destroy REST server", e);
            }
        }

        log.info("REST server stopped");
    }

    /**
     * Get the URL to advertise to other workers and clients. This uses the default connector from the embedded Jetty
     * server, unless overrides for advertised hostname and/or port are provided via configs. {@link #initializeServer()}
     * must be invoked successfully before calling this method.
     */
    public URI advertisedUrl() {
        UriBuilder builder = UriBuilder.fromUri(jettyServer.getURI());

        String advertisedSecurityProtocol = determineAdvertisedProtocol();
        ServerConnector serverConnector = findConnector(advertisedSecurityProtocol);
        builder.scheme(advertisedSecurityProtocol);

        String advertisedHostname = config.advertisedHostName();
        if (advertisedHostname != null && !advertisedHostname.isEmpty())
            builder.host(advertisedHostname);
        else if (serverConnector != null && serverConnector.getHost() != null && !serverConnector.getHost().isEmpty())
            builder.host(serverConnector.getHost());

        Integer advertisedPort = config.advertisedPort();
        if (advertisedPort != null)
            builder.port(advertisedPort);
        else if (serverConnector != null && serverConnector.getPort() > 0)
            builder.port(serverConnector.getPort());
        else if (serverConnector != null && serverConnector.getLocalPort() > 0)
            builder.port(serverConnector.getLocalPort());

        log.info("Advertised URI: {}", builder.build());

        return builder.build();
    }

    /**
     * @return the admin url for this worker. Can be null if admin endpoints are disabled.
     */
    public URI adminUrl() {
        ServerConnector adminConnector = null;
        for (Connector connector : jettyServer.getConnectors()) {
            if (ADMIN_SERVER_CONNECTOR_NAME.equals(connector.getName()))
                adminConnector = (ServerConnector) connector;
        }

        if (adminConnector == null) {
            List<String> adminListeners = config.adminListeners();
            if (adminListeners == null) {
                return advertisedUrl();
            } else if (adminListeners.isEmpty()) {
                return null;
            } else {
                log.error("No admin connector found for listeners {}", adminListeners);
                return null;
            }
        }

        UriBuilder builder = UriBuilder.fromUri(jettyServer.getURI());
        builder.port(adminConnector.getLocalPort());

        return builder.build();
    }

    // For testing only
    public void requestTimeout(long requestTimeoutMs) {
        this.requestTimeout.timeoutMs(requestTimeoutMs);
    }

    String determineAdvertisedProtocol() {
        String advertisedSecurityProtocol = config.advertisedListener();
        if (advertisedSecurityProtocol == null) {
            String listeners = config.rawListeners();

            if (listeners == null)
                return PROTOCOL_HTTP;
            else
                listeners = listeners.toLowerCase(Locale.ENGLISH);

            if (listeners.contains(String.format("%s://", PROTOCOL_HTTP)))
                return PROTOCOL_HTTP;
            else if (listeners.contains(String.format("%s://", PROTOCOL_HTTPS)))
                return PROTOCOL_HTTPS;
            else
                return PROTOCOL_HTTP;
        } else {
            return advertisedSecurityProtocol.toLowerCase(Locale.ENGLISH);
        }
    }

    /**
     * Locate a Jetty connector for the standard (non-admin) REST API that uses the given protocol.
     * @param protocol the protocol for the connector (e.g., "http" or "https").
     * @return a {@link ServerConnector} for the server that uses the requested protocol, or
     * {@code null} if none exist.
     */
    ServerConnector findConnector(String protocol) {
        for (Connector connector : jettyServer.getConnectors()) {
            String connectorName = connector.getName();
            // We set the names for these connectors when instantiating them, beginning with the
            // protocol for the connector and then an underscore ("_"). We rely on that format here
            // when trying to locate a connector with the requested protocol; if the naming format
            // for the connectors we create is ever changed, we'll need to adjust the logic here
            // accordingly.
            if (connectorName.startsWith(protocol + "_") && !ADMIN_SERVER_CONNECTOR_NAME.equals(connectorName))
                return (ServerConnector) connector;
        }

        return null;
    }

    protected final void registerRestExtensions(Herder herder, ResourceConfig resourceConfig) {
        connectRestExtensions = herder.plugins().newPlugins(
            config.restExtensions(),
            config, ConnectRestExtension.class);

        long herderRequestTimeoutMs = DEFAULT_REST_REQUEST_TIMEOUT_MS;

        Integer rebalanceTimeoutMs = config.rebalanceTimeoutMs();

        if (rebalanceTimeoutMs != null) {
            herderRequestTimeoutMs = Math.min(herderRequestTimeoutMs, rebalanceTimeoutMs.longValue());
        }

        ConnectClusterDetails connectClusterDetails = new ConnectClusterDetailsImpl(
            herder.kafkaClusterId()
        );

        ConnectRestExtensionContext connectRestExtensionContext =
            new ConnectRestExtensionContextImpl(
                new ConnectRestConfigurable(resourceConfig),
                new ConnectClusterStateImpl(herderRequestTimeoutMs, connectClusterDetails, herder)
            );
        for (ConnectRestExtension connectRestExtension : connectRestExtensions) {
            connectRestExtension.register(connectRestExtensionContext);
        }

    }

    /**
     * Register header filter to ServletContextHandler.
     * @param context The servlet context handler
     */
    protected void configureHttpResponseHeaderFilter(ServletContextHandler context, String headerConfig) {
        FilterHolder headerFilterHolder = new FilterHolder(HeaderFilter.class);
        headerFilterHolder.setInitParameter("headerConfig", headerConfig);
        context.addFilter(headerFilterHolder, "/*", EnumSet.of(DispatcherType.REQUEST));
    }

    private static class RequestTimeout implements RestRequestTimeout {

        private final RequestBinder binder;
        private volatile long timeoutMs;

        public RequestTimeout(long initialTimeoutMs) {
            this.timeoutMs = initialTimeoutMs;
            this.binder = new RequestBinder();
        }

        @Override
        public long timeoutMs() {
            return timeoutMs;
        }

        public void timeoutMs(long timeoutMs) {
            this.timeoutMs = timeoutMs;
        }

        public Binder binder() {
            return binder;
        }

        private class RequestBinder extends AbstractBinder {
            @Override
            protected void configure() {
                bind(RequestTimeout.this).to(RestRequestTimeout.class);
            }
        }
    }
}
