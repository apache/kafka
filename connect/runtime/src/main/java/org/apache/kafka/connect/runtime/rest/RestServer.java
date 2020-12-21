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
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.health.ConnectClusterDetails;
import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.rest.ConnectRestExtensionContext;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.health.ConnectClusterDetailsImpl;
import org.apache.kafka.connect.runtime.health.ConnectClusterStateImpl;
import org.apache.kafka.connect.runtime.rest.errors.ConnectExceptionMapper;
import org.apache.kafka.connect.runtime.rest.resources.ConnectorPluginsResource;
import org.apache.kafka.connect.runtime.rest.resources.ConnectorsResource;
import org.apache.kafka.connect.runtime.rest.resources.LoggingResource;
import org.apache.kafka.connect.runtime.rest.resources.RootResource;
import org.apache.kafka.connect.runtime.rest.util.SSLUtils;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.Slf4jRequestLogWriter;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.eclipse.jetty.servlets.HeaderFilter;
import org.eclipse.jetty.util.ssl.SslContextFactory;
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
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.kafka.connect.runtime.WorkerConfig.ADMIN_LISTENERS_HTTPS_CONFIGS_PREFIX;

/**
 * Embedded server for the REST API that provides the control plane for Kafka Connect workers.
 */
public class RestServer {
    private static final Logger log = LoggerFactory.getLogger(RestServer.class);

    // Used to distinguish between Admin connectors and regular REST API connectors when binding admin handlers
    private static final String ADMIN_SERVER_CONNECTOR_NAME = "Admin";

    private static final Pattern LISTENER_PATTERN = Pattern.compile("^(.*)://\\[?([0-9a-zA-Z\\-%._:]*)\\]?:(-?[0-9]+)");
    private static final long GRACEFUL_SHUTDOWN_TIMEOUT_MS = 60 * 1000;

    private static final String PROTOCOL_HTTP = "http";
    private static final String PROTOCOL_HTTPS = "https";

    private final WorkerConfig config;
    private ContextHandlerCollection handlers;
    private Server jettyServer;

    private List<ConnectRestExtension> connectRestExtensions = Collections.emptyList();

    /**
     * Create a REST server for this herder using the specified configs.
     */
    public RestServer(WorkerConfig config) {
        this.config = config;

        List<String> listeners = parseListeners();
        List<String> adminListeners = config.getList(WorkerConfig.ADMIN_LISTENERS_CONFIG);

        jettyServer = new Server();
        handlers = new ContextHandlerCollection();

        createConnectors(listeners, adminListeners);
    }

    @SuppressWarnings("deprecation")
    List<String> parseListeners() {
        List<String> listeners = config.getList(WorkerConfig.LISTENERS_CONFIG);
        if (listeners == null || listeners.size() == 0) {
            String hostname = config.getString(WorkerConfig.REST_HOST_NAME_CONFIG);

            if (hostname == null)
                hostname = "";

            listeners = Collections.singletonList(String.format("%s://%s:%d", PROTOCOL_HTTP, hostname, config.getInt(WorkerConfig.REST_PORT_CONFIG)));
        }

        return listeners;
    }

    /**
     * Adds Jetty connector for each configured listener
     */
    public void createConnectors(List<String> listeners, List<String> adminListeners) {
        List<Connector> connectors = new ArrayList<>();

        for (String listener : listeners) {
            if (!listener.isEmpty()) {
                Connector connector = createConnector(listener);
                connectors.add(connector);
                log.info("Added connector for {}", listener);
            }
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
    public Connector createConnector(String listener) {
        return createConnector(listener, false);
    }

    /**
     * Creates Jetty connector according to configuration
     */
    public Connector createConnector(String listener, boolean isAdmin) {
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
            SslContextFactory ssl;
            if (isAdmin) {
                ssl = SSLUtils.createServerSideSslContextFactory(config, ADMIN_LISTENERS_HTTPS_CONFIGS_PREFIX);
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
        log.info("REST admin endpoints at " + adminUrl());
    }

    public void initializeResources(Herder herder) {
        log.info("Initializing REST resources");

        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.register(new JacksonJsonProvider());

        resourceConfig.register(new RootResource(herder));
        resourceConfig.register(new ConnectorsResource(herder, config));
        resourceConfig.register(new ConnectorPluginsResource(herder));

        resourceConfig.register(ConnectExceptionMapper.class);
        resourceConfig.property(ServerProperties.WADL_FEATURE_DISABLE, true);

        registerRestExtensions(herder, resourceConfig);

        List<String> adminListeners = config.getList(WorkerConfig.ADMIN_LISTENERS_CONFIG);
        ResourceConfig adminResourceConfig;
        if (adminListeners == null) {
            log.info("Adding admin resources to main listener");
            adminResourceConfig = resourceConfig;
            adminResourceConfig.register(new LoggingResource());
        } else if (adminListeners.size() > 0) {
            // TODO: we need to check if these listeners are same as 'listeners'
            // TODO: the following code assumes that they are different
            log.info("Adding admin resources to admin listener");
            adminResourceConfig = new ResourceConfig();
            adminResourceConfig.register(new JacksonJsonProvider());
            adminResourceConfig.register(new LoggingResource());
            adminResourceConfig.register(ConnectExceptionMapper.class);
        } else {
            log.info("Skipping adding admin resources");
            // set up adminResource but add no handlers to it
            adminResourceConfig = resourceConfig;
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

        String allowedOrigins = config.getString(WorkerConfig.ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG);
        if (allowedOrigins != null && !allowedOrigins.trim().isEmpty()) {
            FilterHolder filterHolder = new FilterHolder(new CrossOriginFilter());
            filterHolder.setName("cross-origin");
            filterHolder.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, allowedOrigins);
            String allowedMethods = config.getString(WorkerConfig.ACCESS_CONTROL_ALLOW_METHODS_CONFIG);
            if (allowedMethods != null && !allowedOrigins.trim().isEmpty()) {
                filterHolder.setInitParameter(CrossOriginFilter.ALLOWED_METHODS_PARAM, allowedMethods);
            }
            context.addFilter(filterHolder, "/*", EnumSet.of(DispatcherType.REQUEST));
        }

        String headerConfig = config.getString(WorkerConfig.RESPONSE_HTTP_HEADERS_CONFIG);
        if (headerConfig != null && !headerConfig.trim().isEmpty()) {
            configureHttpResponsHeaderFilter(context);
        }

        RequestLogHandler requestLogHandler = new RequestLogHandler();
        Slf4jRequestLogWriter slf4jRequestLogWriter = new Slf4jRequestLogWriter();
        slf4jRequestLogWriter.setLoggerName(RestServer.class.getCanonicalName());
        CustomRequestLog requestLog = new CustomRequestLog(slf4jRequestLogWriter, CustomRequestLog.EXTENDED_NCSA_FORMAT + " %msT");
        requestLogHandler.setRequestLog(requestLog);

        contextHandlers.add(new DefaultHandler());
        contextHandlers.add(requestLogHandler);

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

    public URI serverUrl() {
        return jettyServer.getURI();
    }

    public void stop() {
        log.info("Stopping REST server");

        try {
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
            jettyServer.destroy();
            throw new ConnectException("Unable to stop REST server", e);
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

        String advertisedHostname = config.getString(WorkerConfig.REST_ADVERTISED_HOST_NAME_CONFIG);
        if (advertisedHostname != null && !advertisedHostname.isEmpty())
            builder.host(advertisedHostname);
        else if (serverConnector != null && serverConnector.getHost() != null && serverConnector.getHost().length() > 0)
            builder.host(serverConnector.getHost());

        Integer advertisedPort = config.getInt(WorkerConfig.REST_ADVERTISED_PORT_CONFIG);
        if (advertisedPort != null)
            builder.port(advertisedPort);
        else if (serverConnector != null && serverConnector.getPort() > 0)
            builder.port(serverConnector.getPort());

        log.info("Advertised URI: {}", builder.build());

        return builder.build();
    }

    /**
     * @return the admin url for this worker. can be null if admin endpoints are disabled.
     */
    public URI adminUrl() {
        ServerConnector adminConnector = null;
        for (Connector connector : jettyServer.getConnectors()) {
            if (ADMIN_SERVER_CONNECTOR_NAME.equals(connector.getName()))
                adminConnector = (ServerConnector) connector;
        }

        if (adminConnector == null) {
            List<String> adminListeners = config.getList(WorkerConfig.ADMIN_LISTENERS_CONFIG);
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

    String determineAdvertisedProtocol() {
        String advertisedSecurityProtocol = config.getString(WorkerConfig.REST_ADVERTISED_LISTENER_CONFIG);
        if (advertisedSecurityProtocol == null) {
            String listeners = (String) config.originals().get(WorkerConfig.LISTENERS_CONFIG);

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

    void registerRestExtensions(Herder herder, ResourceConfig resourceConfig) {
        connectRestExtensions = herder.plugins().newPlugins(
            config.getList(WorkerConfig.REST_EXTENSION_CLASSES_CONFIG),
            config, ConnectRestExtension.class);

        long herderRequestTimeoutMs = ConnectorsResource.REQUEST_TIMEOUT_MS;

        Integer rebalanceTimeoutMs = config.getRebalanceTimeout();

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

    public static String urlJoin(String base, String path) {
        if (base.endsWith("/") && path.startsWith("/"))
            return base + path.substring(1);
        else
            return base + path;
    }

    /**
     * Register header filter to ServletContextHandler.
     * @param context The serverlet context handler
     */
    protected void configureHttpResponsHeaderFilter(ServletContextHandler context) {
        String headerConfig = config.getString(WorkerConfig.RESPONSE_HTTP_HEADERS_CONFIG);
        FilterHolder headerFilterHolder = new FilterHolder(HeaderFilter.class);
        headerFilterHolder.setInitParameter("headerConfig", headerConfig);
        context.addFilter(headerFilterHolder, "/*", EnumSet.of(DispatcherType.REQUEST));
    }
}
