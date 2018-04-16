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
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.rest.errors.ConnectExceptionMapper;
import org.apache.kafka.connect.runtime.rest.resources.ConnectorPluginsResource;
import org.apache.kafka.connect.runtime.rest.resources.ConnectorsResource;
import org.apache.kafka.connect.runtime.rest.resources.RootResource;
import org.apache.kafka.connect.runtime.rest.util.SSLUtils;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.Slf4jRequestLog;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.DispatcherType;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Embedded server for the REST API that provides the control plane for Kafka Connect workers.
 */
public class RestServer {
    private static final Logger log = LoggerFactory.getLogger(RestServer.class);

    private static final Pattern LISTENER_PATTERN = Pattern.compile("^(.*)://\\[?([0-9a-zA-Z\\-%._:]*)\\]?:(-?[0-9]+)");
    private static final long GRACEFUL_SHUTDOWN_TIMEOUT_MS = 60 * 1000;

    private static final String PROTOCOL_HTTP = "http";
    private static final String PROTOCOL_HTTPS = "https";

    private final WorkerConfig config;
    private Server jettyServer;

    /**
     * Create a REST server for this herder using the specified configs.
     */
    public RestServer(WorkerConfig config) {
        this.config = config;

        List<String> listeners = parseListeners();

        jettyServer = new Server();

        createConnectors(listeners);
    }

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
    public void createConnectors(List<String> listeners) {
        List<Connector> connectors = new ArrayList<>();

        for (String listener : listeners) {
            if (!listener.isEmpty()) {
                Connector connector = createConnector(listener);
                connectors.add(connector);
                log.info("Added connector for " + listener);
            }
        }

        jettyServer.setConnectors(connectors.toArray(new Connector[connectors.size()]));
    }

    /**
     * Creates Jetty connector according to configuration
     */
    public Connector createConnector(String listener) {
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
            SslContextFactory ssl = SSLUtils.createSslContextFactory(config);
            connector = new ServerConnector(jettyServer, ssl);
            connector.setName(String.format("%s_%s%d", PROTOCOL_HTTPS, hostname, port));
        } else {
            connector = new ServerConnector(jettyServer);
            connector.setName(String.format("%s_%s%d", PROTOCOL_HTTP, hostname, port));
        }

        if (!hostname.isEmpty())
            connector.setHost(hostname);

        connector.setPort(port);

        return connector;
    }

    public void start(Herder herder) {
        log.info("Starting REST server");

        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.register(new JacksonJsonProvider());

        resourceConfig.register(new RootResource(herder));
        resourceConfig.register(new ConnectorsResource(herder, config));
        resourceConfig.register(new ConnectorPluginsResource(herder));

        resourceConfig.register(ConnectExceptionMapper.class);

        ServletContainer servletContainer = new ServletContainer(resourceConfig);
        ServletHolder servletHolder = new ServletHolder(servletContainer);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.addServlet(servletHolder, "/*");

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

        RequestLogHandler requestLogHandler = new RequestLogHandler();
        Slf4jRequestLog requestLog = new Slf4jRequestLog();
        requestLog.setLoggerName(RestServer.class.getCanonicalName());
        requestLog.setLogLatency(true);
        requestLogHandler.setRequestLog(requestLog);

        HandlerCollection handlers = new HandlerCollection();
        handlers.setHandlers(new Handler[]{context, new DefaultHandler(), requestLogHandler});

        /* Needed for graceful shutdown as per `setStopTimeout` documentation */
        StatisticsHandler statsHandler = new StatisticsHandler();
        statsHandler.setHandler(handlers);
        jettyServer.setHandler(statsHandler);
        jettyServer.setStopTimeout(GRACEFUL_SHUTDOWN_TIMEOUT_MS);
        jettyServer.setStopAtShutdown(true);

        try {
            jettyServer.start();
        } catch (Exception e) {
            throw new ConnectException("Unable to start REST server", e);
        }

        log.info("REST server listening at " + jettyServer.getURI() + ", advertising URL " + advertisedUrl());
    }

    public void stop() {
        log.info("Stopping REST server");

        try {
            jettyServer.stop();
            jettyServer.join();
        } catch (Exception e) {
            throw new ConnectException("Unable to stop REST server", e);
        } finally {
            jettyServer.destroy();
        }

        log.info("REST server stopped");
    }

    /**
     * Get the URL to advertise to other workers and clients. This uses the default connector from the embedded Jetty
     * server, unless overrides for advertised hostname and/or port are provided via configs.
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
        else if (serverConnector != null)
            builder.port(serverConnector.getPort());

        log.info("Advertised URI: {}", builder.build());

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

    ServerConnector findConnector(String protocol) {
        for (Connector connector : jettyServer.getConnectors()) {
            if (connector.getName().startsWith(protocol))
                return (ServerConnector) connector;
        }

        return null;
    }

    public static String urlJoin(String base, String path) {
        if (base.endsWith("/") && path.startsWith("/"))
            return base + path.substring(1);
        else
            return base + path;
    }

}