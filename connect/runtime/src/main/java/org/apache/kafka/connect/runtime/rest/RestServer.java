/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.runtime.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.rest.entities.ErrorMessage;
import org.apache.kafka.connect.runtime.rest.errors.ConnectExceptionMapper;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.runtime.rest.resources.ConnectorPluginsResource;
import org.apache.kafka.connect.runtime.rest.resources.ConnectorsResource;
import org.apache.kafka.connect.runtime.rest.resources.RootResource;
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
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import javax.servlet.DispatcherType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

/**
 * Embedded server for the REST API that provides the control plane for Kafka Connect workers.
 */
public class RestServer {
    private static final Logger log = LoggerFactory.getLogger(RestServer.class);

    private static final long GRACEFUL_SHUTDOWN_TIMEOUT_MS = 60 * 1000;

    private static final ObjectMapper JSON_SERDE = new ObjectMapper();

    private final WorkerConfig config;
    private Server jettyServer;

    /**
     * Create a REST server for this herder using the specified configs.
     */
    public RestServer(WorkerConfig config) {
        this.config = config;

        // To make the advertised port available immediately, we need to do some configuration here
        String hostname = config.getString(WorkerConfig.REST_HOST_NAME_CONFIG);
        Integer port = config.getInt(WorkerConfig.REST_PORT_CONFIG);

        jettyServer = new Server();

        ServerConnector connector = new ServerConnector(jettyServer);
        if (hostname != null && !hostname.isEmpty())
            connector.setHost(hostname);
        connector.setPort(port);
        jettyServer.setConnectors(new Connector[]{connector});
    }

    public void start(Herder herder) {
        log.info("Starting REST server");

        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.register(new JacksonJsonProvider());

        resourceConfig.register(RootResource.class);
        resourceConfig.register(new ConnectorsResource(herder));
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
        String advertisedHostname = config.getString(WorkerConfig.REST_ADVERTISED_HOST_NAME_CONFIG);
        if (advertisedHostname != null && !advertisedHostname.isEmpty())
            builder.host(advertisedHostname);
        Integer advertisedPort = config.getInt(WorkerConfig.REST_ADVERTISED_PORT_CONFIG);
        if (advertisedPort != null)
            builder.port(advertisedPort);
        else
            builder.port(config.getInt(WorkerConfig.REST_PORT_CONFIG));
        return builder.build();
    }

    /**
     * @param url               HTTP connection will be established with this url.
     * @param method            HTTP method ("GET", "POST", "PUT", etc.)
     * @param requestBodyData   Object to serialize as JSON and send in the request body.
     * @param responseFormat    Expected format of the response to the HTTP request.
     * @param <T>               The type of the deserialized response to the HTTP request.
     * @return The deserialized response to the HTTP request, or null if no data is expected.
     */
    public static <T> HttpResponse<T> httpRequest(String url, String method, Object requestBodyData,
                                    TypeReference<T> responseFormat) {
        HttpURLConnection connection = null;
        try {
            String serializedBody = requestBodyData == null ? null : JSON_SERDE.writeValueAsString(requestBodyData);
            log.debug("Sending {} with input {} to {}", method, serializedBody, url);

            connection = (HttpURLConnection) new URL(url).openConnection();
            connection.setRequestMethod(method);

            connection.setRequestProperty("User-Agent", "kafka-connect");
            connection.setRequestProperty("Accept", "application/json");

            // connection.getResponseCode() implicitly calls getInputStream, so always set to true.
            // On the other hand, leaving this out breaks nothing.
            connection.setDoInput(true);

            connection.setUseCaches(false);

            if (requestBodyData != null) {
                connection.setRequestProperty("Content-Type", "application/json");
                connection.setDoOutput(true);

                OutputStream os = connection.getOutputStream();
                os.write(serializedBody.getBytes());
                os.flush();
                os.close();
            }

            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_NO_CONTENT) {
                return new HttpResponse<>(responseCode, connection.getHeaderFields(), null);
            } else if (responseCode >= 400) {
                InputStream es = connection.getErrorStream();
                ErrorMessage errorMessage = JSON_SERDE.readValue(es, ErrorMessage.class);
                es.close();
                throw new ConnectRestException(responseCode, errorMessage.errorCode(), errorMessage.message());
            } else if (responseCode >= 200 && responseCode < 300) {
                InputStream is = connection.getInputStream();
                T result = JSON_SERDE.readValue(is, responseFormat);
                is.close();
                return new HttpResponse<>(responseCode, connection.getHeaderFields(), result);
            } else {
                throw new ConnectRestException(Response.Status.INTERNAL_SERVER_ERROR,
                        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
                        "Unexpected status code when handling forwarded request: " + responseCode);
            }
        } catch (IOException e) {
            log.error("IO error forwarding REST request: ", e);
            throw new ConnectRestException(Response.Status.INTERNAL_SERVER_ERROR, "IO Error trying to forward REST request: " + e.getMessage(), e);
        } finally {
            if (connection != null)
                connection.disconnect();
        }
    }

    public static class HttpResponse<T> {
        private int status;
        private Map<String, List<String>> headers;
        private T body;

        public HttpResponse(int status, Map<String, List<String>> headers, T body) {
            this.status = status;
            this.headers = headers;
            this.body = body;
        }

        public int status() {
            return status;
        }

        public Map<String, List<String>> headers() {
            return headers;
        }

        public T body() {
            return body;
        }
    }

    public static String urlJoin(String base, String path) {
        if (base.endsWith("/") && path.startsWith("/"))
            return base + path.substring(1);
        else
            return base + path;
    }

}