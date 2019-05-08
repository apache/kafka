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

package org.apache.kafka.trogdor.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.trogdor.common.ThreadUtils;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Embedded server for the REST API that provides the control plane for Trogdor.
 */
public class JsonRestServer {
    private static final Logger log = LoggerFactory.getLogger(JsonRestServer.class);

    private static final long GRACEFUL_SHUTDOWN_TIMEOUT_MS = 100;

    private final ScheduledExecutorService shutdownExecutor;

    private final Server jettyServer;

    private final ServerConnector connector;

    /**
     * Create a REST server for this herder using the specified configs.
     *
     * @param port              The port number to use for the REST server, or
     *                          0 to use a random port.
     */
    public JsonRestServer(int port) {
        this.shutdownExecutor = Executors.newSingleThreadScheduledExecutor(
            ThreadUtils.createThreadFactory("JsonRestServerCleanupExecutor", false));
        this.jettyServer = new Server();
        this.connector = new ServerConnector(jettyServer);
        if (port > 0) {
            connector.setPort(port);
        }
        jettyServer.setConnectors(new Connector[]{connector});
    }

    /**
     * Start the JsonRestServer.
     *
     * @param resources         The path handling resources to register.
     */
    @SuppressWarnings("deprecation")
    public void start(Object... resources) {
        log.info("Starting REST server");
        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.register(new JacksonJsonProvider(JsonUtil.JSON_SERDE));
        for (Object resource : resources) {
            resourceConfig.register(resource);
            log.info("Registered resource {}", resource);
        }
        resourceConfig.register(RestExceptionMapper.class);
        ServletContainer servletContainer = new ServletContainer(resourceConfig);
        ServletHolder servletHolder = new ServletHolder(servletContainer);
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.addServlet(servletHolder, "/*");

        RequestLogHandler requestLogHandler = new RequestLogHandler();
        // Use fully qualified name to avoid deprecation warning in import statement
        org.eclipse.jetty.server.Slf4jRequestLog requestLog = new org.eclipse.jetty.server.Slf4jRequestLog();
        requestLog.setLoggerName(JsonRestServer.class.getCanonicalName());
        requestLog.setLogLatency(true);
        requestLogHandler.setRequestLog(requestLog);

        HandlerCollection handlers = new HandlerCollection();
        handlers.setHandlers(new Handler[]{context, new DefaultHandler(), requestLogHandler});
        StatisticsHandler statsHandler = new StatisticsHandler();
        statsHandler.setHandler(handlers);
        jettyServer.setHandler(statsHandler);
        /* Needed for graceful shutdown as per `setStopTimeout` documentation */
        jettyServer.setStopTimeout(GRACEFUL_SHUTDOWN_TIMEOUT_MS);
        jettyServer.setStopAtShutdown(true);

        try {
            jettyServer.start();
        } catch (Exception e) {
            throw new RuntimeException("Unable to start REST server", e);
        }
        log.info("REST server listening at " + jettyServer.getURI());
    }

    public int port() {
        return connector.getLocalPort();
    }

    /**
     * Initiate shutdown, but do not wait for it to complete.
     */
    public void beginShutdown() {
        if (!shutdownExecutor.isShutdown()) {
            shutdownExecutor.submit((Callable<Void>) () -> {
                try {
                    log.info("Stopping REST server");
                    jettyServer.stop();
                    jettyServer.join();
                    log.info("REST server stopped");
                } catch (Exception e) {
                    log.error("Unable to stop REST server", e);
                } finally {
                    jettyServer.destroy();
                }
                shutdownExecutor.shutdown();
                return null;
            });
        }
    }

    /**
     * Wait for shutdown to complete.  May be called prior to beginShutdown.
     */
    public void waitForShutdown() throws InterruptedException {
        while (!shutdownExecutor.isShutdown()) {
            shutdownExecutor.awaitTermination(1, TimeUnit.DAYS);
        }
    }

    /**
     * Make an HTTP request.
     *
     * @param url               HTTP connection will be established with this url.
     * @param method            HTTP method ("GET", "POST", "PUT", etc.)
     * @param requestBodyData   Object to serialize as JSON and send in the request body.
     * @param responseFormat    Expected format of the response to the HTTP request.
     * @param <T>               The type of the deserialized response to the HTTP request.
     * @return The deserialized response to the HTTP request, or null if no data is expected.
     */
    public static <T> HttpResponse<T> httpRequest(String url, String method, Object requestBodyData,
                                                  TypeReference<T> responseFormat) throws IOException {
        return httpRequest(log, url, method, requestBodyData, responseFormat);
    }

    /**
     * Make an HTTP request.
     *
     * @param logger            The logger to use.
     * @param url               HTTP connection will be established with this url.
     * @param method            HTTP method ("GET", "POST", "PUT", etc.)
     * @param requestBodyData   Object to serialize as JSON and send in the request body.
     * @param responseFormat    Expected format of the response to the HTTP request.
     * @param <T>               The type of the deserialized response to the HTTP request.
     * @return The deserialized response to the HTTP request, or null if no data is expected.
     */
    public static <T> HttpResponse<T> httpRequest(Logger logger, String url, String method,
            Object requestBodyData, TypeReference<T> responseFormat) throws IOException {
        HttpURLConnection connection = null;
        try {
            String serializedBody = requestBodyData == null ? null :
                JsonUtil.JSON_SERDE.writeValueAsString(requestBodyData);
            logger.debug("Sending {} with input {} to {}", method, serializedBody, url);
            connection = (HttpURLConnection) new URL(url).openConnection();
            connection.setRequestMethod(method);
            connection.setRequestProperty("User-Agent", "kafka");
            connection.setRequestProperty("Accept", "application/json");

            // connection.getResponseCode() implicitly calls getInputStream, so always set
            // this to true.
            connection.setDoInput(true);

            connection.setUseCaches(false);

            if (requestBodyData != null) {
                connection.setRequestProperty("Content-Type", "application/json");
                connection.setDoOutput(true);

                OutputStream os = connection.getOutputStream();
                os.write(serializedBody.getBytes(StandardCharsets.UTF_8));
                os.flush();
                os.close();
            }

            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_NO_CONTENT) {
                return new HttpResponse<>(null, new ErrorResponse(responseCode, connection.getResponseMessage()));
            } else if ((responseCode >= 200) && (responseCode < 300)) {
                InputStream is = connection.getInputStream();
                T result = JsonUtil.JSON_SERDE.readValue(is, responseFormat);
                is.close();
                return new HttpResponse<>(result, null);
            } else {
                // If the resposne code was not in the 200s, we assume that this is an error
                // response.
                InputStream es = connection.getErrorStream();
                if (es == null) {
                    // Handle the case where HttpURLConnection#getErrorStream returns null.
                    return new HttpResponse<>(null, new ErrorResponse(responseCode, ""));
                }
                // Try to read the error response JSON.
                ErrorResponse error = JsonUtil.JSON_SERDE.readValue(es, ErrorResponse.class);
                es.close();
                return new HttpResponse<>(null, error);
            }
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    /**
     * Make an HTTP request with retries.
     *
     * @param url               HTTP connection will be established with this url.
     * @param method            HTTP method ("GET", "POST", "PUT", etc.)
     * @param requestBodyData   Object to serialize as JSON and send in the request body.
     * @param responseFormat    Expected format of the response to the HTTP request.
     * @param <T>               The type of the deserialized response to the HTTP request.
     * @return The deserialized response to the HTTP request, or null if no data is expected.
     */
    public static <T> HttpResponse<T> httpRequest(String url, String method, Object requestBodyData,
                                                  TypeReference<T> responseFormat, int maxTries)
            throws IOException, InterruptedException {
        return httpRequest(log, url, method, requestBodyData, responseFormat, maxTries);
    }

    /**
     * Make an HTTP request with retries.
     *
     * @param logger            The logger to use.
     * @param url               HTTP connection will be established with this url.
     * @param method            HTTP method ("GET", "POST", "PUT", etc.)
     * @param requestBodyData   Object to serialize as JSON and send in the request body.
     * @param responseFormat    Expected format of the response to the HTTP request.
     * @param <T>               The type of the deserialized response to the HTTP request.
     * @return The deserialized response to the HTTP request, or null if no data is expected.
     */
    public static <T> HttpResponse<T> httpRequest(Logger logger, String url, String method,
            Object requestBodyData, TypeReference<T> responseFormat, int maxTries)
            throws IOException, InterruptedException {
        IOException exc = null;
        for (int tries = 0; tries < maxTries; tries++) {
            if (tries > 0) {
                Thread.sleep(tries > 1 ? 10 : 2);
            }
            try {
                return httpRequest(logger, url, method, requestBodyData, responseFormat);
            } catch (IOException e) {
                logger.info("{} {}: error: {}", method, url, e.getMessage());
                exc = e;
            }
        }
        throw exc;
    }

    public static class HttpResponse<T> {
        private final T body;
        private final ErrorResponse error;

        HttpResponse(T body, ErrorResponse error) {
            this.body = body;
            this.error = error;
        }

        public T body() throws Exception {
            if (error != null) {
                throw RestExceptionMapper.toException(error.code(), error.message());
            }
            return body;
        }

        public ErrorResponse error() {
            return error;
        }
    }
}
