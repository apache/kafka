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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.runtime.WorkerConfig.ADMIN_LISTENERS_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class RestServerTest {

    private Herder herder;
    private Plugins plugins;
    private RestServer server;
    private CloseableHttpClient httpClient;
    private Collection<CloseableHttpResponse> responses = new ArrayList<>();

    protected static final String KAFKA_CLUSTER_ID = "Xbafgnagvar";

    @Before
    public void setUp() {
        herder = mock(Herder.class);
        plugins = mock(Plugins.class);
        httpClient = HttpClients.createMinimal();
    }

    @After
    public void tearDown() throws IOException {
        for (CloseableHttpResponse response: responses) {
            response.close();
        }
        if (httpClient != null) {
            httpClient.close();
        }
        if (server != null) {
            server.stop();
        }
    }

    private Map<String, String> baseWorkerProps() {
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "status-topic");
        workerProps.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "config-topic");
        workerProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        workerProps.put(DistributedConfig.GROUP_ID_CONFIG, "connect-test-group");
        workerProps.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "connect-offsets");
        workerProps.put(WorkerConfig.LISTENERS_CONFIG, "HTTP://localhost:0");

        return workerProps;
    }

    @Test
    public void testCORSEnabled() throws IOException {
        checkCORSRequest("*", "http://bar.com", "http://bar.com", "PUT");
    }

    @Test
    public void testCORSDisabled() throws IOException {
        checkCORSRequest("", "http://bar.com", null, null);
    }

    @Test
    public void testAdvertisedUri() {
        // Advertised URI from listeners without protocol
        Map<String, String> configMap = new HashMap<>(baseWorkerProps());
        configMap.put(WorkerConfig.LISTENERS_CONFIG, "http://localhost:8080,https://localhost:8443");
        DistributedConfig config = new DistributedConfig(configMap);

        server = new RestServer(config, null);
        Assert.assertEquals("http://localhost:8080/", server.advertisedUrl().toString());
        server.stop();

        // Advertised URI from listeners with protocol
        configMap = new HashMap<>(baseWorkerProps());
        configMap.put(WorkerConfig.LISTENERS_CONFIG, "http://localhost:8080,https://localhost:8443");
        configMap.put(WorkerConfig.REST_ADVERTISED_LISTENER_CONFIG, "https");
        config = new DistributedConfig(configMap);

        server = new RestServer(config, null);
        Assert.assertEquals("https://localhost:8443/", server.advertisedUrl().toString());
        server.stop();

        // Advertised URI from listeners with only SSL available
        configMap = new HashMap<>(baseWorkerProps());
        configMap.put(WorkerConfig.LISTENERS_CONFIG, "https://localhost:8443");
        config = new DistributedConfig(configMap);

        server = new RestServer(config, null);
        Assert.assertEquals("https://localhost:8443/", server.advertisedUrl().toString());
        server.stop();

        // Listener is overriden by advertised values
        configMap = new HashMap<>(baseWorkerProps());
        configMap.put(WorkerConfig.LISTENERS_CONFIG, "https://localhost:8443");
        configMap.put(WorkerConfig.REST_ADVERTISED_LISTENER_CONFIG, "http");
        configMap.put(WorkerConfig.REST_ADVERTISED_HOST_NAME_CONFIG, "somehost");
        configMap.put(WorkerConfig.REST_ADVERTISED_PORT_CONFIG, "10000");
        config = new DistributedConfig(configMap);

        server = new RestServer(config, null);
        Assert.assertEquals("http://somehost:10000/", server.advertisedUrl().toString());
        server.stop();

        // correct listener is chosen when https listener is configured before http listener and advertised listener is http
        configMap = new HashMap<>(baseWorkerProps());
        configMap.put(WorkerConfig.LISTENERS_CONFIG, "https://encrypted-localhost:42069,http://plaintext-localhost:4761");
        configMap.put(WorkerConfig.REST_ADVERTISED_LISTENER_CONFIG, "http");
        config = new DistributedConfig(configMap);
        server = new RestServer(config, null);
        Assert.assertEquals("http://plaintext-localhost:4761/", server.advertisedUrl().toString());
        server.stop();
    }

    @Test
    public void testOptionsDoesNotIncludeWadlOutput() throws IOException {
        Map<String, String> configMap = new HashMap<>(baseWorkerProps());
        DistributedConfig workerConfig = new DistributedConfig(configMap);

        doReturn(KAFKA_CLUSTER_ID).when(herder).kafkaClusterId();
        doReturn(plugins).when(herder).plugins();
        doReturn(Collections.emptyList()).when(plugins).newPlugins(Collections.emptyList(), workerConfig, ConnectRestExtension.class);

        server = new RestServer(workerConfig, null);
        server.initializeServer();
        server.initializeResources(herder);

        HttpOptions request = new HttpOptions("/connectors");
        request.addHeader("Content-Type", MediaType.WILDCARD);
        HttpResponse response = executeRequest(server.advertisedUrl(), request);
        Assert.assertEquals(MediaType.TEXT_PLAIN, response.getEntity().getContentType().getValue());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        response.getEntity().writeTo(baos);
        Assert.assertArrayEquals(
            request.getAllowedMethods(response).toArray(),
            new String(baos.toByteArray(), StandardCharsets.UTF_8).split(", ")
        );
    }

    public void checkCORSRequest(String corsDomain, String origin, String expectedHeader, String method)
        throws IOException {
        Map<String, String> workerProps = baseWorkerProps();
        workerProps.put(WorkerConfig.ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG, corsDomain);
        workerProps.put(WorkerConfig.ACCESS_CONTROL_ALLOW_METHODS_CONFIG, method);
        WorkerConfig workerConfig = new DistributedConfig(workerProps);

        doReturn(KAFKA_CLUSTER_ID).when(herder).kafkaClusterId();
        doReturn(plugins).when(herder).plugins();
        doReturn(Collections.emptyList()).when(plugins).newPlugins(Collections.emptyList(), workerConfig, ConnectRestExtension.class);
        doReturn(Arrays.asList("a", "b")).when(herder).connectors();

        server = new RestServer(workerConfig, null);
        server.initializeServer();
        server.initializeResources(herder);
        URI serverUrl = server.advertisedUrl();

        HttpRequest request = new HttpGet("/connectors");
        request.addHeader("Referer", origin + "/page");
        request.addHeader("Origin", origin);
        HttpResponse response = executeRequest(serverUrl, request);

        Assert.assertEquals(200, response.getStatusLine().getStatusCode());

        if (expectedHeader != null) {
            Assert.assertEquals(expectedHeader,
                response.getFirstHeader("Access-Control-Allow-Origin").getValue());
        }

        request = new HttpOptions("/connector-plugins/FileStreamSource/validate");
        request.addHeader("Referer", origin + "/page");
        request.addHeader("Origin", origin);
        request.addHeader("Access-Control-Request-Method", method);
        response = executeRequest(serverUrl, request);
        Assert.assertEquals(404, response.getStatusLine().getStatusCode());
        if (expectedHeader != null) {
            Assert.assertEquals(expectedHeader,
                response.getFirstHeader("Access-Control-Allow-Origin").getValue());
        }
        if (method != null) {
            Assert.assertEquals(method,
                response.getFirstHeader("Access-Control-Allow-Methods").getValue());
        }
    }

    @Test
    public void testStandaloneConfig() throws IOException  {
        Map<String, String> workerProps = baseWorkerProps();
        workerProps.put("offset.storage.file.filename", "/tmp");
        WorkerConfig workerConfig = new StandaloneConfig(workerProps);

        doReturn(KAFKA_CLUSTER_ID).when(herder).kafkaClusterId();
        doReturn(plugins).when(herder).plugins();
        doReturn(Collections.emptyList()).when(plugins).newPlugins(Collections.emptyList(), workerConfig, ConnectRestExtension.class);
        doReturn(Arrays.asList("a", "b")).when(herder).connectors();

        server = new RestServer(workerConfig, null);
        server.initializeServer();
        server.initializeResources(herder);
        HttpRequest request = new HttpGet("/connectors");
        HttpResponse response = executeRequest(server.advertisedUrl(), request);

        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testLoggersEndpointWithDefaults() throws IOException {
        Map<String, String> configMap = new HashMap<>(baseWorkerProps());
        DistributedConfig workerConfig = new DistributedConfig(configMap);

        doReturn(KAFKA_CLUSTER_ID).when(herder).kafkaClusterId();
        doReturn(plugins).when(herder).plugins();
        doReturn(Collections.emptyList()).when(plugins).newPlugins(Collections.emptyList(), workerConfig, ConnectRestExtension.class);

        // create some loggers in the process
        LoggerFactory.getLogger("a.b.c.s.W");

        server = new RestServer(workerConfig, null);
        server.initializeServer();
        server.initializeResources(herder);

        ObjectMapper mapper = new ObjectMapper();

        URI serverUrl = server.advertisedUrl();

        executePut(serverUrl, "/admin/loggers/a.b.c.s.W", "{\"level\": \"INFO\"}");

        String responseStr = executeGet(serverUrl, "/admin/loggers");
        Map<String, Map<String, ?>> loggers = mapper.readValue(responseStr, new TypeReference<Map<String, Map<String, ?>>>() {
        });
        assertNotNull("expected non null response for /admin/loggers" + prettyPrint(loggers), loggers);
        assertTrue("expect at least 1 logger. instead found " + prettyPrint(loggers), loggers.size() >= 1);
        assertEquals("expected to find logger a.b.c.s.W set to INFO level", loggers.get("a.b.c.s.W").get("level"), "INFO");
    }

    @Test
    public void testIndependentAdminEndpoint() throws IOException {
        Map<String, String> configMap = new HashMap<>(baseWorkerProps());
        configMap.put(ADMIN_LISTENERS_CONFIG, "http://localhost:0");

        DistributedConfig workerConfig = new DistributedConfig(configMap);

        doReturn(KAFKA_CLUSTER_ID).when(herder).kafkaClusterId();
        doReturn(plugins).when(herder).plugins();
        doReturn(Collections.emptyList()).when(plugins).newPlugins(Collections.emptyList(), workerConfig, ConnectRestExtension.class);

        // create some loggers in the process
        LoggerFactory.getLogger("a.b.c.s.W");
        LoggerFactory.getLogger("a.b.c.p.X");
        LoggerFactory.getLogger("a.b.c.p.Y");
        LoggerFactory.getLogger("a.b.c.p.Z");

        server = new RestServer(workerConfig, null);
        server.initializeServer();
        server.initializeResources(herder);

        assertNotEquals(server.advertisedUrl(), server.adminUrl());

        executeGet(server.adminUrl(), "/admin/loggers");

        HttpRequest request = new HttpGet("/admin/loggers");
        HttpResponse response = executeRequest(server.advertisedUrl(), request);
        Assert.assertEquals(404, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testDisableAdminEndpoint() throws IOException {
        Map<String, String> configMap = new HashMap<>(baseWorkerProps());
        configMap.put(ADMIN_LISTENERS_CONFIG, "");

        DistributedConfig workerConfig = new DistributedConfig(configMap);

        doReturn(KAFKA_CLUSTER_ID).when(herder).kafkaClusterId();
        doReturn(plugins).when(herder).plugins();
        doReturn(Collections.emptyList()).when(plugins).newPlugins(Collections.emptyList(), workerConfig, ConnectRestExtension.class);

        server = new RestServer(workerConfig, null);
        server.initializeServer();
        server.initializeResources(herder);

        assertNull(server.adminUrl());

        HttpRequest request = new HttpGet("/admin/loggers");
        HttpResponse response = executeRequest(server.advertisedUrl(), request);
        Assert.assertEquals(404, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testRequestLogs() throws IOException, InterruptedException {
        Map<String, String> configMap = new HashMap<>(baseWorkerProps());
        DistributedConfig workerConfig = new DistributedConfig(configMap);

        doReturn(KAFKA_CLUSTER_ID).when(herder).kafkaClusterId();
        doReturn(plugins).when(herder).plugins();
        doReturn(Collections.emptyList()).when(plugins).newPlugins(Collections.emptyList(), workerConfig, ConnectRestExtension.class);

        server = new RestServer(workerConfig, null);
        server.initializeServer();
        server.initializeResources(herder);

        LogCaptureAppender restServerAppender = LogCaptureAppender.createAndRegister();
        HttpRequest request = new HttpGet("/");
        HttpResponse response = executeRequest(server.advertisedUrl(), request);

        // Stop the server to flush all logs
        server.stop();

        Collection<String> logMessages = restServerAppender.getMessages();
        LogCaptureAppender.unregister(restServerAppender);
        restServerAppender.close();
        String expectedlogContent = "\"GET / HTTP/1.1\" " + response.getStatusLine().getStatusCode();
        assertTrue(logMessages.stream().anyMatch(logMessage -> logMessage.contains(expectedlogContent)));
    }

    @Test
    public void testValidCustomizedHttpResponseHeaders() throws IOException  {
        String headerConfig =
                "add X-XSS-Protection: 1; mode=block, \"add Cache-Control: no-cache, no-store, must-revalidate\"";
        Map<String, String> expectedHeaders = new HashMap<>();
        expectedHeaders.put("X-XSS-Protection", "1; mode=block");
        expectedHeaders.put("Cache-Control", "no-cache, no-store, must-revalidate");
        checkCustomizedHttpResponseHeaders(headerConfig, expectedHeaders);
    }

    @Test
    public void testDefaultCustomizedHttpResponseHeaders() throws IOException  {
        String headerConfig = "";
        Map<String, String> expectedHeaders = new HashMap<>();
        checkCustomizedHttpResponseHeaders(headerConfig, expectedHeaders);
    }

    private void checkCustomizedHttpResponseHeaders(String headerConfig, Map<String, String> expectedHeaders)
            throws IOException  {
        Map<String, String> workerProps = baseWorkerProps();
        workerProps.put("offset.storage.file.filename", "/tmp");
        workerProps.put(WorkerConfig.RESPONSE_HTTP_HEADERS_CONFIG, headerConfig);
        WorkerConfig workerConfig = new DistributedConfig(workerProps);

        doReturn(KAFKA_CLUSTER_ID).when(herder).kafkaClusterId();
        doReturn(plugins).when(herder).plugins();
        doReturn(Collections.emptyList()).when(plugins).newPlugins(Collections.emptyList(), workerConfig, ConnectRestExtension.class);
        doReturn(Arrays.asList("a", "b")).when(herder).connectors();

        server = new RestServer(workerConfig, null);
        server.initializeServer();
        server.initializeResources(herder);
        HttpRequest request = new HttpGet("/connectors");
        HttpResponse response = executeRequest(server.advertisedUrl(), request);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        if (!headerConfig.isEmpty()) {
            expectedHeaders.forEach((k, v) ->
                    Assert.assertEquals(response.getFirstHeader(k).getValue(), v));
        } else {
            Assert.assertNull(response.getFirstHeader("X-Frame-Options"));
        }
    }

    private String executeGet(URI serverUrl, String endpoint) throws IOException {
        HttpRequest request = new HttpGet(endpoint);
        HttpResponse response = executeRequest(serverUrl, request);

        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        return new BasicResponseHandler().handleResponse(response);
    }

    private String executePut(URI serverUrl, String endpoint, String jsonBody) throws IOException {
        HttpPut request = new HttpPut(endpoint);
        StringEntity entity = new StringEntity(jsonBody, StandardCharsets.UTF_8.name());
        entity.setContentType("application/json");
        request.setEntity(entity);
        HttpResponse response = executeRequest(serverUrl, request);

        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        return new BasicResponseHandler().handleResponse(response);
    }

    private HttpResponse executeRequest(URI serverUrl, HttpRequest request) throws IOException {
        HttpHost httpHost = new HttpHost(serverUrl.getHost(), serverUrl.getPort());
        CloseableHttpResponse response = httpClient.execute(httpHost, request);
        responses.add(response);
        return response;
    }

    private static String prettyPrint(Map<String, ?> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(map);
    }
}
