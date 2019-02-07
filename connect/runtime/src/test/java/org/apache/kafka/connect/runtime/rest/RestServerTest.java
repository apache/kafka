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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.HerderProvider;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.clients.HttpClient;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.MockStrict;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
public class RestServerTest {

    @MockStrict
    private Herder herder;
    @MockStrict
    private Plugins plugins;
    private RestServer server;

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
        server.stop();
    }

    @SuppressWarnings("deprecation")
    private Map<String, String> baseWorkerProps() {
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "status-topic");
        workerProps.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "config-topic");
        workerProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        workerProps.put(DistributedConfig.GROUP_ID_CONFIG, "connect-test-group");
        workerProps.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put(WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put(WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "connect-offsets");

        return workerProps;
    }

    @Test
    public void testCORSEnabled() {
        checkCORSRequest("*", "http://bar.com", "http://bar.com", "PUT");
    }

    @Test
    public void testCORSDisabled() {
        checkCORSRequest("", "http://bar.com", null, null);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testParseListeners() {
        // Use listeners field
        Map<String, String> configMap = new HashMap<>(baseWorkerProps());
        configMap.put(WorkerConfig.LISTENERS_CONFIG, "http://localhost:8080,https://localhost:8443");
        DistributedConfig config = new DistributedConfig(configMap);

        server = new RestServer(config);
        Assert.assertArrayEquals(new String[] {"http://localhost:8080", "https://localhost:8443"}, server.parseListeners().toArray());

        // Build listener from hostname and port
        configMap = new HashMap<>(baseWorkerProps());
        configMap.put(WorkerConfig.REST_HOST_NAME_CONFIG, "my-hostname");
        configMap.put(WorkerConfig.REST_PORT_CONFIG, "8080");
        config = new DistributedConfig(configMap);
        server = new RestServer(config);
        Assert.assertArrayEquals(new String[] {"http://my-hostname:8080"}, server.parseListeners().toArray());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testAdvertisedUri() {
        // Advertised URI from listeenrs without protocol
        Map<String, String> configMap = new HashMap<>(baseWorkerProps());
        configMap.put(WorkerConfig.LISTENERS_CONFIG, "http://localhost:8080,https://localhost:8443");
        DistributedConfig config = new DistributedConfig(configMap);

        server = new RestServer(config);
        Assert.assertEquals("http://localhost:8080/", server.advertisedUrl().toString());

        // Advertised URI from listeners with protocol
        configMap = new HashMap<>(baseWorkerProps());
        configMap.put(WorkerConfig.LISTENERS_CONFIG, "http://localhost:8080,https://localhost:8443");
        configMap.put(WorkerConfig.REST_ADVERTISED_LISTENER_CONFIG, "https");
        config = new DistributedConfig(configMap);

        server = new RestServer(config);
        Assert.assertEquals("https://localhost:8443/", server.advertisedUrl().toString());

        // Advertised URI from listeners with only SSL available
        configMap = new HashMap<>(baseWorkerProps());
        configMap.put(WorkerConfig.LISTENERS_CONFIG, "https://localhost:8443");
        config = new DistributedConfig(configMap);

        server = new RestServer(config);
        Assert.assertEquals("https://localhost:8443/", server.advertisedUrl().toString());

        // Listener is overriden by advertised values
        configMap = new HashMap<>(baseWorkerProps());
        configMap.put(WorkerConfig.LISTENERS_CONFIG, "https://localhost:8443");
        configMap.put(WorkerConfig.REST_ADVERTISED_LISTENER_CONFIG, "http");
        configMap.put(WorkerConfig.REST_ADVERTISED_HOST_NAME_CONFIG, "somehost");
        configMap.put(WorkerConfig.REST_ADVERTISED_PORT_CONFIG, "10000");
        config = new DistributedConfig(configMap);

        server = new RestServer(config);
        Assert.assertEquals("http://somehost:10000/", server.advertisedUrl().toString());

        // listener from hostname and port
        configMap = new HashMap<>(baseWorkerProps());
        configMap.put(WorkerConfig.REST_HOST_NAME_CONFIG, "my-hostname");
        configMap.put(WorkerConfig.REST_PORT_CONFIG, "8080");
        config = new DistributedConfig(configMap);
        server = new RestServer(config);
        Assert.assertEquals("http://my-hostname:8080/", server.advertisedUrl().toString());
    }

    @Test
    public void testOptionsDoesNotIncludeWadlOutput() {
        Map<String, String> configMap = new HashMap<>(baseWorkerProps());
        DistributedConfig workerConfig = new DistributedConfig(configMap);

        EasyMock.expect(herder.plugins()).andStubReturn(plugins);
        EasyMock.expect(plugins.newPlugins(Collections.emptyList(),
            workerConfig,
            ConnectRestExtension.class))
            .andStubReturn(Collections.emptyList());
        PowerMock.replayAll();

        server = new RestServer(workerConfig);
        server.start(new HerderProvider(herder), herder.plugins());

        HttpClient httpClient = new HttpClient(String.valueOf(server.advertisedUrl()));
        Response response = httpClient.executeOptions("/connectors", MediaType.WILDCARD_TYPE, Collections.emptyMap());
        Assert.assertEquals(MediaType.TEXT_PLAIN_TYPE, response.getMediaType());
        Assert.assertArrayEquals(
            response.getAllowedMethods().toArray(new String[0]),
            response.readEntity(String.class).split(", ")
        );

        PowerMock.verifyAll();
    }

    public void checkCORSRequest(String corsDomain, String origin, String expectedHeader, String method) {
        Map<String, String> workerProps = baseWorkerProps();
        workerProps.put(WorkerConfig.ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG, corsDomain);
        workerProps.put(WorkerConfig.ACCESS_CONTROL_ALLOW_METHODS_CONFIG, method);
        WorkerConfig workerConfig = new DistributedConfig(workerProps);

        EasyMock.expect(herder.plugins()).andStubReturn(plugins);
        EasyMock.expect(plugins.newPlugins(Collections.emptyList(),
                                           workerConfig,
                                           ConnectRestExtension.class))
            .andStubReturn(Collections.emptyList());

        final Capture<Callback<Collection<String>>> connectorsCallback = EasyMock.newCapture();
        herder.connectors(EasyMock.capture(connectorsCallback));
        PowerMock.expectLastCall().andAnswer(() -> {
            connectorsCallback.getValue().onCompletion(null, Arrays.asList("a", "b"));
            return null;
        });

        PowerMock.replayAll();


        server = new RestServer(workerConfig);
        server.start(new HerderProvider(herder), herder.plugins());

        HttpClient httpClient = new HttpClient(String.valueOf(server.advertisedUrl()));
        Response response = httpClient.executeGet("/connectors",
            new HashMap<String, String>() {{
                    put("Referer", origin + "/page");
                    put("Origin", origin);
                }});
        assertEquals(200, response.getStatus());

        assertEquals(expectedHeader, response.getHeaderString("Access-Control-Allow-Origin"));

        response = httpClient.executeOptions("/connector-plugins/FileStreamSource/validate",
            new HashMap<String, String>() {{
                    put("Referer", origin + "/page");
                    put("Origin", origin);
                    put("Access-Control-Request-Method", method);
                }});
        assertEquals(404, response.getStatus());
        assertEquals(expectedHeader, response.getHeaderString("Access-Control-Allow-Origin"));
        assertEquals(method, response.getHeaderString("Access-Control-Allow-Methods"));
        PowerMock.verifyAll();
    }
}
