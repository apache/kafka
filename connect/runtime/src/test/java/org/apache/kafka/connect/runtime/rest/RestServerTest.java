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

import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.util.Callback;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.MockStrict;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
public class RestServerTest {

    @MockStrict
    private Herder herder;
    private RestServer server;

    @After
    public void tearDown() {
        server.stop();
    }

    private Map<String, String> baseWorkerProps() {
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.key.converter.schemas.enable", "false");
        workerProps.put("internal.value.converter.schemas.enable", "false");
        workerProps.put("offset.storage.file.filename", "/tmp/connect.offsets");
        return workerProps;
    }

    @Test
    public void testCORS() {
        // To be able to set the Origin, we need to toggle this flag
        System.setProperty("sun.net.http.allowRestrictedHeaders", "true");

        final Capture<Callback<Collection<String>>> connectorsCallback = EasyMock.newCapture();
        herder.connectors(EasyMock.capture(connectorsCallback));
        PowerMock.expectLastCall().andAnswer(new IAnswer<Object>() {
            @Override
            public Object answer() throws Throwable {
                connectorsCallback.getValue().onCompletion(null, Arrays.asList("a", "b"));
                return null;
            }
        });
        PowerMock.replayAll();

        Map<String, String> workerProps = baseWorkerProps();
        workerProps.put(WorkerConfig.ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG, "*");
        WorkerConfig workerConfig = new StandaloneConfig(workerProps);
        server = new RestServer(workerConfig);
        server.start(herder);

        Response response = request("/connectors")
                .header("Referer", "http://bar.com/page")
                .header("Origin", "http://bar.com")
                .get();
        assertEquals(200, response.getStatus());
        assertEquals("http://bar.com", response.getHeaderString("Access-Control-Allow-Origin"));
        PowerMock.verifyAll();
    }

    protected Invocation.Builder request(String path) {
        return request(path, null, null, null);
    }

    protected Invocation.Builder request(String path, Map<String, String> queryParams) {
        return request(path, null, null, queryParams);
    }

    protected Invocation.Builder request(String path, String templateName, Object templateValue) {
        return request(path, templateName, templateValue, null);
    }

    protected Invocation.Builder request(String path, String templateName, Object templateValue,
                                         Map<String, String> queryParams) {
        Client client = ClientBuilder.newClient();
        WebTarget target;
        URI pathUri = null;
        try {
            pathUri = new URI(path);
        } catch (URISyntaxException e) {
            // Ignore, use restConnect and assume this is a valid path part
        }
        if (pathUri != null && pathUri.isAbsolute()) {
            target = client.target(path);
        } else {
            target = client.target(server.advertisedUrl()).path(path);
        }
        if (templateName != null && templateValue != null) {
            target = target.resolveTemplate(templateName, templateValue);
        }
        if (queryParams != null) {
            for (Map.Entry<String, String> queryParam : queryParams.entrySet()) {
                target = target.queryParam(queryParam.getKey(), queryParam.getValue());
            }
        }
        return target.request();
    }
}
