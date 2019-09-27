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
package org.apache.kafka.connect.runtime.rest.resources;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.runtime.WorkerConfig.ADMIN_LISTENERS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests to test admin endpoints and {@link org.apache.kafka.connect.runtime.rest.admin.LoggingResource}.
 */
@Category(IntegrationTest.class)
public class LoggingResourceIntegrationTest {

    private EmbeddedConnectCluster connect;

    @After
    public void close() {
        connect.stop();
    }

    @Test
    public void testAdminEndpoint() throws IOException {
        // setup Connect worker properties
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put(OFFSET_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(5_000));

        // build a Connect cluster backed by Kafka and Zk
        connect = new EmbeddedConnectCluster.Builder()
                .name("connect-cluster")
                .numWorkers(1)
                .workerProps(workerProps)
                .build();

        // start the clusters
        connect.start();

        ObjectMapper mapper = new ObjectMapper();
        int statusCode;

        // create some loggers in the process
        LoggerFactory.getLogger("a.b.c.s.W");
        LoggerFactory.getLogger("a.b.c.p.X");
        LoggerFactory.getLogger("a.b.c.p.Y");
        LoggerFactory.getLogger("a.b.c.p.Z");

        // set log levels for individual loggers
        statusCode = connect.executePut(connect.endpointForResource("admin/loggers/a.b.c.s.W"), "{\"level\": \"INFO\"}");
        assertEquals("bad response code found when setting log level of a.b.c.s.w", 200, statusCode);
        statusCode = connect.executePut(connect.endpointForResource("admin/loggers/a.b.c.p.X"), "{\"level\": \"INFO\"}");
        assertEquals("bad response code found when setting log level of a.b.c.p.X", 200, statusCode);
        statusCode = connect.executePut(connect.endpointForResource("admin/loggers/a.b.c.p.Y"), "{\"level\": \"INFO\"}");
        assertEquals("bad response code found when setting log level of a.b.c.p.Y", 200, statusCode);
        statusCode = connect.executePut(connect.endpointForResource("admin/loggers/a.b.c.p.Z"), "{\"level\": \"INFO\"}");
        assertEquals("bad response code found when setting log level of a.b.c.p.Z", 200, statusCode);

        String url = connect.endpointForResource("admin/loggers");
        Map<String, Map<String, ?>> loggers = mapper.readValue(connect.executeGet(url), new TypeReference<Map<String, Map<String, ?>>>() {
        });
        assertNotNull("expected non null response for /admin/loggers" + prettyPrint(loggers), loggers);
        assertTrue("expect at least 4 loggers. instead found " + prettyPrint(loggers), loggers.size() >= 4);

        assertEquals("expected to find logger a.b.c.s.W set to INFO level", loggers.get("a.b.c.s.W").get("level"), "INFO");
        assertEquals("expected to find logger a.b.c.p.X set to INFO level", loggers.get("a.b.c.p.X").get("level"), "INFO");
        assertEquals("expected to find logger a.b.c.p.Y set to INFO level", loggers.get("a.b.c.p.Y").get("level"), "INFO");
        assertEquals("expected to find logger a.b.c.p.Z set to INFO level", loggers.get("a.b.c.p.Z").get("level"), "INFO");

        // set log levels for ancestor
        statusCode = connect.executePut(connect.endpointForResource("admin/loggers/a.b.c.p"), "{\"level\": \"DEBUG\"}");
        assertEquals("bad response code found when setting log level of a.b.c.p to DEBUG", 200, statusCode);

        loggers = mapper.readValue(connect.executeGet(url), new TypeReference<Map<String, ?>>() {
        });
        assertEquals("expected to find logger a.b.c.s.W set to INFO level", loggers.get("a.b.c.s.W").get("level"), "INFO");
        assertEquals("expected to find logger a.b.c.p.X set to DEBUG level", loggers.get("a.b.c.p.X").get("level"), "DEBUG");
        assertEquals("expected to find logger a.b.c.p.Y set to DEBUG level", loggers.get("a.b.c.p.Y").get("level"), "DEBUG");
        assertEquals("expected to find logger a.b.c.p.Z set to DEBUG level", loggers.get("a.b.c.p.Z").get("level"), "DEBUG");

        // set log levels for ancestor
        connect.executePut(connect.endpointForResource("admin/loggers/a.b.c"), "{\"level\": \"ERROR\"}");
        assertEquals("bad response code found when setting log level of a.b.c to ERROR", 200, statusCode);

        loggers = mapper.readValue(connect.executeGet(url), new TypeReference<Map<String, ?>>() {
        });
        assertEquals("expected to find logger a.b.c.s.W set to ERROR level", loggers.get("a.b.c.s.W").get("level"), "ERROR");
        assertEquals("expected to find logger a.b.c.p.X set to ERROR level", loggers.get("a.b.c.p.X").get("level"), "ERROR");
        assertEquals("expected to find logger a.b.c.p.Y set to ERROR level", loggers.get("a.b.c.p.Y").get("level"), "ERROR");
        assertEquals("expected to find logger a.b.c.p.Z set to ERROR level", loggers.get("a.b.c.p.Z").get("level"), "ERROR");
    }

    @Test
    public void testIndependentAdminEndpoint() throws Exception {
        // setup Connect worker properties
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put(ADMIN_LISTENERS_CONFIG, "http://localhost:0");

        // build a Connect cluster backed by Kafka and Zk
        connect = new EmbeddedConnectCluster.Builder()
                .name("connect-cluster")
                .numWorkers(1)
                .workerProps(workerProps)
                .build();

        // start the clusters
        connect.start();

        ObjectMapper mapper = new ObjectMapper();

        String url = connect.adminEndpoint("admin/loggers");
        Map<String, ?> loggers = mapper.readValue(connect.executeGet(url), new TypeReference<Map<String, ?>>() {
        });
        assertNotNull("expected non null response for /admin/loggers" + prettyPrint(loggers), loggers);

        assertNotEquals("admin endpoints should be different from regular endpoints",
                connect.adminEndpoint(""), connect.endpointForResource(""));
    }

    @Test(expected = IOException.class)
    public void testDisableAdminEndpoint() throws Exception {
        // setup Connect worker properties
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put(ADMIN_LISTENERS_CONFIG, "");

        // build a Connect cluster backed by Kafka and Zk
        connect = new EmbeddedConnectCluster.Builder()
                .name("connect-cluster")
                .numWorkers(1)
                .workerProps(workerProps)
                .build();

        // start the clusters
        connect.start();

        // attempt to get a non-null admin endpoint
        connect.adminEndpoint("admin/loggers");
    }

    private static String prettyPrint(Map<String, ?> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(map);
    }
}
