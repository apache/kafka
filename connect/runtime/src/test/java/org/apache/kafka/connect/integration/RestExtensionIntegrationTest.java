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
package org.apache.kafka.connect.integration;

import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.rest.ConnectRestExtensionContext;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.REST_EXTENSION_CLASSES_CONFIG;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A simple integration test to ensure that REST extensions are registered correctly.
 */
@Category(IntegrationTest.class)
public class RestExtensionIntegrationTest {

  private static final int NUM_WORKERS = 3;

  private EmbeddedConnectCluster connect;

  @Test
  public void testImmediateRequestForListOfConnectors() throws IOException {
    // setup Connect worker properties
    Map<String, String> workerProps = new HashMap<>();
    workerProps.put(REST_EXTENSION_CLASSES_CONFIG, IntegrationTestRestExtension.class.getName());

    // build a Connect cluster backed by Kafka and Zk
    connect = new EmbeddedConnectCluster.Builder()
        .name("connect-cluster")
        .numWorkers(NUM_WORKERS)
        .numBrokers(1)
        .workerProps(workerProps)
        .build();

    // start the clusters
    connect.start();
  }

  @After
  public void close() {
    // stop all Connect, Kafka and Zk threads.
    connect.stop();
  }

  public static class IntegrationTestRestExtension implements ConnectRestExtension {
    @Override
    public void register(ConnectRestExtensionContext restPluginContext) {
      restPluginContext.clusterState().connectors();
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public String version() {
      return "test";
    }
  }
}
