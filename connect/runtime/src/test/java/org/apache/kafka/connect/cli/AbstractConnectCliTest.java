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
package org.apache.kafka.connect.cli;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.isolation.TestPlugins;
import org.apache.kafka.connect.runtime.rest.ConnectRestServer;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.RestServer;

import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.net.URI;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.spy;

public class AbstractConnectCliTest {

    /**
     * Verifies that createConfig is called after compareAndSwapWithDelegatingLoader in startConnect.
     * If the order is wrong, ConfigProvider classes in plugin.path cannot be loaded.
     */
    @Test
    public void testStartConnectEnforcesCorrectOrder() {
        ClassLoader originalTCCL = Thread.currentThread().getContextClassLoader();

        try {
            // Create worker props with ConfigProvider that's only in plugin.path
            Set<Path> pluginPaths = TestPlugins.pluginPath(TestPlugins.TestPlugin.SAMPLING_CONFIG_PROVIDER);
            String pluginPath = String.join(",", pluginPaths.stream().map(Path::toString).toList());

            Map<String, String> workerProps = createBaseWorkerProps(pluginPath);
            workerProps.put(WorkerConfig.CONFIG_PROVIDERS_CONFIG, "testProvider");
            String providerClassName = TestPlugins.TestPlugin.SAMPLING_CONFIG_PROVIDER.className();
            workerProps.put(WorkerConfig.CONFIG_PROVIDERS_CONFIG + ".testProvider.class", providerClassName);

            // Use a restricted classloader that cannot find the ConfigProvider class
            ClassLoader restrictedClassLoader = new RestrictedClassLoader(providerClassName);
            Thread.currentThread().setContextClassLoader(restrictedClassLoader);

            // Verify the restricted classloader cannot load the ConfigProvider class
            assertThrows(ClassNotFoundException.class, () ->
                restrictedClassLoader.loadClass(providerClassName));

            // Config creation should fail when ConfigProvider class cannot be loaded
            assertThrows(ConfigException.class, () -> new DistributedConfig(workerProps));

            // Call startConnect and verify the order is correct
            TestConnectCli testConnectCli = new TestConnectCli();

            // Mock ConnectRestServer to avoid actual server initialization
            try (MockedConstruction<ConnectRestServer> mockRestServer = mockConstruction(
                ConnectRestServer.class,
                (mock, context) -> {
                    doReturn(URI.create("http://localhost:8083")).when(mock).advertisedUrl();
                    doNothing().when(mock).initializeServer();
                })) {

                // If order is correct, createConfig succeeds and we reach createHerder which throws ExpectedException
                assertThrows(ExpectedException.class, () -> testConnectCli.startConnect(workerProps));
            }
        } finally {
            Thread.currentThread().setContextClassLoader(originalTCCL);
        }
    }

    private Map<String, String> createBaseWorkerProps(String pluginPath) {
        Map<String, String> props = new HashMap<>();
        props.put(WorkerConfig.PLUGIN_PATH_CONFIG, pluginPath);
        props.put("bootstrap.servers", "localhost:9092");
        props.put(DistributedConfig.GROUP_ID_CONFIG, "test-connect-cluster");
        props.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "connect-configs");
        props.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "connect-offsets");
        props.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "connect-status");
        props.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        props.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        return props;
    }

    /**
     * Test implementation that calls the parent's startConnect to verify correct order.
     */
    private static class TestConnectCli extends AbstractConnectCli<Herder, DistributedConfig> {
        TestConnectCli() {
            super();
        }

        @Override
        protected String usage() {
            return "test";
        }

        @Override
        protected Herder createHerder(DistributedConfig config, String workerId, Plugins plugins,
                                      ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy,
                                      RestServer restServer,
                                      RestClient restClient) {
            // Reaching createHerder means createConfig succeeded, indicating correct order was maintained
            throw new ExpectedException();
        }

        @Override
        protected DistributedConfig createConfig(Map<String, String> workerProps) {
            DistributedConfig config = new DistributedConfig(workerProps);
            // Mock kafkaClusterId() to avoid connecting to Kafka broker
            DistributedConfig spyConfig = spy(config);
            doReturn("test-cluster-id").when(spyConfig).kafkaClusterId();
            return spyConfig;
        }
    }

    /**
     * ClassLoader that cannot load a specific class, simulating plugin classes only in plugin.path.
     */
    private static class RestrictedClassLoader extends ClassLoader {
        private final String restrictedClassName;
        private final ClassLoader systemLoader;

        RestrictedClassLoader(String restrictedClassName) {
            super(null); // No parent to prevent delegation
            this.restrictedClassName = restrictedClassName;
            this.systemLoader = ClassLoader.getSystemClassLoader();
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            // Block the restricted class to simulate it being only in plugin.path, not classpath.
            if (name.equals(restrictedClassName)) {
                throw new ClassNotFoundException("Class " + name + " not found (restricted for testing)");
            }
            // For other classes, delegate to system classloader
            return systemLoader.loadClass(name);
        }
    }

    /**
     * Exception thrown by createHerder to indicate that createConfig succeeded and correct order was maintained.
     * If this exception is thrown, it means compareAndSwapWithDelegatingLoader was called before createConfig.
     */
    private static class ExpectedException extends RuntimeException {
        ExpectedException() {
            super("Expected exception, createConfig succeeded");
        }
    }
}
