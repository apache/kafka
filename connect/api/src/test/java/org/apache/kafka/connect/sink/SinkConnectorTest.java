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
package org.apache.kafka.connect.sink;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.ConnectorTest;
import org.apache.kafka.connect.connector.Task;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class SinkConnectorTest extends ConnectorTest {

    @Override
    protected TestSinkConnectorContext createContext() {
        return new TestSinkConnectorContext();
    }

    @Override
    protected TestSinkConnector createConnector() {
        return new TestSinkConnector();
    }

    private static class TestSinkConnectorContext implements SinkConnectorContext {

        @Override
        public void requestTaskReconfiguration() {
            // Unexpected in these tests
            throw new UnsupportedOperationException();
        }

        @Override
        public void raiseError(Exception e) {
            // Unexpected in these tests
            throw new UnsupportedOperationException();
        }
    }

    protected static class TestSinkConnector extends SinkConnector implements ConnectorTest.AssertableConnector {

        public static final String VERSION = "an entirely different version";

        private boolean initialized;
        private List<Map<String, String>> taskConfigs;
        private Map<String, String> props;
        private boolean started;
        private boolean stopped;

        @Override
        public String version() {
            return VERSION;
        }

        @Override
        public void initialize(ConnectorContext ctx) {
            super.initialize(ctx);
            initialized = true;
            this.taskConfigs = null;
        }

        @Override
        public void initialize(ConnectorContext ctx, List<Map<String, String>> taskConfigs) {
            super.initialize(ctx, taskConfigs);
            initialized = true;
            this.taskConfigs = taskConfigs;
        }

        @Override
        public void start(Map<String, String> props) {
            this.props = props;
            started = true;
        }

        @Override
        public Class<? extends Task> taskClass() {
            return null;
        }

        @Override
        public List<Map<String, String>> taskConfigs(int maxTasks) {
            return null;
        }

        @Override
        public void stop() {
            stopped = true;
        }

        @Override
        public ConfigDef config() {
            return new ConfigDef()
                    .define("required", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "required docs")
                    .define("optional", ConfigDef.Type.STRING, "defaultVal", ConfigDef.Importance.HIGH, "optional docs");
        }

        @Override
        public void assertContext(ConnectorContext expected) {
            assertSame(expected, context);
            assertSame(expected, context());
        }

        @Override
        public void assertInitialized() {
            assertTrue(initialized);
        }

        @Override
        public void assertTaskConfigs(List<Map<String, String>> expectedTaskConfigs) {
            assertSame(expectedTaskConfigs, taskConfigs);
        }

        @Override
        public void assertStarted(boolean expected) {
            assertEquals(expected, started);
        }

        @Override
        public void assertStopped(boolean expected) {
            assertEquals(expected, stopped);
        }

        @Override
        public void assertProperties(Map<String, String> expected) {
            assertSame(expected, props);
        }
    }
}