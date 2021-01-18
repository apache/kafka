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
package org.apache.kafka.connect.connector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class ConnectorTest {

    protected ConnectorContext context;
    protected Connector connector;
    protected AssertableConnector assertableConnector;

    @BeforeEach
    public void beforeEach() {
        connector = createConnector();
        context = createContext();
        assertableConnector = (AssertableConnector) connector;
    }

    @Test
    public void shouldInitializeContext() {
        connector.initialize(context);
        assertableConnector.assertInitialized();
        assertableConnector.assertContext(context);
        assertableConnector.assertTaskConfigs(null);
    }

    @Test
    public void shouldInitializeContextWithTaskConfigs() {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        connector.initialize(context, taskConfigs);
        assertableConnector.assertInitialized();
        assertableConnector.assertContext(context);
        assertableConnector.assertTaskConfigs(taskConfigs);
    }

    @Test
    public void shouldStopAndStartWhenReconfigure() {
        Map<String, String> props = new HashMap<>();
        connector.initialize(context);
        assertableConnector.assertContext(context);
        assertableConnector.assertStarted(false);
        assertableConnector.assertStopped(false);
        connector.reconfigure(props);
        assertableConnector.assertStarted(true);
        assertableConnector.assertStopped(true);
        assertableConnector.assertProperties(props);
    }

    protected abstract ConnectorContext createContext();

    protected abstract Connector createConnector();

    public interface AssertableConnector {

        void assertContext(ConnectorContext expected);

        void assertInitialized();

        void assertTaskConfigs(List<Map<String, String>> expectedTaskConfigs);

        void assertStarted(boolean expected);

        void assertStopped(boolean expected);

        void assertProperties(Map<String, String> expected);
    }
}