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
package org.apache.kafka.connect.tools;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.List;
import java.util.Map;

/**
 * Mock sink implementation which delegates to {@link MockConnector}.
 */
public class MockSinkConnector extends SinkConnector {

    private MockConnector delegate = new MockConnector();

    @Override
    public void initialize(ConnectorContext ctx) {
        delegate.initialize(ctx);
    }

    @Override
    public void initialize(ConnectorContext ctx, List<Map<String, String>> taskConfigs) {
        delegate.initialize(ctx, taskConfigs);
    }

    @Override
    public void reconfigure(Map<String, String> props) {
        delegate.reconfigure(props);
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        return delegate.validate(connectorConfigs);
    }

    @Override
    public String version() {
        return delegate.version();
    }

    @Override
    public void start(Map<String, String> props) {
        delegate.start(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MockSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return delegate.taskConfigs(maxTasks);
    }

    @Override
    public void stop() {
        delegate.stop();
    }

    @Override
    public ConfigDef config() {
        return delegate.config();
    }
}
