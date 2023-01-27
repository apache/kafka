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
package org.apache.kafka.connect.runtime.isolation;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;

import java.util.List;
import java.util.Map;

public abstract class IsolatedConnector<P extends Connector> extends IsolatedPlugin<P> {

    IsolatedConnector(Plugins plugins, P delegate, PluginType type) {
        super(plugins, delegate, type);
    }

    public String version() throws Exception {
        return isolate(delegate::version);
    }

    public void initialize(ConnectorContext ctx) throws Exception {
        isolateV(delegate::initialize, ctx);
    }

    public void initialize(ConnectorContext ctx, List<Map<String, String>> taskConfigs) throws Exception {
        isolateV(delegate::initialize, ctx, taskConfigs);
    }

    public void reconfigure(Map<String, String> props) throws Exception {
        isolateV(delegate::reconfigure, props);
    }

    public Config validate(Map<String, String> connectorConfigs) throws Exception {
        return isolate(delegate::validate, connectorConfigs);
    }

    public void start(Map<String, String> props) throws Exception {
        isolateV(delegate::start, props);
    }

    public Class<? extends Task> taskClass() throws Exception {
        return isolate(delegate::taskClass);
    }

    public List<Map<String, String>> taskConfigs(int maxTasks) throws Exception {
        return isolate(delegate::taskConfigs, maxTasks);
    }

    public void stop() throws Exception {
        isolateV(delegate::stop);
    }

    public ConfigDef config() throws Exception {
        return isolate(delegate::config);
    }
}
