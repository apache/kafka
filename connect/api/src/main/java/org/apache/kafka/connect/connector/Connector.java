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

package org.apache.kafka.connect.connector;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * Connectors manage integration of Kafka Connect with another system, either as an input that ingests
 * data into Kafka or an output that passes data to an external system. Implementations should
 * not use this class directly; they should inherit from SourceConnector or SinkConnector.
 * </p>
 * <p>
 * Connectors have two primary tasks. First, given some configuration, they are responsible for
 * creating configurations for a set of {@link Task}s that split up the data processing. For
 * example, a database Connector might create Tasks by dividing the set of tables evenly among
 * tasks. Second, they are responsible for monitoring inputs for changes that require
 * reconfiguration and notifying the Kafka Connect runtime via the ConnectorContext. Continuing the
 * previous example, the connector might periodically check for new tables and notify Kafka Connect of
 * additions and deletions. Kafka Connect will then request new configurations and update the running
 * Tasks.
 * </p>
 */
public abstract class Connector {

    protected ConnectorContext context;

    /**
     * Get the version of this connector.
     *
     * @return the version, formatted as a String
     */
    public abstract String version();

    /**
     * Initialize this connector, using the provided ConnectorContext to notify the runtime of
     * input configuration changes.
     * @param ctx context object used to interact with the Kafka Connect runtime
     */
    public void initialize(ConnectorContext ctx) {
        context = ctx;
    }

    /**
     * <p>
     * Initialize this connector, using the provided ConnectorContext to notify the runtime of
     * input configuration changes and using the provided set of Task configurations.
     * This version is only used to recover from failures.
     * </p>
     * <p>
     * The default implementation ignores the provided Task configurations. During recovery, Kafka Connect will request
     * an updated set of configurations and update the running Tasks appropriately. However, Connectors should
     * implement special handling of this case if it will avoid unnecessary changes to running Tasks.
     * </p>
     *
     * @param ctx context object used to interact with the Kafka Connect runtime
     * @param taskConfigs existing task configurations, which may be used when generating new task configs to avoid
     *                    churn in partition to task assignments
     */
    public void initialize(ConnectorContext ctx, List<Map<String, String>> taskConfigs) {
        context = ctx;
        // Ignore taskConfigs. May result in more churn of tasks during recovery if updated configs
        // are very different, but reduces the difficulty of implementing a Connector
    }

    /**
     * Start this Connector. This method will only be called on a clean Connector, i.e. it has
     * either just been instantiated and initialized or {@link #stop()} has been invoked.
     *
     * @param props configuration settings
     */
    public abstract void start(Map<String, String> props);

    /**
     * Reconfigure this Connector. Most implementations will not override this, using the default
     * implementation that calls {@link #stop()} followed by {@link #start(Map)}.
     * Implementations only need to override this if they want to handle this process more
     * efficiently, e.g. without shutting down network connections to the external system.
     *
     * @param props new configuration settings
     */
    public void reconfigure(Map<String, String> props) {
        stop();
        start(props);
    }

    /**
     * Returns the Task implementation for this Connector.
     */
    public abstract Class<? extends Task> taskClass();

    /**
     * Returns a set of configurations for Tasks based on the current configuration,
     * producing at most count configurations.
     *
     * @param maxTasks maximum number of configurations to generate
     * @return configurations for Tasks
     */
    public abstract List<Map<String, String>> taskConfigs(int maxTasks);

    /**
     * Stop this connector.
     */
    public abstract void stop();

    /**
     * Validate the connector configuration values against configuration definitions.
     * @param connectorConfigs the provided configuration values
     * @return List of Config, each Config contains the updated configuration information given
     * the current configuration values.
     */
    public Config validate(Map<String, String> connectorConfigs) {
        ConfigDef configDef = config();
        List<ConfigValue> configValues = configDef.validate(connectorConfigs);
        return new Config(configValues);
    }

    /**
     * Define the configuration for the connector.
     * @return The ConfigDef for this connector.
     */
    public abstract ConfigDef config();
}
