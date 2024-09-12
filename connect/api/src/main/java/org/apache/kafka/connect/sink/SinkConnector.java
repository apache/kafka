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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.connector.Connector;

import java.util.Map;

/**
 * SinkConnectors implement the Connector interface to send Kafka data to another system.
 * <p>Kafka Connect may discover implementations of this interface using the Java {@link java.util.ServiceLoader} mechanism.
 * To support this, implementations of this interface should also contain a service provider configuration file in
 * {@code META-INF/services/org.apache.kafka.connect.sink.SinkConnector}.
 */
public abstract class SinkConnector extends Connector {

    /**
     * <p>
     * Configuration key for the list of input topics for this connector.
     * </p>
     * <p>
     * Usually this setting is only relevant to the Kafka Connect framework, but is provided here for
     * the convenience of Connector developers if they also need to know the set of topics.
     * </p>
     */
    public static final String TOPICS_CONFIG = "topics";

    @Override
    protected SinkConnectorContext context() {
        return (SinkConnectorContext) context;
    }

    /**
     * Invoked when users request to manually alter/reset the offsets for this connector via the Connect worker's REST
     * API. Connectors that manage offsets externally can propagate offset changes to their external system in this
     * method. Connectors may also validate these offsets if, for example, an offset is out of range for what can be
     * feasibly written to the external system.
     * <p>
     * Connectors that neither manage offsets externally nor require custom offset validation need not implement this
     * method beyond simply returning {@code true}.
     * <p>
     * User requests to alter/reset offsets will be handled by the Connect runtime and will be reflected in the offsets
     * for this connector's consumer group.
     * <p>
     * Note that altering / resetting offsets is expected to be an idempotent operation and this method should be able
     * to handle being called more than once with the same arguments (which could occur if a user retries the request
     * due to a failure in altering the consumer group offsets, for example).
     * <p>
     * Similar to {@link #validate(Map) validate}, this method may be called by the runtime before the
     * {@link #start(Map) start} method is invoked.
     *
     * @param connectorConfig the configuration of the connector
     * @param offsets a map from topic partition to offset, containing the offsets that the user has requested to
     *                alter/reset. For any topic partitions whose offsets are being reset instead of altered, their
     *                corresponding value in the map will be {@code null}. This map may be empty, but never null. An
     *                empty offsets map could indicate that the offsets were reset previously or that no offsets have
     *                been committed yet.
     * @return whether this method has been overridden by the connector; the default implementation returns
     * {@code false}, and all other implementations (that do not unconditionally throw exceptions) should return
     * {@code true}
     * @throws UnsupportedOperationException if it is impossible to alter/reset the offsets for this connector
     * @throws org.apache.kafka.connect.errors.ConnectException if the offsets for this connector cannot be
     * reset for any other reason (for example, they have failed custom validation logic specific to this connector)
     * @since 3.6
     */
    public boolean alterOffsets(Map<String, String> connectorConfig, Map<TopicPartition, Long> offsets) {
        return false;
    }
}
