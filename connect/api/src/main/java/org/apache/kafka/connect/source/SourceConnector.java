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
package org.apache.kafka.connect.source;

import org.apache.kafka.connect.connector.Connector;

import java.util.Map;

/**
 * SourceConnectors implement the connector interface to pull data from another system and send
 * it to Kafka.
 * <p>Kafka Connect may discover implementations of this interface using the Java {@link java.util.ServiceLoader} mechanism.
 * To support this, implementations of this interface should also contain a service provider configuration file in
 * {@code META-INF/services/org.apache.kafka.connect.source.SourceConnector}.
 */
public abstract class SourceConnector extends Connector {

    @Override
    protected SourceConnectorContext context() {
        return (SourceConnectorContext) context;
    }

    /**
     * Signals whether the connector supports exactly-once semantics with a proposed configuration.
     * Connector authors can assume that worker-level exactly-once support is enabled when this method is invoked.
     *
     * <p>For backwards compatibility, the default implementation will return {@code null}, but connector authors are
     * strongly encouraged to override this method to return a non-null value such as
     * {@link ExactlyOnceSupport#SUPPORTED SUPPORTED} or {@link ExactlyOnceSupport#UNSUPPORTED UNSUPPORTED}.
     *
     * <p>Similar to {@link #validate(Map) validate}, this method may be called by the runtime before the
     * {@link #start(Map) start} method is invoked when the connector will be run with exactly-once support.
     *
     * @param connectorConfig the configuration that will be used for the connector.
     * @return {@link ExactlyOnceSupport#SUPPORTED} if the connector can provide exactly-once support with the given
     * configuration, and {@link ExactlyOnceSupport#UNSUPPORTED} if it cannot. If this method is overridden by a
     * connector, should not be {@code null}, but if {@code null}, it will be assumed that the connector cannot provide
     * exactly-once semantics.
     * @since 3.3
     */
    public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> connectorConfig) {
        return null;
    }

    /**
     * Signals whether the connector implementation is capable of defining the transaction boundaries for a
     * connector with the given configuration. This method is called before {@link #start(Map)}, only when the
     * runtime supports exactly-once and the connector configuration includes {@code transaction.boundary=connector}.
     *
     * <p>This method need not be implemented if the connector implementation does not support defining
     * transaction boundaries.
     *
     * @param connectorConfig the configuration that will be used for the connector
     * @return {@link ConnectorTransactionBoundaries#SUPPORTED} if the connector will define its own transaction boundaries,
     * or {@link ConnectorTransactionBoundaries#UNSUPPORTED} otherwise; may never be {@code null}. The default implementation
     * returns {@link ConnectorTransactionBoundaries#UNSUPPORTED}.
     * @since 3.3
     * @see TransactionContext
     */
    public ConnectorTransactionBoundaries canDefineTransactionBoundaries(Map<String, String> connectorConfig) {
        return ConnectorTransactionBoundaries.UNSUPPORTED;
    }

    /**
     * Invoked when users request to manually alter/reset the offsets for this connector via the Connect worker's REST
     * API. Connectors that manage offsets externally can propagate offset changes to their external system in this
     * method. Connectors may also validate these offsets to ensure that the source partitions and source offsets are
     * in a format that is recognizable to them.
     * <p>
     * Connectors that neither manage offsets externally nor require custom offset validation need not implement this
     * method beyond simply returning {@code true}.
     * <p>
     * User requests to alter/reset offsets will be handled by the Connect runtime and will be reflected in the offsets
     * returned by any {@link org.apache.kafka.connect.storage.OffsetStorageReader OffsetStorageReader instances}
     * provided to this connector and its tasks.
     * <p>
     * Note that altering / resetting offsets is expected to be an idempotent operation and this method should be able
     * to handle being called more than once with the same arguments (which could occur if a user retries the request
     * due to a failure in writing the new offsets to the offsets store, for example).
     * <p>
     * Similar to {@link #validate(Map) validate}, this method may be called by the runtime before the
     * {@link #start(Map) start} method is invoked.
     *
     * @param connectorConfig the configuration of the connector
     * @param offsets a map from source partition to source offset, containing the offsets that the user has requested
     *                to alter/reset. For any source partitions whose offsets are being reset instead of altered, their
     *                corresponding source offset value in the map will be {@code null}. This map may be empty, but
     *                never null. An empty offsets map could indicate that the offsets were reset previously or that no
     *                offsets have been committed yet.
     * @return whether this method has been overridden by the connector; the default implementation returns
     * {@code false}, and all other implementations (that do not unconditionally throw exceptions) should return
     * {@code true}
     * @throws UnsupportedOperationException if it is impossible to alter/reset the offsets for this connector
     * @throws org.apache.kafka.connect.errors.ConnectException if the offsets for this connector cannot be
     * reset for any other reason (for example, they have failed custom validation logic specific to this connector)
     * @since 3.6
     */
    public boolean alterOffsets(Map<String, String> connectorConfig, Map<Map<String, ?>, Map<String, ?>> offsets) {
        return false;
    }
}
