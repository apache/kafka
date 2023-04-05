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
}
