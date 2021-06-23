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
     * Signals whether the connector supports exactly-once delivery guarantees with a proposed configuration.
     * Developers can assume that worker-level exactly-once support is enabled when this method is invoked.
     * The default implementation will return {@code null}.
     * @param connectorConfig the configuration that will be used for the connector.
     * @return {@link ExactlyOnceSupport#SUPPORTED} if the connector can provide exactly-once support,
     * and {@link ExactlyOnceSupport#UNSUPPORTED} if it cannot. If {@code null}, it is assumed that the
     * connector cannot.
     */
    public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> connectorConfig) {
        return null;
    }

    /**
     * Signals whether the connector can define its own transaction boundaries with the proposed
     * configuration. Developers must override this method if they wish to add connector-defined
     * transaction boundary support; if they do not, users will be unable to create instances of
     * this connector that use connector-defined transaction boundaries. The default implementation
     * will return {@code UNSUPPORTED}.
     * @param connectorConfig the configuration that will be used for the connector
     * @return whether the connector can define its own transaction boundaries  with the given
     * config.
     */
    public ConnectorTransactionBoundaries canDefineTransactionBoundaries(Map<String, String> connectorConfig) {
        return ConnectorTransactionBoundaries.UNSUPPORTED;
    }
}
