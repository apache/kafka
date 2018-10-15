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
package org.apache.kafka.connect.util;

import org.slf4j.MDC;

/**
 * A utility for defining Mapped Diagnostic Context (MDC) for SLF4J logs.
 */
public final class LoggingContext {

    /**
     * The parameter keys used by Connect.
     */
    public static final class Parameters {

        /**
         * The name of the Mapped Diagnostic Context (MDC) key that defines the name of a <i>connector</i>.
         */
        public static final String CONNECTOR_NAME = "connector.name";

        /**
         * The name of the Mapped Diagnostic Context (MDC) key that defines the task number of a <i>connector</i>.
         */
        public static final String CONNECTOR_TASK = "connector.task";

        /**
         * The name of the Mapped Diagnostic Context (MDC) key that defines a <i>scope</i> within a connector.
         */
        public static final String CONNECTOR_SCOPE = "connector.scope";
    }

    /**
     * The {@link Parameters#CONNECTOR_SCOPE scope} values used by Connect.
     */
    public enum Scope {
        /**
         * The {@link Parameters#CONNECTOR_SCOPE scope} value for the worker as it starts a connector.
         */
        WORKER("worker"),

        /**
         * The {@link Parameters#CONNECTOR_SCOPE scope} value for {@link org.apache.kafka.connect.connector.Task}
         * implementations.
         */
        TASK("task"),

        /**
         * The {@link Parameters#CONNECTOR_SCOPE scope} value for committing offsets.
         */
        OFFSETS("offsets");

        private final String value;
        Scope(String value) {
            this.value = value;
        }
        public String value() {
            return this.value;
        }
    }

    /**
     * Modify the current {@link MDC} logging context to set the {@link Parameters#CONNECTOR_NAME connector name} to the
     * supplied name and the {@link Parameters#CONNECTOR_SCOPE scope} to {@link Scope#WORKER}.
     *
     * @param connectorName the connector name; may not be null
     */
    public static void forConnector(String connectorName) {
        MDC.put(Parameters.CONNECTOR_NAME, connectorName);
        MDC.remove(Parameters.CONNECTOR_TASK);
        MDC.put(Parameters.CONNECTOR_SCOPE, Scope.WORKER.value());
    }

    /**
     * Modify the current {@link MDC} logging context to set the {@link Parameters#CONNECTOR_NAME connector name} and
     * {@link Parameters#CONNECTOR_TASK task number} to the same values in the supplied {@link ConnectorTaskId}.
     *
     * @param id the connector task ID; may not be null
     */
    public static void forTask(ConnectorTaskId id) {
        MDC.put(Parameters.CONNECTOR_NAME, id.connector());
        MDC.put(Parameters.CONNECTOR_TASK, Integer.toString(id.task()));
        MDC.put(Parameters.CONNECTOR_SCOPE, Scope.TASK.value());
    }

    /**
     * Modify the current {@link MDC} logging context to set the  {@link Parameters#CONNECTOR_NAME connector name},
     * {@link Parameters#CONNECTOR_TASK task number} and to the same values in the supplied {@link ConnectorTaskId}, and to set
     * the scope to {@link Scope#OFFSETS}.
     *
     * @param id the connector task ID; may not be null
     */
    public static void forOffsets(ConnectorTaskId id) {
        MDC.put(Parameters.CONNECTOR_NAME, id.connector());
        MDC.put(Parameters.CONNECTOR_TASK, Integer.toString(id.task()));
        MDC.put(Parameters.CONNECTOR_SCOPE, Scope.OFFSETS.value());
    }

    /**
     * Remove all of the Connect-specific {@link Parameters parameters} from the current {@link MDC} logging context.
     */
    public static void clear() {
        MDC.remove(Parameters.CONNECTOR_NAME);
        MDC.remove(Parameters.CONNECTOR_TASK);
        MDC.remove(Parameters.CONNECTOR_SCOPE);
    }
}
