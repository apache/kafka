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

import java.util.HashMap;
import java.util.Map;

/**
 * A utility for defining Mapped Diagnostic Context (MDC) for SLF4J logs.
 *
 * <p>Logging contexts may be nested. In this case, the nested context may override some or all of the parameters of the outer context.
 * In these cases, both contexts may be active but the nested context takes priority. It is presumed that the nexted context will be
 * stopped before the outer context.
 *
 */
public final class LoggingContext implements AutoCloseable {

    /**
     * The parameter keys used by Connect.
     */
    public static final class Parameters {

        /**
         * The name of the Mapped Diagnostic Context (MDC) key that defines the type of <i>connector</i>.
         */
        public static final String CONNECTOR_TYPE = "connector.type";

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
     * Modify the current {@link MDC} logging context to set the {@link Parameters#CONNECTOR_SCOPE scope} to the specified value.
     *
     * @param scope the scope; may be null
     * @return the resulting LoggingContext that is already {@link LoggingContext#start() started}; never null
     */
    public static LoggingContext forScope(Scope scope) {
        Map<String, String> context = new HashMap<>();
        context.put(Parameters.CONNECTOR_SCOPE, scope.value());
        return new LoggingContext(context).start();
    }

    /**
     * Modify the current {@link MDC} logging context to set the {@link Parameters#CONNECTOR_NAME connector name} and
     * {@link Parameters#CONNECTOR_TASK task number} to the same values in the supplied {@link ConnectorTaskId}.
     *
     * @param id the connector task ID; may be null
     * @return the resulting LoggingContext that is already {@link LoggingContext#start() started}; never null
     */
    public static LoggingContext forTask(ConnectorTaskId id) {
        Map<String, String> context = new HashMap<>();
        context.put(Parameters.CONNECTOR_NAME, id.connector());
        context.put(Parameters.CONNECTOR_TASK, Integer.toString(id.task()));
        context.put(Parameters.CONNECTOR_SCOPE, Scope.TASK.value());
        return new LoggingContext(context).start();
    }

    /**
     * Modify the current {@link MDC} logging context to set the {@link Parameters#CONNECTOR_TYPE connector type} to a shortened name
     * for the supplied class and the {@link Parameters#CONNECTOR_NAME connector name} to the supplied name.
     *
     * @param connectorClassName the fully qualified or shortened connector class name; may be null
     * @return the resulting LoggingContext that is already {@link LoggingContext#start() started}; never null
     */
    public static LoggingContext forConnector(String connectorClassName, String connectorName) {
        Map<String, String> context = new HashMap<>();
        context.put(Parameters.CONNECTOR_TYPE, shortNameFor(connectorClassName));
        context.put(Parameters.CONNECTOR_NAME, connectorName);
        context.put(Parameters.CONNECTOR_SCOPE, Scope.WORKER.value());
        return new LoggingContext(context).start();
    }

    /**
     * Modify the current {@link MDC} logging context to set the  {@link Parameters#CONNECTOR_NAME connector name},
     * {@link Parameters#CONNECTOR_TASK task number} and to the same values in the supplied {@link ConnectorTaskId}, and to set
     * the scope to {@link Scope#OFFSETS}.
     *
     * @param id the connector task ID; may be null
     * @return the resulting LoggingContext that is already {@link LoggingContext#start() started}; never null
     */
    public static LoggingContext forOffsets(ConnectorTaskId id) {
        Map<String, String> context = new HashMap<>();
        context.put(Parameters.CONNECTOR_NAME, id.connector());
        context.put(Parameters.CONNECTOR_TASK, Integer.toString(id.task()));
        context.put(Parameters.CONNECTOR_SCOPE, Scope.OFFSETS.value());
        return new LoggingContext(context).start();
    }

    protected static String shortNameFor(String className) {
        return className.replaceAll("^(.*)[.]", "") // greedy wildcard
                        .replaceAll("^(.*)[$]", "") // greedy wildcard
                        .replaceAll("Connector", "")
                        .replaceAll("Task", "");
    }

    private static final String NULL_VALUE = null;

    final Map<String, String> previous = new HashMap<>();
    final Map<String, String> context;
    private boolean active = false;

    protected LoggingContext(Map<String, String> context) {
        this.context = context;
    }

    /**
     * Change the {@link MDC} to use this context's parameters.
     *
     * <p>This method has no effect when this context is already active.
     *
     * @return this instance, for chaining purposes; never null
     */
    public LoggingContext start() {
        if (!active) {
            previous.clear();
            for (Map.Entry<String, String> entry : context.entrySet()) {
                previous.put(entry.getKey(), MDC.get(entry.getKey()));
                if (entry.getValue() != NULL_VALUE && entry.getValue() != null) {
                    MDC.put(entry.getKey(), entry.getValue());
                } else {
                    MDC.remove(entry.getKey());
                }
            }
            active = true;
        }
        return this;
    }

    /**
     * Change the {@link MDC} to no longer use the parameters of this context, restoring any values for the parameters that existed
     * when {@link #start()} was called. This method does not affect other parameters not defined on this context.
     *
     * <p>This method has no effect when this context is not active.
     *
     * @return this instance, for chaining purposes; never null
     */
    public LoggingContext stop() {
        if (active) {
            for (Map.Entry<String, String> entry : previous.entrySet()) {
                if (entry.getValue() != NULL_VALUE && entry.getValue() != null) {
                    MDC.put(entry.getKey(), entry.getValue());
                } else {
                    MDC.remove(entry.getKey());
                }
            }
            active = false;
        }
        return this;
    }

    @Override
    public void close() {
        stop();
    }

    @Override
    public String toString() {
        return "LoggingContext:" + context;
    }

    /**
     * Return whether this context still active. Note that even if this is active other "nested" contexts may have been started since
     * this was started and may take priority (e.g., they may have overwritten some or all of the parameters of this context),
     * but once they are stopped this context will still be active.
     *
     * @return true if active, or false otherwise
     */
    protected boolean isActive() {
        return active;
    }
}
