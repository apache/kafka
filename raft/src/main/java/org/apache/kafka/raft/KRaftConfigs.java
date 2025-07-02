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
package org.apache.kafka.raft;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SaslConfigs;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LIST;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public class KRaftConfigs {
    /** KRaft mode configs */
    public static final String PROCESS_ROLES_CONFIG = "process.roles";
    public static final String PROCESS_ROLES_DOC = "The roles that this process plays: 'broker', 'controller', or 'broker,controller' if it is both. ";
    
    public static final String INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_CONFIG = "initial.broker.registration.timeout.ms";
    public static final int INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_DEFAULT = 60000;
    public static final String INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_DOC = "When initially registering with the controller quorum, the number of milliseconds to wait before declaring failure and exiting the broker process.";

    public static final String BROKER_HEARTBEAT_INTERVAL_MS_CONFIG = "broker.heartbeat.interval.ms";
    public static final int BROKER_HEARTBEAT_INTERVAL_MS_DEFAULT = 2000;
    public static final String BROKER_HEARTBEAT_INTERVAL_MS_DOC = "The length of time in milliseconds between broker heartbeats.";

    public static final String BROKER_SESSION_TIMEOUT_MS_CONFIG = "broker.session.timeout.ms";
    public static final int BROKER_SESSION_TIMEOUT_MS_DEFAULT = 9000;
    public static final String BROKER_SESSION_TIMEOUT_MS_DOC = "The length of time in milliseconds that a broker lease lasts if no heartbeats are made.";


    public static final String NODE_ID_CONFIG = "node.id";
    public static final String NODE_ID_DOC = "The node ID associated with the roles this process is playing when <code>process.roles</code> is non-empty. " +
            "This is required configuration when running in KRaft mode.";

    public static final String CONTROLLER_LISTENER_NAMES_CONFIG = "controller.listener.names";
    public static final String CONTROLLER_LISTENER_NAMES_DOC = "A comma-separated list of the names of the listeners used by the controller. This is required " +
            "when communicating with the controller quorum, the broker will always use the first listener in this list.";

    public static final String SASL_MECHANISM_CONTROLLER_PROTOCOL_CONFIG = "sasl.mechanism.controller.protocol";
    public static final String SASL_MECHANISM_CONTROLLER_PROTOCOL_DOC = "SASL mechanism used for communication with controllers. Default is GSSAPI.";

    public static final String SERVER_MAX_STARTUP_TIME_MS_CONFIG = "server.max.startup.time.ms";
    public static final long SERVER_MAX_STARTUP_TIME_MS_DEFAULT = Long.MAX_VALUE;
    public static final String SERVER_MAX_STARTUP_TIME_MS_DOC = "The maximum number of milliseconds we will wait for the server to come up. " +
            "By default there is no limit. This should be used for testing only.";

    public static final String CONTROLLER_PERFORMANCE_SAMPLE_PERIOD_MS = "controller.performance.sample.period.ms";
    public static final long CONTROLLER_PERFORMANCE_SAMPLE_PERIOD_MS_DEFAULT = 60000;
    public static final String CONTROLLER_PERFORMANCE_SAMPLE_PERIOD_MS_DOC = "The number of milliseconds between periodic controller event performance log messages.";

    public static final String CONTROLLER_PERFORMANCE_ALWAYS_LOG_THRESHOLD_MS = "controller.performance.always.log.threshold.ms";
    public static final long CONTROLLER_PERFORMANCE_ALWAYS_LOG_THRESHOLD_MS_DEFAULT = 2000;
    public static final String CONTROLLER_PERFORMANCE_ALWAYS_LOG_THRESHOLD_MS_DOC = "We will log an error message about controller events that take longer than this threshold.";

    public static final ConfigDef CONFIG_DEF =  new ConfigDef()
            .define(PROCESS_ROLES_CONFIG, LIST, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.ValidList.in(false, "broker", "controller"), HIGH, PROCESS_ROLES_DOC)
            .define(NODE_ID_CONFIG, INT, ConfigDef.NO_DEFAULT_VALUE, atLeast(0), HIGH, NODE_ID_DOC)
            .define(INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_CONFIG, INT, INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_DEFAULT, null, MEDIUM, INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_DOC)
            .define(BROKER_HEARTBEAT_INTERVAL_MS_CONFIG, INT, BROKER_HEARTBEAT_INTERVAL_MS_DEFAULT, null, MEDIUM, BROKER_HEARTBEAT_INTERVAL_MS_DOC)
            .define(BROKER_SESSION_TIMEOUT_MS_CONFIG, INT, BROKER_SESSION_TIMEOUT_MS_DEFAULT, null, MEDIUM, BROKER_SESSION_TIMEOUT_MS_DOC)
            .define(CONTROLLER_LISTENER_NAMES_CONFIG, LIST, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.ValidList.anyNonDuplicateValues(false, false), HIGH, CONTROLLER_LISTENER_NAMES_DOC)
            .define(SASL_MECHANISM_CONTROLLER_PROTOCOL_CONFIG, STRING, SaslConfigs.DEFAULT_SASL_MECHANISM, null, HIGH, SASL_MECHANISM_CONTROLLER_PROTOCOL_DOC)
            .defineInternal(CONTROLLER_PERFORMANCE_SAMPLE_PERIOD_MS, LONG, CONTROLLER_PERFORMANCE_SAMPLE_PERIOD_MS_DEFAULT, atLeast(100), MEDIUM, CONTROLLER_PERFORMANCE_SAMPLE_PERIOD_MS_DOC)
            .defineInternal(CONTROLLER_PERFORMANCE_ALWAYS_LOG_THRESHOLD_MS, LONG, CONTROLLER_PERFORMANCE_ALWAYS_LOG_THRESHOLD_MS_DEFAULT, atLeast(0), MEDIUM, CONTROLLER_PERFORMANCE_ALWAYS_LOG_THRESHOLD_MS_DOC)
            .defineInternal(SERVER_MAX_STARTUP_TIME_MS_CONFIG, LONG, SERVER_MAX_STARTUP_TIME_MS_DEFAULT, atLeast(0), MEDIUM, SERVER_MAX_STARTUP_TIME_MS_DOC);
}
