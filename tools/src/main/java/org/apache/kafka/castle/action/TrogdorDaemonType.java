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

package org.apache.kafka.castle.action;

import static org.apache.kafka.castle.action.ActionPaths.LOGS_ROOT;
import static org.apache.kafka.castle.action.ActionPaths.TROGDOR_AGENT_ROOT;
import static org.apache.kafka.castle.action.ActionPaths.TROGDOR_CONF_SUFFIX;
import static org.apache.kafka.castle.action.ActionPaths.TROGDOR_COORDIINATOR_ROOT;
import static org.apache.kafka.castle.action.ActionPaths.TROGDOR_LOG4J_SUFFIX;
import static org.apache.kafka.castle.action.ActionPaths.TROGDOR_PROPERTIES_SUFFIX;

public class TrogdorDaemonType {
    private final String name;
    private final String typePrefix;
    private final String root;
    private final String className;

    private TrogdorDaemonType(String name, String typePrefix, String root, String className) {
        this.name = name;
        this.typePrefix = typePrefix;
        this.root = root;
        this.className = className;
    }

    public String name() {
        return name;
    }

    public String root() {
        return root;
    }

    public String className() {
        return className;
    }

    public final String startType() {
        return typePrefix + "Start";
    }

    public final String statusType() {
        return typePrefix + "Status";
    }

    public final String stopType() {
        return typePrefix + "Stop";
    }

    public String confDir() {
        return String.format("%s%s", root, TROGDOR_CONF_SUFFIX);
    }

    public String propertiesPath() {
        return String.format("%s%s%s", root, TROGDOR_CONF_SUFFIX, TROGDOR_PROPERTIES_SUFFIX);
    }

    public String log4jConfPath() {
        return String.format("%s%s%s", root, TROGDOR_CONF_SUFFIX, TROGDOR_LOG4J_SUFFIX);
    }

    public String logDir() {
        return String.format("%s/trogdor-%s", LOGS_ROOT, name);
    }

    public String logPath() {
        return String.format("%s/trogdor-%s/%s.log", LOGS_ROOT, name, name);
    }

    public static final TrogdorDaemonType AGENT = new TrogdorDaemonType(
        "agent",
        "trogdorAgent",
        TROGDOR_AGENT_ROOT,
        "org.apache.kafka.trogdor.agent.Agent");

    public static final TrogdorDaemonType COORDINATOR = new TrogdorDaemonType(
        "coordinator",
        "trogdorCoordinator",
        TROGDOR_COORDIINATOR_ROOT,
        "org.apache.kafka.trogdor.coordinator.Coordinator");
}
