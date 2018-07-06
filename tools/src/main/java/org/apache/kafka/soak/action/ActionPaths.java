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

package org.apache.kafka.soak.action;

public final class ActionPaths {
    public static final String LOGS_ROOT = "/mnt/logs";

    public static final String KAFKA_ROOT = "/mnt/kafka";
    public static final String KAFKA_SRC = KAFKA_ROOT + "/src";
    public static final String KAFKA_START_SCRIPT = KAFKA_SRC + "/bin/kafka-server-start.sh";
    public static final String KAFKA_CONF = KAFKA_ROOT + "/conf";
    public static final String KAFKA_BROKER_PROPERTIES = KAFKA_CONF + "/broker.properties";
    public static final String KAFKA_BROKER_LOG4J = KAFKA_CONF + "/log4j.properties";
    public static final String KAFKA_OPLOGS = KAFKA_ROOT + "/oplogs";
    public static final String KAFKA_LOGS = LOGS_ROOT + "/kafka";

    public static final String ZK_ROOT = "/mnt/zookeeper";
    public static final String ZK_CONF = ZK_ROOT + "/conf";
    public static final String ZK_START_SCRIPT = KAFKA_SRC + "/bin/zookeeper-server-start.sh";
    public static final String ZK_PROPERTIES = ZK_CONF + "/zookeeper.properties";
    public static final String ZK_LOG4J = ZK_CONF + "/log4j.properties";
    public static final String ZK_OPLOGS = ZK_ROOT + "/oplogs";
    public static final String ZK_LOGS = LOGS_ROOT + "/zookeeper";

    public static final String TROGDOR_AGENT_ROOT = "/mnt/trogdor-agent";
    public static final String TROGDOR_COORDIINATOR_ROOT = "/mnt/trogdor-coordinator";
    public static final String TROGDOR_START_SCRIPT = KAFKA_SRC + "/bin/trogdor.sh";
    public static final String TROGDOR_CONF_SUFFIX = "/conf";
    public static final String TROGDOR_PROPERTIES_SUFFIX = "/trogdor.conf";
    public static final String TROGDOR_LOG4J_SUFFIX = "/log4j.properties";

    public static final String COLLECTD_ROOT = "/mnt/collectd";
    public static final String COLLECTD_PROPERTIES = COLLECTD_ROOT + "/collectd.conf";
    public static final String COLLECTD = "collectd";
    public static final String COLLECTD_LOGS = LOGS_ROOT + "/collectd";

    public static final String KAFKA_RUN_CLASS = KAFKA_SRC + "/bin/kafka-run-class.sh";
    public static final String JMX_DUMPER_ROOT = "/mnt/jmx";
    public static final String JMX_DUMPER_PROPERTIES = JMX_DUMPER_ROOT  + "/jmx.conf";
    public static final String JMX_DUMPER_LOGS = LOGS_ROOT + "/jmx";
};
