/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.cli;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Properties;

/**
 * Configuration for standalone workers.
 */
@InterfaceStability.Unstable
public class WorkerConfig extends AbstractConfig {

    public static final String CLUSTER_CONFIG = "cluster";
    private static final String CLUSTER_CONFIG_DOC =
            "ID for this cluster, which is used to provide a namespace so multiple Copycat clusters "
                    + "or instances may co-exist while sharing a single Kafka cluster.";
    public static final String CLUSTER_DEFAULT = "copycat";

    public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
    public static final String BOOTSTRAP_SERVERS_DOC
            = "A list of host/port pairs to use for establishing the initial connection to the Kafka "
            + "cluster. The client will make use of all servers irrespective of which servers are "
            + "specified here for bootstrapping&mdash;this list only impacts the initial hosts used "
            + "to discover the full set of servers. This list should be in the form "
            + "<code>host1:port1,host2:port2,...</code>. Since these servers are just used for the "
            + "initial connection to discover the full cluster membership (which may change "
            + "dynamically), this list need not contain the full set of servers (you may want more "
            + "than one, though, in case a server is down).";
    public static final String BOOTSTRAP_SERVERS_DEFAULT = "localhost:9092";

    public static final String KEY_CONVERTER_CLASS_CONFIG = "key.converter";
    public static final String KEY_CONVERTER_CLASS_DOC =
            "Converter class for key Copycat data that implements the <code>Converter</code> interface.";

    public static final String VALUE_CONVERTER_CLASS_CONFIG = "value.converter";
    public static final String VALUE_CONVERTER_CLASS_DOC =
            "Converter class for value Copycat data that implements the <code>Converter</code> interface.";

    public static final String INTERNAL_KEY_CONVERTER_CLASS_CONFIG = "internal.key.converter";
    public static final String INTERNAL_KEY_CONVERTER_CLASS_DOC =
            "Converter class for internal key Copycat data that implements the <code>Converter</code> interface. Used for converting data like offsets and configs.";

    public static final String INTERNAL_VALUE_CONVERTER_CLASS_CONFIG = "internal.value.converter";
    public static final String INTERNAL_VALUE_CONVERTER_CLASS_DOC =
            "Converter class for offset value Copycat data that implements the <code>Converter</code> interface. Used for converting data like offsets and configs.";

    public static final String TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG
            = "task.shutdown.graceful.timeout.ms";
    private static final String TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_DOC =
            "Amount of time to wait for tasks to shutdown gracefully. This is the total amount of time,"
                    + " not per task. All task have shutdown triggered, then they are waited on sequentially.";
    private static final String TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_DEFAULT = "5000";

    public static final String OFFSET_COMMIT_INTERVAL_MS_CONFIG = "offset.flush.interval.ms";
    private static final String OFFSET_COMMIT_INTERVAL_MS_DOC
            = "Interval at which to try committing offsets for tasks.";
    public static final long OFFSET_COMMIT_INTERVAL_MS_DEFAULT = 60000L;

    public static final String OFFSET_COMMIT_TIMEOUT_MS_CONFIG = "offset.flush.timeout.ms";
    private static final String OFFSET_COMMIT_TIMEOUT_MS_DOC
            = "Maximum number of milliseconds to wait for records to flush and partition offset data to be"
            + " committed to offset storage before cancelling the process and restoring the offset "
            + "data to be committed in a future attempt.";
    public static final long OFFSET_COMMIT_TIMEOUT_MS_DEFAULT = 5000L;

    private static ConfigDef config;

    static {
        config = new ConfigDef()
                .define(CLUSTER_CONFIG, Type.STRING, CLUSTER_DEFAULT, Importance.HIGH, CLUSTER_CONFIG_DOC)
                .define(BOOTSTRAP_SERVERS_CONFIG, Type.LIST, BOOTSTRAP_SERVERS_DEFAULT,
                        Importance.HIGH, BOOTSTRAP_SERVERS_DOC)
                .define(KEY_CONVERTER_CLASS_CONFIG, Type.CLASS,
                        Importance.HIGH, KEY_CONVERTER_CLASS_DOC)
                .define(VALUE_CONVERTER_CLASS_CONFIG, Type.CLASS,
                        Importance.HIGH, VALUE_CONVERTER_CLASS_DOC)
                .define(INTERNAL_KEY_CONVERTER_CLASS_CONFIG, Type.CLASS,
                        Importance.HIGH, INTERNAL_KEY_CONVERTER_CLASS_DOC)
                .define(INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, Type.CLASS,
                        Importance.HIGH, INTERNAL_VALUE_CONVERTER_CLASS_DOC)
                .define(TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG, Type.LONG,
                        TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_DEFAULT, Importance.LOW,
                        TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_DOC)
                .define(OFFSET_COMMIT_INTERVAL_MS_CONFIG, Type.LONG, OFFSET_COMMIT_INTERVAL_MS_DEFAULT,
                        Importance.LOW, OFFSET_COMMIT_INTERVAL_MS_DOC)
                .define(OFFSET_COMMIT_TIMEOUT_MS_CONFIG, Type.LONG, OFFSET_COMMIT_TIMEOUT_MS_DEFAULT,
                        Importance.LOW, OFFSET_COMMIT_TIMEOUT_MS_DOC);
    }

    public WorkerConfig() {
        this(new Properties());
    }

    public WorkerConfig(Properties props) {
        super(config, props);
    }
}
