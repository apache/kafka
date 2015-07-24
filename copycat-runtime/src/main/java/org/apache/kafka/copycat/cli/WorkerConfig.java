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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Properties;
import java.util.Set;

/**
 * Configuration for standalone workers.
 */
public class WorkerConfig extends AbstractConfig {

    public static final String CLUSTER_CONFIG = "cluster";
    private static final String
            CLUSTER_CONFIG_DOC =
            "ID for this cluster, which is used to provide a namespace so multiple Copycat clusters "
                    + "or instances may co-exist while sharing a single Kafka cluster.";
    public static final String CLUSTER_DEFAULT = "copycat";

    public static final String ZOOKEEPER_CONNECT_CONFIG = "zookeeper.connect";
    private static final String ZOOKEEPER_CONNECT_DOC =
            "Specifies the ZooKeeper connection string in the form "
                    + "hostname:port where host and port are the host and port of a ZooKeeper server. To allow connecting "
                    + "through other ZooKeeper nodes when that ZooKeeper machine is down you can also specify multiple hosts "
                    + "in the form hostname1:port1,hostname2:port2,hostname3:port3.\n"
                    + "\n"
                    + "The server may also have a ZooKeeper chroot path as part of it's ZooKeeper connection string which puts "
                    + "its data under some path in the global ZooKeeper namespace. If so the consumer should use the same "
                    + "chroot path in its connection string. For example to give a chroot path of /chroot/path you would give "
                    + "the connection string as hostname1:port1,hostname2:port2,hostname3:port3/chroot/path.";
    public static final String ZOOKEEPER_CONNECT_DEFAULT = "localhost:2181";

    public static final String ZOOKEEPER_SESSION_TIMEOUT_MS_CONFIG = "zookeeper.session.timeout.ms";
    private static final String ZOOKEEPER_SESSION_TIMEOUT_MS_DOC
            = "Session timeout for ZooKeeper connections.";
    public static final String ZOOKEEPER_SESSION_TIMEOUT_MS_DEFAULT = "30000";

    public static final String ZOOKEEPER_CONNECTION_TIMEOUT_MS_CONFIG
            = "zookeeper.session.connection.ms";
    private static final String ZOOKEEPER_CONNECTION_TIMEOUT_MS_DOC
            = "Connection timeout for ZooKeeper.";
    public static final String ZOOKEEPER_CONNECTION_TIMEOUT_MS_DEFAULT = "30000";

    public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
    public static final String BOOSTRAP_SERVERS_DOC
            = "A list of host/port pairs to use for establishing the initial connection to the Kafka "
            + "cluster. The client will make use of all servers irrespective of which servers are "
            + "specified here for bootstrapping&mdash;this list only impacts the initial hosts used "
            + "to discover the full set of servers. This list should be in the form "
            + "<code>host1:port1,host2:port2,...</code>. Since these servers are just used for the "
            + "initial connection to discover the full cluster membership (which may change "
            + "dynamically), this list need not contain the full set of servers (you may want more "
            + "than one, though, in case a server is down).";
    public static final String BOOTSTRAP_SERVERS_DEFAULT = "localhost:9092";

    public static final String CONVERTER_CLASS_CONFIG = "converter";
    public static final String CONVERTER_CLASS_DOC =
            "Converter class for Copycat data that implements the <code>Converter</code> interface.";
    public static final String CONVERTER_CLASS_DEFAULT
            = "org.apache.kafka.copycat.avro.AvroConverter"; // TODO: Non-avro built-in?

    public static final String KEY_SERIALIZER_CLASS_CONFIG = "key.serializer";
    public static final String KEY_SERIALIZER_CLASS_DOC =
            "Serializer class for key that implements the <code>Serializer</code> interface.";
    public static final String KEY_SERIALIZER_CLASS_DEFAULT
            = "io.confluent.kafka.serializers.KafkaAvroSerializer"; // TODO: Non-avro built-in?

    public static final String VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer";
    public static final String VALUE_SERIALIZER_CLASS_DOC =
            "Serializer class for value that implements the <code>Serializer</code> interface.";
    public static final String VALUE_SERIALIZER_CLASS_DEFAULT
            = "io.confluent.kafka.serializers.KafkaAvroSerializer"; // TODO: Non-avro built-in?

    public static final String OFFSET_KEY_SERIALIZER_CLASS_CONFIG = "offset.key.serializer";
    public static final String OFFSET_KEY_SERIALIZER_CLASS_DOC =
            "Serializer class for key that implements the <code>OffsetSerializer</code> interface.";
    public static final String OFFSET_KEY_SERIALIZER_CLASS_DEFAULT
            = "org.apache.kafka.copycat.avro.AvroSerializer"; // TODO: Non-avro built-in?

    public static final String OFFSET_VALUE_SERIALIZER_CLASS_CONFIG = "offset.value.serializer";
    public static final String OFFSET_VALUE_SERIALIZER_CLASS_DOC =
            "Serializer class for value that implements the <code>OffsetSerializer</code> interface.";
    public static final String OFFSET_VALUE_SERIALIZER_CLASS_DEFAULT
            = "org.apache.kafka.copycat.avro.AvroSerializer"; // TODO: Non-avro built-in?


    public static final String KEY_DESERIALIZER_CLASS_CONFIG = "key.deserializer";
    public static final String KEY_DESERIALIZER_CLASS_DOC =
            "Serializer class for key that implements the <code>Deserializer</code> interface.";
    public static final String KEY_DESERIALIZER_CLASS_DEFAULT
            = "io.confluent.kafka.serializers.KafkaAvroDeserializer"; // TODO: Non-avro built-in?

    public static final String VALUE_DESERIALIZER_CLASS_CONFIG = "value.deserializer";
    public static final String VALUE_DESERIALIZER_CLASS_DOC =
            "Deserializer class for value that implements the <code>Deserializer</code> interface.";
    public static final String VALUE_DESERIALIZER_CLASS_DEFAULT
            = "io.confluent.kafka.serializers.KafkaAvroDeserializer"; // TODO: Non-avro built-in?

    public static final String OFFSET_KEY_DESERIALIZER_CLASS_CONFIG = "offset.key.deserializer";
    public static final String OFFSET_KEY_DESERIALIZER_CLASS_DOC =
            "Deserializer class for key that implements the <code>OffsetDeserializer</code> interface.";
    public static final String OFFSET_KEY_DESERIALIZER_CLASS_DEFAULT
            = "org.apache.kafka.copycat.avro.AvroDeserializer"; // TODO: Non-avro built-in?

    public static final String OFFSET_VALUE_DESERIALIZER_CLASS_CONFIG = "offset.value.deserializer";
    public static final String OFFSET_VALUE_DESERIALIZER_CLASS_DOC =
            "Deserializer class for value that implements the <code>OffsetDeserializer</code> interface.";
    public static final String OFFSET_VALUE_DESERIALIZER_CLASS_DEFAULT
            = "org.apache.kafka.copycat.avro.AvroDeserializer"; // TODO: Non-avro built-in?


    public static final String TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG
            = "task.shutdown.graceful.timeout.ms";
    private static final String TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_DOC =
            "Amount of time to wait for tasks to shutdown gracefully. This is the total amount of time,"
                    + " not per task. All task have shutdown triggered, then they are waited on sequentially.";
    private static final String TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_DEFAULT = "5000";

    public static final String OFFSET_STORAGE_CLASS_CONFIG = "offset.storage.class";
    private static final String OFFSET_STORAGE_CLASS_DOC =
            "OffsetBackingStore implementation to use for storing stream offset data";
    public static final String OFFSET_STORAGE_CLASS_DEFAULT
            = "org.apache.kafka.copycat.storage.MemoryOffsetBackingStore";

    public static final String OFFSET_COMMIT_INTERVAL_MS_CONFIG = "offset.flush.interval.ms";
    private static final String OFFSET_COMMIT_INTERVAL_MS_DOC
            = "Interval at which to try committing offsets for tasks.";
    public static final long OFFSET_COMMIT_INTERVAL_MS_DEFAULT = 60000L;

    public static final String OFFSET_COMMIT_TIMEOUT_MS_CONFIG = "offset.flush.timeout.ms";
    private static final String OFFSET_COMMIT_TIMEOUT_MS_DOC
            = "Maximum number of milliseconds to wait for records to flush and stream offset data to be"
            + " committed to offset storage before cancelling the process and restoring the offset "
            + "data to be committed in a future attempt.";
    public static final long OFFSET_COMMIT_TIMEOUT_MS_DEFAULT = 5000L;

    private static ConfigDef config;

    static {
        config = new ConfigDef()
                .define(CLUSTER_CONFIG, Type.STRING, CLUSTER_DEFAULT, Importance.HIGH, CLUSTER_CONFIG_DOC)
                .define(ZOOKEEPER_CONNECT_CONFIG, Type.STRING, ZOOKEEPER_CONNECT_DEFAULT,
                        Importance.HIGH, ZOOKEEPER_CONNECT_DOC)
                .define(ZOOKEEPER_SESSION_TIMEOUT_MS_CONFIG, Type.INT,
                        ZOOKEEPER_SESSION_TIMEOUT_MS_DEFAULT,
                        Importance.LOW, ZOOKEEPER_SESSION_TIMEOUT_MS_DOC)
                .define(ZOOKEEPER_CONNECTION_TIMEOUT_MS_CONFIG, Type.INT,
                        ZOOKEEPER_CONNECTION_TIMEOUT_MS_DEFAULT,
                        Importance.LOW, ZOOKEEPER_CONNECTION_TIMEOUT_MS_DOC)
                .define(BOOTSTRAP_SERVERS_CONFIG, Type.LIST, BOOTSTRAP_SERVERS_DEFAULT,
                        Importance.HIGH, BOOSTRAP_SERVERS_DOC)
                .define(CONVERTER_CLASS_CONFIG, Type.CLASS, CONVERTER_CLASS_DEFAULT,
                        Importance.HIGH, CONVERTER_CLASS_DOC)
                .define(KEY_SERIALIZER_CLASS_CONFIG, Type.CLASS, KEY_SERIALIZER_CLASS_DEFAULT,
                        Importance.HIGH, KEY_SERIALIZER_CLASS_DOC)
                .define(VALUE_SERIALIZER_CLASS_CONFIG, Type.CLASS, VALUE_SERIALIZER_CLASS_DEFAULT,
                        Importance.HIGH, VALUE_SERIALIZER_CLASS_DOC)
                .define(OFFSET_KEY_SERIALIZER_CLASS_CONFIG, Type.CLASS, OFFSET_KEY_SERIALIZER_CLASS_DEFAULT,
                        Importance.HIGH, OFFSET_KEY_SERIALIZER_CLASS_DOC)
                .define(OFFSET_VALUE_SERIALIZER_CLASS_CONFIG, Type.CLASS,
                        OFFSET_VALUE_SERIALIZER_CLASS_DEFAULT,
                        Importance.HIGH, OFFSET_VALUE_SERIALIZER_CLASS_DOC)
                .define(KEY_DESERIALIZER_CLASS_CONFIG, Type.CLASS, KEY_DESERIALIZER_CLASS_DEFAULT,
                        Importance.HIGH, KEY_DESERIALIZER_CLASS_DOC)
                .define(VALUE_DESERIALIZER_CLASS_CONFIG, Type.CLASS, VALUE_DESERIALIZER_CLASS_DEFAULT,
                        Importance.HIGH, VALUE_DESERIALIZER_CLASS_DOC)
                .define(OFFSET_KEY_DESERIALIZER_CLASS_CONFIG, Type.CLASS,
                        OFFSET_KEY_DESERIALIZER_CLASS_DEFAULT,
                        Importance.HIGH, OFFSET_KEY_DESERIALIZER_CLASS_DOC)
                .define(OFFSET_VALUE_DESERIALIZER_CLASS_CONFIG, Type.CLASS,
                        OFFSET_VALUE_DESERIALIZER_CLASS_DEFAULT,
                        Importance.HIGH, OFFSET_VALUE_DESERIALIZER_CLASS_DOC)
                .define(TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG, Type.LONG,
                        TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_DEFAULT, Importance.LOW,
                        TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_DOC)
                .define(OFFSET_STORAGE_CLASS_CONFIG, Type.CLASS, OFFSET_STORAGE_CLASS_DEFAULT,
                        Importance.LOW, OFFSET_STORAGE_CLASS_DOC)
                .define(OFFSET_COMMIT_INTERVAL_MS_CONFIG, Type.LONG, OFFSET_COMMIT_INTERVAL_MS_DEFAULT,
                        Importance.LOW, OFFSET_COMMIT_INTERVAL_MS_DOC)
                .define(OFFSET_COMMIT_TIMEOUT_MS_CONFIG, Type.LONG, OFFSET_COMMIT_TIMEOUT_MS_DEFAULT,
                        Importance.LOW, OFFSET_COMMIT_TIMEOUT_MS_DOC);
    }

    private Properties originalProperties;

    public WorkerConfig() {
        this(new Properties());
    }

    public WorkerConfig(Properties props) {
        super(config, props);
        this.originalProperties = props;
    }

    public Properties getUnusedProperties() {
        Set<String> unusedKeys = this.unused();
        Properties unusedProps = new Properties();
        for (String key : unusedKeys) {
            unusedProps.setProperty(key, originalProperties.getProperty(key));
        }
        return unusedProps;
    }

    public Properties getOriginalProperties() {
        return originalProperties;
    }
}
