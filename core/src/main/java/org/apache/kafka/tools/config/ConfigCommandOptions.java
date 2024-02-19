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
package org.apache.kafka.tools.config;

import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;
import kafka.server.DynamicConfig;
import kafka.server.KafkaConfig;
import org.apache.kafka.server.config.ConfigType;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.storage.internals.log.LogConfig;

import java.util.stream.Collectors;

public class ConfigCommandOptions extends CommandDefaultOptions {
    public static final String nl = System.getProperty("line.separator");

    OptionSpec<String> zkConnectOpt;
    OptionSpec<String> bootstrapServerOpt;
    OptionSpec<String> bootstrapControllerOpt;
    OptionSpec<String> commandConfigOpt;
    OptionSpecBuilder alterOpt;
    OptionSpecBuilder describeOpt;
    OptionSpecBuilder allOpt;
    OptionSpec<String> entityType;
    OptionSpec<String> entityName;
    private OptionSpecBuilder entityDefault;
    OptionSpec<String> addConfig;
    OptionSpec<String> addConfigFile;
    OptionSpec<String> deleteConfig;
    OptionSpecBuilder forceOpt;
    OptionSpec<String> topic;
    OptionSpec<String> client;
    private OptionSpec<?> clientDefaults;
    OptionSpec<String> user;
    private OptionSpec<?> userDefaults;
    OptionSpec<String> broker;
    private OptionSpec<?> brokerDefaults;
    private OptionSpec<String> brokerLogger;
    private OptionSpec<?> ipDefaults;
    OptionSpec<String> ip;
    OptionSpec<String> zkTlsConfigFile;

    public ConfigCommandOptions(String[] args) {
        super(args);

        zkConnectOpt = parser.accepts("zookeeper", "DEPRECATED. The connection string for the zookeeper connection in the form host:port. " +
                        "Multiple URLS can be given to allow fail-over. Required when configuring SCRAM credentials for users or " +
                        "dynamic broker configs when the relevant broker(s) are down. Not allowed otherwise.")
                .withRequiredArg()
                .describedAs("urls")
                .ofType(String.class);
        bootstrapServerOpt = parser.accepts("bootstrap-server", "The Kafka servers to connect to.")
                .withRequiredArg()
                .describedAs("server to connect to")
                .ofType(String.class);
        bootstrapControllerOpt = parser.accepts("bootstrap-controller", "The Kafka controllers to connect to.")
                .withRequiredArg()
                .describedAs("controller to connect to")
                .ofType(String.class);
        commandConfigOpt = parser.accepts("command-config", "Property file containing configs to be passed to Admin Client. " +
                        "This is used only with --bootstrap-server option for describing and altering broker configs.")
                .withRequiredArg()
                .describedAs("command config property file")
                .ofType(String.class);
        alterOpt = parser.accepts("alter", "Alter the configuration for the entity.");
        describeOpt = parser.accepts("describe", "List configs for the given entity.");
        allOpt = parser.accepts("all", "List all configs for the given topic, broker, or broker-logger entity (includes static configuration when the entity type is brokers)");
        entityType = parser.accepts("entity-type", "Type of entity (topics/clients/users/brokers/broker-loggers/ips/client-metrics)")
                .withRequiredArg()
                .ofType(String.class);
        entityName = parser.accepts("entity-name", "Name of entity (topic name/client id/user principal name/broker id/ip/client metrics)")
                .withRequiredArg()
                .ofType(String.class);
        entityDefault = parser.accepts("entity-default", "Default entity name for clients/users/brokers/ips (applies to corresponding entity type in command line)");

        addConfig = parser.accepts("add-config", "Key Value pairs of configs to add. Square brackets can be used to group values which contain commas: 'k1=v1,k2=[v1,v2,v2],k3=v3'. The following is a list of valid configurations: " +
                        "For entity-type '" + ConfigType.TOPIC + "': " + LogConfig.configNames().stream().map(n -> "\t" + n).collect(Collectors.joining(nl, nl, nl)) +
                        "For entity-type '" + ConfigType.BROKER + "': " + DynamicConfig.Broker.names().stream().sorted().map(n -> "\t" + n).collect(Collectors.joining(nl, nl, nl)) +
                        "For entity-type '" + ConfigType.USER + "': " + DynamicConfig.User.names().stream().sorted().map(n -> "\t" + n).collect(Collectors.joining(nl, nl, nl)) +
                        "For entity-type '" + ConfigType.CLIENT + "': " + DynamicConfig.Client.names().stream().sorted().map(n -> "\t" + n).collect(Collectors.joining(nl, nl, nl)) +
                        "For entity-type '" + ConfigType.IP + "': " + DynamicConfig.Ip.names().stream().sorted().map(n -> "\t" + n).collect(Collectors.joining(nl, nl, nl)) +
                        "For entity-type '" + ConfigType.CLIENT_METRICS + "': " + DynamicConfig.ClientMetrics.names().stream().sorted().map(n -> "\t" + n).collect(Collectors.joining(nl, nl, nl)) +
                        "Entity types '" + ConfigType.USER + "' and '" + ConfigType.CLIENT + "' may be specified together to update config for clients of a specific user.")
                .withRequiredArg()
                .ofType(String.class);
        addConfigFile = parser.accepts("add-config-file", "Path to a properties file with configs to add. See add-config for a list of valid configurations.")
                .withRequiredArg()
                .ofType(String.class);
        deleteConfig = parser.accepts("delete-config", "config keys to remove 'k1,k2'")
                .withRequiredArg()
                .ofType(String.class)
                .withValuesSeparatedBy(',');
        forceOpt = parser.accepts("force", "Suppress console prompts");
        topic = parser.accepts("topic", "The topic's name.")
                .withRequiredArg()
                .ofType(String.class);
        client = parser.accepts("client", "The client's ID.")
                .withRequiredArg()
                .ofType(String.class);
        clientDefaults = parser.accepts("client-defaults", "The config defaults for all clients.");
        user = parser.accepts("user", "The user's principal name.")
                .withRequiredArg()
                .ofType(String.class);
        userDefaults = parser.accepts("user-defaults", "The config defaults for all users.");
        broker = parser.accepts("broker", "The broker's ID.")
                .withRequiredArg()
                .ofType(String.class);
        brokerDefaults = parser.accepts("broker-defaults", "The config defaults for all brokers.");
        brokerLogger = parser.accepts("broker-logger", "The broker's ID for its logger config.")
                .withRequiredArg()
                .ofType(String.class);
        ipDefaults = parser.accepts("ip-defaults", "The config defaults for all IPs.");
        ip = parser.accepts("ip", "The IP address.")
                .withRequiredArg()
                .ofType(String.class);
        zkTlsConfigFile = parser.accepts("zk-tls-config-file",
                        "Identifies the file where ZooKeeper client TLS connectivity properties are defined.  Any properties other than " +
                                KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.toList.sorted.mkString(", ") + " are ignored.")
                .withRequiredArg().describedAs("ZooKeeper TLS configuration").ofType(String.class);
    }
}
