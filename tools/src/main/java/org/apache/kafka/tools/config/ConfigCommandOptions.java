/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.tools.config;

import joptsimple.OptionSpec;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.storage.internals.log.LogConfig;

import static org.apache.kafka.tools.ToolsUtils.NL;

public class ConfigCommandOptions extends CommandDefaultOptions {
    final OptionSpec<String> zkConnectOpt;
    final OptionSpec<String> bootstrapServerOpt;
    final OptionSpec<String> bootstrapControllerOpt;
    final OptionSpec<String> commandConfigOpt;
    final OptionSpec<?> alterOpt;
    final OptionSpec<?> describeOpt;
    final OptionSpec<?> allOpt;
    final OptionSpec<String> entityType;
    final OptionSpec<String> entityName;
    final OptionSpec<?> entityDefault;
    final OptionSpec<String> addConfig;
    final OptionSpec<String> addConfigFile;
    final OptionSpec<String> deleteConfig;
    final OptionSpec<?> forceOpt;
    final OptionSpec<String> topic;
    final OptionSpec<String> client;
    final OptionSpec<?> clientDefaults;
    final OptionSpec<String> user;
    final OptionSpec<?> userDefaults;
    final OptionSpec<String> broker;
    final OptionSpec<?> brokerDefaults;
    final OptionSpec<String> brokerLogger;
    final OptionSpec<?> ipDefaults;
    final OptionSpec<String> ip;
    final OptionSpec<String> zkTlsConfigFile;

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

        entityType = parser.accepts("entity-type", "Type of entity (topics/clients/users/brokers/broker-loggers/ips)")
            .withRequiredArg()
            .ofType(String.class);
        entityName = parser.accepts("entity-name", "Name of entity (topic name/client id/user principal name/broker id/ip)")
            .withRequiredArg()
            .ofType(String.class);
        entityDefault = parser.accepts("entity-default", "Default entity name for clients/users/brokers/ips (applies to corresponding entity type in command line)");

        addConfig = parser.accepts("add-config", "Key Value pairs of configs to add. Square brackets can be used to group values which contain commas: 'k1=v1,k2=[v1,v2,v2],k3=v3'. The following is a list of valid configurations: " +
                "For entity-type '" + ConfigType.Topic + "': " + LogConfig.configNames.asScala.map("\t" + _).mkString(NL, NL, NL) +
                "For entity-type '" + ConfigType.Broker + "': " + DynamicConfig.Broker.names.asScala.toSeq.sorted.map("\t" + _).mkString(NL, NL, NL) +
                "For entity-type '" + ConfigType.User + "': " + DynamicConfig.User.names.asScala.toSeq.sorted.map("\t" + _).mkString(NL, NL, NL) +
                "For entity-type '" + ConfigType.Client + "': " + DynamicConfig.Client.names.asScala.toSeq.sorted.map("\t" + _).mkString(NL, NL, NL) +
                "For entity-type '" + ConfigType.Ip + "': " + DynamicConfig.Ip.names.asScala.toSeq.sorted.map("\t" + _).mkString(NL, NL, NL) +
                "Entity types '" + ConfigType.User + "' and '" + ConfigType.Client + "' may be specified together to update config for clients of a specific user.")
            .withRequiredArg()
            .ofType(String.class);
        addConfigFile = parser.accepts("add-config-file", "Path to a properties file with configs to add. See add-config for a list of valid configurations.")
            .withRequiredArg()
            .ofType(String.class);
        deleteConfig = parser.accepts("delete-config", "config keys to remove 'k1,k2'")
            .withRequiredArg()
            .ofType(String.class)
            .withValuesSeparatedBy(',');
        forceOpt = parser.accepts("force", "Suppress console prompts")
        topic = parser.accepts("topic", "The topic's name.")
            .withRequiredArg()
            .ofType(String.class);
        client = parser.accepts("client", "The client's ID.")
            .withRequiredArg()
            .ofType(String.class);
        clientDefaults = parser.accepts("client-defaults", "The config defaults for all clients.")
        user = parser.accepts("user", "The user's principal name.")
            .withRequiredArg()
            .ofType(String.class);
        userDefaults = parser.accepts("user-defaults", "The config defaults for all users.")
        broker = parser.accepts("broker", "The broker's ID.")
            .withRequiredArg()
            .ofType(String.class);
        brokerDefaults = parser.accepts("broker-defaults", "The config defaults for all brokers.")
        brokerLogger = parser.accepts("broker-logger", "The broker's ID for its logger config.")
            .withRequiredArg()
            .ofType(String.class);
        ipDefaults = parser.accepts("ip-defaults", "The config defaults for all IPs.")
        ip = parser.accepts("ip", "The IP address.")
            .withRequiredArg()
            .ofType(String.class);
        zkTlsConfigFile = parser.accepts("zk-tls-config-file",
                "Identifies the file where ZooKeeper client TLS connectivity properties are defined.  Any properties other than " +
                    KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.toList.sorted.mkString(", ") + " are ignored.")
            .withRequiredArg().describedAs("ZooKeeper TLS configuration").ofType(String.class);
    }
}
