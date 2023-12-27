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

import joptsimple.OptionException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.config.ConfigType;
import org.apache.kafka.server.util.CommandLineUtils;
import org.apache.kafka.tools.PushHttpMetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * This script can be used to change configs for topics/clients/users/brokers/ips/client-metrics dynamically
 * An entity described or altered by the command may be one of:
 * <ul>
 *     <li> topic: --topic <topic> OR --entity-type topics --entity-name <topic>
 *     <li> client: --client <client> OR --entity-type clients --entity-name <client-id>
 *     <li> user: --user <user-principal> OR --entity-type users --entity-name <user-principal>
 *     <li> <user, client>: --user <user-principal> --client <client-id> OR
 *                          --entity-type users --entity-name <user-principal> --entity-type clients --entity-name <client-id>
 *     <li> broker: --broker <broker-id> OR --entity-type brokers --entity-name <broker-id>
 *     <li> broker-logger: --broker-logger <broker-id> OR --entity-type broker-loggers --entity-name <broker-id>
 *     <li> ip: --ip <ip> OR --entity-type ips --entity-name <ip>
 *     <li> client-metrics: --client-metrics <name> OR --entity-type client-metrics --entity-name <name>
 * </ul>
 * --entity-type <users|clients|brokers|ips> --entity-default may be specified in place of --entity-type <users|clients|brokers|ips> --entity-name <entityName>
 * when describing or altering default configuration for users, clients, brokers, or ips, respectively.
 * Alternatively, --user-defaults, --client-defaults, --broker-defaults, or --ip-defaults may be specified in place of
 * --entity-type <users|clients|brokers|ips> --entity-default, respectively.
 *
 * For most use cases, this script communicates with a kafka cluster (specified via the
 * `--bootstrap-server` option). There are three exceptions where direct communication with a
 * ZooKeeper ensemble (specified via the `--zookeeper` option) is allowed:
 *
 * 1. Describe/alter user configs where the config is a SCRAM mechanism name (i.e. a SCRAM credential for a user)
 * 2. Describe/alter broker configs for a particular broker when that broker is down
 * 3. Describe/alter broker default configs when all brokers are down
 *
 * For example, this allows password configs to be stored encrypted in ZK before brokers are started,
 * avoiding cleartext passwords in `server.properties`.
 */
public class ConfigCommand {
    private static final Logger logger = LoggerFactory.getLogger(ConfigCommand.class);

    public static final String BROKER_DEFAULT_ENTITY_NAME = "";
    public static final String BROKER_LOGGER_CONFIG_TYPE = "broker-loggers";
    public static final List<String> BROKER_SUPPORTED_CONFIG_TYPES;
    public static final List<String> ZK_SUPPORTED_CONFIG_TYPES = Arrays.asList(ConfigType.USER, ConfigType.BROKER);
    public static final int DEFAULT_SCRAM_ITERATIONS = 4096;

    static {
        List<String> brokerSupportedConfigTypes = new ArrayList<>();

        brokerSupportedConfigTypes.addAll(ConfigType.ALL);
        brokerSupportedConfigTypes.add(BROKER_LOGGER_CONFIG_TYPE);
        brokerSupportedConfigTypes.add(ConfigType.CLIENT_METRICS);

        BROKER_SUPPORTED_CONFIG_TYPES = Collections.unmodifiableList(brokerSupportedConfigTypes);
    }

    public static void main(String[] args) {
        try {
            ConfigCommandOptions opts = new ConfigCommandOptions(args);

            CommandLineUtils.maybePrintHelpOrVersion(opts, "This tool helps to manipulate and describe entity config for a topic, client, user, broker, ip or client-metrics");

            opts.checkArgs();

            if (opts.options.has(opts.zkConnectOpt)) {
                System.out.println("Warning: --zookeeper is deprecated and will be removed in a future version of Kafka.");
                System.out.println("Use --bootstrap-server instead to specify a broker to connect to.");
                processCommandWithZk(opts.options.valueOf(opts.zkConnectOpt), opts);
            } else {
                processCommand(opts);
            }
        } catch (IllegalArgumentException | InvalidConfigurationException | OptionException e) {
            logger.debug("Failed config command with args '" + Utils.join(args, " ") + "'", e);
            System.err.println(e.getMessage());
            Exit.exit(1);
        } catch (Throwable t) {
            logger.debug("Error while executing config command with args '" + Utils.join(args, " ") + "'", t);
            System.err.println("Error while executing config command with args '" + Utils.join(args, " ") + "'");
            t.printStackTrace(System.err);
            Exit.exit(1);
        }
    }
}
