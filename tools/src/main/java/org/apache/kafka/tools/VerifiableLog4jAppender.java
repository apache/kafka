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
package org.apache.kafka.tools;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Exit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.properties.PropertiesConfigurationBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/**
 * Primarily intended for use with system testing, this appender produces message
 * to Kafka on each "append" request. For example, this helps with end-to-end tests
 * of KafkaLog4jAppender.
 *
 * When used as a command-line tool, it appends increasing integers. It will produce a
 * fixed number of messages unless the default max-messages -1 is used, in which case
 * it appends indefinitely.
 */

public class VerifiableLog4jAppender {
    static {
        // VerifiableLog4jAppender should not be interfered by log4 1.x configuration.
        System.clearProperty("log4j.configuration");
    }

    Logger logger = LogManager.getLogger(VerifiableLog4jAppender.class);

    // If maxMessages < 0, log until the process is killed externally
    private long maxMessages = -1;

    // Hook to trigger logging thread to stop logging messages
    private volatile boolean stopLogging = false;

    /** Get the command-line argument parser. */
    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("verifiable-log4j-appender")
            .defaultHelp(true)
            .description("This tool produces increasing integers to the specified topic using KafkaLog4jAppender.");

        parser.addArgument("--topic")
            .action(store())
            .required(true)
            .type(String.class)
            .metavar("TOPIC")
            .help("Produce messages to this topic.");

        parser.addArgument("--broker-list")
            .action(store())
            .required(true)
            .type(String.class)
            .metavar("HOST1:PORT1[,HOST2:PORT2[...]]")
            .dest("brokerList")
            .help("Comma-separated list of Kafka brokers in the form HOST1:PORT1,HOST2:PORT2,...");

        parser.addArgument("--max-messages")
            .action(store())
            .required(false)
            .setDefault(-1)
            .type(Integer.class)
            .metavar("MAX-MESSAGES")
            .dest("maxMessages")
            .help("Produce this many messages. If -1, produce messages until the process is killed externally.");

        parser.addArgument("--acks")
            .action(store())
            .required(false)
            .setDefault("-1")
            .type(String.class)
            .choices("0", "1", "-1")
            .metavar("ACKS")
            .help("Acks required on each produced message. See Kafka docs on request.required.acks for details.");

        parser.addArgument("--security-protocol")
            .action(store())
            .required(false)
            .setDefault("PLAINTEXT")
            .type(String.class)
            .choices("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL")
            .metavar("SECURITY-PROTOCOL")
            .dest("securityProtocol")
            .help("Security protocol to be used while communicating with Kafka brokers.");

        parser.addArgument("--ssl-truststore-location")
            .action(store())
            .required(false)
            .type(String.class)
            .metavar("SSL-TRUSTSTORE-LOCATION")
            .dest("sslTruststoreLocation")
            .help("Location of SSL truststore to use.");

        parser.addArgument("--ssl-truststore-password")
            .action(store())
            .required(false)
            .type(String.class)
            .metavar("SSL-TRUSTSTORE-PASSWORD")
            .dest("sslTruststorePassword")
            .help("Password for SSL truststore to use.");

        parser.addArgument("--appender.config")
            .action(store())
            .required(false)
            .type(String.class)
            .metavar("CONFIG_FILE")
            .help("Log4jAppender config properties file.");

        parser.addArgument("--sasl-kerberos-service-name")
            .action(store())
            .required(false)
            .type(String.class)
            .metavar("SASL-KERBEROS-SERVICE-NAME")
            .dest("saslKerberosServiceName")
            .help("Name of sasl kerberos service.");

        parser.addArgument("--client-jaas-conf-path")
            .action(store())
            .required(false)
            .type(String.class)
            .metavar("CLIENT-JAAS-CONF-PATH")
            .dest("clientJaasConfPath")
            .help("Path of JAAS config file of Kafka client.");

        parser.addArgument("--kerb5-conf-path")
            .action(store())
            .required(false)
            .type(String.class)
            .metavar("KERB5-CONF-PATH")
            .dest("kerb5ConfPath")
            .help("Path of Kerb5 config file.");

        return parser;
    }

    /**
     * Read a properties file from the given path
     * @param filename The path of the file to read
     *
     * Note: this duplication of org.apache.kafka.common.utils.Utils.loadProps is unfortunate
     * but *intentional*. In order to use VerifiableProducer in compatibility and upgrade tests,
     * we use VerifiableProducer from the development tools package, and run it against 0.8.X.X kafka jars.
     * Since this method is not in Utils in the 0.8.X.X jars, we have to cheat a bit and duplicate.
     */
    public static Properties loadProps(String filename) throws IOException {
        Properties props = new Properties();
        try (InputStream propStream = Files.newInputStream(Paths.get(filename))) {
            props.load(propStream);
        }
        return props;
    }

    /** Construct a VerifiableLog4jAppender object from command-line arguments. */
    public static VerifiableLog4jAppender createFromArgs(String[] args) {
        ArgumentParser parser = argParser();
        VerifiableLog4jAppender producer = null;

        try {
            Namespace res = parser.parseArgs(args);

            int maxMessages = res.getInt("maxMessages");
            String topic = res.getString("topic");
            String configFile = res.getString("appender.config");

            Properties props = new Properties();
            props.setProperty("status", "OFF");
            props.setProperty("name", "VerifiableLog4jAppenderConfig");

            // appenders
            props.setProperty("appenders", "kafkaAppender");

            props.setProperty("appender.kafkaAppender.type", "Kafka");
            props.setProperty("appender.kafkaAppender.name", "KAFKA_APPENDER");
            props.setProperty("appender.kafkaAppender.layout.type", "PatternLayout");
            props.setProperty("appender.kafkaAppender.layout.pattern", "[%d] %p %m (%c)%n");
            props.setProperty("appender.kafkaAppender.topic", topic);
            props.setProperty("appender.kafkaAppender.syncSend", Boolean.TRUE.toString());
            props.setProperty("appender.kafkaAppender.bootstrap_servers.type", "Property");
            props.setProperty("appender.kafkaAppender.bootstrap_servers.name", "bootstrap.servers");
            props.setProperty("appender.kafkaAppender.bootstrap_servers.value", res.getString("brokerList"));
            props.setProperty("appender.kafkaAppender.acks.type", "Property");
            props.setProperty("appender.kafkaAppender.acks.name", "acks");
            props.setProperty("appender.kafkaAppender.acks.value", res.getString("acks"));

            final String securityProtocol = res.getString("securityProtocol");
            if (securityProtocol != null && !securityProtocol.equals(SecurityProtocol.PLAINTEXT.toString())) {
                props.setProperty("appender.kafkaAppender.securityProtocol.type", "Property");
                props.setProperty("appender.kafkaAppender.securityProtocol.name", "securityProtocol");
                props.setProperty("appender.kafkaAppender.securityProtocol.value", securityProtocol);
            }
            if (securityProtocol != null && securityProtocol.contains("SSL")) {
                props.setProperty("appender.kafkaAppender.sslTruststoreLocation.type", "Property");
                props.setProperty("appender.kafkaAppender.sslTruststoreLocation.name", "sslTruststoreLocation");
                props.setProperty("appender.kafkaAppender.sslTruststoreLocation.value", res.getString("sslTruststoreLocation"));

                props.setProperty("appender.kafkaAppender.sslTruststorePassword.type", "Property");
                props.setProperty("appender.kafkaAppender.sslTruststorePassword.name", "sslTruststorePassword");
                props.setProperty("appender.kafkaAppender.sslTruststorePassword.value", res.getString("sslTruststorePassword"));
            }
            if (securityProtocol != null && securityProtocol.contains("SASL")) {
                props.setProperty("appender.kafkaAppender.saslKerberosServiceName.type", "Property");
                props.setProperty("appender.kafkaAppender.saslKerberosServiceName.name", "saslKerberosServiceName");
                props.setProperty("appender.kafkaAppender.saslKerberosServiceName.value", res.getString("saslKerberosServiceName"));

                props.setProperty("appender.kafkaAppender.clientJaasConfPath.type", "Property");
                props.setProperty("appender.kafkaAppender.clientJaasConfPath.name", "clientJaasConfPath");
                props.setProperty("appender.kafkaAppender.clientJaasConfPath.value", res.getString("clientJaasConfPath"));

                props.setProperty("appender.kafkaAppender.kerb5ConfPath.type", "Property");
                props.setProperty("appender.kafkaAppender.kerb5ConfPath.name", "kerb5ConfPath");
                props.setProperty("appender.kafkaAppender.kerb5ConfPath.value", res.getString("kerb5ConfPath"));
            }

            // loggers
            props.setProperty("rootLogger.level", "INFO");
            props.setProperty("rootLogger.appenderRefs", "kafkaAppender");
            props.setProperty("rootLogger.appenderRef.kafkaAppender.ref", "KAFKA_APPENDER");

            if (configFile != null) {
                try {
                    props.putAll(loadProps(configFile));
                } catch (IOException e) {
                    throw new ArgumentParserException(e.getMessage(), parser);
                }
            }

            producer = new VerifiableLog4jAppender(props, maxMessages);
        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                Exit.exit(0);
            } else {
                parser.handleError(e);
                Exit.exit(1);
            }
        }

        return producer;
    }


    public VerifiableLog4jAppender(Properties props, int maxMessages) {
        this.maxMessages = maxMessages;
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configuration config = new PropertiesConfigurationBuilder()
            .setRootProperties(props)
            .setLoggerContext(context)
            .build();
        context.setConfiguration(config);
        Configurator.initialize(config);
    }

    public static void main(String[] args) {

        final VerifiableLog4jAppender appender = createFromArgs(args);
        boolean infinite = appender.maxMessages < 0;

        // Trigger main thread to stop producing messages when shutting down
        Exit.addShutdownHook("verifiable-log4j-appender-shutdown-hook", () -> appender.stopLogging = true);

        long maxMessages = infinite ? Long.MAX_VALUE : appender.maxMessages;
        for (long i = 0; i < maxMessages; i++) {
            if (appender.stopLogging) {
                break;
            }
            appender.append(String.format("%d", i));
        }
    }

    private void append(String msg) {
        logger.info(msg);
    }
}
