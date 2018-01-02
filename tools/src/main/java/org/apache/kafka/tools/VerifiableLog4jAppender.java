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
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
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
    Logger logger = Logger.getLogger(VerifiableLog4jAppender.class);

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
    public static Properties loadProps(String filename) throws IOException, FileNotFoundException {
        Properties props = new Properties();
        try (InputStream propStream = new FileInputStream(filename)) {
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
            props.setProperty("log4j.rootLogger", "INFO, KAFKA");
            props.setProperty("log4j.appender.KAFKA", "org.apache.kafka.log4jappender.KafkaLog4jAppender");
            props.setProperty("log4j.appender.KAFKA.layout", "org.apache.log4j.PatternLayout");
            props.setProperty("log4j.appender.KAFKA.layout.ConversionPattern", "%-5p: %c - %m%n");
            props.setProperty("log4j.appender.KAFKA.BrokerList", res.getString("brokerList"));
            props.setProperty("log4j.appender.KAFKA.Topic", topic);
            props.setProperty("log4j.appender.KAFKA.RequiredNumAcks", res.getString("acks"));
            props.setProperty("log4j.appender.KAFKA.SyncSend", "true");
            final String securityProtocol = res.getString("securityProtocol");
            if (securityProtocol != null && !securityProtocol.equals(SecurityProtocol.PLAINTEXT.toString())) {
                props.setProperty("log4j.appender.KAFKA.SecurityProtocol", securityProtocol);
            }
            if (securityProtocol != null && securityProtocol.contains("SSL")) {
                props.setProperty("log4j.appender.KAFKA.SslTruststoreLocation", res.getString("sslTruststoreLocation"));
                props.setProperty("log4j.appender.KAFKA.SslTruststorePassword", res.getString("sslTruststorePassword"));
            }
            if (securityProtocol != null && securityProtocol.contains("SASL")) {
                props.setProperty("log4j.appender.KAFKA.SaslKerberosServiceName", res.getString("saslKerberosServiceName"));
                props.setProperty("log4j.appender.KAFKA.clientJaasConfPath", res.getString("clientJaasConfPath"));
                props.setProperty("log4j.appender.KAFKA.kerb5ConfPath", res.getString("kerb5ConfPath"));
            }
            props.setProperty("log4j.logger.kafka.log4j", "INFO, KAFKA");

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
        PropertyConfigurator.configure(props);
    }

    public static void main(String[] args) throws IOException {

        final VerifiableLog4jAppender appender = createFromArgs(args);
        boolean infinite = appender.maxMessages < 0;

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Trigger main thread to stop producing messages
                appender.stopLogging = true;
            }
        });

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
