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

package org.apache.kafka.tools.metadata;

import kafka.raft.KafkaRaftManager;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import kafka.server.MetaProperties;
import kafka.tools.TerseFailure;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.compat.java8.OptionConverters;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * The Kafka metadata tool.
 */
public final class MetadataTool {
    private static final Logger log = LoggerFactory.getLogger(MetadataTool.class);

    public static class Builder {
        private String controllers;
        private String configPath;
        private File tempDir;

        public Builder setControllers(String controllers) {
            this.controllers = controllers;
            return this;
        }

        public Builder setConfigPath(String configPath) {
            this.configPath = configPath;
            return this;
        }

        public Builder setTempDir(File tempDir) {
            this.tempDir = tempDir;
            return this;
        }

        public MetadataTool build() throws Exception {
            Properties properties = null;
            if (configPath != null) {
                properties = Utils.loadProps(configPath);
            } else {
                properties = new Properties();
            }
            if (controllers != null) {
                properties.setProperty(KafkaConfig$.MODULE$.ControllerQuorumVotersProp(),
                    controllers);
            }
            if (properties.getProperty(KafkaConfig$.MODULE$.ControllerQuorumVotersProp()) == null) {
                throw new TerseFailure("Please use --controllers to specify the quorum voters.");
            }
            // TODO: we really shouldn't have to set up a fake broker config like this.
            // In particular, it should be possible to run the KafkRaftManager without
            // using a log directory at all.  And we should be able to set -1 as our ID,
            // since we're not a voter.
            final int fakeId = Integer.MAX_VALUE;
            properties.setProperty(KafkaConfig$.MODULE$.MetadataLogDirProp(),
                tempDir.getAbsolutePath());
            properties.remove(KafkaConfig$.MODULE$.LogDirProp());
            properties.remove(KafkaConfig$.MODULE$.LogDirsProp());
            properties.remove(KafkaConfig$.MODULE$.ControllerIdProp());
            properties.setProperty(KafkaConfig$.MODULE$.BrokerIdProp(), Integer.toString(fakeId));
            properties.setProperty(KafkaConfig$.MODULE$.ProcessRolesProp(), "broker");
            KafkaConfig config = new KafkaConfig(properties);
            MetaProperties metaProperties = MetaProperties.apply(Uuid.ZERO_UUID,
                OptionConverters.toScala(Optional.of(fakeId)),
                OptionConverters.toScala(Optional.empty()));
            TopicPartition metadataPartition =
                new TopicPartition(KafkaServer.metadataTopicName(), 0);
            KafkaRaftManager raftManager = null;
            MetadataNodeManager nodeManager = null;
            try {
                raftManager = new KafkaRaftManager(metaProperties,
                    metadataPartition,
                    config,
                    Time.SYSTEM,
                    new Metrics());
                nodeManager = new MetadataNodeManager();
            } catch (Throwable e) {
                log.error("Initialization error", e);
                if (raftManager != null) {
                    raftManager.shutdown();
                }
                if (nodeManager != null) {
                    nodeManager.close();
                }
                throw e;
            }
            return new MetadataTool(raftManager, nodeManager);
        }
    }

    private final KafkaRaftManager raftManager;

    private final MetadataNodeManager nodeManager;

    public MetadataTool(KafkaRaftManager raftManager,
                        MetadataNodeManager nodeManager) {
        this.raftManager = raftManager;
        this.nodeManager = nodeManager;
    }

    public void run(List<String> args) {
        if (args == null || args.isEmpty()) {
            runShell();
        } else {
            runCommand(args);
        }
    }

    public void runShell() {
        log.info("runShell");
//        Console console = System.console();
//        if (console == null) {
//            log.error("No system console available.");
//        } else {
//            while (true) {
//                String line = console.readLine("metadata> ");
//                // ok... so we need to get this into "list of strings" format, which is annoying.
//                // we need to implement quoting and so forth, I guess.
//                // OR just start out using jline from the start.
//                Command.PARSER.par
//            }
//        }
    }

    void runCommand(List<String> args) {
        log.info("runCommand " + String.join(",", args));
    }

    public void close() {
        raftManager.shutdown();
    }

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("metadata-tool")
            .defaultHelp(true)
            .description("The Apache Kafka metadata tool");
        parser.addArgument("--controllers", "-C")
            .type(String.class)
            .help("The quorum voter connection string to use.");
        parser.addArgument("--config", "-c")
            .type(String.class)
            .help("The configuration file to use.");
        parser.addArgument("command")
            .nargs("*")
            .help("The command to run.");
        Namespace res = parser.parseArgsOrFail(args);
        Builder builder = new Builder();
        builder.setControllers(res.getString("controllers"));
        builder.setConfigPath(res.getString("config"));
        Path tempDir = Files.createTempDirectory("MetadataTool");
        Exit.addShutdownHook("agent-shutdown-hook", () -> {
            log.debug("Removing temporary directory " + tempDir.toAbsolutePath().toString());
            try {
                Utils.delete(tempDir.toFile());
            } catch (Exception e) {
                log.error("Got exception while removing temporary directory " +
                    tempDir.toAbsolutePath().toString());
            }
        });
        builder.setTempDir(tempDir.toFile());
        MetadataTool tool = null;
        try {
            tool = builder.build();
        } catch (TerseFailure e) {
            System.err.println("Error: " + e.getMessage());
            Exit.exit(1);
        }
        try {
            tool.run(res.getList("command"));
        } finally {
            tool.close();
        }
        Exit.exit(0);
    }
}
