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

package org.apache.kafka.shell;

import kafka.raft.KafkaRaftManager;
import kafka.tools.TerseFailure;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.image.loader.MetadataLoader;
import org.apache.kafka.metadata.util.SnapshotFileReader;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.fault.FaultHandler;
import org.apache.kafka.server.fault.LoggingFaultHandler;
import org.apache.kafka.shell.command.Commands;
import org.apache.kafka.shell.state.MetadataShellPublisher;
import org.apache.kafka.shell.state.MetadataShellState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


/**
 * The Kafka metadata shell entry point.
 */
public final class MetadataShell {
    private static final Logger log = LoggerFactory.getLogger(MetadataShell.class);

    public static class Builder {
        private KafkaRaftManager<ApiMessageAndVersion> raftManager = null;
        private String snapshotPath = null;
        private FaultHandler faultHandler = new LoggingFaultHandler("shell", () -> { });

        public Builder setRaftManager(KafkaRaftManager<ApiMessageAndVersion> raftManager) {
            this.raftManager = raftManager;
            return this;
        }

        public Builder setSnapshotPath(String snapshotPath) {
            this.snapshotPath = snapshotPath;
            return this;
        }

        public Builder setFaultHandler(FaultHandler faultHandler) {
            this.faultHandler = faultHandler;
            return this;
        }

        public MetadataShell build() {
            return new MetadataShell(raftManager,
                snapshotPath,
                faultHandler);
        }
    }

    private final MetadataShellState state;

    private final KafkaRaftManager<ApiMessageAndVersion> raftManager;

    private final String snapshotPath;

    private final FaultHandler faultHandler;

    private final MetadataShellPublisher publisher;

    private SnapshotFileReader snapshotFileReader;

    private MetadataLoader loader;

    public MetadataShell(
        KafkaRaftManager<ApiMessageAndVersion> raftManager,
        String snapshotPath,
        FaultHandler faultHandler
    ) {
        this.state = new MetadataShellState();
        this.raftManager = raftManager;
        this.snapshotPath = snapshotPath;
        this.faultHandler = faultHandler;
        this.publisher = new MetadataShellPublisher(state);
        this.snapshotFileReader = null;
    }

    private void initializeWithRaftManager() {
        raftManager.startup();
        this.loader = new MetadataLoader.Builder().
                setFaultHandler(faultHandler).
                setNodeId(-1).
                setHighWaterMarkAccessor(() -> raftManager.client().highWatermark()).
                build();
        raftManager.register(loader);
    }

    private void initializeWithSnapshotFileReader() throws Exception {
        this.loader = new MetadataLoader.Builder().
                setFaultHandler(faultHandler).
                setNodeId(-1).
                setHighWaterMarkAccessor(() -> snapshotFileReader.highWaterMark()).
                build();
        snapshotFileReader = new SnapshotFileReader(snapshotPath, loader);
        snapshotFileReader.startup();
    }

    public void run(List<String> args) throws Exception {
        if (raftManager != null) {
            if (snapshotPath != null) {
                throw new RuntimeException("Can't specify both a raft manager and " +
                        "snapshot file reader.");
            }
            initializeWithRaftManager();
        } else if (snapshotPath != null) {
            initializeWithSnapshotFileReader();
        } else {
            throw new RuntimeException("You must specify either a raft manager or a " +
                    "snapshot file reader.");
        }
        loader.installPublishers(Collections.singletonList(publisher)).get(15, TimeUnit.MINUTES);
        if (args == null || args.isEmpty()) {
            // Interactive mode.
            System.out.println("Loading...");
            waitUntilCaughtUp();
            System.out.println("Starting...");
            try (InteractiveShell shell = new InteractiveShell(state)) {
                shell.runMainLoop();
            }
        } else {
            // Non-interactive mode.
            waitUntilCaughtUp();
            Commands commands = new Commands(false);
            try (PrintWriter writer = new PrintWriter(new BufferedWriter(
                    new OutputStreamWriter(System.out, StandardCharsets.UTF_8)))) {
                Commands.Handler handler = commands.parseCommand(args);
                handler.run(Optional.empty(), writer, state);
                writer.flush();
            }
        }
    }

    public void close() throws Exception {
        Utils.closeQuietly(loader, "loader");
        if (raftManager != null) {
            try {
                raftManager.shutdown();
            } catch (Exception e) {
                log.error("Error shutting down RaftManager", e);
            }
        }
        Utils.closeQuietly(snapshotFileReader, "raftManager");
    }

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("kafka-metadata-shell")
            .defaultHelp(true)
            .description("The Apache Kafka metadata shell");
        parser.addArgument("--snapshot", "-s")
            .type(String.class)
            .help("The snapshot file to read.");
        parser.addArgument("command")
            .nargs("*")
            .help("The command to run.");
        Namespace res = parser.parseArgsOrFail(args);
        try {
            Builder builder = new Builder();
            builder.setSnapshotPath(res.getString("snapshot"));
            Path tempDir = Files.createTempDirectory("MetadataShell");
            Exit.addShutdownHook("agent-shutdown-hook", () -> {
                log.debug("Removing temporary directory " + tempDir.toAbsolutePath());
                try {
                    Utils.delete(tempDir.toFile());
                } catch (Exception e) {
                    log.error("Got exception while removing temporary directory " +
                        tempDir.toAbsolutePath());
                }
            });
            MetadataShell shell = builder.build();
            try {
                shell.run(res.getList("command"));
            } finally {
                shell.close();
            }
            Exit.exit(0);
        } catch (TerseFailure e) {
            System.err.println("Error: " + e.getMessage());
            Exit.exit(1);
        } catch (Throwable e) {
            System.err.println("Unexpected error: " +
                (e.getMessage() == null ? "" : e.getMessage()));
            e.printStackTrace(System.err);
            Exit.exit(1);
        }
    }

    void waitUntilCaughtUp() throws ExecutionException, InterruptedException {
        while (true) {
            if (loader.lastAppliedOffset() > 0) {
                return;
            }
            Thread.sleep(10);
        }
        //snapshotFileReader.caughtUpFuture().get();
    }
}
