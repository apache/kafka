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

import kafka.tools.TerseFailure;

import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.image.loader.MetadataLoader;
import org.apache.kafka.metadata.util.SnapshotFileReader;
import org.apache.kafka.server.fault.FaultHandler;
import org.apache.kafka.server.fault.LoggingFaultHandler;
import org.apache.kafka.server.util.FileLock;
import org.apache.kafka.shell.command.Commands;
import org.apache.kafka.shell.state.MetadataShellPublisher;
import org.apache.kafka.shell.state.MetadataShellState;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * The Kafka metadata shell entry point.
 */
public final class MetadataShell {
    private static final Logger log = LoggerFactory.getLogger(MetadataShell.class);

    public static class Builder {
        private String snapshotPath = null;
        private FaultHandler faultHandler = new LoggingFaultHandler("shell", () -> { });

        public Builder setSnapshotPath(String snapshotPath) {
            this.snapshotPath = snapshotPath;
            return this;
        }

        public Builder setFaultHandler(FaultHandler faultHandler) {
            this.faultHandler = faultHandler;
            return this;
        }

        public MetadataShell build() {
            return new MetadataShell(snapshotPath, faultHandler);
        }
    }

    /**
     * Return the parent directory of a file. This works around Java's quirky API,
     * which does not honor the UNIX convention of the parent of root being root itself.
     */
    static File parent(File file) {
        File parent = file.getParentFile();
        return parent == null ? file : parent;
    }

    static File parentParent(File file) {
        return parent(parent(file));
    }

    /**
     * Take the FileLock in the given directory, if it already exists. Technically, there is a
     * TOCTOU bug here where someone could create and lock the lockfile in between our check
     * and our use. However, this is very unlikely to ever be a problem in practice, and closing
     * this hole would require the parent directory to always be writable when loading a
     * snapshot so that we could create our .lock file there.
     */
    static FileLock takeDirectoryLockIfExists(File directory) throws IOException {
        if (new File(directory, ".lock").exists()) {
            return takeDirectoryLock(directory);
        } else {
            return null;
        }
    }

    /**
     * Take the FileLock in the given directory.
     */
    static FileLock takeDirectoryLock(File directory) throws IOException {
        FileLock fileLock = new FileLock(new File(directory, ".lock"));
        try {
            if (!fileLock.tryLock()) {
                throw new RuntimeException("Unable to lock " + directory.getAbsolutePath() +
                    ". Please ensure that no broker or controller process is using this " +
                    "directory before proceeding.");
            }
        } catch (Throwable e) {
            fileLock.unlockAndClose();
            throw e;
        }
        return fileLock;
    }

    private final MetadataShellState state;

    private final String snapshotPath;

    private final FaultHandler faultHandler;

    private final MetadataShellPublisher publisher;

    private FileLock fileLock;

    private SnapshotFileReader snapshotFileReader;

    private MetadataLoader loader;

    public MetadataShell(
        String snapshotPath,
        FaultHandler faultHandler
    ) {
        this.state = new MetadataShellState();
        this.snapshotPath = snapshotPath;
        this.faultHandler = faultHandler;
        this.publisher = new MetadataShellPublisher(state);
        this.fileLock = null;
        this.snapshotFileReader = null;
    }

    private void initializeWithSnapshotFileReader() throws Exception {
        this.fileLock = takeDirectoryLockIfExists(parentParent(new File(snapshotPath)));
        this.loader = new MetadataLoader.Builder().
                setFaultHandler(faultHandler).
                setNodeId(-1).
                setHighWaterMarkAccessor(() -> snapshotFileReader.highWaterMark()).
                build();
        snapshotFileReader = new SnapshotFileReader(snapshotPath, loader);
        snapshotFileReader.startup();
    }

    public void run(List<String> args) throws Exception {
        initializeWithSnapshotFileReader();
        loader.installPublishers(List.of(publisher)).get(15, TimeUnit.MINUTES);
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

    public void close() {
        Utils.closeQuietly(loader, "loader");
        Utils.closeQuietly(snapshotFileReader, "snapshotFileReader");
        if (fileLock != null) {
            try {
                fileLock.unlockAndClose();
            } catch (Exception e) {
                log.error("Error cleaning up fileLock", e);
            } finally {
                fileLock = null;
            }
        }
    }

    public static void main(String[] args) {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("kafka-metadata-shell")
            .defaultHelp(true)
            .description("The Apache Kafka metadata shell");
        parser.addArgument("--snapshot", "-s")
            .type(String.class)
            .required(true)
            .help("The metadata snapshot file to read.");
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

    void waitUntilCaughtUp() throws InterruptedException {
        while (true) {
            if (loader.lastAppliedOffset() > 0) {
                return;
            }
            Thread.sleep(10);
        }
    }
}
