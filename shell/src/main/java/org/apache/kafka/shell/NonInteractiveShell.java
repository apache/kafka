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

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.util.ClusterMetadataSource;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * Runs metadata shell commands in non-interactive mode.
 */
public final class NonInteractiveShell implements AutoCloseable {
    private final ClusterMetadataSource source;
    private final Commands commands;
    private final MetadataNodeManager nodeManager;

    public NonInteractiveShell(
        ClusterMetadataSource source,
        Commands commands
    ) throws Exception {
        this.source = source;
        this.commands = commands;
        this.nodeManager = new MetadataNodeManager();
        nodeManager.setup(source);
        source.start(nodeManager.logListener());
    }

    public NonInteractiveShell(
        ClusterMetadataSource source
    ) throws Exception {
        this(source, new Commands(false));
    }

    public void run(
        OutputStream out,
        List<String> command
    ) throws Exception {
        source.caughtUpFuture().get();
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(
                new OutputStreamWriter(out, StandardCharsets.UTF_8)))) {
            Commands.Handler handler = commands.parseCommand(command);
            handler.run(Optional.empty(), writer, nodeManager);
            writer.flush();
        }
    }

    @Override
    public void close() throws Exception {
        Utils.closeQuietly(source, "source");
        Utils.closeQuietly(nodeManager, "nodeManager");
    }
}
