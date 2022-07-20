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

package kafka.shell;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.metadata.util.ClusterMetadataSource;
import org.apache.kafka.metadata.util.LocalDirectorySource;
import org.apache.kafka.raft.RaftConfig;
import org.apache.kafka.shell.InteractiveShell;
import org.apache.kafka.shell.NonInteractiveShell;

import java.util.List;


/**
 * The entry point for the Kafka metadata shell, which can be used to analyze KRaft cluster metadata.
 *
 * Note: nearly all of the code for the metadata shell is in the "shell" gradle module. However, this file and
 * MetadataShellObserver.java are in the core module so that they can access KafkaRaftManager, which is also part of the
 * core gradle module.
 */
class MetadataShellTool {
    static public void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("metadata-shell-tool").
            defaultHelp(true).
            description("The Apache Kafka metadata tool");
        parser.addArgument("--cluster-id", "-t").
            help("The cluster id. Required when using --controllers");
        MutuallyExclusiveGroup accessGroup = parser.addMutuallyExclusiveGroup().
            required(true);
        accessGroup.addArgument("--directory", "-d").
            help("The __cluster_metadata-0 directory to read.");
        accessGroup.addArgument("--controllers", "-q").
            help("The " + RaftConfig.QUORUM_VOTERS_CONFIG + ".");
        parser.addArgument("command").
            nargs("*").
            help("The command to run.");
        Namespace res = parser.parseArgsOrFail(args);
        ClusterMetadataSource source;
        if (res.getString("directory") != null) {
            source = new LocalDirectorySource(res.getString("directory"));
        } else if (res.getString("controllers") != null) {
            String clusterId = res.getString("cluster_id");
            if (clusterId == null || clusterId.isEmpty()) {
                throw new RuntimeException("You must provide --cluster-id when connecting " +
                    "directly to the controllers.");
            }
            source = MetadataShellObserver.create(res.getString("controllers"), clusterId);
        } else {
            throw new RuntimeException("You must set either --directory or --controllers");
        }
        try {
            List<String> command = res.<String>getList("command");
            if (command.isEmpty()) {
                InteractiveShell shell = new InteractiveShell(source);
                try {
                    shell.run();
                } finally {
                    shell.close();
                }
            } else {
                NonInteractiveShell shell = new NonInteractiveShell(source);
                try {
                    shell.run(System.out, command);
                } finally {
                    shell.close();
                }
            }
            Exit.exit(0);
        } catch (Throwable e) {
            System.err.println("Unexpected error: " + ((e.getMessage() == null) ? "" : e.getMessage()));
            e.printStackTrace(System.err);
            Exit.exit(1);
        }
    }
}
