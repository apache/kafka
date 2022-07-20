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

import kafka.server.KafkaRaftServer;
import kafka.testkit.KafkaClusterTestKit;
import kafka.testkit.TestKitNodes;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.util.ClusterMetadataSource;
import org.apache.kafka.metadata.util.LocalDirectorySource;
import org.apache.kafka.shell.Commands;
import org.apache.kafka.shell.NonInteractiveShell;
import org.apache.kafka.snapshot.Snapshots;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;


@Timeout(20)
@Tag("integration")
public class MetadataShellToolIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(MetadataShellToolIntegrationTest.class);

    static class CaptureStream implements AutoCloseable {
        private final ByteArrayOutputStream out;
        private final PrintStream stream;

        CaptureStream() throws Exception {
            this.out = new ByteArrayOutputStream();
            this.stream = new PrintStream(out, true, "utf8");
        }

        @Override
        public void close() throws Exception {
            Utils.closeQuietly(stream, "stream");
            Utils.closeQuietly(out, "out");
        }

        List<String> outputLines() throws IOException {
            out.flush();
            return Arrays.asList(out.toString().split("\n"));
        }
    }

    static void assertLinesMatch(
        List<String> expected,
        List<String> actual
    ) {
        for (int i = 0; i < Math.max(expected.size(), actual.size()); i++) {
            String expectedLine = i < expected.size() ? expected.get(i) : "";
            String actualLine = i < actual.size() ? actual.get(i) : "";
            boolean matched = (expectedLine.startsWith("^")) ?
                Pattern.matches(expectedLine, actualLine) :
                expectedLine.equals(actualLine);
            if (!matched) {
                throw new RuntimeException("Mismatch on line " + i + ": Expected: " +
                    expectedLine + ", but got: " + actualLine + ". FULL OUTPUT: " +
                    String.join("\n", actual));
            }
        }
    }

    static void assertCommandOutput(
        NonInteractiveShell shell,
        List<String> args,
        String... expected
    ) throws Exception {
        try (CaptureStream captureStream = new CaptureStream()) {
            shell.run(captureStream.out, args);
            assertLinesMatch(Arrays.asList(expected), captureStream.outputLines());
        }
    }

    private static String getMetadataLogDir(KafkaClusterTestKit cluster) throws Exception {
        String metadataDir = cluster.nodes().controllerNodes().
            values().iterator().next().metadataDirectory() + File.separator +
                KafkaRaftServer.MetadataTopic() + "-0";
        return metadataDir;
    }

    private static ClusterMetadataSource createSource(
        KafkaClusterTestKit cluster,
        boolean useDirectory
    ) throws Exception {
        if (useDirectory) {
            return new LocalDirectorySource(getMetadataLogDir(cluster));
        } else {
            return MetadataShellObserver.create(cluster.quorumVotersString().get(),
                cluster.nodes().clusterId().toString());
        }
    }

    private static KafkaClusterTestKit createCluster1(boolean shutdown) throws Exception {
        KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder().
                setNumBrokerNodes(3).
                setNumControllerNodes(3).build()).build();
        try {
            cluster.format();
            cluster.startup();
            try (Admin admin = Admin.create(cluster.clientProperties())) {
                ArrayList<NewTopic> newTopics = new ArrayList<>();
                Map<Integer, List<Integer>> fooPartitions = new HashMap<>();
                fooPartitions.put(0, Arrays.asList(0, 1, 2));
                fooPartitions.put(1, Arrays.asList(1, 2, 0));
                newTopics.add(new NewTopic("foo", fooPartitions));
                Map<Integer, List<Integer>> barPartitions = new HashMap<>();
                barPartitions.put(0, Arrays.asList(2, 0, 1));
                newTopics.add(new NewTopic("bar", barPartitions));
                admin.createTopics(newTopics).all().get();
            }
            if (shutdown) {
                cluster.shutdownAll();
            }
            return cluster;
        } catch (Throwable e) {
            cluster.close();
            throw e;
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testLs(boolean useDirectory) throws Exception {
        try (KafkaClusterTestKit cluster = createCluster1(useDirectory)) {
            try (NonInteractiveShell shell = new NonInteractiveShell(
                    createSource(cluster, useDirectory),
                    new Commands(true))) {
                assertCommandOutput(shell, Arrays.asList("ls"),
                    "brokers",
                    "features",
                    "metadataQuorum",
                    "shell",
                    "topicIds",
                    "topics");
                assertCommandOutput(shell, Arrays.asList("ls", "brokers"),
                    "0",
                    "1",
                    "2");
                assertCommandOutput(shell, Arrays.asList("ls", "topics/foo"),
                    "0",
                    "1",
                    "id",
                    "name");
                assertCommandOutput(shell, Arrays.asList("ls", "topics/bar"),
                    "0",
                    "id",
                    "name");
                assertCommandOutput(shell, Arrays.asList("ls", "topics/baz"),
                    "ls: topics/baz: no such file or directory.");
                assertCommandOutput(shell, Arrays.asList("ls", "./shell"),
                        "commitId",
                        "release",
                        "source");
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPathCommands(boolean useDirectory) throws Exception {
        try (KafkaClusterTestKit cluster = createCluster1(useDirectory)) {
            try (NonInteractiveShell shell = new NonInteractiveShell(
                    createSource(cluster, useDirectory),
                    new Commands(true))) {
                assertCommandOutput(shell, Arrays.asList("cd", "brokers",
                    ""));
                assertCommandOutput(shell, Arrays.asList("pwd"),
                    "/brokers");
                assertCommandOutput(shell, Arrays.asList("ls"),
                    "0",
                    "1",
                    "2");
                assertCommandOutput(shell, Arrays.asList("ls", "."),
                    "0",
                    "1",
                    "2");
                assertCommandOutput(shell, Arrays.asList("man", "pwd"),
                    "pwd: Print the current working directory.",
                    "",
                    "usage: pwd");
                assertCommandOutput(shell, Arrays.asList("cd"));
                assertCommandOutput(shell, Arrays.asList("pwd"),
                    "/");
            }
        }
    }

    @Test
    public void testLoadSnapshot() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
                new TestKitNodes.Builder().
                        setNumBrokerNodes(3).
                        setNumControllerNodes(1).build()).build()) {
            cluster.format();
            cluster.startup();
            try (Admin admin = Admin.create(cluster.clientProperties())) {
                admin.createTopics(Collections.singleton(new NewTopic("foobar", 3, (short) 3))).all().get();
                admin.incrementalAlterConfigs(Collections.singletonMap(
                        new ConfigResource(ConfigResource.Type.TOPIC, "foobar"),
                            Collections.singleton(new AlterConfigOp(new ConfigEntry("flush.ms", "1234567"),
                                AlterConfigOp.OpType.SET)))).all().get();
            }
            log.info("Writing a snapshot...");
            cluster.controllers().values().iterator().next().controller().beginWritingSnapshot().get();
            log.info("Waiting for snapshot file to be created...");
            TestUtils.waitForCondition(() -> Files.list(Paths.get(getMetadataLogDir(cluster))).
                    anyMatch(p -> p.toString().endsWith(Snapshots.SUFFIX)), "snapshot creation");
            cluster.shutdownAll();
            try (NonInteractiveShell shell = new NonInteractiveShell(
                    createSource(cluster, true),
                    new Commands(true))) {
                assertCommandOutput(shell, Arrays.asList("ls", "topics/foobar"),
                        "0",
                        "1",
                        "2",
                        "id",
                        "name");
            }
        }
    }
}
