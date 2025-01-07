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

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.common.test.api.ClusterTests;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(value = ClusterTestExtensions.class)
class MetadataQuorumCommandTest {

    /**
     * 1. The same number of broker controllers
     * 2. More brokers than controllers
     * 3. Fewer brokers than controllers
     */
    @ClusterTests({
        @ClusterTest(brokers = 2, controllers = 2),
        @ClusterTest(brokers = 2, controllers = 1),
        @ClusterTest(brokers = 1, controllers = 2),
    })
    public void testDescribeQuorumReplicationSuccessful(ClusterInstance cluster) throws InterruptedException {
        cluster.waitForReadyBrokers();
        String describeOutput = ToolsTestUtils.captureStandardOut(() ->
            MetadataQuorumCommand.mainNoExit("--bootstrap-server", cluster.bootstrapServers(), "describe", "--replication")
        );

        List<String> outputs = Arrays.stream(describeOutput.split("\n")).collect(Collectors.toList());
        String header = outputs.get(0);
        List<String> data = outputs.subList(1, outputs.size());

        assertTrue(header.matches("NodeId\\s+DirectoryId\\s+LogEndOffset\\s+Lag\\s+LastFetchTimestamp\\s+LastCaughtUpTimestamp\\s+Status\\s+"));

        if (cluster.type() == Type.CO_KRAFT) 
            assertEquals(Math.max(cluster.config().numControllers(), cluster.config().numBrokers()), data.size());
        else 
            assertEquals(cluster.config().numBrokers() + cluster.config().numControllers(), data.size());

        Pattern leaderPattern = Pattern.compile("\\d+\\s+\\S+\\s+\\d+\\s+\\d+\\s+-?\\d+\\s+-?\\d+\\s+Leader\\s*");
        assertTrue(leaderPattern.matcher(data.get(0)).find());
        assertTrue(data.stream().skip(1).noneMatch(o -> leaderPattern.matcher(o).find()));

        Pattern followerPattern = Pattern.compile("\\d+\\s+\\S+\\s+\\d+\\s+\\d+\\s+-?\\d+\\s+-?\\d+\\s+Follower\\s*");
        assertEquals(cluster.config().numControllers() - 1, data.stream().filter(o -> followerPattern.matcher(o).find()).count());

        Pattern observerPattern = Pattern.compile("\\d+\\s+\\S+\\s+\\d+\\s+\\d+\\s+-?\\d+\\s+-?\\d+\\s+Observer\\s*");
        if (cluster.type() == Type.CO_KRAFT)
            assertEquals(Math.max(0, cluster.config().numBrokers() - cluster.config().numControllers()),
                data.stream().filter(o -> observerPattern.matcher(o).find()).count());
        else
            assertEquals(cluster.config().numBrokers(), data.stream().filter(o -> observerPattern.matcher(o).find()).count());
    }

    /**
     * 1. The same number of broker controllers
     * 2. More brokers than controllers
     * 3. Fewer brokers than controllers
     */
    @ClusterTests({
        @ClusterTest(brokers = 2, controllers = 2),
        @ClusterTest(brokers = 2, controllers = 1),
        @ClusterTest(brokers = 1, controllers = 2),
    })
    public void testDescribeQuorumStatusSuccessful(ClusterInstance cluster) throws InterruptedException {
        testDescribeQuorumStatusSuccessful(cluster, false);
        testDescribeQuorumStatusSuccessful(cluster, true);
    }

    private void testDescribeQuorumStatusSuccessful(ClusterInstance cluster, boolean usingBootstrapController) throws InterruptedException {
        cluster.waitForReadyBrokers();

        String describeOutput = ToolsTestUtils.captureStandardOut(
            () -> MetadataQuorumCommand.mainNoExit(
                usingBootstrapController ? "--bootstrap-controller" : "--bootstrap-server",
                usingBootstrapController ? cluster.bootstrapControllers() : cluster.bootstrapServers(),
                "describe",
                "--status"
            )
        );
        String[] outputs = describeOutput.split("\n");

        assertTrue(outputs[0].matches("ClusterId:\\s+\\S{22}"), describeOutput);
        assertTrue(outputs[1].matches("LeaderId:\\s+\\d+"), describeOutput);
        assertTrue(outputs[2].matches("LeaderEpoch:\\s+\\d+"), describeOutput);
        // HighWatermark may be -1
        assertTrue(outputs[3].matches("HighWatermark:\\s+-?\\d+"), describeOutput);
        assertTrue(outputs[4].matches("MaxFollowerLag:\\s+\\d+"), describeOutput);
        assertTrue(outputs[5].matches("MaxFollowerLagTimeMs:\\s+-?\\d+"), describeOutput);
        assertTrue(
            outputs[6].matches("CurrentVoters:\\s+\\[\\{\"id\":\\s+\\d+,\\s+\"directoryId\":\\s+\\S+,\\s+" +
                "\"endpoints\":\\s+\\[\"\\S+://\\[?\\S+]?:\\d+\",?.*]"),
            describeOutput
        );

        // There are no observers if we have fewer brokers than controllers
        if (cluster.type() == Type.CO_KRAFT && cluster.config().numBrokers() <= cluster.config().numControllers()) {
            assertTrue(outputs[7].matches("CurrentObservers:\\s+\\[]"), describeOutput);
        } else {
            assertTrue(
                outputs[7].matches("CurrentObservers:\\s+\\[\\{\"id\":\\s+\\d+,\\s+\"directoryId\":\\s+\\S+}" +
                    "(,\\s+\\{\"id\":\\s+\\d+,\\s+\"directoryId\":\\s+\\S+})*]"),
                describeOutput
            );
        }
    }

    @ClusterTest
    public void testOnlyOneBrokerAndOneController(ClusterInstance cluster) {
        testOnlyOneBrokerAndOneController(cluster, false);
        testOnlyOneBrokerAndOneController(cluster, true);
    }

    public void testOnlyOneBrokerAndOneController(ClusterInstance cluster, boolean usingBootstrapController) {
        String statusOutput = ToolsTestUtils.captureStandardOut(() ->
            MetadataQuorumCommand.mainNoExit(
                    usingBootstrapController ? "--bootstrap-controller" : "--bootstrap-server",
                    usingBootstrapController ? cluster.bootstrapControllers() : cluster.bootstrapServers(),
                    "describe", "--status")
        );
        assertEquals("MaxFollowerLag:         0", statusOutput.split("\n")[4]);
        assertEquals("MaxFollowerLagTimeMs:   0", statusOutput.split("\n")[5]);

        String replicationOutput = ToolsTestUtils.captureStandardOut(() ->
            MetadataQuorumCommand.mainNoExit(
                    usingBootstrapController ? "--bootstrap-controller" : "--bootstrap-server",
                    usingBootstrapController ? cluster.bootstrapControllers() : cluster.bootstrapServers(),
                    "describe", "--replication")
        );
        assertEquals("0", replicationOutput.split("\n")[1].split("\\s+")[3]);
    }

    @Test
    public void testCommandConfig() throws IOException {
        // specifying a --command-config containing properties that would prevent login must fail
        File tmpfile = TestUtils.tempFile(AdminClientConfig.SECURITY_PROTOCOL_CONFIG + "=SSL_PLAINTEXT");
        assertEquals(1, MetadataQuorumCommand.mainNoExit("--bootstrap-server", "localhost:9092",
                        "--command-config", tmpfile.getAbsolutePath(), "describe", "--status"));
    }

    @ClusterTest(types = {Type.CO_KRAFT})
    public void testHumanReadableOutput(ClusterInstance cluster) {
        assertEquals(1, MetadataQuorumCommand.mainNoExit("--bootstrap-server", cluster.bootstrapServers(), "describe", "--human-readable"));
        assertEquals(1, MetadataQuorumCommand.mainNoExit("--bootstrap-server", cluster.bootstrapServers(), "describe", "--status", "--human-readable"));
        String out0 = ToolsTestUtils.captureStandardOut(() ->
            MetadataQuorumCommand.mainNoExit("--bootstrap-server", cluster.bootstrapServers(), "describe", "--replication")
        );
        assertFalse(out0.split("\n")[1].matches("\\d*"));
        String out1 = ToolsTestUtils.captureStandardOut(() ->
            MetadataQuorumCommand.mainNoExit("--bootstrap-server", cluster.bootstrapServers(), "describe", "--replication", "--human-readable")
        );
        assertHumanReadable(out1);
        String out2 = ToolsTestUtils.captureStandardOut(() ->
            MetadataQuorumCommand.mainNoExit("--bootstrap-server", cluster.bootstrapServers(), "describe", "--re", "--hu")
         );
        assertHumanReadable(out2);
    }

    private static void assertHumanReadable(String output) {
        String dataRow = output.split("\n")[1];
        String lastFetchTimestamp = dataRow.split("\t")[4];
        String lastFetchTimestampValue = lastFetchTimestamp.split(" ")[0];
        String lastCaughtUpTimestamp = dataRow.split("\t")[5];
        String lastCaughtUpTimestampValue = lastCaughtUpTimestamp.split(" ")[0];
        assertTrue(lastFetchTimestamp.contains("ms ago"));
        assertTrue(lastFetchTimestampValue.matches("\\d*"));
        assertTrue(lastCaughtUpTimestamp.contains("ms ago"));
        assertTrue(lastCaughtUpTimestampValue.matches("\\d*"));
    }
}
