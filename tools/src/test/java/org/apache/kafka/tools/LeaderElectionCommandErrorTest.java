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

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.server.common.AdminCommandFailedException;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * For some error cases, we can save a little build time by avoiding the overhead for
 * cluster creation and cleanup because the command is expected to fail immediately.
 */
public class LeaderElectionCommandErrorTest {
    @Test
    public void testTopicWithoutPartition() {
        String out = ToolsTestUtils.captureStandardErr(() -> LeaderElectionCommand.main(
                "--bootstrap-server", "nohost:9092",
                "--election-type", "unclean",
                "--topic", "some-topic"
            ));
        assertTrue(out.startsWith("Missing required option(s)"));
        assertTrue(out.contains(" partition"));
    }

    @Test
    public void testPartitionWithoutTopic() {
        String out = ToolsTestUtils.captureStandardErr(() -> LeaderElectionCommand.main(
            "--bootstrap-server", "nohost:9092",
            "--election-type", "unclean",
            "--all-topic-partitions",
            "--partition", "0"
        ));
        String[] rows = out.split("\n");
        assertTrue(out.startsWith("Option partition is only allowed if topic is used"));
    }

    @Test
    public void testMissingElectionType() {
        String out = ToolsTestUtils.captureStandardErr(() -> LeaderElectionCommand.main(
            "--bootstrap-server", "nohost:9092",
            "--topic", "some-topic",
            "--partition", "0"
        ));
        assertTrue(out.startsWith("Missing required option(s)"));
        assertTrue(out.contains(" election-type"));
    }

    @Test
    public void testMissingTopicPartitionSelection() {
        String out = ToolsTestUtils.captureStandardErr(() -> LeaderElectionCommand.main(
            "--bootstrap-server", "nohost:9092",
            "--election-type", "preferred"
        ));
        assertTrue(out.startsWith("One and only one of the following options is required: "));
        assertTrue(out.contains(" all-topic-partitions"));
        assertTrue(out.contains(" topic"));
        assertTrue(out.contains(" path-to-json-file"));
    }

    @Test
    public void testInvalidBroker() {
        Throwable e = assertThrows(AdminCommandFailedException.class, () -> LeaderElectionCommand.run(
            Duration.ofSeconds(1),
            "--bootstrap-server", "example.com:1234",
            "--election-type", "unclean",
            "--all-topic-partitions"
        ));
        assertTrue(e.getCause() instanceof TimeoutException);
    }
}
