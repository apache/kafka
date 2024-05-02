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

package org.apache.kafka.trogdor.workload;

import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.OperatingSystem;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.trogdor.task.AgentWorkerStatusTracker;
import org.apache.kafka.trogdor.task.WorkerStatusTracker;

import java.io.File;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 120)
public class ExternalCommandWorkerTest {

    static class ExternalCommandWorkerBuilder {
        private final String id;
        private final ObjectNode workload;
        private int shutdownGracePeriodMs = 3000000;
        private String[] command = new String[0];

        ExternalCommandWorkerBuilder(String id) {
            this.id = id;
            this.workload = new ObjectNode(JsonNodeFactory.instance);
            this.workload.set("foo", new TextNode("value1"));
            this.workload.set("bar", new IntNode(123));
        }

        ExternalCommandWorker build() {
            ExternalCommandSpec spec = new ExternalCommandSpec(0,
                30000,
                "node0",
                Arrays.asList(command),
                workload,
                Optional.of(shutdownGracePeriodMs));
            return new ExternalCommandWorker(id, spec);
        }

        ExternalCommandWorkerBuilder command(String... command) {
            this.command = command;
            return this;
        }

        ExternalCommandWorkerBuilder shutdownGracePeriodMs(int shutdownGracePeriodMs) {
            this.shutdownGracePeriodMs = shutdownGracePeriodMs;
            return this;
        }
    }

    /**
     * Test running a process which exits successfully-- in this case, /bin/true.
     */
    @Test
    public void testProcessWithNormalExit() throws Exception {
        if (OperatingSystem.IS_WINDOWS) return;
        ExternalCommandWorker worker =
            new ExternalCommandWorkerBuilder("trueTask").command("true").build();
        KafkaFutureImpl<String> doneFuture = new KafkaFutureImpl<>();
        worker.start(null, new AgentWorkerStatusTracker(), doneFuture);
        assertEquals("", doneFuture.get());
        worker.stop(null);
    }

    /**
     * Test running a process which exits unsuccessfully-- in this case, /bin/false.
     */
    @Test
    public void testProcessWithFailedExit() throws Exception {
        if (OperatingSystem.IS_WINDOWS) return;
        ExternalCommandWorker worker =
            new ExternalCommandWorkerBuilder("falseTask").command("false").build();
        KafkaFutureImpl<String> doneFuture = new KafkaFutureImpl<>();
        worker.start(null, new AgentWorkerStatusTracker(), doneFuture);
        assertEquals("exited with return code 1", doneFuture.get());
        worker.stop(null);
    }

    /**
     * Test attempting to run an executable which doesn't exist.
     * We use a path which starts with /dev/null, since that should never be a
     * directory in UNIX.
     */
    @Test
    public void testProcessNotFound() throws Exception {
        ExternalCommandWorker worker =
            new ExternalCommandWorkerBuilder("notFoundTask").
                command("/dev/null/non/existent/script/path").build();
        KafkaFutureImpl<String> doneFuture = new KafkaFutureImpl<>();
        worker.start(null, new AgentWorkerStatusTracker(), doneFuture);
        String errorString = doneFuture.get();
        assertTrue(errorString.startsWith("Unable to start process"));
        worker.stop(null);
    }

    /**
     * Test running a process which times out.  We will send it a SIGTERM.
     */
    @Test
    public void testProcessStop() throws Exception {
        if (OperatingSystem.IS_WINDOWS) return;
        ExternalCommandWorker worker =
            new ExternalCommandWorkerBuilder("testStopTask").
                command("sleep", "3600000").build();
        KafkaFutureImpl<String> doneFuture = new KafkaFutureImpl<>();
        worker.start(null, new AgentWorkerStatusTracker(), doneFuture);
        worker.stop(null);
        // We don't check the numeric return code, since that will vary based on
        // platform.
        assertTrue(doneFuture.get().startsWith("exited with return code "));
    }

    /**
     * Test running a process which needs to be force-killed.
     */
    @Test
    public void testProcessForceKillTimeout() throws Exception {
        if (OperatingSystem.IS_WINDOWS) return;
        File tempFile = null;
        try {
            tempFile = TestUtils.tempFile();
            try (OutputStream stream = Files.newOutputStream(tempFile.toPath())) {
                for (String line : new String[] {
                    "echo hello world\n",
                    "# Test that the initial message is sent correctly.\n",
                    "read -r line\n",
                    "[[ $line == '{\"id\":\"testForceKillTask\",\"workload\":{\"foo\":\"value1\",\"bar\":123}}' ]] || exit 0\n",
                    "\n",
                    "# Ignore SIGTERM signals.  This ensures that we test SIGKILL delivery.\n",
                    "trap 'echo SIGTERM' SIGTERM\n",
                    "\n",
                    "# Update the process status.  This will also unblock the junit test.\n",
                    "# It is important that we do this after we disabled SIGTERM, to ensure\n",
                    "# that we are testing SIGKILL.\n",
                    "echo '{\"status\": \"green\", \"log\": \"my log message.\"}'\n",
                    "\n",
                    "# Wait for the SIGKILL.\n",
                    "while true; do sleep 0.01; done\n"}) {
                    stream.write(line.getBytes(StandardCharsets.UTF_8));
                }
            }
            CompletableFuture<String> statusFuture = new CompletableFuture<>();
            final WorkerStatusTracker statusTracker = status -> statusFuture .complete(status.textValue());
            ExternalCommandWorker worker = new ExternalCommandWorkerBuilder("testForceKillTask").
                shutdownGracePeriodMs(1).
                command("bash", tempFile.getAbsolutePath()).
                build();
            KafkaFutureImpl<String> doneFuture = new KafkaFutureImpl<>();
            worker.start(null, statusTracker, doneFuture);
            assertEquals("green", statusFuture.get());
            worker.stop(null);
            assertTrue(doneFuture.get().startsWith("exited with return code "));
        } finally {
            if (tempFile != null) {
                Files.delete(tempFile.toPath());
            }
        }
    }
}
