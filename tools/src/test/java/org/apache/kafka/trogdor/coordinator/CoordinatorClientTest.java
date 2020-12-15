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

package org.apache.kafka.trogdor.coordinator;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.kafka.trogdor.rest.TaskDone;
import org.apache.kafka.trogdor.rest.TaskPending;
import org.apache.kafka.trogdor.rest.TaskRunning;
import org.apache.kafka.trogdor.rest.TaskStopping;
import org.apache.kafka.trogdor.task.NoOpTaskSpec;

import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(value = 120000, unit = MILLISECONDS)
public class CoordinatorClientTest {

    @Test
    public void testPrettyPrintTaskInfo() {
        assertEquals("Will start at 2019-01-08T07:05:59.85Z",
            CoordinatorClient.prettyPrintTaskInfo(
                new TaskPending(new NoOpTaskSpec(1546931159850L, 9000)),
                ZoneOffset.UTC));
        assertEquals("Started 2009-07-07T01:45:59.85Z; will stop after 9s",
            CoordinatorClient.prettyPrintTaskInfo(
                new TaskRunning(new NoOpTaskSpec(1146931159850L, 9000),
                    1246931159850L,
                    JsonNodeFactory.instance.objectNode()), ZoneOffset.UTC));
        assertEquals("Started 2009-07-07T01:45:59.85Z",
            CoordinatorClient.prettyPrintTaskInfo(
                new TaskStopping(new NoOpTaskSpec(1146931159850L, 9000),
                    1246931159850L,
                    JsonNodeFactory.instance.objectNode()), ZoneOffset.UTC));
        assertEquals("FINISHED at 2019-01-08T20:59:29.85Z after 10s",
            CoordinatorClient.prettyPrintTaskInfo(
                new TaskDone(new NoOpTaskSpec(0, 1000),
                    1546981159850L,
                    1546981169850L,
                    "",
                    false,
                    JsonNodeFactory.instance.objectNode()), ZoneOffset.UTC));
        assertEquals("CANCELLED at 2019-01-08T20:59:29.85Z after 10s",
            CoordinatorClient.prettyPrintTaskInfo(
                new TaskDone(new NoOpTaskSpec(0, 1000),
                    1546981159850L,
                    1546981169850L,
                    "",
                    true,
                    JsonNodeFactory.instance.objectNode()), ZoneOffset.UTC));
        assertEquals("FAILED at 2019-01-08T20:59:29.85Z after 10s",
            CoordinatorClient.prettyPrintTaskInfo(
                new TaskDone(new NoOpTaskSpec(0, 1000),
                    1546981159850L,
                    1546981169850L,
                    "foobar",
                    true,
                    JsonNodeFactory.instance.objectNode()), ZoneOffset.UTC));
    }
};
