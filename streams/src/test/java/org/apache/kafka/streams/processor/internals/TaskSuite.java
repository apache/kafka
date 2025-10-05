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
package org.apache.kafka.streams.processor.internals;


import org.apache.kafka.streams.processor.internals.assignment.LegacyStickyTaskAssignorTest;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetricsTest;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * This suite runs all the tests related to task management. It's intended to simplify feature testing from IDEs.
 *
 * If desired, it can also be added to a Gradle build task, although this isn't strictly necessary, since all
 * these tests are already included in the `:streams:test` task.
 */
@Suite
@SelectClasses({
    StreamTaskTest.class,
    StandbyTaskTest.class,
    GlobalStateTaskTest.class,
    TaskManagerTest.class,
    TaskMetricsTest.class,
    LegacyStickyTaskAssignorTest.class,
    StreamsPartitionAssignorTest.class,
})
public class TaskSuite {
}
