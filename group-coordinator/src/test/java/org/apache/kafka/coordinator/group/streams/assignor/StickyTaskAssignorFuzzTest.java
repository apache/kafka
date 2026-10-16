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
package org.apache.kafka.coordinator.group.streams.assignor;

import org.apache.kafka.coordinator.group.streams.assignor.TaskAssignorTestbed.Profile;

import org.junit.jupiter.api.Test;

/**
 * Runs {@link StickyTaskAssignor} through the {@link TaskAssignorTestbed}. The tests fail when an assignment breaks
 * {@link AssignmentInvariants} and print the metrics over fixed scenarios; {@link #main} runs the seeds set in the
 * environment.
 */
public class StickyTaskAssignorFuzzTest {

    private final TaskAssignorTestbed testbed = new TaskAssignorTestbed(new StickyTaskAssignor());

    /** Runs both profiles and prints the summaries, to compare assignors or versions by hand. */
    public static void main(final String[] args) {
        final TaskAssignorTestbed testbed = new StickyTaskAssignorFuzzTest().testbed;
        testbed.runFromEnvironment(Profile.SMALL);
        testbed.runFromEnvironment(Profile.LARGE);
    }

    @Test
    public void shouldProduceValidAssignmentsForSmallGroupsUnderRandomChanges() {
        testbed.run(Profile.SMALL);
    }

    @Test
    public void shouldProduceValidAssignmentsForLargeGroupsUnderRandomChanges() {
        testbed.run(Profile.LARGE);
    }
}
