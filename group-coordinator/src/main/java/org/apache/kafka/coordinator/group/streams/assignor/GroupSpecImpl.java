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

import java.util.Map;
import java.util.Objects;

/**
 * The assignment specification for a Streams group.
 *
 * @param members           The member metadata keyed by member ID.
 * @param assignmentConfigs Any configurations passed to the assignor.
 */
public record GroupSpecImpl(Map<String, AssignmentMemberSpec> members,
                            Map<String, String> assignmentConfigs) implements GroupSpec {

    public GroupSpecImpl {
        Objects.requireNonNull(members);
        Objects.requireNonNull(assignmentConfigs);
    }

}
