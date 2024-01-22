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
package org.apache.kafka.streams.processor.internals.assignment;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

class StandbyTaskAssignorFactory {
    private StandbyTaskAssignorFactory() {}

    static StandbyTaskAssignor create(final AssignorConfiguration.AssignmentConfigs configs,
                                      final RackAwareTaskAssignor rackAwareTaskAssignor) {
        if (!configs.rackAwareAssignmentTags.isEmpty()) {
            return new ClientTagAwareStandbyTaskAssignor();
        } else if (rackAwareTaskAssignor != null && rackAwareTaskAssignor.validClientRack()) {
            // racksForProcess should be populated if rackAwareTaskAssignor isn't null
            final Map<UUID, String> racksForProcess = rackAwareTaskAssignor.racksForProcess();
            return new ClientTagAwareStandbyTaskAssignor(
                (processId, clientState) -> mkMap(mkEntry("rack", racksForProcess.get(processId))),
                assignmentConfigs -> Collections.singletonList("rack")
            );
        } else {
            return new DefaultStandbyTaskAssignor();
        }
    }
}
