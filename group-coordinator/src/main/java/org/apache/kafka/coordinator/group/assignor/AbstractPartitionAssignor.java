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
package org.apache.kafka.coordinator.group.assignor;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.common.RackAwareTopicIdPartition;
import org.apache.kafka.coordinator.group.common.TopicAndClusterMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class AbstractPartitionAssignor implements PartitionAssignor {

  @Override
  public String name() {
      return null;
  }

  public GroupAssignment assign(TopicAndClusterMetadata metadataImages, AssignmentSpec assignmentSpec) {
    List<RackAwareTopicIdPartition> rackAwareTopicIdPartitions = new ArrayList<>(assignmentSpec.topics().entrySet().stream()
        .flatMap(entry -> {
          Uuid topicId = entry.getKey();
          AssignmentTopicMetadata topicSpec = entry.getValue();
          return IntStream.range(0, topicSpec.numPartitions())
              .mapToObj(partition -> {
                List<String> racks = new ArrayList<>();
                for (int replica : Objects.requireNonNull(metadataImages.topicsImage().getPartition(topicId, partition)).replicas) {
                  String rack = String.valueOf(metadataImages.clusterImage().broker(replica).rack());
                  racks.add(rack);
                }
                return new RackAwareTopicIdPartition(topicId, partition, Optional.of(racks));
              });
        }).collect(Collectors.toList()));

        return assign(Optional.of(rackAwareTopicIdPartitions), assignmentSpec);
  }

  public GroupAssignment assign(Optional<List<RackAwareTopicIdPartition>> partitionRackInfo, AssignmentSpec assignmentSpec) throws PartitionAssignorException {
      throw new PartitionAssignorException("Implementation doesn't exist");
  }

}
