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

package org.apache.kafka.server.group.share;

import org.apache.kafka.common.Uuid;

import java.util.Objects;

public class GroupTopicPartitionData {
  private final String groupId;
  private final Uuid topicId;
  private final int partition;

  public GroupTopicPartitionData(String groupId, Uuid topicId, int partition) {
    this.groupId = groupId;
    this.topicId = topicId;
    this.partition = partition;
  }

  public String groupId() {
    return groupId;
  }

  public Uuid topicId() {
    return topicId;
  }

  public int partition() {
    return partition;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GroupTopicPartitionData that = (GroupTopicPartitionData) o;
    return partition == that.partition && Objects.equals(groupId, that.groupId) && Objects.equals(topicId, that.topicId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(groupId, topicId, partition);
  }

  public static class Builder {
    private String groupId;
    private Uuid topicId;
    private int partition;

    public Builder setGroupId(String groupId) {
      this.groupId = groupId;
      return this;
    }

    public Builder setTopicId(Uuid topicId) {
      this.topicId = topicId;
      return this;
    }

    public Builder setPartition(int partition) {
      this.partition = partition;
      return this;
    }

    public Builder setGroupTopicPartition(GroupTopicPartitionData groupTopicPartitionData) {
      this.groupId = groupTopicPartitionData.groupId();
      this.topicId = groupTopicPartitionData.topicId();
      this.partition = groupTopicPartitionData.partition();
      return this;
    }

    public GroupTopicPartitionData build() {
      return new GroupTopicPartitionData(this.groupId, this.topicId, this.partition);
    }
  }
}
