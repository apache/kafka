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

import org.apache.kafka.common.message.InitializeShareGroupStateRequestData;

public class InitializeShareGroupStateParameters implements PersisterParameters {

  private final GroupTopicPartitionData groupTopicPartitionData;
  private final int stateEpoch;
  private final long startOffset;

  private InitializeShareGroupStateParameters(GroupTopicPartitionData groupTopicPartitionData, int stateEpoch, long startOffset) {
    this.groupTopicPartitionData = groupTopicPartitionData;
    this.stateEpoch = stateEpoch;
    this.startOffset = startOffset;
  }

  public GroupTopicPartitionData groupTopicPartitionData() {
    return groupTopicPartitionData;
  }

  public int stateEpoch() {
    return stateEpoch;
  }

  public long startOffset() {
    return startOffset;
  }

  public static InitializeShareGroupStateParameters from(InitializeShareGroupStateRequestData data) {
    return new Builder()
        .setGroupTopicPartitionData(new GroupTopicPartitionData(data.groupId(), data.topicId(), data.partition()))
        .setStateEpoch(data.stateEpoch())
        .setStartOffset(data.startOffset())
        .build();
  }

  public static class Builder {
    private GroupTopicPartitionData groupTopicPartitionData;
    private int stateEpoch;
    private long startOffset;

    public Builder setGroupTopicPartitionData(GroupTopicPartitionData groupTopicPartitionData) {
      this.groupTopicPartitionData = groupTopicPartitionData;
      return this;
    }

    public Builder setStateEpoch(int stateEpoch) {
      this.stateEpoch = stateEpoch;
      return this;
    }

    public Builder setStartOffset(long startOffset) {
      this.startOffset = startOffset;
      return this;
    }

    public InitializeShareGroupStateParameters build() {
      return new InitializeShareGroupStateParameters(this.groupTopicPartitionData, this.stateEpoch, this.startOffset);
    }
  }
}
