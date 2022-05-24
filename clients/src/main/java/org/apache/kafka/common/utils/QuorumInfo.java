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
package org.apache.kafka.common;

import java.util.List;

/**
 * This is used to describe per-partition state in the DescribeQuorumResponse.
 */
public class QuorumInfo {
  private final String topic;
  private final Integer leaderId;
  private final List<ReplicaState> voters;
  private final List<ReplicaState> observers;

  public QuorumInfo(String topic, Integer leaderId, List<ReplicaState> voters, List<ReplicaState> observers) {
    this.topic = topic;
    this.leaderId = leaderId;
    this.voters = voters;
    this.observers = observers;
  }

  public String topic() {
    return topic;
  }

  public Integer leaderId() {
    return leaderId;
  }

  public List<ReplicaState> voters() {
    return voters;
  }

  public List<ReplicaState> observers() {
    return observers;
  }

  public static class ReplicaState {
    private final int replicaId;
    private final long logEndOffset;
    private final long lastFetchTimeMs;
    private final long lastCaughtUpTimeMs;

    public ReplicaState(int replicaId, long logEndOffset) {
      this.replicaId = replicaId;
      this.logEndOffset = logEndOffset;
      this.lastFetchTimeMs = -1;
      this.lastCaughtUpTimeMs = -1;
    }

    public ReplicaState(int replicaId, long logEndOffset,
        long lastFetchTimeMs, long lastCaughtUpTimeMs) {
      this.replicaId = replicaId;
      this.logEndOffset = logEndOffset;
      this.lastFetchTimeMs = lastFetchTimeMs;
      this.lastCaughtUpTimeMs = lastCaughtUpTimeMs;
    }

    public int replicaId() {
      return replicaId;
    }

    public long logEndOffset() {
      return logEndOffset;
    }

    public long lastFetchTimeMs() {
      return lastFetchTimeMs;
    }

    public long lastCaughtUpTimeMs() {
      return lastCaughtUpTimeMs;
    }
  }
}
