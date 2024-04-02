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

import org.apache.kafka.common.message.ReadShareGroupStateResponseData;

import java.util.List;
import java.util.stream.Collectors;

public class ReadShareGroupStateResult implements PersisterResult {
  private final short errorCode;
  private final int stateEpoch;
  private final long startOffset;
  private final List<PersisterStateBatch> stateBatches;

  private ReadShareGroupStateResult(short errorCode, int stateEpoch, long startOffset, List<PersisterStateBatch> stateBatches) {
    this.errorCode = errorCode;
    this.stateEpoch = stateEpoch;
    this.startOffset = startOffset;
    this.stateBatches = stateBatches;
  }

  public short errorCode() {
    return errorCode;
  }

  public int stateEpoch() {
    return stateEpoch;
  }

  public long startOffset() {
    return startOffset;
  }

  public List<PersisterStateBatch> stateBatches() {
    return stateBatches;
  }

  public static ReadShareGroupStateResult from(ReadShareGroupStateResponseData data) {
    return new Builder()
        .setErrorCode(data.errorCode())
        .setStateEpoch(data.stateEpoch())
        .setStartOffset(data.startOffset())
        .setStateBatches(data.stateBatches().stream()
            .map(PersisterStateBatch::from)
            .collect(Collectors.toList()))
        .build();
  }

  public static class Builder {

    private short errorCode;
    private int stateEpoch;
    private long startOffset;
    private List<PersisterStateBatch> stateBatches;

    public Builder setErrorCode(short errorCode) {
      this.errorCode = errorCode;
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

    public Builder setStateBatches(List<PersisterStateBatch> stateBatches) {
      this.stateBatches = stateBatches;
      return this;
    }

    public ReadShareGroupStateResult build() {
      return new ReadShareGroupStateResult(errorCode, stateEpoch, startOffset, stateBatches);
    }
  }
}
