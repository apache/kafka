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
import org.apache.kafka.common.message.WriteShareGroupStateRequestData;

import java.util.Objects;

public class PersisterStateBatch {
  private final long firstOffset;
  private final long lastOffset;
  private final byte deliveryState;
  private final short deliveryCount;

  public PersisterStateBatch(long firstOffset, long lastOffset, byte deliveryState, short deliveryCount) {
    this.firstOffset = firstOffset;
    this.lastOffset = lastOffset;
    this.deliveryState = deliveryState;
    this.deliveryCount = deliveryCount;
  }

  public long firstOffset() {
    return firstOffset;
  }

  public long lastOffset() {
    return lastOffset;
  }

  public byte deliveryState() {
    return deliveryState;
  }

  public short deliveryCount() {
    return deliveryCount;
  }

  public static PersisterStateBatch from(ReadShareGroupStateResponseData.StateBatch batch) {
    return new PersisterStateBatch(
        batch.firstOffset(),
        batch.lastOffset(),
        batch.deliveryState(),
        batch.deliveryCount());
  }

  public static PersisterStateBatch from(WriteShareGroupStateRequestData.StateBatch batch) {
    return new PersisterStateBatch(
        batch.firstOffset(),
        batch.lastOffset(),
        batch.deliveryState(),
        batch.deliveryCount());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PersisterStateBatch that = (PersisterStateBatch) o;
    return firstOffset == that.firstOffset && lastOffset == that.lastOffset && deliveryState == that.deliveryState && deliveryCount == that.deliveryCount;
  }

  @Override
  public int hashCode() {
    return Objects.hash(firstOffset, lastOffset, deliveryState, deliveryCount);
  }

  @Override
  public String toString() {
    return "PersisterStateBatch(" +
        "firstOffset=" + firstOffset + "," +
        "lastOffset=" + lastOffset + "," +
        "deliveryState=" + deliveryState + "," +
        "deliveryCount=" + deliveryCount +
        ")";
  }
}
