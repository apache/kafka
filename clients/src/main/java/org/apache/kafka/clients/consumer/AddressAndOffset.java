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
package org.apache.kafka.clients.consumer;

import java.util.Objects;
import java.util.Optional;

/**
 * A container class for offset and timestamp.
 */
public final class AddressAndOffset {
    private final long offset;
    private final long address;
    private final int rkey;
    private final int length;
    private final Optional<Integer> leaderEpoch;

    public AddressAndOffset(long offset, long address, int rkey, int length, Optional<Integer> leaderEpoch) {
        if (offset < 0)
            throw new IllegalArgumentException("Invalid negative offset");
        if (length < 0)
            throw new IllegalArgumentException("Invalid negative length");
        if (address < 0)
            throw new IllegalArgumentException("Invalid negative timestamp");

        this.offset = offset;
        this.address = address;
        this.rkey = rkey;
        this.length = length;
        this.leaderEpoch = leaderEpoch;
    }

    public long address() {
        return address;
    }
    public long offset() {
        return offset;
    }
    public long rkey() {
        return rkey;
    }
    public long length() {
        return length;
    }

    /**
     * Get the leader epoch corresponding to the offset that was found (if one exists).
     * This can be provided to seek() to ensure that the log hasn't been truncated prior to fetching.
     *
     * @return The leader epoch or empty if it is not known
     */
    public Optional<Integer> leaderEpoch() {
        return leaderEpoch;
    }

    @Override
    public String toString() {
        return "(address=" + address +
                ", leaderEpoch=" + leaderEpoch.orElse(null) +
                ", offset=" + offset + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AddressAndOffset that = (AddressAndOffset) o;
        return address == that.address &&
                offset == that.offset &&
                rkey == that.rkey &&
                length == that.length &&
                Objects.equals(leaderEpoch, that.leaderEpoch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(address, offset, leaderEpoch);
    }
}
