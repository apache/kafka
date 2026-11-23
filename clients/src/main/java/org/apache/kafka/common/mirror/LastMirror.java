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
package org.apache.kafka.common.mirror;

import java.util.Objects;

/**
 * The last mirroring position of a partition before the mirroring stopped (failover).
 */
public final class LastMirror {
    /**
     * The last mirroring position when it is not available.
     */
    public static final LastMirror EMPTY = new LastMirror(-1L, -1);

    private final long offset;
    private final int epoch;

    /**
     * @param offset The last mirror offset, or -1 if not available.
     * @param epoch  The last mirror epoch, or -1 if not available.
     */
    public LastMirror(long offset, int epoch) {
        this.offset = offset;
        this.epoch = epoch;
    }

    public long offset() {
        return offset;
    }

    public int epoch() {
        return epoch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LastMirror that = (LastMirror) o;
        return offset == that.offset && epoch == that.epoch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, epoch);
    }

    @Override
    public String toString() {
        return "LastMirror(offset=" + offset + ", epoch=" + epoch + ")";
    }
}
