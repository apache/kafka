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
package org.apache.kafka.raft;

public class OffsetAndEpoch implements Comparable<OffsetAndEpoch> {
    public final long offset;
    public final int epoch;

    public OffsetAndEpoch(long offset, int epoch) {
        this.offset = offset;
        this.epoch = epoch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OffsetAndEpoch that = (OffsetAndEpoch) o;

        if (offset != that.offset) return false;
        return epoch == that.epoch;
    }

    @Override
    public int hashCode() {
        int result = (int) (offset ^ (offset >>> 32));
        result = 31 * result + epoch;
        return result;
    }

    @Override
    public String toString() {
        return "OffsetAndEpoch(" +
                "offset=" + offset +
                ", epoch=" + epoch +
                ')';
    }

    @Override
    public int compareTo(OffsetAndEpoch o) {
        if (epoch == o.epoch)
            return Long.compare(offset, o.offset);
        return Integer.compare(epoch, o.epoch);
    }
}
