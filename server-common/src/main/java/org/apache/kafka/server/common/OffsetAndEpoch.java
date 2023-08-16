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
package org.apache.kafka.server.common;

public class OffsetAndEpoch {
    private final long offset;
    private final int leaderEpoch;

    public OffsetAndEpoch(long offset, int leaderEpoch) {
        this.offset = offset;
        this.leaderEpoch = leaderEpoch;
    }

    public long offset() {
        return offset;
    }

    public int leaderEpoch() {
        return leaderEpoch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OffsetAndEpoch that = (OffsetAndEpoch) o;
        return offset == that.offset && leaderEpoch == that.leaderEpoch;
    }

    @Override
    public int hashCode() {
        int result = leaderEpoch;
        result = 31 * result + Long.hashCode(offset);
        return result;
    }

    @Override
    public String toString() {
        return "(offset=" + offset + ", leaderEpoch=" + leaderEpoch + ")";
    }
}
