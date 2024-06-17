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

import java.util.Objects;

public final class ValidOffsetAndEpoch {
    final private Kind kind;
    final private OffsetAndEpoch offsetAndEpoch;

    private ValidOffsetAndEpoch(Kind kind, OffsetAndEpoch offsetAndEpoch) {
        this.kind = kind;
        this.offsetAndEpoch = offsetAndEpoch;
    }

    public Kind kind() {
        return kind;
    }

    public OffsetAndEpoch offsetAndEpoch() {
        return offsetAndEpoch;
    }

    public enum Kind {
        DIVERGING, SNAPSHOT, VALID
    }

    public static ValidOffsetAndEpoch diverging(OffsetAndEpoch offsetAndEpoch) {
        return new ValidOffsetAndEpoch(Kind.DIVERGING, offsetAndEpoch);
    }

    public static ValidOffsetAndEpoch snapshot(OffsetAndEpoch offsetAndEpoch) {
        return new ValidOffsetAndEpoch(Kind.SNAPSHOT, offsetAndEpoch);
    }

    public static ValidOffsetAndEpoch valid(OffsetAndEpoch offsetAndEpoch) {
        return new ValidOffsetAndEpoch(Kind.VALID, offsetAndEpoch);
    }

    public static ValidOffsetAndEpoch valid() {
        return valid(new OffsetAndEpoch(-1, -1));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ValidOffsetAndEpoch that = (ValidOffsetAndEpoch) obj;
        return kind == that.kind &&
                offsetAndEpoch.equals(that.offsetAndEpoch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, offsetAndEpoch);
    }

    @Override
    public String toString() {
        return String.format(
            "ValidOffsetAndEpoch(kind=%s, offsetAndEpoch=%s)",
            kind,
            offsetAndEpoch
        );
    }
}
