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

package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import java.util.Objects;

public class CombinedKey<KF, KP> {
    private final KF foreignKey;
    private final KP primaryKey;

    CombinedKey(final KF foreignKey, final KP primaryKey) {
        Objects.requireNonNull(foreignKey, "foreignKey can't be null");
        Objects.requireNonNull(primaryKey, "primaryKey can't be null");
        this.foreignKey = foreignKey;
        this.primaryKey = primaryKey;
    }

    public KF getForeignKey() {
        return foreignKey;
    }

    public KP getPrimaryKey() {
        return primaryKey;
    }

    @Override
    public int hashCode() {
        return Objects.hash(foreignKey, primaryKey);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final CombinedKey<?, ?> that = (CombinedKey<?, ?>) o;
        return Objects.equals(foreignKey, that.foreignKey) && Objects.equals(
            primaryKey, that.primaryKey);
    }

    @Override
    public String toString() {
        return "CombinedKey{" +
                "foreignKey=" + foreignKey +
                ", primaryKey=" + primaryKey +
                '}';
    }
}
