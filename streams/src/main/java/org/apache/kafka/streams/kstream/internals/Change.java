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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.util.Objects;

public class Change<T> {

    public final T newValue;
    public final Headers newHeaders;
    public final T oldValue;
    public final Headers oldHeaders;
    public final boolean isLatest;

    public Change(final T newValue, final T oldValue) {
        this(newValue, new RecordHeaders(), oldValue, new RecordHeaders(), true);
    }

    public Change(final T newValue, final T oldValue, final boolean isLatest) {
        this(newValue, new RecordHeaders(), oldValue, new RecordHeaders(), isLatest);
    }

    public Change(final T newValue, final Headers newHeaders, final T oldValue, final Headers oldHeaders, final boolean isLatest) {
        this.newValue = newValue;
        this.newHeaders = newHeaders;
        this.oldValue = oldValue;
        this.oldHeaders = oldHeaders;
        this.isLatest = isLatest;
    }

    @Override
    public String toString() {
        return "(" + newValue + "<-" + oldValue + ")";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Change<?> change = (Change<?>) o;
        return Objects.equals(newValue, change.newValue)
            && Objects.equals(newHeaders, change.newHeaders)
            && Objects.equals(oldValue, change.oldValue)
            && Objects.equals(oldHeaders, change.oldHeaders)
            && isLatest == change.isLatest;
    }

    @Override
    public int hashCode() {
        return Objects.hash(newValue, newHeaders, oldValue, oldHeaders, isLatest);
    }
}
