/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.kstream;

import org.apache.kafka.streams.kstream.internals.UnlimitedWindow;

import java.util.HashMap;
import java.util.Map;

/**
 * The unlimited window specifications.
 */
public class UnlimitedWindows extends Windows<UnlimitedWindow> {

    private static final long DEFAULT_START_TIMESTAMP = 0L;

    public final long start;

    private UnlimitedWindows(String name, long start) {
        super(name);

        this.start = start;
    }

    /**
     * Returns an unlimited window definition
     */
    public static UnlimitedWindows of(String name) {
        return new UnlimitedWindows(name, DEFAULT_START_TIMESTAMP);
    }

    public UnlimitedWindows startOn(long start) {
        return new UnlimitedWindows(this.name, start);
    }

    @Override
    public Map<Long, UnlimitedWindow> windowsFor(long timestamp) {
        // always return the single unlimited window

        // we cannot use Collections.singleMap since it does not support remove() call
        Map<Long, UnlimitedWindow> windows = new HashMap<>();
        windows.put(start, new UnlimitedWindow(start));


        return windows;
    }

    @Override
    public final boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof UnlimitedWindows)) {
            return false;
        }

        UnlimitedWindows other = (UnlimitedWindows) o;
        return this.start == other.start;
    }

    @Override
    public int hashCode() {
        return (int) (start ^ (start >>> 32));
    }

}