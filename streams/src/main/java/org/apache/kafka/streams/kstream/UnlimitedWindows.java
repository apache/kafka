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

import java.util.Collection;
import java.util.Collections;

public class UnlimitedWindows extends Windows<UnlimitedWindow> {

    private static final long DEFAULT_START_TIMESTAMP = 0L;

    private long start;

    private UnlimitedWindows(String name) {
        super(name);

        this.start = DEFAULT_START_TIMESTAMP;
    }

    /**
     * Returns an unlimited window definition
     */
    public static UnlimitedWindows of(String name) {
        return new UnlimitedWindows(name);
    }

    public UnlimitedWindows startOn(long start) {
        this.start = start;

        return this;
    }

    @Override
    public Collection<UnlimitedWindow> windowsFor(long timestamp) {
        // TODO
        return Collections.<UnlimitedWindow>emptyList();
    }

    @Override
    public boolean equalTo(Windows other) {
        if (!other.getClass().equals(UnlimitedWindows.class))
            return false;

        UnlimitedWindows otherWindows = (UnlimitedWindows) other;

        return this.start == otherWindows.start;
    }
}
