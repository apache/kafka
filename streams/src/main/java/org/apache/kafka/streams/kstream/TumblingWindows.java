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


import org.apache.kafka.streams.kstream.internals.TumblingWindow;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TumblingWindows extends Windows<TumblingWindow> {

    private static final long DEFAULT_SIZE_MS = 1000L;

    public final long size;

    private TumblingWindows(String name, long size) {
        super(name);

        this.size = size;
    }

    /**
     * Returns a half-interval sliding window definition with the default window size
     */
    public static TumblingWindows of(String name) {
        return new TumblingWindows(name, DEFAULT_SIZE_MS);
    }

    /**
     * Returns a half-interval sliding window definition with the window size in milliseconds
     */
    public TumblingWindows with(long size) {
        return new TumblingWindows(this.name, size);
    }

    @Override
    public Map<Long, TumblingWindow> windowsFor(long timestamp) {
        long windowStart = timestamp - timestamp % size;

        // we cannot use Collections.singleMap since it does not support remove() call
        Map<Long, TumblingWindow> windows = new HashMap<>();
        windows.put(windowStart, new TumblingWindow(windowStart, windowStart + size));

        return windows;
    }

    @Override
    public boolean equalTo(Windows other) {
        if (!other.getClass().equals(TumblingWindows.class))
            return false;

        TumblingWindows otherWindows = (TumblingWindows) other;

        return this.size == otherWindows.size;
    }
}
