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

import org.apache.kafka.streams.kstream.internals.HoppingWindow;

import java.util.Collection;
import java.util.Collections;

public class HoppingWindows extends Windows<HoppingWindow> {

    private static final long DEFAULT_SIZE_MS = 1000L;

    public final long size;

    public final long period;

    private HoppingWindows(String name, long size, long period) {
        super(name);

        this.size = size;
        this.period = period;
    }

    /**
     * Returns a half-interval hopping window definition with the window size in milliseconds
     * of the form &#91; N &#42; default_size, N &#42; default_size + default_size &#41;
     */
    public static HoppingWindows of(String name) {
        return new HoppingWindows(name, DEFAULT_SIZE_MS, DEFAULT_SIZE_MS);
    }

    /**
     * Returns a new hopping window definition with the original size but reassign the window
     * period in milliseconds of the form &#91; N &#42; period, N &#42; period + size &#41;
     */
    public HoppingWindows with(long size) {
        return new HoppingWindows(this.name, size, this.period);
    }

    /**
     * Returns a new hopping window definition with the original size but reassign the window
     * period in milliseconds of the form &#91; N &#42; period, N &#42; period + size &#41;
     */
    public HoppingWindows every(long period) {
        return new HoppingWindows(this.name, this.size, period);
    }

    @Override
    public Collection<HoppingWindow> windowsFor(long timestamp) {
        // TODO
        return Collections.<HoppingWindow>emptyList();
    }

    @Override
    public boolean equalTo(Windows other) {
        if (!other.getClass().equals(HoppingWindows.class))
            return false;

        HoppingWindows otherWindows = (HoppingWindows) other;

        return this.size == otherWindows.size && this.period == otherWindows.period;
    }
}
