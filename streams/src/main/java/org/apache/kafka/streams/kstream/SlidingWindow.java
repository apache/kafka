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


import java.util.Collection;
import java.util.Collections;

public class SlidingWindow extends WindowDef<Window> {

    private long size;

    private SlidingWindow(long size) {
        this.size = size;
    }

    /**
     * Returns a half-interval sliding window definition with the window size in milliseconds
     */
    public static SlidingWindow of(long size) {
        return new SlidingWindow(size);
    }

    @Override
    public Collection<Window> windowsFor(long timestamp) {
        // TODO
        return Collections.EMPTY_LIST;
    }


    @Override
    public boolean equals(WindowDef<Window> other) {
        if (!other.getClass().equals(SlidingWindow.class))
            return false;

        SlidingWindow otherWindow = (SlidingWindow) other;

        return this.size == otherWindow.size;
    }
}
