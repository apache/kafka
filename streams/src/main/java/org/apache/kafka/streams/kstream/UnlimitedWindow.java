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

import org.apache.kafka.streams.kstream.internals.AbstractWindows;

import java.util.Collection;
import java.util.Collections;

public class UnlimitedWindow extends AbstractWindows implements WindowDef<UnlimitedWindow> {

    private long start;

    private UnlimitedWindow(long start) {
        super();

        this.start = start;
    }

    /**
     * Returns an unlimited window definition
     */
    public static UnlimitedWindow on(long start) {
        return new UnlimitedWindow(start);
    }

    @Override
    public Collection<Window> windowsFor(long timestamp) {
        // TODO
        return Collections.<Window>emptyList();
    }

    @Override
    public boolean equalTo(UnlimitedWindow other) {
        return true;
    }
}
